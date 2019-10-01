-module(couch_sqlite_engine).
-behavior(couch_db_engine).

-include_lib("couch/include/couch_db.hrl").

-export([
    exists/1,

    delete/3,
    delete_compaction_files/3,

    init/2,
    terminate/2,
    handle_db_updater_call/2,
    handle_db_updater_info/2,

    incref/1,
    decref/1,
    monitored_by/1,

    last_activity/1,

    get_compacted_seq/1,
    get_del_doc_count/1,
    get_disk_version/1,
    get_doc_count/1,
    get_epochs/1,
    get_purge_seq/1,
    get_oldest_purge_seq/1,
    get_purge_infos_limit/1,
    get_revs_limit/1,
    get_security/1,
    get_size_info/1,
    get_update_seq/1,
    get_uuid/1,

    set_revs_limit/2,
    set_purge_infos_limit/2,
    set_security/2,

    open_docs/2,
    open_local_docs/2,
    read_doc_body/2,
    load_purge_infos/2,

    serialize_doc/2,
    write_doc_body/2,
    write_doc_infos/3,
    purge_docs/3,

    commit_data/1,

    open_write_stream/2,
    open_read_stream/2,
    is_active_stream/2,

    fold_docs/4,
    fold_local_docs/4,
    fold_changes/5,
    fold_purge_infos/5,
    count_changes_since/2,

    start_compaction/4,
    finish_compaction/4
]).

-record(sqldb, {
  db = nil,
  update_epochs = false, % whether or not to write out new epochs on next write
  epochs = [],
  epochs_flushed = false, % set to true after next write
  last_activity = 0
}).

-define(DISK_VERSION, 1).
-define(ADMIN_ONLY_SEC_PROPS, {[
    {<<"members">>, {[
        {<<"roles">>, [<<"_admin">>]}
    ]}},
    {<<"admins">>, {[
        {<<"roles">>, [<<"_admin">>]}
    ]}}
]}).

% TODO:
% dont rely on rowid for changes, as vacuum might reorder things:
%   The VACUUM command may change the ROWIDs of entries in any tables that do not have an explicit INTEGER PRIMARY KEY.
%   https://sqlite.org/lang_vacuum.html
% strip out _id and _rev from doc body
% compaction: prune rev trees


exists(FilePath) ->
    couch_log:info("~n> exists(FilePath: ~p)~n", [FilePath]),
    filelib:is_file(FilePath).
delete(_RootDir, FilePath, _Async) -> file:delete(FilePath).
delete_compaction_files(_RootDir, _FilePath, _DelOpts) -> ok.

init(FilePath, _Options) ->
    couch_log:info("~n> init(FilePath: ~p)~n", [FilePath]),
    {ok, Db} = esqlite3:open(FilePath),
    ok = esqlite3:exec("PRAGMA journal_mode=WAL;", Db),
    Meta = "CREATE TABLE IF NOT EXISTS meta ("
        ++ "key TEXT, value TEXT, UNIQUE(key))",
    Epochs = "CREATE TABLE IF NOT EXISTS epochs (node TEXT, seq INT, UNIQUE(node, seq))",
    Purges = "CREATE TABLE IF NOT EXISTS purges (purge_seq INT, uuid TEXT, docid TEXT, rev TEXT)",
    PurgeIndexes = [
        "CREATE INDEX IF NOT EXISTS purge_seq ON purges (purge_seq)",
        "CREATE INDEX IF NOT EXISTS uuid ON purges (uuid)"
    ],
    Documents = "CREATE TABLE IF NOT EXISTS documents ("
        ++ "id TEXT, "
        ++ "rev TEXT, "
        ++ "revtree TEXT, "
        ++ "deleted INT DEFAULT 0, "
        ++ "latest INT DEFAULT 1, "
        ++ "local INT DEFAULT 0, "
        ++ "body BLOB, "
        ++ "UNIQUE(id, rev))",
    DocumentsIndexes = [
        "CREATE INDEX IF NOT EXISTS id ON documents (id)",
        "CREATE INDEX IF NOT EXISTS idrev ON documents (id, rev)",
        % "CREATE INDEX IF NOT EXISTS seq ON documents (seq)",
        "CREATE INDEX IF NOT EXISTS deleted ON documents (deleted)",
        "CREATE INDEX IF NOT EXISTS latest ON documents (latest)",
        "CREATE INDEX IF NOT EXISTS latest ON documents (local)",
        "CREATE INDEX IF NOT EXISTS latest ON documents (rowid)"
    ],
    ok = esqlite3:exec(Meta, Db),
    ok = esqlite3:exec(Epochs, Db),
    ok = esqlite3:exec(Purges, Db),
    ok = esqlite3:exec(Documents, Db),
    lists:foreach(fun (IdxDef) ->
        ok = esqlite3:exec(IdxDef, Db)
    end, DocumentsIndexes ++ PurgeIndexes),
    % TODO Purges = "CREATE TABLE purges IF NOT EXISTS",
    % TODO Attachments = "CREATE TABLE attachments ()"
    State = init_state(Db),
    case load_meta(uuid, Db) of
        undefined -> init_uuid(Db);
        _Else -> ok
    end,
    {ok, State}.

init_state(Db) ->
    ok = init_security(Db),
    {UpateEpochs, Epochs} = init_epochs(Db),
    #sqldb {
       db = Db,
       update_epochs = UpateEpochs,
       epochs = Epochs,
       last_activity = os:timestamp()
    }.

init_security(Db) ->
    couch_log:info("~n> init_security() ~n", []),
    case load_meta(security, undefined, Db) of
        undefined -> % only init once
            couch_log:info("~n> DO INIT~n", []),
            Config = config:get("couchdb", "default_security", "admin_local"),
            DefaultSecurity = case Config of
                "admin_only" -> ?ADMIN_ONLY_SEC_PROPS;
                _Else -> {[]}
            end,
            write_meta(security, couch_util:json_encode(DefaultSecurity), Db);
        _Else ->
            ok
    end.

init_epochs(Db) ->
    SQL = "SELECT node, seq FROM epochs ORDER BY seq DESC;",
    Me = node(),
    case esqlite3:q(SQL, Db) of
        [] ->
            {true, [{Me, 0}]}; % init
        [{Me, _Seq} | _RestEpochs] = Epochs ->
            % if top result node = me() all good
            {false, Epochs};
        [{_Node, _Seq} | _RestEpochs] = Epochs ->
            % else, add me on top
            {true, [{Me, get_update_seq(Db)} | Epochs]};
        _Else ->
            throw({error, sqlite_engine_invalid_epoch_query})
    end.

init_uuid(Db) ->
    UUID = couch_uuids:random(),
    SQL = "INSERT INTO meta (key, value) VALUES (?1, ?2);",
    {ok, Insert} = esqlite3:prepare(SQL, Db),
    ok = esqlite3:bind(Insert, ["uuid", UUID]),
    '$done' = esqlite3:step(Insert).

maybe_update_epochs(#sqldb{ update_epochs = false} = State) -> State;
maybe_update_epochs(#sqldb{ db = Db, epochs = [{Node, Seq} | _RestEpochs]} = State) ->
    couch_log:info("~n> maybe_update_epochs(Node: ~p, Seq: ~p)~n", [Node, Seq]),
    SQL = "INSERT INTO epochs (node, seq) VALUES (?1, ?2)",
    {ok, Insert} = esqlite3:prepare(SQL, Db),
    ok = esqlite3:bind(Insert, [Node, Seq]),
    '$done' = esqlite3:step(Insert),
    State#sqldb{update_epochs = false}.

terminate(Reason, #sqldb{db = Db}) ->
    couch_log:info("~n> terminate(Reason: ~p)~n", [Reason]),
    ok = esqlite3:close(Db).
    
handle_db_updater_call(Msg, State) ->
    couch_log:info("~n> handle_db_updater_call(~p) ~n", [Msg]),
    {reply, ok, State}.
handle_db_updater_info(Msg, State) ->
    couch_log:info("~n> handle_db_updater_info(~p) ~n", [Msg]),
    {noreply, State}.

incref(State) -> {ok, State}.
decref(_Db) -> ok.
monitored_by(_Db) -> [].

last_activity(#sqldb{last_activity = LastActivity}) -> LastActivity.

load_meta(Key, Db) ->
    load_meta(Key, undefined, Db).

load_meta(Key, Default, Db) when is_atom(Key) ->
    load_meta(atom_to_list(Key), Default, Db);
load_meta(Key, Default, Db) when is_list(Key) ->
    SQL = "SELECT value FROM meta WHERE key = ?1",
    R = esqlite3:q(SQL, [Key], Db),
    couch_log:info("~n> load_meta(~p) R: ~p ~n", [Key, R]),
    case R of
        [] -> Default;
        [{Value}] -> Value
    end;
load_meta(_Key, _Default, _Db) ->
    throw({error, sqlite_engine_invalid_meta_load_key}).

write_meta(Key, Value, Db) when is_atom(Key) ->
    write_meta(atom_to_list(Key), Value, Db);
write_meta(Key, Value, Db) when is_list(Key) ->
    couch_log:info("~n> write_meta(~p, ~p) ~n", [Key, Value]),
    SQL = "INSERT INTO meta (key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = ?2 WHERE key = ?1",
    {ok, Insert} = esqlite3:prepare(SQL, Db),
    ok = esqlite3:bind(Insert, [Key, Value]),
    '$done' = esqlite3:step(Insert),
    ok;
write_meta(_Key, _Value, _Db) ->
    throw({error, sqlite_engine_invalid_meta_write_key}).

get_compacted_seq(#sqldb{db = Db}) ->
    couch_log:info("~n> get_compacted_seq()~n", []),
    load_meta(compacted_seq, 0, Db).

get_del_doc_count(#sqldb{db = Db}) ->
    couch_log:info("~n> get_del_doc_count()~n", []),
    SQL = "SELECT COUNT(*) FROM documents WHERE latest=1 AND deleted=1",
    case esqlite3:q(SQL, Db) of
        [] -> 0;
        [{DelDocCount}] ->
            couch_log:info("~n< get_del_doc_count() -> ~p~n", [DelDocCount]),
            DelDocCount
    end.

get_disk_version(_Db) ->
    couch_log:info("~n> get_disk_version()~n", []),
    ?DISK_VERSION.
get_doc_count(#sqldb{db = Db}) ->
    couch_log:info("~n> get_doc_count()~n", []),
    SQL = "SELECT COUNT(*) FROM documents WHERE latest=1 AND deleted=0",
    case esqlite3:q(SQL, Db) of
        [] -> 0;
        [{DocCount}] ->
            couch_log:info("~n< get_doc_count() -> ~p~n", [DocCount]),
            DocCount
    end.
get_epochs(#sqldb{db = Db}) ->
    couch_log:info("~n> get_epochs()~n", []),
    SQL = "SELECT MAX(rowid) FROM documents WHERE latest=1 AND deleted=0",
    case esqlite3:q(SQL, Db) of
        [] -> 0;
        [{undefined}] -> [{node(), 0}];
        [{MaxRowId}] ->
            couch_log:info("~n< get_epochs() -> ~p~n", [MaxRowId]),
            [{node(), MaxRowId}]
    end.
get_purge_seq(#sqldb{db = Db}) ->
    couch_log:info("~n> get_purge_seq()~n", []),
    SQL = "SELECT MAX(purge_seq) FROM purges",
    case esqlite3:q(SQL, Db) of
        [] -> 0;
        [{undefined}] -> 0;
        [{PurgeSeq}] ->
            couch_log:info("~n< get_purge_seq() -> ~p~n", [PurgeSeq]),
            PurgeSeq
    end.

get_oldest_purge_seq(#sqldb{db = Db}) ->
    couch_log:info("~n> get_oldest_purge_seq()~n", []),
    SQL = "SELECT MIN(purge_seq) FROM purges",
    case esqlite3:q(SQL, Db) of
        [] -> 0;
        [{undefined}] -> 0;
        [{PurgeSeq}] ->
            couch_log:info("~n< get_oldest_purge_seq() -> ~p~n", [PurgeSeq]),
            PurgeSeq
    end.
get_purge_infos_limit(#sqldb{db = Db}) ->
    couch_log:info("~n> get_purge_infos_limit()~n", []),
    load_meta(purge_infos_limit, 1000, Db).
get_revs_limit(#sqldb{db = Db}) ->
    couch_log:info("~n> get_revs_limit()~n", []),
    binary_to_integer(load_meta(revs_limit, <<"1000">>, Db)).

get_security(#sqldb{db = Db}) ->
    couch_log:info("~n> get_security()~n", []),
    Value = load_meta(security, {[]}, Db),
    couch_log:info("~n> VValue: ~p~n", [Value]),
    {JSONValue} = couch_util:json_decode(Value),
    couch_log:info("~n> JSONVValue: ~p~n", [JSONValue]),
    JSONValue.
get_size_info(#sqldb{db = _Db}) ->
    couch_log:info("~n> get_size_info()~n", []),
    [
        {file, 123},
        {active, 234},
        {external, 345}
    ].

get_update_seq(#sqldb{db = Db}) ->
    get_update_seq(Db);
get_update_seq(Db) ->
    couch_log:info("~n> get_update_seq(Db: ~p)~n", [Db]),
    SQL = "SELECT rowid FROM documents ORDER BY rowid DESC LIMIT 1;",
    case esqlite3:q(SQL, Db) of
        [] -> 0;
        [{undefined}] -> 0;
        [{UpdateSeq}] ->
            couch_log:info("~n< get_update_seq() -> ~p~n", [UpdateSeq]),
            UpdateSeq
    end.

get_uuid(#sqldb{db = Db}) ->
    couch_log:info("~n> get_uuid()~n", []),
    load_meta(uuid, Db).

set_revs_limit(#sqldb{db = Db} = State, Value) ->
    ok = write_meta(revs_limit, Value, Db),
    {ok, State}.

set_purge_infos_limit(#sqldb{db = Db} = State, Value) ->
    ok = write_meta(purge_infos_limit, Value, Db),
    {ok, State}.
set_security(#sqldb{db = Db} = State, Value) ->
    couch_log:info("~n> set_security(Value: ~p)~n", [Value]),
    JSONValue = couch_util:json_encode({Value}),
    ok = write_meta(security, ?b2l(JSONValue), Db),
    {ok, State}.

make_qs(N) ->
    Qs = make_qs(N, []),
    lists:flatten(lists:join(",", Qs)).
    
make_qs(0, Qs) -> Qs;
make_qs(N, Qs) ->
    make_qs(N - 1, ["?"|Qs]).

open_docs(#sqldb{}, []) -> [];
open_docs(#sqldb{db = Db}, DocIds) ->
    couch_log:info("~n> open_docs(~p)~n", [DocIds]),
    JoinedDocIds = lists:map(fun binary_to_list/1, DocIds),
    Qs = make_qs(length(DocIds)),
    SQL = "SELECT id, rowid, revtree FROM documents WHERE id IN (" ++ Qs++ ") AND latest=1",
    couch_log:info("~n~nSQL: ~p, ~p,", [SQL, JoinedDocIds]),

    Result = esqlite3:q(SQL, JoinedDocIds, Db),
    couch_log:info("~n~nResult: ~p", [Result]),
    
    R = lists:map(fun(DocId) ->
        % couch_log:info("~n~nDocId: ~p", [DocId]),
        case lists:keyfind(DocId, 1, Result) of
            false -> not_found;
            {DocId, RowId, RevTree0} ->
                RevTree = case RevTree0 of
                    undefined -> [];
                    _Else -> binary_to_term(base64:decode(RevTree0))
                end,
                #full_doc_info{
                    id = DocId,
                    update_seq = RowId,
                    deleted = false,
                    rev_tree = RevTree,
                    sizes = #size_info{}
                }
        end
    end, DocIds),
    couch_log:info("~n> open_docs R: ~p~n", [R]),
    R.

open_local_docs(#sqldb{db = Db}, DocIds) ->
    couch_log:info("~n> open_local_docs(~p)~n", [DocIds]),

    JoinedDocIds = lists:join(",", DocIds),
    SQL = "SELECT id, rev, body FROM documents WHERE id IN (?) AND latest=1",
    couch_log:info("~n~nSQL: ~p, ~p", [SQL, DocIds]),

    Result = esqlite3:q(SQL, [JoinedDocIds], Db),
    couch_log:info("~n~nResult: ~p", [Result]),

    lists:map(fun(DocId) ->
        couch_log:info("~n~nDocId: ~p", [DocId]),
        case lists:keyfind(DocId, 1, Result) of
            false -> not_found;
            {DocId, Rev, Body} ->
                couch_log:info("~n~nRev: ~p", [Rev]),
                [Pos, RevId] = string:split(?b2l(Rev), "-"),
                #doc{
                    id = DocId,
                    revs = {list_to_integer(Pos), [RevId]},
                    body = couch_util:json_decode(Body)
                }
        end
    end, DocIds).

read_doc_body(#sqldb{db = Db}, #doc{id = Id, revs = {Start, RevIds}} = Doc) ->
    couch_log:info("~n> read_doc_body(~p)~n", [Doc]),
    [{_, JsonRev}] = couch_doc:to_json_rev(Start, RevIds),
    SQL = "SELECT body, deleted FROM documents WHERE id=?1 AND rev=?2",
    SQLResult = esqlite3:q(SQL, [Id, JsonRev], Db),
    couch_log:info("~n> SQL: ~p, SQLResult: ~p~n", [SQL, SQLResult]),
    {Deleted, Result} = case SQLResult of
        [] -> {false, not_found};
        [{Body, Deleted0}] -> {Deleted0, Body}
    end,
    {BodyList} = couch_util:json_decode(Result),
    FilterIdAndRev = fun({Key, _Value}) -> Key =:= <<"_id">> orelse Key =:= <<"_rev">> end,
    BodyTerm = {lists:dropwhile(FilterIdAndRev, BodyList)},
    DeletedBool = case Deleted of
        1 -> true;
        _Zero -> false
    end,
    Doc#doc{body = BodyTerm, deleted = DeletedBool}.
load_purge_infos(#sqldb{db = Db}, UUIDs) ->
    couch_log:info("~n> load_purge_infos(~p)~n", [UUIDs]),
    lists:map(fun (UUID) -> load_purge_info(Db, UUID) end, UUIDs).

load_purge_info(Db, UUID) ->
    couch_log:info("~n> load_purge_info(~p)~n", [UUID]),
    SQL = "SELECT purge_seq, docid, rev FROM purges WHERE uuid = ?1",
    case esqlite3:q(SQL, [UUID], Db) of
        [] ->
            not_found;
        Result ->
            couch_log:info("~n>  Result: ~p~n", [Result]),
            lists:foldl(fun({PurgeSeq, DocId, Rev}, {_PurgeSeq, _DocId, Revs}) ->
                {PurgeSeq, DocId, [Rev|Revs]}
            end, {null, null, []}, Result)
    end.

serialize_doc(_Db, Doc) ->
    couch_log:info("~n> serialize_doc(~p)~n", [Doc]),
    Doc.

write_doc_body(#sqldb{db = Db}, #doc{id=Id, revs={Start, RevIds}}=Doc) ->
    couch_log:info("~n> write_doc_body(~p)~n", [Doc]),
    JsonDoc = couch_util:json_encode(couch_doc:to_json_obj(Doc, [])),
    [{_, JsonRevs}] = couch_doc:to_json_rev(Start, RevIds),
    ok = esqlite3:exec("begin;", Db),

    % set all previous revisions to latest = 0
    {ok, Update} = esqlite3:prepare("UPDATE documents SET latest=0 WHERE id=?1", Db),
    ok = esqlite3:bind(Update, [Id]),
    '$done' = esqlite3:step(Update),

    % insert new revision, set latest = 1
    couch_log:info("~n> JsonRevs: ~p~n", [JsonRevs]),
    couch_log:info("~n> JsonDoc: ~p~n", [JsonDoc]),
    SQL = "INSERT INTO documents (id, rev, deleted, latest, body)"
        ++ " VALUES (?1, ?2, 0, 1, ?3)",
    {ok, Insert} = esqlite3:prepare(SQL, Db),
    Bind = esqlite3:bind(Insert, [Id, JsonRevs, JsonDoc]),
    Step = esqlite3:step(Insert),
    couch_log:info("~n> SQL: ~p, Bind, ~p, Step: ~p~n", [SQL, Bind, Step]),
    ok = esqlite3:exec("commit;", Db), % TODO: maybe move into write_doc_infos
    {ok, Doc#doc{body=JsonDoc}, size(JsonDoc)}.

write_doc_infos(#sqldb{db = Db} = State, Pairs, LocalDocs) ->
    couch_log:info("~n> write_doc_infos(~p, ~p)~n", [Pairs, LocalDocs]),
    lists:foreach(fun({_OldFDI, NewFDI}) ->
        RevTreeBin = term_to_binary(NewFDI#full_doc_info.rev_tree),
        RevTreeList = base64:encode(?b2l(RevTreeBin)),
        couch_log:info("~n> RevTreeList: ~p~n", [RevTreeList]),
        Deleted = case NewFDI#full_doc_info.deleted of
            true -> 1;
            _False -> 0
        end,
        SQL = "UPDATE documents SET revtree=?1, deleted=?2 WHERE id=?3 AND latest=1",
        {ok, Update} = esqlite3:prepare(SQL, Db),
        ok = esqlite3:bind(Update, [RevTreeList, Deleted, NewFDI#full_doc_info.id]),
        '$done' = esqlite3:step(Update)
    end, Pairs),

    lists:foreach(fun(#doc{id=Id,revs={Start, [Idx]}}=LocalDoc0) ->
        LocalDoc = LocalDoc0#doc{revs={Start, [integer_to_list(Idx)]}},
        JsonRevs = ?l2b([integer_to_list(Start), "-", Idx+48]),
        couch_log:info("~n> LocalDoc: ~p, JsonRevs: ~p~n", [LocalDoc, JsonRevs]),
        JsonDoc = couch_util:json_encode(couch_doc:to_json_obj(LocalDoc, [])),
        couch_log:info("~n> JsonDoc: ~p~n", [JsonDoc]),
        SQL = "INSERT INTO documents (id, rev, body, local, latest, deleted) VALUES (?1, ?2, ?3, 1, 1, 0)",
        couch_log:info("~n> SQL: ~p~n", [SQL]),
        {ok, Upsert} = esqlite3:prepare(SQL, Db),
        ok = esqlite3:bind(Upsert, [Id, JsonRevs, JsonDoc]),
        '$done' = esqlite3:step(Upsert)
    end, LocalDocs),
    NewState = maybe_update_epochs(State),
    {ok, NewState}.

purge_docs(#sqldb{db = Db}=State, Pairs, PurgeInfos) ->
    couch_log:info("~n> purge_docs(PurgeInfos: ~p)~n", [PurgeInfos]),
    lists:foreach(fun
        ({not_found, not_found}) ->
            ok;
        ({OldFDI, not_found}) ->
            SQL = "DELETE FROM documents WHERE id=?1",
            {ok, Update} = esqlite3:prepare(SQL, Db),
            ok = esqlite3:bind(Update, [OldFDI#full_doc_info.id]),
            '$done' = esqlite3:step(Update);
        ({OldFDI, NewFDI}) ->
            RevTreeBin = term_to_binary(NewFDI#full_doc_info.rev_tree),
            RevTreeList = base64:encode(?b2l(RevTreeBin)),
            couch_log:info("~n> RevTreeList: ~p~n", [RevTreeList]),
            SQL = "UPDATE documents SET revtree=?1 WHERE id=?2 AND latest=1",
            {ok, Update} = esqlite3:prepare(SQL, Db),
            ok = esqlite3:bind(Update, [RevTreeList, NewFDI#full_doc_info.id]),
            '$done' = esqlite3:step(Update)
    end, Pairs),
    lists:foreach(fun (PurgeInfo) -> write_purge_info(Db, PurgeInfo) end, PurgeInfos),
    {ok, State}.

write_purge_info(Db, {PurgeSeq, UUID, DocId, Revs}) ->
    lists:foreach(fun (Rev) -> write_purge_info(Db, PurgeSeq, UUID, DocId, Rev) end, Revs).

write_purge_info(Db, PurgeSeq, UUID, DocId, {Start, Rev}) ->
    SQL = "INSERT INTO purges (purge_seq, uuid, docid, rev) VALUES (?1, ?2, ?3, ?4)",
    {ok, Insert} = esqlite3:prepare(SQL, Db),
    [{_, JsonRev}] = couch_doc:to_json_rev(Start, [Rev]),
    couch_log:info("~n> write_purge_info(): SQL ~p PurgeSeq: ~p, UUID: ~p, DocId: ~p, JsonRev: ~p~n", [SQL, PurgeSeq, UUID, DocId, JsonRev]),
    ok = esqlite3:bind(Insert, [PurgeSeq, UUID, DocId, JsonRev]),
    '$done' = esqlite3:step(Insert),
    ok.

commit_data(State) ->
    couch_log:info("~n> commit_data()~n", []),
    {ok, State}.

open_write_stream(_Db, Options) ->
    couch_log:info("~n> open_write_stream(Options)~n", [Options]),
    throw(not_supported).
open_read_stream(_Db, _Stream) ->
    couch_log:info("~n> open_read_stream()~n", []),
    throw(not_supported).
is_active_stream(_Db, _Stream) ->
    couch_log:info("~n> is_active_stream()~n", []),
    false.

% This function is called to fold over the documents in
% the database sorted by the raw byte collation order of
% the document id. For each document id, the supplied user
% function should be invoked with the first argument set
% to the #full_doc_info{} record and the second argument
% set to the current user supplied accumulator.

% TODO source docu: UserFun needs OffsetReductions as second para, Acc as third

% The return
% value of the user function is a 2-tuple of {Go, NewUserAcc}.
% The NewUserAcc value should then replace the current
% user accumulator. If Go is the atom ok, iteration over
% documents should continue. If Go is the atom stop, then
% iteration should halt and the return value should be
% {ok, NewUserAcc}.
%
fold_docs(#sqldb{db = Db}, UserFun, UserAcc, Options) ->
    couch_log:info("~n> fold_docs(_, _, _, Options: ~p)~n", [Options]),
    fold_docs_int(Db, UserFun, UserAcc, Options, global).
fold_local_docs(#sqldb{db = Db}, UserFun, UserAcc, Options) ->
    couch_log:info("~n> fold_local_docs(_, _, _, Options: ~p)~n", [Options]),
    fold_docs_int(Db, UserFun, UserAcc, Options, local).
fold_docs_int(Db, UserFun, UserAcc, Options, Type) ->
    couch_log:info("~n> fold_docs(_, _, _, Options: ~p)~n", [Options]),
    SQL0 = "SELECT id, rowid, revtree, rev FROM documents WHERE latest = 1 AND deleted = 0 ",
    LocalSQL = case Type of
        local -> "AND local = 1 ";
        _Global -> "AND local != 1 "
    end,
    AdditionalWhere = case Type of
        changes -> changes_options_to_sql(Options, Type);
        _ -> options_to_sql(Options, Type)
    end,
    SQL1 = SQL0 ++ LocalSQL ++ AdditionalWhere,
    Order = options_to_order_sql(Options, Type),
    SQL = SQL1 ++ Order,
    couch_log:info("~n> fold_docs() SQL: ~p~n", [SQL]),
    Result = esqlite3:q(SQL, Db),
    couch_log:info("~n> fold_docs() Result: ~p~n", [Result]),
    FinalNewUserAcc = try
        lists:foldl(fun({Id, RowId, RevTree0, Rev}, Acc) ->
            RevTree = case RevTree0 of
                undefined -> [];
                % TODO: hackety hack: split out into separate docs
                _Else -> binary_to_term(base64:decode(RevTree0))
            end,
            FDI = case Type of
                local ->
                    [Pos, RevId] = string:split(?b2l(Rev), "-"),
                    #doc{
                        id = Id,
                        revs = {list_to_integer(Pos), [RevId]}
                    };
                _Global1 ->
                    #full_doc_info{
                        id = Id,
                        rev_tree = RevTree,
                        deleted = false,
                        update_seq = RowId,
                        sizes = #size_info{}
                    }
            end,

            case Type of
                changes -> 
                    case UserFun(FDI, Acc) of
                        {ok, NewUserAcc} -> NewUserAcc;
                        {stop, LastUserAcc} -> throw({stop, LastUserAcc})
                    end;
                _Other ->
                    case lists:member(include_reductions, Options) of
                        true ->
                            couch_log:info("~n> fold_docs() call user fun, include reductions: Acc~n", []),
                            % make cpse tests happy which passes in a `nil` Acc.
                            Reductions = case Acc of
                                List when is_list(List) -> length(List);
                                _ -> 0
                            end,
                            case UserFun(FDI, Reductions, Acc) of
                            % case UserFun(FDI, {[], []}, Acc) of % stolen from the in-mem db, might be out of date
                                {ok, NewUserAcc} -> NewUserAcc;
                                {stop, LastUserAcc} -> throw({stop, LastUserAcc})
                            end;
                        false ->
                            couch_log:info("~n> fold_docs() call user fun~n", []),
                            case UserFun(FDI, Acc) of
                            % case UserFun(FDI, {[], []}, Acc) of % stolen from the in-mem db, might be out of date
                                {ok, NewUserAcc} -> NewUserAcc;
                                {stop, LastUserAcc} -> throw({stop, LastUserAcc})
                            end
                    end
            end
        end, UserAcc, Result)
    catch
        throw:{stop, FinalAcc} -> FinalAcc
    end,
    case lists:member(include_reductions, Options) of
        true -> couch_log:info("~n> fold_docs() return value, include reductions~n", []), {ok, length(Result), FinalNewUserAcc};
        _False -> couch_log:info("~n> fold_docs() return value~n", []), {ok, FinalNewUserAcc}
    end.

fold_changes(#sqldb{db = Db}, SinceSeq, UserFun, UserAcc, Options0) ->
    Options = [{start_key, SinceSeq+1} | Options0],
    couch_log:info("~n> fold_changes(~p)~n", [Options]),
    fold_docs_int(Db, UserFun, UserAcc, Options, changes).

rev_to_binary(JSONRev) -> couch_doc:parse_rev(JSONRev).
    
fold_purge_infos(#sqldb{db = Db}, StartSeq, UserFun, UserAcc, Options) ->
    Dir = case proplists:get_value(dir, Options, fwd) of
        rev -> " DESC";
        _ -> ""
    end,
    couch_log:info("~n> fold_purge_infos(SinceSeq: ~p, Options: ~p)~n", [StartSeq, Options]),
    SQL = "SELECT purge_seq, uuid, docid, rev FROM purges WHERE purge_seq > ?1 ORDER BY purge_seq" ++ Dir,
    couch_log:info("~n> fold_purge_infos() SQL: ~p ~n", [SQL]),
    Result = esqlite3:q(SQL, [StartSeq], Db),
    couch_log:info("~n> fold_purge_infos() Result: ~p ~n", [Result]),
    GroupedPurgeInfos = lists:foldl(fun
        ({PurgeSeq, UUID, DocId, Rev}, []) -> [{PurgeSeq, UUID, DocId, [rev_to_binary(Rev)]}];
        ({PurgeSeq, UUID, DocId, Rev}, [{LastPurgeSeq, LastUUID, LastDocId, LastRevs}=LastPurgeInfo | RestAcc]) ->
            case LastPurgeSeq of
                PurgeSeq -> [{LastPurgeSeq, LastUUID, LastDocId, [rev_to_binary(Rev) | LastRevs]} | RestAcc]; % keep collecting
                _ -> [{PurgeSeq, UUID, DocId, [rev_to_binary(Rev)]}] ++ [LastPurgeInfo] ++ RestAcc % start collecting revs for this docId
            end
    end, [], Result),
    couch_log:info("~n> fold_purge_infos() GroupedPurgeInfos: ~p ~n", [GroupedPurgeInfos]),
    FinalResult = try 
        lists:foldl(fun(PurgeInfo, UserAcc) ->
            case UserFun(PurgeInfo, UserAcc) of
                {ok, NewUserAcc} -> NewUserAcc;
                {stop, NewUserAcc} -> throw({stop, NewUserAcc})
            end
        end, UserAcc, GroupedPurgeInfos)
    catch
        throw:{stop, LastUserAcc} -> LastUserAcc
    end,
    couch_log:info("~n> fold_purge_infos() FinalResult: ~p ~n", [FinalResult]),
    {ok, lists:reverse(FinalResult)}.

count_changes_since(#sqldb{db = Db}, SinceSeq) ->
    couch_log:info("~n> count_changes_since(~p)~n", [SinceSeq]),
    SQL = "SELECT COUNT(*) FROM documents WHERE latest = 1",
    [{ChangesSince}] = esqlite3:q(SQL, Db),
    couch_log:info("~n> count_changes_since() -> ChangesSince: ~p~n", [ChangesSince]),
    ChangesSince - SinceSeq.

start_compaction(#sqldb{db = Db} = State, DbName, Options, Parent) ->
    couch_log:info("~n> start_compaction(DbName: ~p, Options: ~p)~n", [DbName, Options]),
    Pid = spawn_link(fun() ->
        SQL = "DELETE FROM documents WHERE latest != 1;",
        ok = esqlite3:exec(SQL, Db),
        SQL2 = "VACUUM;",
        ok = esqlite3:exec(SQL2, Db),
        gen_server:cast(Parent, {compact_done, ?MODULE, {}})
    end),
    {ok, State, Pid}.
finish_compaction(#sqldb{db = Db} = State, DbName, Options, _CompactFilePath) ->
    couch_log:info("~n> finish_compaction(DbName: ~p, Options: ~p)~n", [DbName, Options]),
    Seq = get_update_seq(Db),
    ok = write_meta(compacted_seq, Seq, Db),
    NewState = maybe_update_epochs(State),
    {ok, NewState, undefined}.


-define(START_KEY_MIN, <<"">>).
-define(END_KEY_MAX, <<255>>).

% if dir !fwd reverse defaults
get_start_key(fwd, Options) ->
    proplists:get_value(start_key, Options, ?START_KEY_MIN);
get_start_key(rev, Options) ->
    proplists:get_value(start_key, Options, ?END_KEY_MAX).

% if dir !fwd reverse defaults
get_end_key(Dir, Options) ->
    case proplists:get_value(end_key, Options, nil) of
        nil -> get_end_key_gt(Dir, Options);
        _ -> get_end_key1(Dir, Options)
    end.
get_end_key1(fwd, Options) ->
    proplists:get_value(end_key, Options, ?END_KEY_MAX);
get_end_key1(rev, Options) ->
    proplists:get_value(end_key, Options, ?START_KEY_MIN).

get_end_key_gt(fwd, Options) ->
    proplists:get_value(end_key_gt, Options, ?END_KEY_MAX);
get_end_key_gt(rev, Options) ->
    proplists:get_value(end_key_gt, Options, ?START_KEY_MIN).

end_key_gt_sql(fwd, true) -> "<";
end_key_gt_sql(fwd, _False) -> "<=";
end_key_gt_sql(rev, true) -> ">";
end_key_gt_sql(rev, _False) -> ">=".

% Utilities
% [include_reductions,{dir,fwd},{start_key,<<"asd">>},{end_key,<<"?">>},{finalizer,null},{namespace,undefined}]
changes_options_to_sql([], _Type) -> "";
changes_options_to_sql(Options, _Type) ->
    Dir = proplists:get_value(dir, Options, fwd),
    Cmp = case Dir of
        fwd -> ">=";
        _ -> "<="
    end,
    Since = proplists:get_value(start_key, Options, 0),
    " AND rowid " ++ Cmp ++ " " ++ integer_to_list(Since).

options_to_sql([], _Type) -> "";
options_to_sql([{dir,_}], _Type) -> ""; % shortcut, maybe better parse options into easier to build-from structure
options_to_sql(Options, Type) ->
    Dir = proplists:get_value(dir, Options, fwd),
    StartKey = get_start_key(Dir, Options),
    EndKey = get_end_key(Dir, Options),
    HasEndKeyGt = proplists:lookup(end_key_gt, Options) /= none,
    case Type of
        changes ->
            " AND rowid >= '" ++ ?b2l(StartKey) ++ "'";
        _Else ->
            case Dir of
                fwd ->
                    " AND id >= '" ++ ?b2l(StartKey) ++ "' AND id " ++ end_key_gt_sql(Dir, HasEndKeyGt) ++ " '" ++ ?b2l(EndKey) ++ "'";
                _Rev ->
                    " AND id " ++ end_key_gt_sql(Dir, HasEndKeyGt) ++ " '" ++ ?b2l(EndKey) ++ "' AND id <= '" ++ ?b2l(StartKey) ++ "'"
            end
    end.

options_to_order_sql(Options, Type) ->
    parse_order(proplists:get_value(dir, Options, fwd), Type).

parse_order(fwd, changes) -> " ORDER BY rowid ASC";
parse_order(rev, changes) -> " ORDER BY rowid DESC";
parse_order(fwd, _) -> " ORDER BY id ASC";
parse_order(rev, _) -> " ORDER BY id DESC".

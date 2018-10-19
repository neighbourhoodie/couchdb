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

% TODO:
% dont rely on rowid for changes, as vacuum might reorder things:
%   The VACUUM command may change the ROWIDs of entries in any tables that do not have an explicit INTEGER PRIMARY KEY.
%   https://sqlite.org/lang_vacuum.html
% strip out _id and _rev from doc body


exists(FilePath) -> filelib:is_file(FilePath).
delete(_RootDir, FilePath, _Async) -> file:delete(FilePath).
delete_compaction_files(_RootDir, _FilePath, _DelOpts) -> ok.

init(FilePath, _Options) ->
    {ok, Db} = esqlite3:open(FilePath),
    Meta = "CREATE TABLE IF NOT EXISTS meta ("
        ++ "key TEXT, value TEXT)",
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
        "CREATE INDEX IF NOT EXISTS seq ON documents (seq)",
        "CREATE INDEX IF NOT EXISTS deleted ON documents (deleted)",
        "CREATE INDEX IF NOT EXISTS latest ON documents (latest)",
        "CREATE INDEX IF NOT EXISTS latest ON documents (local)",
        "CREATE INDEX IF NOT EXISTS latest ON documents (rowid)"
    ],
    ok = esqlite3:exec(Meta, Db),
    ok = esqlite3:exec(Documents, Db),
    lists:foreach(fun (IdxDef) ->
        esqlite3:exec(IdxDef, Db)
    end, DocumentsIndexes),
    % TODO Purges = "CREATE TABLE purges IF NOT EXISTS",
    % TODO Attachments = "CREATE TABLE attachments ()"
    
    {ok, update_meta(Db)}.

update_meta(Db) ->
    Db.
    % LastActivity = "INSERT INTO meta(key, value) VALUES (last_updated, NOW())",
    % esqlite3:exec(LastActivity, Db)

terminate(_Reason, St) ->
    esqlite3:close(St).
    
handle_db_updater_call(_Msg, _St) -> ok.
handle_db_updater_info(_Msg, _St) -> ok.

incref(St) -> {ok, St}.
decref(_Db) -> ok.
monitored_by(_Db) -> [].

last_activity(_Db) -> os:timestamp().

get_compacted_seq(_Db) ->
    couch_log:info("~n> get_compacted_seq()~n", []),
    0.
get_del_doc_count(Db) ->
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
    1.
get_doc_count(Db) ->
    couch_log:info("~n> get_doc_count()~n", []),
    SQL = "SELECT COUNT(*) FROM documents WHERE latest=1 AND deleted=0",
    case esqlite3:q(SQL, Db) of
        [] -> 0;
        [{DocCount}] ->
            couch_log:info("~n< get_doc_count() -> ~p~n", [DocCount]),
            DocCount
    end.
get_epochs(Db) ->
    couch_log:info("~n> get_epochs()~n", []),
    SQL = "SELECT MAX(rowid) FROM documents WHERE latest=1 AND deleted=0",
    case esqlite3:q(SQL, Db) of
        [] -> 0;
        [{MaxRowId}] ->
            couch_log:info("~n< get_epochs() -> ~p~n", [MaxRowId]),
            [{node(), MaxRowId}]
    end.
get_purge_seq(_Db) ->
    couch_log:info("~n> get_purge_seq()~n", []),
    0.
get_oldest_purge_seq(_Db) ->
    couch_log:info("~n> get_ol, Dbdest_purge_seq()~n", []),
    0.
get_purge_infos_limit(_Db) ->
    couch_log:info("~n> get_purge_infos_limit()~n", []),
    999.
get_revs_limit(_Db) ->
    couch_log:info("~n> get_revs_limit()~n", []),
    888.
get_security(_Db) ->
    couch_log:info("~n> get_security()~n", []),
    [].
get_size_info(_Db) ->
    couch_log:info("~n> get_size_info()~n", []),
    [
        {file, 123},
        {active, 234},
        {external, 345}
    ].
get_update_seq(Db) ->
    couch_log:info("~n> get_update_seq()~n", []),
    SQL = "SELECT rowid FROM documents ORDER BY rowid DESC LIMIT 1;",
    case esqlite3:q(SQL, Db) of
        [] -> 0;
        [{undefined}] -> 0;
        [{UpdateSeq}] ->
            couch_log:info("~n< get_update_seq() -> ~p~n", [UpdateSeq]),
            UpdateSeq
    end.

get_uuid(_Db) ->
    couch_log:info("~n> get_uuid()~n", []),
    <<"666666666666666666666666">>.

set_revs_limit(_Db, _Val) -> ok.
set_purge_infos_limit(_Db, _Val) -> ok.
set_security(_Db, _Val) -> ok.

open_docs(Db, DocIds) ->
    couch_log:info("~n> open_docs(~p)~n", [DocIds]),
    JoinedDocIds = lists:join(",", DocIds),
    SQL = "SELECT id, rowid, revtree FROM documents WHERE id IN (?) AND latest=1",
    couch_log:info("~n~nSQL: ~p, ~p", [SQL, DocIds]),

    Result = esqlite3:q(SQL, [JoinedDocIds], Db),
    couch_log:info("~n~nResult: ~p", [Result]),
    
    lists:map(fun(DocId) ->
        couch_log:info("~n~nDocId: ~p", [DocId]),
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
    end, DocIds).

open_local_docs(Db, DocIds) ->
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

read_doc_body(Db, Doc) ->
    couch_log:info("~n> read_doc_body(~p)~n", [Doc]),
    SQL = "SELECT body FROM documents WHERE id=?1",
    Result = case esqlite3:q(SQL, [Doc#doc.id], Db) of
        [] -> not_found;
        [{Body}] -> Body
    end,
    Doc#doc{body = couch_util:json_decode(Result)}.
load_purge_infos(_Db, UUIDs) ->
    couch_log:info("~n> load_purge_infos(~p)~n", [UUIDs]),
    ok.

serialize_doc(_Db, Doc) ->
    couch_log:info("~n> serialize_doc(~p)~n", [Doc]),
    Doc.

write_doc_body(Db, #doc{id=Id, revs={Start, RevIds}}=Doc) ->
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

write_doc_infos(Db, Pairs, LocalDocs) ->
    couch_log:info("~n> write_doc_infos(~p, ~p)~n", [Pairs, LocalDocs]),
    lists:foreach(fun({_OldFDI, NewFDI}) ->
        RevTreeBin = term_to_binary(NewFDI#full_doc_info.rev_tree),
        RevTreeList = base64:encode(?b2l(RevTreeBin)),
        couch_log:info("~n> RevTreeList: ~p~n", [RevTreeList]),
        SQL = "UPDATE documents SET revtree=?1 WHERE id=?2 AND latest=1",
        {ok, Update} = esqlite3:prepare(SQL, Db),
        ok = esqlite3:bind(Update, [RevTreeList, NewFDI#full_doc_info.id]),
        '$done' = esqlite3:step(Update)
    end, Pairs),

    lists:foreach(fun(#doc{id=Id,revs={Start, [Idx]}}=LocalDoc0) ->
        LocalDoc = LocalDoc0#doc{revs={Start, [integer_to_list(Idx)]}},
        JsonRevs = ?l2b([integer_to_list(Start), "-", Idx+48]),
        couch_log:info("~n> LocalDoc: ~p, JsonRevs: ~p~n", [LocalDoc, JsonRevs]),
        JsonDoc = couch_util:json_encode(couch_doc:to_json_obj(LocalDoc, [])),
        couch_log:info("~n> JsonDoc: ~p~n", [JsonDoc]),
        SQL = "INSERT INTO documents (id, rev, body, local, latest, deleted) VALUES (?1, ?2, ?3, 1, 1, 0)",
        {ok, Upsert} = esqlite3:prepare(SQL, Db),
        ok = esqlite3:bind(Upsert, [Id, JsonRevs, JsonDoc]),
        '$done' = esqlite3:step(Upsert)
    end, LocalDocs),

    {ok, Db}.
purge_docs(_Db, _Pairs, _PurgeInfos) ->
    couch_log:info("~n> purge_docs()~n", []),
    ok.

commit_data(Db) ->
    couch_log:info("~n> commit_data()~n", []),
    {ok, Db}.

open_write_stream(_Db, Options) ->
    couch_log:info("~n> open_write_stream(Options)~n", [Options]),
    ok.
open_read_stream(_Db, _Stream) ->
    couch_log:info("~n> open_read_stream()~n", []),
    ok.
is_active_stream(_Db, _Stream) ->
    couch_log:info("~n> is_active_stream()~n", []),
    ok.

% This function is called to fold over the documents in
% the database sorted by the raw byte collation order of
% the document id. For each document id, the supplied user
% function should be invoked with the first argument set
% to the #full_doc_info{} record and the second argument
% set to the current user supplied accumulator.

% TODO: UserFun needs OffsetReductions as second para, Acc as third

% The return
% value of the user function is a 2-tuple of {Go, NewUserAcc}.
% The NewUserAcc value should then replace the current
% user accumulator. If Go is the atom ok, iteration over
% documents should continue. If Go is the atom stop, then
% iteration should halt and the return value should be
% {ok, NewUserAcc}.
%
fold_docs(Db, UserFun, UserAcc, Options) ->
    couch_log:info("~n> fold_docs(_, _, _, Options: ~p)~n", [Options]),
    fold_docs_int(Db, UserFun, UserAcc, Options, global).
fold_local_docs(Db, UserFun, UserAcc, Options) ->
    couch_log:info("~n> fold_local_docs(_, _, _, Options: ~p)~n", [Options]),
    fold_docs_int(Db, UserFun, UserAcc, Options, local).
fold_docs_int(Db, UserFun, UserAcc, Options, Type) ->
    couch_log:info("~n> fold_docs(_, _, _, Options: ~p)~n", [Options]),
    SQL0 = "SELECT id, rowid, revtree, rev FROM documents WHERE latest = 1 AND deleted = 0 ",
    LocalSQL = case Type of
        local -> "AND local = 1 ";
        _Global -> "AND local != 1 "
    end,
    AdditionalWhere = options_to_sql(Options, Type),
    SQL1 = SQL0 ++ LocalSQL ++ AdditionalWhere,
    Order = options_to_order_sql(Options, Type),
    SQL = SQL1 ++ Order,
    couch_log:info("~n> fold_docs() SQL: ~p~n", [SQL]),
    Result = esqlite3:q(SQL, Db),
    FinalNewUserAcc = lists:foldl(fun({Id, RowId, RevTree0, Rev}, Acc) ->
        RevTree = case RevTree0 of
            undefined -> [];
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
        couch_log:info("~n> UserFun()~p~n", [UserFun]),
        case Type of
            changes -> 
                case UserFun(couch_doc:to_doc_info(FDI), Acc) of
                    {ok, NewUserAcc} -> NewUserAcc;
                    {stop, LastUserAcc} -> LastUserAcc % TODO: actually stop
                end;
            _Other ->
                case UserFun(FDI, {[], []}, Acc) of
                    {ok, NewUserAcc} -> NewUserAcc;
                    {stop, LastUserAcc} -> LastUserAcc % TODO: actually stop
                end
        end
    end, UserAcc, Result),
    case lists:member(include_reductions, Options) of
        true -> {ok, 0, FinalNewUserAcc};
        _False -> {ok, FinalNewUserAcc}
    end.

fold_changes(Db, SinceSeq, UserFun, UserAcc, Options0) ->
    Options = [{stert_key, SinceSeq} | Options0],
    couch_log:info("~n> fold_changes(~p)~n", [Options]),
    fold_docs_int(Db, UserFun, UserAcc, Options, changes).
fold_purge_infos(_Db, _StartSeq, _UserFun, _UserAcc, Options) ->
    couch_log:info("~n> fold_purge_infos(~p)~n", [Options]),
    ok.
count_changes_since(Db, SinceSeq) ->
    couch_log:info("~n> count_changes_since(~p)~n", [SinceSeq]),
    get_update_seq(Db) - SinceSeq.

start_compaction(Db, DbName, Options, Parent) ->
    couch_log:info("~n> start_compaction(DbName: ~p, Options: ~p)~n", [DbName, Options]),
    Pid = spawn_link(fun() ->
        SQL = "DELETE FROM documents WHERE latest != 1;",
        ok = esqlite3:exec(SQL, Db),
        SQL2 = "VACUUM;",
        ok = esqlite3:exec(SQL2, Db),
        gen_server:cast(Parent, {compact_done, ?MODULE, {}})
    end),
    {ok, Db, Pid}.
finish_compaction(Db, DbName, Options, _CompactFilePath) ->
    couch_log:info("~n> finish_compaction(DbName: ~p, Options: ~p)~n", [DbName, Options]),
    {ok, Db, undefined}.


% Utilities
% [include_reductions,{dir,fwd},{start_key,<<"asd">>},{end_key,<<"?">>},{finalizer,null},{namespace,undefined}]
options_to_sql(Options, Type) ->
    StartKey = proplists:get_value(start_key, Options, <<"0">>),
    EndKey = proplists:get_value(end_key, Options, <<"">>),
    Dir = proplists:get_value(dir, Options, fwd),
    case Type of
        changes ->
            " AND rowid >= " ++ ?b2l(StartKey) ++ " AND rowid <= '" ++ ?b2l(EndKey) ++ "'";
        _Else ->
            case Dir of
                fwd ->
                    " AND id >= '" ++ ?b2l(StartKey) ++ "' AND id <= '" ++ ?b2l(EndKey) ++ "'";
                _Rev ->
                    " AND id >= '" ++ ?b2l(EndKey) ++ "' AND id <= '" ++ ?b2l(StartKey) ++ "'"
            end
    end.

options_to_order_sql(Options, Type) ->
    parse_order(proplists:get_value(dir, Options, fwd), Type).

parse_order(fwd, changes) -> " ORDER BY rowid ASC";
parse_order(rev, changes) -> " ORDER BY rowid DESC";
parse_order(fwd, _) -> " ORDER BY id ASC";
parse_order(rev, _) -> " ORDER BY id DESC".

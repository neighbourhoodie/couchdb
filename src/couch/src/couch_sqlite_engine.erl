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

exists(FilePath) -> filelib:is_file(FilePath).
delete(_RootDir, FilePath, _Async) -> file:delete(FilePath).
delete_compaction_files(_RootDir, _FilePath, _DelOpts) -> ok.

init(FilePath, _Options) ->
    {ok, Db} = esqlite3:open(FilePath),
    Meta = "CREATE TABLE IF NOT EXISTS meta ("
        ++ "key TEXT, value TEXT)",
    Documents = "CREATE TABLE IF NOT EXISTS documents ("
        ++ "id TEXT, rev TEXT, revtree TEXT, deleted INT, latest INT, body BLOB, UNIQUE(id, rev))",
    DocumentsIndexes = [
        "CREATE INDEX IF NOT EXISTS id ON documents (id)",
        "CREATE INDEX IF NOT EXISTS idrev ON documents (id, rev)",
        "CREATE INDEX IF NOT EXISTS seq ON documents (seq)",
        "CREATE INDEX IF NOT EXISTS deleted ON documents (deleted)",
        "CREATE INDEX IF NOT EXISTS latest ON documents (latest)",
        "CREATE INDEX IF NOT EXISTS latest ON documents (rowid)"
    ],
    ok = esqlite3:exec(Meta, Db),
    ok = esqlite3:exec(Documents, Db),
    lists:foreach(fun (IdxDef) ->
        esqlite3:exec(IdxDef, Db)
    end, DocumentsIndexes),
    % TODO LocalDocuments = "CREATE TABLE local_documents () IF NOT EXISTS"
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
decref(_St) -> ok.
monitored_by(_St) -> [].

last_activity(_St) -> os:timestamp().

get_compacted_seq(_St) ->
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

get_disk_version(_St) ->
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
get_epochs(_St) ->
    couch_log:info("~n> get_epochs()~n", []),
    0.
get_purge_seq(_St) ->
    couch_log:info("~n> get_purge_seq()~n", []),
    0.
get_oldest_purge_seq(_St) ->
    couch_log:info("~n> get_ol, Dbdest_purge_seq()~n", []),
    0.
get_purge_infos_limit(_St) ->
    couch_log:info("~n> get_purge_infos_limit()~n", []),
    999.
get_revs_limit(_St) ->
    couch_log:info("~n> get_revs_limit()~n", []),
    888.
get_security(_St) ->
    couch_log:info("~n> get_security()~n", []),
    [].
get_size_info(_St) ->
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

get_uuid(_St) ->
    couch_log:info("~n> get_uuid()~n", []),
    666.

set_revs_limit(_St, _Val) -> ok.
set_purge_infos_limit(_St, _Val) -> ok.
set_security(_St, _Val) -> ok.

open_docs(Db, DocIds) -> 
    couch_log:info("~n> open_docs(~p)~n", [DocIds]),
    JoinedDocIds = lists:join(",", DocIds),
    Select = "SELECT id, rowid, revtree FROM documents where id IN (?) AND latest=1",
    couch_log:info("~n~nSelect: ~p", [Select]),

    Result = esqlite3:q(Select, [JoinedDocIds], Db),
    couch_log:info("~n~nResult: ~p", [Result]),
    
    lists:map(fun(DocId) ->
        couch_log:info("~n~nDocId: ~p", [DocId]),
        case lists:keyfind(DocId, 1, Result) of
            false -> not_found;
            {_Id, RowId, RevTree} ->
                couch_log:info("~n~nRowId: ~p. ", [RowId]),
                #full_doc_info{
                    id = DocId,
                    update_seq = RowId,
                    deleted = false,
                    rev_tree = binary_to_term(base64:decode(RevTree)),
                    sizes = #size_info{}
                }
        end
    end, DocIds).

open_local_docs(_St, DocIds) -> 
    couch_log:info("~n> open_local_docs(~p)~n", [DocIds]),
    [not_found].
read_doc_body(_St, Doc) ->
    couch_log:info("~n> read_doc_body(~p)~n", [Doc]),
    Doc.
load_purge_infos(_St, UUIDs) -> 
    couch_log:info("~n> load_purge_infos(~p)~n", [UUIDs]),
    ok.

serialize_doc(_St, Doc) -> 
    couch_log:info("~n> serialize_doc(~p)~n", [Doc]),
    Doc.

write_doc_body(Db, #doc{id=Id, revs=Revs}=Doc) ->
    couch_log:info("~n> write_doc_body(~p)~n", [Doc]),
    JsonDoc = couch_util:json_encode(couch_doc:to_json_obj(Doc, [])),
    JsonRevs = ?b2l(couch_doc:rev_to_str(Revs)),
    esqlite3:exec("begin;", Db),

    % set all previous revisions to latest = 0
    {ok, Update} = esqlite3:prepare("UPDATE documents SET latest=0 WHERE id=?1", Db),
    esqlite3:bind(Update, [Id]),
    esqlite3:step(Update),

    % insert new revision, set latest = 1
    couch_log:info("~n> JsonRevs: ~p~n", [JsonRevs]),
    couch_log:info("~n> JsonDoc: ~p~n", [JsonDoc]),
    
    {ok, Insert} = esqlite3:prepare("INSERT INTO documents (id, rev, deleted, latest, body)"
        ++ " VALUES (?1, ?2, ?3, ?4, ?5);", Db),
    esqlite3:bind(Insert, [Id, JsonRevs, 0, 1, JsonDoc]),
    esqlite3:step(Insert),
    esqlite3:exec("commit;", Db), % TODO: maybe move into write_doc_infos
    {ok, Doc, size(JsonDoc)}.
write_doc_infos(Db, Pairs, LocalDocs) -> 
    couch_log:info("~n> write_doc_infos(~p, ~p)~n", [Pairs, LocalDocs]),
    lists:foreach(fun({_OldFDI, NewFDI}) ->
        RevTreeBin = term_to_binary(NewFDI#full_doc_info.rev_tree),
        RevTreeList = base64:encode(?b2l(RevTreeBin)),
        couch_log:info("~n> RevTreeList: ~p~n", [RevTreeList]),
        UpdateSQL = "UPDATE documents SET revtree=?1 WHERE id=?2 AND latest=1",
        {ok, Update} = esqlite3:prepare(UpdateSQL, Db),
        esqlite3:bind(Update, [RevTreeList, NewFDI#full_doc_info.id]),
        esqlite3:step(Update)
    end, Pairs),
    {ok, Db}.
purge_docs(_St, _Pairs, _PurgeInfos) -> 
    couch_log:info("~n> purge_docs()~n", []),
    ok.

commit_data(Db) -> 
    couch_log:info("~n> commit_data()~n", []),
    {ok, Db}.

open_write_stream(_St, Options) -> 
    couch_log:info("~n> open_write_stream(Options)~n", [Options]),
    ok.
open_read_stream(_St, _Stream) -> 
    couch_log:info("~n> open_read_stream()~n", []),
    ok.
is_active_stream(_St, _Stream) -> 
    couch_log:info("~n> is_active_stream()~n", []),
    ok.

fold_docs(_St, _UserFun, UserAcc, Options) ->
    couch_log:info("~n> fold_docs(_, _, _, Options: ~p)~n", [Options]),
    case lists:member(include_reductions, Options) of
        true -> {ok, 0, UserAcc};
        _False -> {ok, UserAcc}
    end.
fold_local_docs(_St, _UserFun, _UserAcc, Options) -> 
    couch_log:info("~n> fold_local_docs(~p)~n", [Options]),
    ok.
fold_changes(_St, _SinceSeq, _UserFun, _UserAcc, Options) -> 
    couch_log:info("~n> fold_changes(~p)~n", [Options]),
    ok.
fold_purge_infos(_St, _StartSeq, _UserFun, _UserAcc, Options) -> 
    couch_log:info("~n> fold_purge_infos(~p)~n", [Options]),
    ok.
count_changes_since(_St, SinceSeq) -> 
    couch_log:info("~n> count_changes_since(~p)~n", [SinceSeq]),
    ok.

start_compaction(_St, DbName, Options, _Parent) -> 
    couch_log:info("~n> start_compaction(DbName: ~p, Options: ~p)~n", [DbName, Options]),
    ok.
finish_compaction(_St, DbName, Options, _CompactFilePath) -> 
    couch_log:info("~n> finish_compaction(DbName: ~p, Options: ~p)~n", [DbName, Options]),
    ok.

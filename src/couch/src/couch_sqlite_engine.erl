% A SQLite Storage Engine for Apache CouchDB™®
%
% (c) 2018-2019 Jan Lehnardt <jan@apache.org>
%     to be relicensed to Neighbourhoodie Software in the future.
%
% # Introduction
%
% This module implements the CouchDB Pluggable Storage Engine API that
% is compatible with CouchDB versions 2.2.0 to 3.x.
%
% Special thanks to Paul Davis for creating that API and for answering
% a few detail questions. And thanks to Russell Branca for answering a
% few detail questions about is lethe implementation of the API.
%
% The goal of this module is to provide an alternative to CouchDB’s
% default B+-Tree-based storage engine that uses less storage space
% when operating. This is mainly advantageous in situations where
% CouchDB is deployed on systems with limited harddrive space like
% point of sales systems or on regular desktop systems where it
% competes with other software for disk resources.
%
% SQLite was chosen for it’s ease-of-use, fit-of-task, as well as its
% robustness and flexibility. No other embedded database combines the
% desirable features in a way that SQLite does.
%
% # Description
%
% The Plugabble Storage Engine (PSE) API for CouchDB defines a common
% set of function calls for persisting databases on disk and making
% that data available to subsequent retrieval while maintaining regular
% CouchDB API semantics. This includes documents, local documents, the
% by-id and by-seq indexes as well as database security and metadata.
%
% A detailed description of that API can be found in CouchDB’s
% `couch_db_engine.erl` file.
%
% A storage engine needs to take care of the following tasks:
%
% - store documents of varying revisions, including local documents
% - retrieve individual documents by their `_id`
% - iterate (fold) over documents in `_id` or `_update_seq` order,
%   including various parameters like start/end-keys, descending order,
%   etc.
% - handle the purging of documents
% - handle folding of purge information
% - manage database metadata like empochs, a last updated timespamp,
%   security, uuid, revs_limit, purge_info_limit, size information etc.
% - manage compaction, if applicable
% - optionally handle attachment storage (this engine does not support
%   attachments yet)
%
% The SQLite Storage Engine for Apache CouchDB uses the SQLite embedded
% database system for the actual data storage and retrieval, it further
% uses the Erlang module esqlite which is a NIF wrapper around SQLite’s
% C-Library to make it accessible to Erlang code.
%
% # Implementation
%
% This storage engine uses a single SQLite database file per CouchDB
% database. It uses the following set of SQL tables inside a database
% to represent the CouchDB data model:
%
% - `meta`: stores all sorts of metadata
% - `epochs`: store the epoch of a database (TODO: explain why this is
%   not in meta)
% - `purges`: stores all purge information up to the `purge_info_limit`
% - `documents`: stores all document data except for document bodies and
%   `attachments`
% - `doc_bodies`: stores all document bodies, keyed by id and revision
% - `local_docs`: like `documents`, but stores all local doc info,
%   including bodies.
%
% The by-id and by-seq indexes are realised as indexes on the
% `documents` database.
%
% ## Document Storage
%
% This section explains in detail how documents are stored and retrieved
% across the various associated tables.
%
% Document information except bodies and attachments is stored in the
% `documents` table. Each document revision is represented by a row in
% the table. In order to enforce this, there is a `UNIQUE` constraint
% on `(id, rev)`. More specifically, each row in the `documets` table
% represents one `update_seq` in CouchDB-terms. In order to represent
% the update sequence, the `seq` column is defined as `INTEGER PRIMARY
% KEY AUTOINCREMENT` which has the effect of an ever increasing integer
% value for each new row in the table. Without the `AUTOINCREMENT`
% keyword, if the last inserted row were to be deleted, the next row
% would get the previous row’s id assigned. We can’t have that for
% update dequences.
%
% // TODO: max rows (row count or int size)
%
%
% ### Writing a Document
%
% Writing a document is a two-step process. First a document revision
% body is written to the storage engine (`write_doc_body()`) and then
% at a later time, the information about that document revision is
% stored (`write_doc_infos()`).
%
% Writing document bodies is straightforward: a new entry is made in
% the `doc_bodies` table, with a UNIQUE key on `(id, rev)`, so each
% document revision is only stored *once and only once* and can be
% *found efficiently*.
%
% Writing doc infos is a little bit more complicated, because of the
% nature of the API (see the API documentation for details), where we
% have to handle a list of pairs of previous document revision and new
% document revision, where a `not_found` atom signifies that there is
% no previous revision and a new document should be created.
%
% A row in the `documents` table represents one infividual update
% sequence. In CouchDB terms, this is either a document creation, a
% document update, a document delete, or a purge of one or more
% documents.
%
% Each document revision is represented by *a single row* in the
% `documents` table. That means each document is be represented
% multiple times, once per revision. We keep track of whether a doc is
% the lastest revision in the `latest` column of the `documents` table.
%
% A single row stores all document information, includding the revision
% tree (or `revtree`) which also holds all information about conflicts.
%
% ### Updating a Document
%
% Similarly to “Writing a Document”, a document body is written into
% the `doc_bodies` table. But before we write the new row for the
% incoming document revision, we set `latest` to `0` (`false`) for the
% previous revision. Then we write the regular row for the new document
% revision. Both of these operations happen in separate SQL statements,
% but they are bracketed by a transaction, so we can guarantee that the
% writing of a document is happening *atomically*.
%
% ### Deleting a Document
%
% Same as with updating , but the `deleted` flag is set on the new
% document revision info. Since we need to keep deleted documents
% around but we need to differentiate between deleted and non-deleted
% documents in by-id and by-changes, we track the deleted info in a
% separate field in the `documents` table.
%
% ### Purging a Document
%
% The operation of purging a documents requires the storage engine to
% do two things:
%
% 1. expunge any data corresponding to a document from storage
% 2. record that the document id/rev combination was purged
%
% The effect of a purge is pretending a document has never been written
% to the database in the first place. Recording a history of purge infos
% allow a cluster to apply purges across all shards of a database in a
% way that replication doesn’t re-create those document revisions.
%
% Purge is implemented by setting all values of a document’s row in the
% `documents` table to `NULL`. (TODO: why not delete directly?)
%
% In addition, the purge needs to increment the CouchDB database’s
% `update_seq`, so we insert a row for that with all `NULL` values, so
% the `seq` can increment by one.
%
% ### Reading a Document
%
% Reading a document is a two-step process, just like writing one. First
% `open_docs()` is called with a list of none or more document ids and
% the function asks the by-id index for information about those document
% ids and it returns a list of `#full_doc_infos`, one per id. CouchDB
% then later reads the associated document body for one of the
% `#full_doc_infos` in `read_doc_body()` that works analogous to
% `write_doc_body()` and reads a specific document body/revision
% combination.
%
% ## Compaction
%
% Compaction instructs the storage engine to remove excess data. It is
% essential in the CouchDB’s storage engine, and this storage engine
% also makes use of it.
%
% 1. SQLite has a VACUUM command that rids a SQLite database of any
% internal-to-SQLite excess data by producting a completely new file on
% disk and swapping the old and new files atomically, very similar to
% the default storage engine.
%
% 2. This storage engine uses additinoal excess data during normal
% operation that can be pruned during compaction:
%
%   1. To increment the update sequence to represent the storage of a
%   purge info, we introduce NULL rows. Those rows can now be removed.
%
%   2. Document bodies stored in the `doc_bodies` table that represent
%   revisions that are not the latest revision and not conflicting
%   revisions can be removed. TBD on how to parse revtree to find
%   non-prphan bodies.
%
%   3. Delete all but the latest `purge_info_limit` entries in the
%   `purges` table.
%
% ### Folding over Documents
%
% by-id: latest=1 && deleted = 0
% by-seq: latest=1
%
% ## Purging
% ## Metadata
%
% ## Future Features
% - document deconstruction
%   - SQL queries?
% - doc body deduplication across multiple databases
%   - needs ATTACH and refcounting
%
%
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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
  last_activity = 0,
  monitor = nil,
  pid = nil,
  att_fd = nil
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
% strip out _id and _rev from doc body
% compaction: prune rev trees
% enforce revs_limit
% write doc infos: prune rev infos from rev_tree
% rename documents table to docs


exists(FilePath) ->
    couch_log:info("~n> exists(FilePath: ~p)~n", [FilePath]),
    filelib:is_file(FilePath).
    

delete(RootDir, FilePath, _Async) -> % TODO: windows
    % file:delete(FilePath),
    case couch_file:delete(RootDir, db_path_to_att_path(FilePath)) of
        {error, Reason} ->
            case Reason of
                enoent -> ok;
                _ -> throw(Reason)
            end;
        ok -> ok
    end.
% delete(_RootDir, FilePath, _Async) -> ok. % uncomment to debug sqlite file after test run

delete_compaction_files(_RootDir, _FilePath, _DelOpts) -> ok.

init(FilePath, _Options) ->
    couch_log:info("~n> init(FilePath: ~p)~n", [FilePath]),
    ok = filelib:ensure_dir(FilePath),
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
        ++ "seq INTEGER PRIMARY KEY AUTOINCREMENT, "
        ++ "id TEXT, "
        ++ "rev TEXT, "
        ++ "revtree TEXT, "
        ++ "latest INT DEFAULT 0, "
        ++ "deleted INT DEFAULT 0, "
        ++ "purged INT DEFAULT 0, "
        ++ "UNIQUE(id, rev))",
    DocumentsIndexes = [
        "CREATE INDEX IF NOT EXISTS id ON documents (id)",
        "CREATE INDEX IF NOT EXISTS idrev ON documents (id, rev)",
        % "CREATE INDEX IF NOT EXISTS seq ON documents (seq)",
        "CREATE INDEX IF NOT EXISTS latest ON documents (latest)",
        "CREATE INDEX IF NOT EXISTS deleted ON documents (deleted)",
        "CREATE INDEX IF NOT EXISTS purged ON documents (purged)"
        % "CREATE INDEX IF NOT EXISTS rowid ON documents (rowid)"
    ],
    DocBodies = "CREATE TABLE IF NOT EXISTS doc_bodies ("
        ++ "id TEXT, "
        ++ "rev TEXT, "
        ++ "body BLOB, "
        ++ "UNIQUE(id, rev))",
    DocBodyIndexes = [
        "CREATE INDEX IF NOT EXISTS idrev ON documents (id, rev)"
    ],
    LocalDocuments = "CREATE TABLE IF NOT EXISTS local_docs ("
        ++ "seq INTEGER PRIMARY KEY AUTOINCREMENT, "
        ++ "id TEXT, "
        ++ "rev TEXT, "
        ++ "revtree TEXT, "
        ++ "deleted INT DEFAULT 0, "
        ++ "purged INT DEFAULT 0, "
        ++ "body BLOB, "
        ++ "UNIQUE(id, rev))",
    LocalDocumentsIndexes = [
        "CREATE INDEX IF NOT EXISTS id ON local_docs (id)",
        "CREATE INDEX IF NOT EXISTS idrev ON local_docs (id, rev)",
        % "CREATE INDEX IF NOT EXISTS seq ON local_docs (seq)",
        "CREATE INDEX IF NOT EXISTS deleted ON local_docs (deleted)",
        "CREATE INDEX IF NOT EXISTS purged ON local_docs (purged)"
        % "CREATE INDEX IF NOT EXISTS rowid ON local_docs (rowid)"
    ],
    ok = esqlite3:exec(Meta, Db),
    ok = esqlite3:exec(Epochs, Db),
    ok = esqlite3:exec(Purges, Db),
    ok = esqlite3:exec(Documents, Db),
    ok = esqlite3:exec(DocBodies, Db),
    ok = esqlite3:exec(LocalDocuments, Db),
    lists:foreach(fun (IdxDef) ->
        ok = esqlite3:exec(IdxDef, Db)
    end, DocumentsIndexes ++ DocBodyIndexes ++ LocalDocumentsIndexes ++ PurgeIndexes),
    % TODO Attachments = "CREATE TABLE attachments ()"
    State = init_state(Db, FilePath),
    case load_meta(uuid, Db) of
        undefined -> init_uuid(Db);
        _Else -> ok
    end,
    {ok, State}.

db_path_to_att_path(FilePath) ->
    DbName = filename:basename(FilePath, ".sqlite"),
    DbPath = filename:dirname(FilePath),
    AttFileName = DbName ++ ".att",
    AttPath = filename:join(DbPath, AttFileName),
    AttPath.

init_attachment_file(FilePath) ->
    AttPath = db_path_to_att_path(FilePath),
    Options = case filelib:is_file(AttPath) of
        false -> [create, read, append];
        _ -> [read, append]
    end,
    {ok, AttFile} = couch_file:open(AttPath, Options),
    AttFile.

init_state(Db, FilePath) ->
    ok = init_security(Db),
    {UpateEpochs, Epochs} = init_epochs(Db),
    #sqldb {
       db = Db,
       update_epochs = UpateEpochs,
       epochs = Epochs,
       last_activity = os:timestamp(),
       pid = self(),
       monitor = nil,
       att_fd = init_attachment_file(FilePath)
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
    '$done' = esqlite3:step(Insert).%

maybe_update_epochs(#sqldb{ update_epochs = false} = State) -> State;
maybe_update_epochs(#sqldb{ db = Db, epochs = [{Node, Seq} | _RestEpochs]} = State) ->
    couch_log:info("~n> maybe_update_epochs(Node: ~p, Seq: ~p)~n", [Node, Seq]),
    SQL = "INSERT INTO epochs (node, seq) VALUES (?1, ?2)",
    {ok, Insert} = esqlite3:prepare(SQL, Db),
    ok = esqlite3:bind(Insert, [Node, Seq]),
    '$done' = esqlite3:step(Insert),
    State#sqldb{update_epochs = false}.

terminate(Reason, #sqldb{db = Db, att_fd = Fd}) ->
    couch_log:info("~n> terminate(Reason: ~p)~n", [Reason]),
    ok = esqlite3:close(Db),
    ok = file:close(Fd).
    
handle_db_updater_call(Msg, State) ->
    couch_log:info("~n> handle_db_updater_call(~p) ~n", [Msg]),
    {reply, ok, State}.
handle_db_updater_info(Msg, State) ->
    couch_log:info("~n> handle_db_updater_info(~p) ~n", [Msg]),
    {noreply, State}.

incref(State) ->
    {ok, State#sqldb{monitor = erlang:monitor(process, State#sqldb.pid)}}.


decref(State) ->
    true = erlang:demonitor(State#sqldb.monitor, [flush]),
    ok.


monitored_by(State) ->
    case erlang:process_info(State#sqldb.pid, monitored_by) of
        {monitored_by, Pids} ->
            lists:filter(fun is_pid/1, Pids);
        _ ->
            []
    end.


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
        [{Value}] -> parse_meta(Key, Value)
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

parse_meta("revs_limit", Value) -> list_to_integer(binary_to_list(Value));
parse_meta("purge_infos_limit", Value) -> list_to_integer(binary_to_list(Value));
parse_meta(_, Value) -> Value.

get_compacted_seq(#sqldb{db = Db}) ->
    couch_log:info("~n> get_compacted_seq()~n", []),
    load_meta(compacted_seq, 0, Db).

get_del_doc_count(#sqldb{db = Db}) ->
    couch_log:info("~n> get_del_doc_count()~n", []),
    SQL = "SELECT COUNT(*) FROM documents WHERE deleted=1 AND purged=0",
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
    SQL = "SELECT COUNT(*) FROM documents WHERE deleted=0 AND purged=0 AND latest=1",
    case esqlite3:q(SQL, Db) of
        [] -> 0;
        [{DocCount}] ->
            couch_log:info("~n< get_doc_count() -> ~p~n", [DocCount]),
            DocCount
    end.
get_epochs(#sqldb{db = Db}) ->
    couch_log:info("~n> get_epochs()~n", []),
    SQL = "SELECT MAX(seq) FROM documents WHERE deleted=0",
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
    load_meta(purge_infos_limit, 10, Db).

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
    SQL = "SELECT MAX(seq) FROM documents;",
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


chunk_collect_query([], _, Acc) -> lists:reverse(lists:flatten(Acc)); % return it all
chunk_collect_query(List, Fun, Acc) when length(List) >= 999 ->
    {Chunk, Rest} = lists:split(999, List),
    chunk_collect_query(Rest, Fun, [Fun(Chunk) | Acc]);
chunk_collect_query(LastChunk, Fun, Acc) ->
    chunk_collect_query([], Fun, [Fun(LastChunk) | Acc]).
        
open_docs(#sqldb{}, []) -> [];
open_docs(#sqldb{db = Db}, DocIds) ->
    couch_log:info("~n> open_docs(~p)~n", [DocIds]),
    % TODO: loop over chunks of 999
    SQLFun = fun(DocIdsChunk) ->
        Qs = make_qs(length(DocIdsChunk)),
        JoinedDocIds = lists:map(fun binary_to_list/1, DocIdsChunk),
        SQL = "SELECT id, seq, revtree FROM documents WHERE latest=1 AND id IN (" ++ Qs++ ")",
        couch_log:info("~n~nSQL: ~p, ~p,", [SQL, JoinedDocIds]),

        Result = esqlite3:q(SQL, JoinedDocIds, Db),
        couch_log:info("~n~nResult: ~p", [Result]),
        Result
    end,

    Result = chunk_collect_query(DocIds, SQLFun, []),

    R = lists:map(fun(DocId) ->
        % couch_log:info("~n~nDocId: ~p", [DocId]),
        case lists:keyfind(DocId, 1, Result) of
            false -> not_found;
            {DocId, Seq, RevTree0} ->
                RevTree = case RevTree0 of
                    undefined -> [];
                    _Else -> binary_to_term(base64:decode(RevTree0))
                end,
                #full_doc_info{
                    id = DocId,
                    update_seq = Seq,
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
    SQLFun = fun(DocIdsChunk) ->
        JoinedDocIds = lists:join(",", DocIdsChunk),
        SQL = "SELECT id, rev, body FROM local_docs WHERE id IN (?)",
        couch_log:info("~n~nSQL: ~p, ~p", [SQL, DocIdsChunk]),

        ResultChunk = esqlite3:q(SQL, [JoinedDocIds], Db),
        couch_log:info("~n~nResult: ~p", [ResultChunk]),
        ResultChunk
    end,

    Result = chunk_collect_query(DocIds, SQLFun, []),

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
    SQL = "SELECT body FROM doc_bodies WHERE id=?1 AND rev=?2",
    SQLResult = esqlite3:q(SQL, [Id, JsonRev], Db),
    couch_log:info("~n> SQL: ~p, ?1: ~p ?2: ~p SQLResult: ~p~n", [SQL, Id, JsonRev, SQLResult]),
    Result = case SQLResult of
        [] -> not_found;
        [{Body}] -> Body
    end,
    % {Deleted, Result} = case SQLResult of
    %     [] -> {false, not_found};
    %     [{Body, Deleted0}] -> {Deleted0, Body}
    % end,
    {BodyList} = case Result of
        not_found -> {[]};
        _ -> couch_util:json_decode(Result)
    end,
    FilterIdAndRev = fun({Key, _Value}) -> Key =:= <<"_id">> orelse Key =:= <<"_rev">> end,
    BodyTerm = {lists:dropwhile(FilterIdAndRev, BodyList)},
    % DeletedBool = case Deleted of
    %     1 -> true;
    %     _Zero -> false
    % end,
    % Doc#doc{body = BodyTerm, deleted = DeletedBool}.
    Doc#doc{body = BodyTerm}.
load_purge_infos(#sqldb{db = Db}, UUIDs) ->
    couch_log:info("~n> load_purge_infos(~p)~n", [UUIDs]),
    lists:map(fun (UUID) -> load_purge_info(Db, UUID) end, UUIDs).

load_purge_info(Db, UUID) ->
    couch_log:info("~n> load_purge_info(~p)~n", [UUID]),
    SQL = "SELECT purge_seq, uuid, docid, rev FROM purges WHERE uuid = ?1",
    case esqlite3:q(SQL, [UUID], Db) of
        [] ->
            not_found;
        Result ->
            couch_log:info("~n>  Result: ~p~n", [Result]),
            lists:foldl(fun({PurgeSeq, RowUUID, DocId, Rev}, {_PurgeSeq, _DocId, Revs}) ->
                {PurgeSeq, RowUUID, DocId, [Rev|Revs]}
            end, {null, null, []}, Result)
    end.

% TODO: add mode via config var where docs are exploded, it is likely slower, but would allow sql queries
% also json1 extension in sqlite
serialize_doc(_Db, Doc) ->
    couch_log:info("~n> serialize_doc(~p)~n", [Doc]),
    Doc.

write_doc_body(#sqldb{db = Db}, #doc{id=Id, revs={Start, RevIds}, body=Body}=Doc) ->
    couch_log:info("~n> write_doc_body(~p)~n", [Doc]),
    JsonDoc = couch_util:json_encode(Body),
    [{_, JsonRevs}] = couch_doc:to_json_rev(Start, RevIds),

    couch_log:info("~n> JsonRevs: ~p~n", [JsonRevs]),
    couch_log:info("~n> JsonDoc: ~p~n", [JsonDoc]),
    SQL = "INSERT INTO doc_bodies (id, rev, body)" % TODO: test if I can write a deleted doc here
        ++ " VALUES (?1, ?2, ?3)",
    {ok, Insert} = esqlite3:prepare(SQL, Db),
    Bind = esqlite3:bind(Insert, [Id, JsonRevs, JsonDoc]),
    Step = esqlite3:step(Insert),
    couch_log:info("~n> SQL: ~p, Bind, ~p, Step: ~p~n", [SQL, Bind, Step]),
    % ok = esqlite3:exec("commit;", Db),
    {ok, Doc#doc{body=JsonDoc}, size(JsonDoc)}.

fdi_to_json_revs(#full_doc_info{} = FDI) ->
    #doc_info{revs=Revs} = couch_doc:to_doc_info(FDI),
    fdi_to_json_revs(Revs, []).
fdi_to_json_revs([], Acc) -> lists:reverse(Acc);
fdi_to_json_revs([#rev_info{rev={Start, Rev}}|Rest], Acc) ->
    [{_, JsonRev}] = couch_doc:to_json_rev(Start, [Rev]),
    fdi_to_json_revs(Rest, [JsonRev|Acc]).

fdi_to_json_rev(FDI) ->
    #doc_info{revs=Revs} = couch_doc:to_doc_info(FDI),
    #rev_info{rev={Start, Rev}} = lists:nth(1, Revs),
    [{_, JsonRev}] = couch_doc:to_json_rev(Start, [Rev]),
    JsonRev.

insert_new_rev(NewFDI, Db) ->
    RevTreeBin = term_to_binary(NewFDI#full_doc_info.rev_tree),
    RevTreeList = base64:encode(?b2l(RevTreeBin)),
    couch_log:info("~n> RevTree: ~p~n", [NewFDI#full_doc_info.rev_tree]),
    Deleted = case NewFDI#full_doc_info.deleted of
        true -> 1;
        _False -> 0
    end,
    JsonRev = fdi_to_json_rev(NewFDI),
    couch_log:info("~n>write_doc_infos(create|update) JsonRev: ~p~n", [JsonRev]),
    SQL = "INSERT INTO documents (id, rev, revtree, latest, deleted) VALUES (?1, ?2, ?3, 1, ?4) "
    ++ "ON CONFLICT (id, rev) DO UPDATE SET revtree=?3, latest=1, deleted=?4 WHERE id=?1 AND rev=?2",
    couch_log:info("~n>write_doc_infos() SQL: ~p, ?1~p, ?2~p, ?3~p, ?4~p ~n", [SQL, NewFDI#full_doc_info.id, JsonRev, RevTreeList, Deleted]),
    {ok, Update} = esqlite3:prepare(SQL, Db),
    ok = esqlite3:bind(Update, [NewFDI#full_doc_info.id, JsonRev, RevTreeList, Deleted]),
    '$done' = esqlite3:step(Update).

maybe_un_latest_old_fdi(not_found, _Db) -> ok;
maybe_un_latest_old_fdi(OldFDI, Db) ->
    OldRev = fdi_to_json_revs(OldFDI),
    OldSQL = "UPDATE documents SET latest=0 WHERE id=?1 AND rev=?2",
    {ok, OldUpdate} = esqlite3:prepare(OldSQL, Db),
    ok = esqlite3:bind(OldUpdate, [OldFDI#full_doc_info.id, OldRev]),
    '$done' = esqlite3:step(OldUpdate).

write_doc_infos(#sqldb{db = Db} = State, Pairs, LocalDocs) ->
    couch_log:info("~n> write_doc_infos(~p, ~p)~n", [Pairs, LocalDocs]),
    ok = esqlite3:exec("begin;", Db),
    lists:foreach(fun
        ({OldFDI, NewFDI}) ->
            maybe_un_latest_old_fdi(OldFDI, Db),
            insert_new_rev(NewFDI, Db)            
    end, Pairs),
    ok = esqlite3:exec("commit;", Db),

    lists:foreach(fun(#doc{id=Id,revs={Start, [Idx]}}=LocalDoc0) ->
        LocalDoc = LocalDoc0#doc{revs={Start, [integer_to_list(Idx)]}},
        JsonRevs = ?l2b([integer_to_list(Start), "-", Idx+48]),
        couch_log:info("~n> LocalDoc: ~p, JsonRevs: ~p~n", [LocalDoc, JsonRevs]),
        JsonDoc = couch_util:json_encode(couch_doc:to_json_obj(LocalDoc, [])),
        couch_log:info("~n> JsonDoc: ~p~n", [JsonDoc]),
        SQL = "INSERT INTO local_docs (id, rev, body, deleted) VALUES (?1, ?2, ?3, 0)",
        couch_log:info("~n> SQL: ~p~n", [SQL]),
        {ok, Upsert} = esqlite3:prepare(SQL, Db),
        ok = esqlite3:bind(Upsert, [Id, JsonRevs, JsonDoc]),
        '$done' = esqlite3:step(Upsert)
    end, LocalDocs),
    NewState = maybe_update_epochs(State),
    {ok, NewState}.

db_for_doc_id(<<"_local/",_/binary>>) -> "local_docs";
db_for_doc_id(_) -> "documents".
    

purge_docs(#sqldb{db = Db}=State, Pairs, PurgeInfos) ->
    couch_log:info("~n> purge_docs(PurgeInfos: ~p) Pairs: ~p~n", [PurgeInfos, Pairs]),
    ok = esqlite3:exec("begin;", Db),
    lists:foreach(fun
        ({not_found, not_found}) ->
            ok;
        ({OldFDI, not_found}) ->
            DocId = OldFDI#full_doc_info.id,
            Rev = fdi_to_json_rev(OldFDI),
            Database = db_for_doc_id(DocId),
            SQL = "UPDATE " ++ Database ++ " SET id=NULL, rev=NULL, revtree=NULL, deleted=NULL, purged=1 WHERE id=?1 AND rev=?2",
            SQL2 = "UPDATE doc_bodies SET id=NULL, rev=NULL, body=NULL WHERE id=?1 AND rev=?2",
            couch_log:info("~n> purge_docs() SQL: ~p ~p id=~p rev=~p~n", [SQL, SQL2, DocId, Rev]),

            {ok, Update} = esqlite3:prepare(SQL, Db),
            ok = esqlite3:bind(Update, [DocId, Rev]),
            '$done' = esqlite3:step(Update),

            {ok, Update2} = esqlite3:prepare(SQL2, Db),
            ok = esqlite3:bind(Update2, [DocId, Rev]),
            '$done' = esqlite3:step(Update2);
        ({_OldFDI, NewFDI}) ->
            DocId = NewFDI#full_doc_info.id,
            Database = db_for_doc_id(DocId),
            RevTreeBin = term_to_binary(NewFDI#full_doc_info.rev_tree),
            RevTreeList = base64:encode(?b2l(RevTreeBin)),
            JsonRev = fdi_to_json_rev(NewFDI),
            couch_log:info("~n> RevTree: ~p and JsonRev: ~p~n", [NewFDI#full_doc_info.rev_tree, JsonRev]),
            SQL = "UPDATE " ++ Database ++ " SET revtree=?1 WHERE id=?2 AND rev=?3",
            {ok, Update} = esqlite3:prepare(SQL, Db),
            ok = esqlite3:bind(Update, [RevTreeList, DocId, JsonRev]),
            '$done' = esqlite3:step(Update)
    end, Pairs),
    lists:foreach(fun (PurgeInfo) -> write_purge_info(Db, PurgeInfo) end, PurgeInfos),
    SQL1 = "INSERT INTO documents (id, rev, revtree, deleted, purged) values (NULL, NULL, NULL, NULL, 1)",
    couch_log:info("~n> purge_docs() SQL1: ~p~n", [SQL1]),
    ok = esqlite3:exec(SQL1, Db),
    ok = esqlite3:exec("commit;", Db),
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

commit_data(#sqldb{att_fd = Fd}=State) ->
    couch_log:info("~n> commit_data()~n", []),
    ok = couch_file:sync(Fd),
    {ok, State};
commit_data(State) ->
    {ok, State}.
    

open_write_stream(#sqldb{} = St, Options) ->
    couch_stream:open({couch_bt_engine_stream, {St#sqldb.att_fd, []}}, Options).

open_read_stream(#sqldb{} = St, StreamSt) ->
    {ok, {couch_bt_engine_stream, {St#sqldb.att_fd, StreamSt}}}.

is_active_stream(#sqldb{} = St, {couch_bt_engine_stream, {Fd, _}}) ->
    St#sqldb.att_fd == Fd;
is_active_stream(_, _) ->
    false.

% open_write_stream(_Db, Options) ->
%     couch_log:info("~n> open_write_stream(Options: ~p)~n", [Options]),
%     throw(not_supported).
% open_read_stream(_Db, _Stream) ->
%     couch_log:info("~n> open_read_stream()~n", []),
%     throw(not_supported).
% is_active_stream(_Db, _Stream) ->
%     couch_log:info("~n> is_active_stream()~n", []),
%     false.

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
    IgnoreDeleted = case Type of
        changes -> "";
        _ -> "AND deleted = 0"
    end,
    SQL0 = case Type of
        local -> "SELECT id, seq, revtree, rev, body FROM local_docs WHERE purged=0 " ++ IgnoreDeleted;
        _Global -> "SELECT id, seq, revtree, rev FROM documents WHERE purged=0 AND latest=1 " ++ IgnoreDeleted
    end,

    AdditionalWhere = case Type of
        changes -> changes_options_to_sql(Options, Type);
        _ -> options_to_sql(Options, Type)
    end,
    SQL1 = SQL0 ++ AdditionalWhere,
    Order = options_to_order_sql(Options, Type),
    SQL = SQL1 ++ Order,
    couch_log:info("~n> fold_docs() SQL: ~p~n", [SQL]),
    Result = esqlite3:q(SQL, Db),
    couch_log:info("~n> fold_docs() Result: ~p~n", [Result]),
    FinalNewUserAcc = try
        lists:foldl(fun(Args, Acc) -> % TODO: fold local docs separately
            Id = element(1, Args),
            Seq = element(2, Args),
            RevTree0 = element(3, Args),
            Rev = element(4, Args),

            RevTree = case RevTree0 of
                undefined -> [];
                % TODO: hackety hack: split out into separate docs
                _Else -> binary_to_term(base64:decode(RevTree0))
            end,
            FDI = case Type of
                local ->
                    Body = element(5, Args),
                    [Pos, RevId] = string:split(?b2l(Rev), "-"),
                    #doc{
                        id = Id,
                        revs = {list_to_integer(Pos), [RevId]},
                        body = couch_util:json_decode(Body)
                    };
                _Global1 ->
                    #full_doc_info{
                        id = Id,
                        rev_tree = RevTree,
                        deleted = false,
                        update_seq = Seq,
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
    Options = [{start_key, SinceSeq + 1} | Options0],
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
                PurgeSeq -> [{LastPurgeSeq, LastUUID, LastDocId, LastRevs ++ [rev_to_binary(Rev)]} | RestAcc]; % keep collecting
                _ -> [{PurgeSeq, UUID, DocId, [rev_to_binary(Rev)]}] ++ [LastPurgeInfo] ++ RestAcc % start collecting revs for this docId
            end
    end, [], Result),
    couch_log:info("~n> fold_purge_infos() GroupedPurgeInfos: ~p ~n", [GroupedPurgeInfos]),
    FinalResult = try 
        lists:foldl(fun(PurgeInfo, InUserAcc) ->
            case UserFun(PurgeInfo, InUserAcc) of
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
    SQL = "SELECT COUNT(*) FROM documents WHERE latest=1",
    [{ChangesSince}] = esqlite3:q(SQL, Db),
    couch_log:info("~n> count_changes_since() -> ChangesSince: ~p~n", [ChangesSince]),
    ChangesSince - SinceSeq.

sql_exec(SQL, Db) ->
    ok = esqlite3:exec(SQL, Db).

start_compaction(#sqldb{db = Db} = State, DbName, Options, Parent) ->
    couch_log:info("~n> start_compaction(DbName: ~p, Options: ~p)~n", [DbName, Options]),
    Pid = spawn_link(fun() ->
        % TODO: delete old doc bodies (DELETE FROM doc_bodies WHERE (id, rev) IN (SELECT id FROM documents UNION SELECT rev FROM documents))
        PurgeInfosLimit = integer_to_list(get_purge_infos_limit(State)),
        sql_exec("BEGIN", Db),
        sql_exec("DELETE FROM documents WHERE id = NULL;", Db),
        sql_exec("DELETE FROM purges WHERE purge_seq NOT IN (SELECT purge_seq FROM purges ORDER BY purge_seq DESC LIMIT " ++ PurgeInfosLimit ++ ")", Db), % TODO use parameter
        sql_exec("COMMIT", Db),
        sql_exec("VACUUM", Db),
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
    Since = proplists:get_value(start_key, Options),
    " AND seq " ++ Cmp ++ " " ++ integer_to_list(Since).
    % " AND seq >= " ++ integer_to_list(Since).

options_to_sql([], _Type) -> "";
options_to_sql([{dir,_}], _Type) -> ""; % shortcut, maybe better parse options into easier to build-from structure
options_to_sql(Options, Type) ->
    Dir = proplists:get_value(dir, Options, fwd),
    StartKey = get_start_key(Dir, Options),
    EndKey = get_end_key(Dir, Options),
    HasEndKeyGt = proplists:lookup(end_key_gt, Options) /= none,
    case Type of
        changes ->
            " AND seq >= '" ++ ?b2l(StartKey) ++ "'";
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

parse_order(fwd, changes) -> " ORDER BY seq ASC";
parse_order(rev, changes) -> " ORDER BY seq DESC";
parse_order(fwd, _) -> " ORDER BY id ASC";
parse_order(rev, _) -> " ORDER BY id DESC".

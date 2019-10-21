%%
%% String and Id mapping server.
%%
%% @copyright 2013-2019 UP FAMNIT and Yahoo Japan Corporation
%% @version 0.3
%% @since August, 2013
%% @author Kiyoshi Nitta <knitta@yahoo-corp.jp>
%% 
%% @see b3s
%% @see triple_distributor
%% @see node_state
%% @see query_node
%% 
%% @doc A server for mapping URI strings and integer ids. This module
%% is implemented as an erlang <A
%% href="http://www.erlang.org/doc/man/gen_server.html">gen_server</A>
%% process.
%%
%% Implementation method of the string-id mapping table can be changed
%% by constant variable DB_IMPLEMENTATION that is defined in
%% "record.hrl". If the constant has value 'epgsql', the table will be
%% built on a PostgreSQL server. This option is highly recommended for
%% storing large-scale triple dataset. In this option, this module
%% uses server-side functions defined in "../plpgsql/encode.plpgsql".
%% 
%% == handle_call (synchronous) message API ==
%% 
%% (LINK: {@section handle_call (synchronous) message API})
%% 
%% === {create_table, TableName} ===
%% 
%% Creats a string-id mapping table which name is
%% TableName::atom(). It returns {created, TabName::atom()}, if the
%% table was successfully created. Otherwise, it returns {not_created,
%% TabName::atom(), Reason::term()}. This request is implemented by
%% {@link hc_create_table/2}. (LINK: {@section @{create_table,
%% TableName@}})
%% 
%% === {load_table, TableName} ===
%% 
%% Loads a string-id mapping table which name is TableName::atom(). It
%% returns {reloaded, TabName::atom()}, if the table was successfully
%% found and loaded. It tries to create a new table, if the table does
%% not exist. It returns {created, TabName::atom()}, if the table was
%% successfully created. Otherwise, it returns {not_created,
%% TabName::atom(), Reason::term()}. This request is implemented by
%% {@link hc_load_table/2}. (LINK: {@section @{create_table,
%% TableName@}})
%% 
%% === make_index ===
%% 
%% Makes an index for the string-id mapping table. The table must be
%% created by a {@section @{create_table, TableName@}} request or
%% loaded by a {@section @{load_table, TableName@}} request. It
%% returns {index_built, TabName::atom()}, if the index was
%% successfully built. Otherwise, it returns {index_not_built,
%% Reason::term()}. This request is implemented by {@link
%% hc_make_index/1}. (LINK: {@section make_index})
%% 
%% === delete_table ===
%% 
%% Deletes the string-id mapping table. The table must be created by a
%% {@section @{create_table, TableName@}} request or loaded by a
%% {@section @{load_table, TableName@}} request. It returns {deleted,
%% TabName::atom()}, if the table was successfully deleted. Otherwise,
%% it returns {not_deleted, Reason::term()}. This request is
%% implemented by {@link hc_delete_table/1}. (LINK: {@section
%% delete_table})
%% 
%% === {append, StrDat} ===
%% 
%% Appends a string data StrDat::string() to the string-id table. The
%% table must be created by a {@section @{create_table, TableName@}}
%% request or loaded by a {@section @{load_table, TableName@}}
%% request. It returns {appended, {Id::integer(), StrDat::string()},
%% if the string was successfully appended to the table. This request
%% is implemented by {@link hc_append/2}. (LINK: {@section @{append,
%% StrDat@}})
%% 
%% === {delete, Id} ===
%% 
%% Deletes a data identified by Id::integer(). The table must be
%% created by a {@section @{create_table, TableName@}} request or
%% loaded by a {@section @{load_table, TableName@}} request. The data
%% must be registered to the table using a {@section @{append,
%% StrDat@}} or {@section @{get_id, StrDat@}} request. It returns
%% {deleted, {TabName::atom(), Id::integer()}}, if the data is
%% successfully deleted. Otherwise, it returns {not_deleted,
%% Reason::term()}. This request is implemented by {@link
%% hc_delete/2}. (LINK: {@section @{delete, Id@}})
%% 
%% === {find, Data} ===
%% 
%% Finds corresponding id or string from Data::integer() |
%% string(). The table must be created by a {@section @{create_table,
%% TableName@}} request or loaded by a {@section @{load_table,
%% TableName@}} request. The table must have index built by a
%% {@section make_index} request. It returns {found,
%% StrDat::string()}, if Data is integer() and the data is
%% successfully found. It returns {found, Id::integer()}, if Data is
%% string() and the data is successfully found. Otherwise, it returns
%% {not_found, Data::integer() | string()}. This request is
%% implemented by {@link hc_find/2}. (LINK: {@section @{find, Data@}})
%% 
%% === {get_id, StrDat} ===
%% 
%% Finds or appends StrDat::string() data. Basically, it finds
%% corresponding id from the table. It automatically appends the data,
%% if it has not been registered to the table. The table must be
%% created by a {@section @{create_table, TableName@}} request or
%% loaded by a {@section @{load_table, TableName@}} request. The table
%% must have index built by a {@section make_index} request. It always
%% returns {id, Id::integer()}. This request is implemented by {@link
%% hc_get_id/2}.  (LINK: {@section @{get_id, StrDat@}})
%% 
%% === get_size ===
%% 
%% Returns the Size::integer() of the string-id mapping table. The
%% table must be created by a {@section @{create_table, TableName@}}
%% request or loaded by a {@section @{load_table, TableName@}}
%% request. This request is implemented by {@link hc_get_size/1}.
%% (LINK: {@section get_size})
%% 
%% == handle_cast (asynchronous) message API ==
%% 
%% (LINK: {@section handle_cast (asynchronous) message API})
%% 
%% === {stream, Command, Triple, DestProc, NotifyProc} ===
%% 
%% Processes a store stream element. It converts Triple::{@link
%% si_str_triple()} to IdTriple::{@link si_id_triple()} using
%% {@section @{get_id, StrDat@}} function. Then, it casts a store
%% stream element {Cmd::term(), IdTriple::{@link si_id_triple()},
%% NotifyProc::pid()} to DestProc::pid() {@link data_server}
%% process. This request is implemented by {@link hc_stream/5}. (LINK:
%% {@section @{stream, Command, Triple, DestProc, NotifyProc@}})
%% 
%% @type si_state() = #{}. This map stores Property::atom() and
%% Value::term() for a string-id mapping service process.
%% 
%% @type si_str_triple() = {si_str_triple_id(), si_str_subject(),
%% si_str_predicate(), si_str_object(), si_str_object_type(),
%% si_str_object_numerical()}. String expression of a RDF data.
%% 
%% @type si_str_triple_id() = term(). Triple id of a RDF data.
%% 
%% @type si_str_subject() = term(). Subject of a RDF data.
%% 
%% @type si_str_predicate() = term(). Predicate of a RDF data.
%% 
%% @type si_str_object() = term(). Object of a RDF data.
%% 
%% @type si_str_object_type() = term(). Object type of a RDF data.
%% 
%% @type si_str_object_numerical() = term(). Numerical value of object
%% of a RDF data.
%% 
%% @type si_id_triple() = {si_id_triple_id(), si_id_subject(),
%% si_id_predicate(), si_id_object(), si_id_object_type(),
%% si_id_object_numerical()}. Integer id expression of a RDF data.
%% 
%% @type si_id_triple_id() = integer(). Triple id of a RDF data.
%% 
%% @type si_id_subject() = integer(). Subject of a RDF data.
%% 
%% @type si_id_predicate() = integer(). Predicate of a RDF data.
%% 
%% @type si_id_object() = integer(). Object of a RDF data.
%% 
%% @type si_id_object_type() = integer(). Object type of a RDF data.
%% 
%% @type si_id_object_numerical() = term(). Numerical value of object
%% of a RDF data.
%% 
-module(string_id).
-behavior(gen_server).

-export([

	 child_spec/0, get_id/1, find/1, encode_triple/1,
	 decode_triple/1, encode_triple_pattern/1,
	 decode_triple_pattern/1, etsi_prepare_mp/0,
	 etsi_parse_datetime/2,

	 init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3

	]).
-include_lib("stdlib/include/qlc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("record.hrl").

%% ======================================================================
%% 
%% gen_server behavior
%% 

%% 
%% init/1
%% 
%% @doc Initialize a string_id process.
%% 
%% @spec init([]) -> {ok, si_state()}
%% 
init([]) ->
    process_flag(trap_exit, true),

    put(created,         true),
    put(pid,             self()),
    put(node,            node()),
    put(start_date_time, calendar:local_time()),
    put(sid_table_name,      []),
    put(sid_max_id,          0),

    State = hc_save_pd(),
    info_msg(init, [], State, -1),
    {ok, State}.

%% 
%% handle_call/3
%% 
%% @doc Handle synchronous query requests.
%% 
%% @spec handle_call(term(), {pid(), term()}, si_state()) -> {reply,
%% term(), si_state()}
%% 

%% Make a string-id mapping table and its index.
handle_call({create_table, TableName}, _, State) ->
    hc_restore_pd(erlang:get(created), State),
    hc_create_table(TableName, State);

%% Create an index for string-id mapping table.
handle_call(make_index, _, State) ->
    hc_restore_pd(erlang:get(created), State),
    hc_make_index(State);

%% Delete the string-id table.
handle_call(delete_table, _, State) ->
    hc_restore_pd(erlang:get(created), State),
    hc_delete_table(State);

%% Load existing table as mapping table.
handle_call({load_table, TableName}, _, State) ->
    hc_restore_pd(erlang:get(created), State),
    hc_load_table(TableName, State);

%% Append a record to the string-id table.
handle_call({append, StrDat}, _, State) ->
    hc_restore_pd(erlang:get(created), State),
    hc_append(StrDat, State);

%% Delete a record from the string-id table.
handle_call({delete, Id}, _, State) ->
    hc_restore_pd(erlang:get(created), State),
    hc_delete(Id, State);

%% Find a mapping for a string or an id from the string-id table.
handle_call({find, Data}, _, State) ->
    hc_restore_pd(erlang:get(created), State),
    hc_find(Data, State);

%% Find or append a mapping for a string from the string-id table.
handle_call({get_id, StrDat}, _, State) ->
    hc_restore_pd(erlang:get(created), State),
    hc_get_id(StrDat, State);

%% Count the size of the string-id table.
handle_call(get_size, _, State) ->
    hc_restore_pd(erlang:get(created), State),
    hc_get_size(State);

handle_call({get, PropertyName}, _, State) ->
    hc_restore_pd(erlang:get(created), State),
    {reply, erlang:get(PropertyName), State};

handle_call({put, PropertyName, Value}, From, State) ->
    hc_restore_pd(erlang:get(created), State),
    erlang:put(PropertyName, Value),
    erlang:put(update_date_time, calendar:local_time()),
    NewState = hc_save_pd(),
    A = [{put, PropertyName, Value}, From, State],
    info_msg(handle_call, A, NewState, 50),
    {reply, ok, NewState};

%% Default reply.
handle_call(Request, From, State) ->
    R = {unknown_request, Request},
    error_msg(handle_call, [Request, From, State], R),
    {reply, R, State}.

%% 
%% handle_cast/2
%% 
%% @doc Handle asynchronous query requests. 
%% 
%% @spec handle_cast(term(), si_state()) -> {noreply, si_state()}
%% 

%% Replace strings with ids in input triple and send it to the
%% subsequent process.
handle_cast({stream, Command, Triple, DestProc, NotifyProc}, State) ->
    hc_restore_pd(erlang:get(created), State),
    hc_stream(Command, Triple, DestProc, NotifyProc, State);

handle_cast(Request, State) ->
    R = {unknown_request, Request},
    error_msg(handle_cast, [Request, State], R),
    {noreply, State}.

%% 
%% handle_info/2
%% 
%% @doc Handle exceptional query requests. 
%% 
%% @spec handle_info(term(), si_state()) -> {noreply, si_state()}
%% 
handle_info(_Info, State) ->
    {noreply, State}.

%% 
%% terminate/2
%% 
%% @doc Process termination. 
%% 
%% @spec terminate(term(), si_state()) -> none()
%% 
terminate(Reason, State) ->
    case db_interface:dip_get_connection() of
	fail -> ok;
	C -> hcte_exec_sqls(["COMMIT;"], C)
    end,

    info_msg(terminate, [Reason, State], terminate, 10),
    ok.

%% 
%% code_change/3
%% 
%% @doc Process code change action. 
%% 
%% @spec code_change(term(), si_state(), term()) -> {ok, si_state()}
%% 
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ======================================================================
%% 
%% utility
%% 

%% 
%% @doc Report an error issue to the error_logger.
%% 
%% @spec error_msg(string(), term(), term()) -> ok
%% 
error_msg(FunName, Argument, Result) ->
    node_state:error_msg(?MODULE, FunName, Argument, Result).

%% 
%% @doc Report an information issue to the error_logger if current
%% debug level is greater than ThresholdDL.
%% 
%% @spec info_msg(string(), term(), term(), integer()) -> ok
%% 
info_msg(FunName, Argument, Result, ThresholdDL) ->
    node_state:info_msg(?MODULE, FunName, Argument, Result, ThresholdDL).

%% 
%% @doc Restore process dictionaries from state map structure.
%% 
%% @spec hc_restore_pd([{atom(), term()}] | undefined, bm_state()) -> ok
%% 
hc_restore_pd(undefined, State) ->
    hc_restore_pd_1(maps:to_list(State));
hc_restore_pd(_, _) ->
    ok.

hc_restore_pd_1([]) ->
    ok;
hc_restore_pd_1([{K, V} | T]) ->
    erlang:put(K, V),
    hc_restore_pd_1(T).

%% 
%% @doc Save process all dictionary contents into state map structure.
%% 
%% @spec hc_save_pd() -> bm_state()
%% 
hc_save_pd() ->
    maps:from_list(get()).

%% ======================================================================
%% 
%% handle call/cast implementation
%% 

%% 
%% @doc Create a string-id mapping table.
%% 
%% @spec hc_create_table(atom(), si_state()) -> {reply, term(),
%% si_state()}
%% 
hc_create_table(TabName, State) ->
    case ?DB_IMPLEMENTATION of
	epgsql ->
	    hc_create_table_epgsql(TabName, State)
    end.

hc_create_table_epgsql(TabName, State) ->
    hcte_perform(db_interface:dip_get_connection(), TabName, State).

hcte_perform(fail, TabName, State) ->
    R = {not_created, TabName, epgsql_open_failed},
    error_msg(hc_create_table_epgsql, [TabName, State], R),
    {reply, R, State};

hcte_perform(C, TabName, State) ->
    case is_atom(TabName) of
	true ->
	    Tab = atom_to_list(TabName);
	false ->
	    Tab = TabName
    end,

    Q01 = "SELECT reset_si('" ++ Tab ++ "');",
    QL  = [Q01],
    A   = [TabName, State],

    case hcte_exec_sqls(QL, C) of
	ok ->
	    hcte_exec_sqls(["COMMIT;"], C),

	    Rep = {created, TabName},
	    Res = [{sql, QL}, Rep],
	    info_msg(hc_create_table_epgsql, A, Res, 10),

	    put(sid_table_name, TabName),
	    put(sid_max_id,     0),

	    {reply, Rep, hc_save_pd()};
	E ->
	    hcte_exec_sqls(["ROLLBACK;"], C),
	    R = {not_created, TabName, epgsql_exec_failed},
	    error_msg(hc_create_table_epgsql, A, {R, E}),
	    {reply, R, State}
    end.

hcte_exec_sqls([], _) ->
    ok;
hcte_exec_sqls([Sql | Rest], C) ->
    hcte_confirm_sql(db_interface:dipp_squery({C, Sql}), Rest, C).

hcte_confirm_sql(ok, Rest, C) ->
    hcte_exec_sqls(Rest, C);
hcte_confirm_sql(1, Rest, C) ->
    hcte_exec_sqls(Rest, C);
hcte_confirm_sql(0, Rest, C) ->
    hcte_exec_sqls(Rest, C);
hcte_confirm_sql(10, Rest, C) ->
    hcte_exec_sqls(Rest, C);
hcte_confirm_sql(Size, _, _) when is_integer(Size) ->
    {size, Size};
hcte_confirm_sql({_, [{IdB, StrB}]}, _, _) ->
    Str = binary_to_list(StrB),
    Id = binary_to_integer(IdB),
    {found, Id, Str};
hcte_confirm_sql(E, Rest, C) ->
    error_msg(hcte_confirm_sql, [E, Rest, C], E),
    E.

hc_create_table_test_() ->
    application:load(b3s),
    {ok, TM} = application:get_env(b3s, test_mode),
    hctt_site(TM, ?DB_IMPLEMENTATION).

hctt_site(local2, epgsql) ->
    SI = string_id,
    T1 = si001,
    T2 = si002,
    T3 = si003,
    G1 = {create_table, T1},
    G2 = {create_table, T2},
    G3 = {create_table, T3},
    R01 = {created, T1},
    R02 = {created, T2},
    R03 = {created, T3},
    %% R11 = {not_created, T1, epgsql_exec_failed},
    %% R12 = {not_created, T2, epgsql_exec_failed},
    %% R13 = {not_created, T3, epgsql_exec_failed},
    {inorder,
     [
      ?_assertMatch(ok,  b3s:start()),
      ?_assertMatch(ok,  b3s:bootstrap()),
      ?_assertMatch(R01, gen_server:call(SI, G1)),
      ?_assertMatch(R01, gen_server:call(SI, G1)),
      ?_assertMatch(R02, gen_server:call(SI, G2)),
      ?_assertMatch(R02, gen_server:call(SI, G2)),
      ?_assertMatch(R03, gen_server:call(SI, G3)),
      ?_assertMatch(R03, gen_server:call(SI, G3)),
      ?_assertMatch(ok,  b3s:stop())
     ]};
hctt_site(_, _) ->
    [].

%% 
%% @doc Create an index for string-id mapping table.
%% 
%% @spec hc_make_index(si_state()) -> {reply, term(), si_state()}
%% 
hc_make_index(State) ->
    case ?DB_IMPLEMENTATION of
	epgsql ->
	    hc_make_index_epgsql(State)
    end.

hc_make_index_epgsql(State) ->
    hmie_perform(db_interface:dip_get_connection(), State).

hmie_perform(fail, State) ->
    R = {index_not_built, epgsql_open_failed},
    error_msg(hc_make_index_epgsql, [State], R),
    {reply, R, State};

hmie_perform(C, State) ->
    TabName = get(sid_table_name),
    Rep = {index_built, TabName},
    Res = [{no_operation, {use_plpgsql, index_si_str_h1k}}, Rep],
    info_msg(hc_make_index_epgsql, [C, State], Res, 10),
    {reply, Rep, State}.

hc_make_index_test_() ->
    application:load(b3s),
    {ok, TM} = application:get_env(b3s, test_mode),
    hmit_site(TM, ?DB_IMPLEMENTATION).

hmit_site(local2, epgsql) ->
    SI = string_id,
    T1 = si001,
    T2 = si002,
    T3 = si003,
    G01 = {create_table, T1},
    G02 = {create_table, T2},
    G03 = {create_table, T3},
    G11 = delete_table,
    G21 = make_index,
    R01 = {created, T1},
    R02 = {created, T2},
    R03 = {created, T3},
    R11 = {deleted, T1},
    R12 = {deleted, T2},
    R13 = {deleted, T3},
    R21 = {index_built, T1},
    R22 = {index_built, T2},
    R23 = {index_built, T3},
    %% R31 = {index_not_built, T1, epgsql_exec_failed},
    %% R32 = {index_not_built, T2, epgsql_exec_failed},
    %% R33 = {index_not_built, T3, epgsql_exec_failed},
    {inorder,
     [
      ?_assertMatch(ok,  b3s:start()),
      ?_assertMatch(ok,  b3s:bootstrap()),
      ?_assertMatch(R01, gen_server:call(SI, G01)),
      ?_assertMatch(R21, gen_server:call(SI, G21)),
      %% ?_assertMatch(R31, gen_server:call(SI, G21)),
      ?_assertMatch(R11, gen_server:call(SI, G11)),
      ?_assertMatch(R02, gen_server:call(SI, G02)),
      ?_assertMatch(R22, gen_server:call(SI, G21)),
      %% ?_assertMatch(R32, gen_server:call(SI, G21)),
      ?_assertMatch(R12, gen_server:call(SI, G11)),
      ?_assertMatch(R03, gen_server:call(SI, G03)),
      ?_assertMatch(R23, gen_server:call(SI, G21)),
      %% ?_assertMatch(R33, gen_server:call(SI, G21)),
      ?_assertMatch(R13, gen_server:call(SI, G11)),
      ?_assertMatch(ok,  b3s:stop())
     ]};
hmit_site(_, _) ->
    [].

%% 
%% @doc Delete the string-id table.
%% 
%% @spec hc_delete_table(si_state()) -> {reply, term(), si_state()}
%% 
hc_delete_table(State) ->
    case ?DB_IMPLEMENTATION of
	epgsql ->
	    hc_delete_table_epgsql(State)
    end.

hc_delete_table_epgsql(State) ->
    hdte_perform(db_interface:dip_get_connection(), State).

hdte_perform(fail, State) ->
    R = {not_deleted, get(sid_table_name), epgsql_open_failed},
    error_msg(hc_delete_table_epgsql, [State], R),
    {reply, R, State};

hdte_perform(C, State) ->
    TabName = get(sid_table_name),
    case is_atom(TabName) of
	true ->
	    Tab = atom_to_list(TabName);
	false when TabName == [] ->
	    Tab = "none";
	false ->
	    Tab = TabName
    end,

    Q02 = "DROP TABLE IF EXISTS " ++ Tab ++ " CASCADE;",
    Q04 = "DROP SEQUENCE IF EXISTS " ++ Tab ++ "_serial;",
    QL  = [Q02, Q04],
    A   = [State],

    case hcte_exec_sqls(QL, C) of
	ok ->
	    Rep = {deleted, TabName},
	    Res = [{sql, QL}, Rep],
	    info_msg(hc_delete_table_epgsql, A, Res, 10),
	    {reply, Rep, State};
	E ->
	    hcte_exec_sqls(["ROLLBACK;"], C),
	    R = {not_deleted, TabName, epgsql_exec_failed},
	    error_msg(hc_delete_table_epgsql, A, {R, E}),
	    {reply, R, State}
    end.

hc_delete_table_test_() ->
    application:load(b3s),
    {ok, TM} = application:get_env(b3s, test_mode),
    hdtt_site(TM, ?DB_IMPLEMENTATION).

hdtt_site(local2, epgsql) ->
    SI = string_id,
    T1 = si001,
    T2 = si002,
    T3 = si003,
    G01 = {create_table, T1},
    G02 = {create_table, T2},
    G03 = {create_table, T3},
    G11 = delete_table,
    R01 = {created, T1},
    R02 = {created, T2},
    R03 = {created, T3},
    R11 = {deleted, T1},
    R12 = {deleted, T2},
    R13 = {deleted, T3},
    %% R21 = {not_deleted, T1, epgsql_exec_failed},
    %% R22 = {not_deleted, T2, epgsql_exec_failed},
    %% R23 = {not_deleted, T3, epgsql_exec_failed},
    %% R24 = {not_deleted, [], epgsql_exec_failed},
    R99 = unknown_request,
    {inorder,
     [
      ?_assertMatch(ok,  b3s:start()),
      ?_assertMatch(ok,  b3s:bootstrap()),
      ?_assertMatch({R99, _}, gen_server:call(SI, qwe)),
      %% ?_assertMatch(R24, gen_server:call(SI, G11)),
      %% ?_assertMatch(R24, gen_server:call(SI, G11)),
      %% ?_assertMatch(R24, gen_server:call(SI, G11)),
      ?_assertMatch(R01, gen_server:call(SI, G01)),
      ?_assertMatch(R11, gen_server:call(SI, G11)),
      %% ?_assertMatch(R21, gen_server:call(SI, G11)),
      ?_assertMatch(R02, gen_server:call(SI, G02)),
      ?_assertMatch(R12, gen_server:call(SI, G11)),
      %% ?_assertMatch(R22, gen_server:call(SI, G11)),
      ?_assertMatch(R03, gen_server:call(SI, G03)),
      ?_assertMatch(R13, gen_server:call(SI, G11)),
      %% ?_assertMatch(R23, gen_server:call(SI, G11)),
      ?_assertMatch(ok,  b3s:stop())
     ]};
hdtt_site(_, _) ->
    [].

%% 
%% @doc Load existing table as mapping table.
%% 
%% @spec hc_load_table(atom(), si_state()) -> {reply, term(),
%% si_state()}
%% 
hc_load_table(TabName, State) ->
    case ?DB_IMPLEMENTATION of
	epgsql ->
	    hc_load_table_epgsql(TabName, State)
    end.

hc_load_table_epgsql(TabName, State) ->
    R = nothing_to_be_done,
    info_msg(hc_load_table_epgsql, [TabName, State], R, 50),
    {reply, R, State}.

hc_load_table_test_() ->
    application:load(b3s),
    {ok, TM} = application:get_env(b3s, test_mode),
    hltt_site(TM, ?DB_IMPLEMENTATION).

hltt_site(local2, epgsql) ->
    {inorder,
     [
     ]};
hltt_site(_, _) ->
    [].

%% 
%% @doc Append a record to the string-id table.
%% 
%% @spec hc_append(string(), si_state()) -> {reply, term(),
%% si_state()}
%% 
hc_append(StrDat, State) ->
    case ?DB_IMPLEMENTATION of
	epgsql ->
	    hc_append_epgsql(StrDat, State)
    end.

hc_append_epgsql(StrDat, State) ->
    hae_perform(db_interface:dip_get_connection(), StrDat, State).

hae_perform(fail, StrDat, State) ->
    R = {not_appended, StrDat, epgsql_open_failed},
    error_msg(hc_append_epgsql, [StrDat, State], R),
    {reply, R, State};

hae_perform(C, StrDat, State) ->
    TabName = get(sid_table_name),
    case is_atom(TabName) of
	true ->
	    Tab = atom_to_list(TabName);
	false ->
	    Tab = TabName
    end,

    Fld = "(str)",
    Val = "('~ts')",
    Fmt = "INSERT INTO ~s " ++ Fld ++ " VALUES " ++ Val ++ ";",
    Rec = [Tab, db_interface:dwp_escape(StrDat)],
    Sql = lists:flatten(io_lib:format(Fmt, Rec)),
    QL  = [Sql],
    A   = [StrDat, State],

    case hcte_exec_sqls(QL, C) of
	ok ->
	    hcte_exec_sqls(["COMMIT;"], C),

	    Rep = {appended, StrDat},
	    Res = [{sql, QL}, Rep],
	    info_msg(hc_append_epgsql, A, Res, 80),

	    put(sid_max_id, get(sid_max_id) + 1),
	    {reply, Rep, hc_save_pd()};
	E ->
	    hcte_exec_sqls(["ROLLBACK;"], C),
	    R = {not_appended, StrDat, epgsql_exec_failed},
	    error_msg(hc_append_epgsql, A, {R, E}),
	    {reply, R, State}
    end.

hc_append_test_() ->
    application:load(b3s),
    {ok, TM} = application:get_env(b3s, test_mode),
    hat_site(TM, ?DB_IMPLEMENTATION).

hat_site(local2, epgsql) ->
    SI = string_id,
    T1 = si001,
    SD1 = "http://www.yahoo.co.jp/",
    G01 = {create_table, T1},
    G11 = make_index,
    G21 = {append, SD1},
    R01 = {created, T1},
    R11 = {index_built, T1},
    R21 = {appended, SD1},
    R22 = {appended, SD1},
    R23 = {appended, SD1},
    {inorder,
     [
      ?_assertMatch(ok,  b3s:start()),
      ?_assertMatch(ok,  b3s:bootstrap()),
      ?_assertMatch(R01, gen_server:call(SI, G01)),
      ?_assertMatch(R11, gen_server:call(SI, G11)),
      ?_assertMatch(R21, gen_server:call(SI, G21)),
      ?_assertMatch(R22, gen_server:call(SI, G21)),
      ?_assertMatch(R23, gen_server:call(SI, G21)),
      ?_assertMatch(ok,  b3s:stop())
     ]};
hat_site(_, _) ->
    [].

%% 
%% @doc Delete a record from the string-id table.
%% 
%% @spec hc_delete(integer(), si_state()) -> {reply, term(),
%% si_state()}
%% 
hc_delete(Id, State) ->
    case ?DB_IMPLEMENTATION of
	epgsql ->
	    hc_delete_epgsql(Id, State)
    end.

hc_delete_epgsql(Id, State) ->
    hde_perform(db_interface:dip_get_connection(), Id, State).

hde_perform(fail, Id, State) ->
    R = {not_deleted, Id, epgsql_open_failed},
    error_msg(hc_delete_epgsql, [Id, State], R),
    {reply, R, State};

hde_perform(C, Id, State) ->
    TabName = get(sid_table_name),
    case is_atom(TabName) of
	true ->
	    Tab = atom_to_list(TabName);
	false ->
	    Tab = TabName
    end,

    Fmt = "DELETE FROM ~s WHERE id = ~w;",
    Sql = lists:flatten(io_lib:format(Fmt, [Tab, Id])),
    QL  = [Sql],
    A   = [Id, State],

    case hcte_exec_sqls(QL, C) of
	ok ->
	    hcte_exec_sqls(["COMMIT;"], C),

	    Rep = {deleted, {TabName, Id}},
	    Res = [{sql, QL}, Rep],
	    info_msg(hc_delete_epgsql, A, Res, 80),

	    {reply, Rep, State};
	E ->
	    hcte_exec_sqls(["ROLLBACK;"], C),
	    R = {not_deleted, Id, epgsql_exec_failed},
	    error_msg(hc_delete_epgsql, A, {R, E}),
	    {reply, R, State}
    end.

hc_delete_test_() ->
    application:load(b3s),
    {ok, TM} = application:get_env(b3s, test_mode),
    hdt_site(TM, ?DB_IMPLEMENTATION).

hdt_site(local2, epgsql) ->
    SI = string_id,
    T1 = si001,
    SD1 = "http://www.yahoo.co.jp/",
    G01 = {create_table, T1},
    G11 = delete_table,
    G21 = {append, SD1},
    G31 = {delete, 0},
    G32 = {delete, 1},
    G33 = {delete, 2},
    R01 = {created, T1},
    R11 = {deleted, T1},
    R21 = {appended, SD1},
    R22 = {appended, SD1},
    R23 = {appended, SD1},
    R31 = {deleted, {T1, 0}},
    R32 = {deleted, {T1, 1}},
    R33 = {deleted, {T1, 2}},
    R34 = {not_deleted, 2, epgsql_exec_failed},
    {inorder,
     [
      ?_assertMatch(ok,  b3s:start()),
      ?_assertMatch(ok,  b3s:bootstrap()),
      ?_assertMatch(R34, gen_server:call(SI, G33)),
      ?_assertMatch(R01, gen_server:call(SI, G01)),
      ?_assertMatch(R33, gen_server:call(SI, G33)),
      ?_assertMatch(R21, gen_server:call(SI, G21)),
      ?_assertMatch(R22, gen_server:call(SI, G21)),
      ?_assertMatch(R23, gen_server:call(SI, G21)),
      ?_assertMatch(R31, gen_server:call(SI, G31)),
      ?_assertMatch(R32, gen_server:call(SI, G32)),
      ?_assertMatch(R33, gen_server:call(SI, G33)),
      ?_assertMatch(R33, gen_server:call(SI, G33)),
      ?_assertMatch(R11, gen_server:call(SI, G11)),
      ?_assertMatch(ok,  b3s:stop())
     ]};
hdt_site(_, _) ->
    [].

%% 
%% @doc Find a mapping for a string from the string-id table.
%% 
%% @spec hc_find(string() | integer(), si_state()) -> {reply, term(),
%% si_state()}
%% 
hc_find(Data, State) ->
    case ?DB_IMPLEMENTATION of
	epgsql ->
	    hc_find_epgsql(Data, State)
    end.

hc_find_epgsql(Data, State) when is_integer(Data) ->
    hc_find_str_epgsql(Data, State);
hc_find_epgsql(Data, State) when is_list(Data) ->
    hc_find_id_epgsql(Data, State);
hc_find_epgsql(Data, State) ->
    R = {exception, {unkown_type_of_data, Data}},
    error_msg(hc_find_epgsql, [Data, State], R),
    {reply, R, State}.

%% find string from id
hc_find_str_epgsql(Id, State) ->
    hfse_perform(db_interface:dip_get_connection(), Id, State).

hfse_perform(fail, Id, State) ->
    R = {not_found, Id, epgsql_open_failed},
    error_msg(hc_find_str_epgsql, [Id, State], R),
    {reply, R, State};

hfse_perform(C, Id, State) ->
    TabName = get(sid_table_name),
    case is_atom(TabName) of
	true ->
	    Tab = atom_to_list(TabName);
	false ->
	    Tab = TabName
    end,

    Fmt = "SELECT * FROM ~s WHERE id = ~w;",
    Sql = lists:flatten(io_lib:format(Fmt, [Tab, Id])),
    QL  = [Sql],
    A   = [Id, State],

    case hcte_exec_sqls(QL, C) of
	{found, _, Str} ->
	    hcte_exec_sqls(["ROLLBACK;"], C),

	    Rep = {found, Str},
	    Res = [{sql, QL}, Rep],
	    info_msg(hc_find_str_epgsql, A, Res, 80),

	    {reply, Rep, State};
	E ->
	    hcte_exec_sqls(["ROLLBACK;"], C),
	    R = {not_found, Id, epgsql_exec_failed},
	    error_msg(hc_find_str_epgsql, A, {R, E}),
	    {reply, R, State}
    end.

%% find id from string
hc_find_id_epgsql(StrDat, State) ->
    hfie_perform(db_interface:dip_get_connection(), StrDat, State).

hfie_perform(fail, StrDat, State) ->
    R = {not_found, StrDat, epgsql_open_failed},
    %% error_msg(hc_find_id_epgsql, [StrDat, State], R),
    {reply, R, State};

hfie_perform(C, StrDat, State) ->
    TabName = get(sid_table_name),
    case is_atom(TabName) of
	true ->
	    Tab = atom_to_list(TabName);
	false ->
	    Tab = TabName
    end,

    Fmt = "SELECT * FROM ~s " ++
	  "WHERE left(str, 1000) = left('~ts', 1000) AND str = '~ts';",
    SDE = db_interface:dwp_escape(StrDat),
    Sql = lists:flatten(io_lib:format(Fmt, [Tab, SDE, SDE])),
    QL  = [Sql],
    A   = [StrDat, State],

    case hcte_exec_sqls(QL, C) of
	{found, Id, _} ->
	    hcte_exec_sqls(["ROLLBACK;"], C),

	    Rep = {found, Id},
	    Res = [{sql, QL}, Rep],
	    info_msg(hc_find_id_epgsql, A, Res, 80),

	    {reply, Rep, State};
	_E ->
	    hcte_exec_sqls(["ROLLBACK;"], C),
	    R = {not_found, StrDat, epgsql_exec_failed},
	    %% error_msg(hc_find_id_epgsql, A, {R, _E}),
	    {reply, R, State}
    end.

hc_find_test_() ->
    application:load(b3s),
    {ok, TM} = application:get_env(b3s, test_mode),
    hft_site(TM, ?DB_IMPLEMENTATION).

hft_site(local2, epgsql) ->
    SI = string_id,
    T1 = si001,
    SD1 = "http://www.yahoo.co.jp/",
    SD2 = "http://dir.yahoo.co.jp/Regional/Japanese_Regions/Kanto/Chiba/Cities/Funabashi/Entertainment/Local_Characters/Funassy/?q=Funassy",
    SD3 = "http://dir.yahoo.co.jp/Computers_and_Internet/Internet/World_Wide_Web/Weblogs/Mini_Blog/Twitter/Celebrities_Twitter/?frc=dsrp_jp0008",
    SD4 = "qwe",
    G01 = {create_table, T1},
    G11 = delete_table,
    G21 = {append, SD1},
    G22 = {append, SD2},
    G23 = {append, SD3},
    G24 = {append, SD4},
    G31 = {delete, 1},
    G32 = {delete, 2},
    G33 = {delete, 3},
    G34 = {delete, 4},
    G41 = {find, SD1},
    G42 = {find, SD2},
    G43 = {find, SD3},
    G44 = {find, SD4},
    G45 = {find, 1},
    G46 = {find, 2},
    G47 = {find, 3},
    G48 = {find, 4},
    G49 = {find, 5},
    G51 = make_index,
    R01 = {created, T1},
    R11 = {deleted, T1},
    R21 = {appended, SD1},
    R22 = {appended, SD2},
    R23 = {appended, SD3},
    R24 = {appended, SD4},
    R25 = {appended, SD4},
    R31 = {deleted, {T1, 1}},
    R32 = {deleted, {T1, 2}},
    R33 = {deleted, {T1, 3}},
    R34 = {deleted, {T1, 4}},
    R41 = {found, 1},
    R42 = {found, 2},
    R43 = {found, 3},
    R44 = {found, 4},
    R45 = {found, 5},
    R51 = {index_built, T1},
    R71 = {not_found, SD1, epgsql_exec_failed},
    R72 = {not_found, SD2, epgsql_exec_failed},
    R73 = {not_found, SD3, epgsql_exec_failed},
    R74 = {not_found, SD4, epgsql_exec_failed},
    R81 = {found, SD1},
    R82 = {found, SD2},
    R83 = {found, SD3},
    R84 = {found, SD4},
    R91 = {not_found, 1, epgsql_exec_failed},
    R92 = {not_found, 2, epgsql_exec_failed},
    R93 = {not_found, 3, epgsql_exec_failed},
    R94 = {not_found, 4, epgsql_exec_failed},
    R95 = {not_found, 5, epgsql_exec_failed},
    {inorder,
     [
      ?_assertMatch(ok,  b3s:start()),
      ?_assertMatch(ok,  b3s:bootstrap()),
      ?_assertMatch(R71, gen_server:call(SI, G41)), % find -> fail
      ?_assertMatch(R91, gen_server:call(SI, G45)), % find -> fail
      ?_assertMatch(R01, gen_server:call(SI, G01)), % create table si001
      ?_assertMatch(R71, gen_server:call(SI, G41)), % find -> fail
      ?_assertMatch(R91, gen_server:call(SI, G45)), % find -> fail
      ?_assertMatch(R21, gen_server:call(SI, G21)), % append
      ?_assertMatch(R22, gen_server:call(SI, G22)), % append
      ?_assertMatch(R23, gen_server:call(SI, G23)), % append
      ?_assertMatch(R41, gen_server:call(SI, G41)), % find
      ?_assertMatch(R81, gen_server:call(SI, G45)), % find
      ?_assertMatch(R51, gen_server:call(SI, G51)), % make index
      ?_assertMatch(R41, gen_server:call(SI, G41)), % find
      ?_assertMatch(R81, gen_server:call(SI, G45)), % find
      ?_assertMatch(R42, gen_server:call(SI, G42)), % find
      ?_assertMatch(R82, gen_server:call(SI, G46)), % find
      ?_assertMatch(R43, gen_server:call(SI, G43)), % find
      ?_assertMatch(R83, gen_server:call(SI, G47)), % find
      ?_assertMatch(R74, gen_server:call(SI, G44)), % find -> fail
      ?_assertMatch(R94, gen_server:call(SI, G48)), % find -> fail
      ?_assertMatch(R24, gen_server:call(SI, G24)), % append
      ?_assertMatch(R44, gen_server:call(SI, G44)), % find
      ?_assertMatch(R84, gen_server:call(SI, G48)), % find
      ?_assertMatch(R31, gen_server:call(SI, G31)), % delete
      ?_assertMatch(R71, gen_server:call(SI, G41)), % not_found
      ?_assertMatch(R91, gen_server:call(SI, G45)), % not_found
      ?_assertMatch(R32, gen_server:call(SI, G32)), % delete
      ?_assertMatch(R72, gen_server:call(SI, G42)), % not_found
      ?_assertMatch(R92, gen_server:call(SI, G46)), % not_found
      ?_assertMatch(R33, gen_server:call(SI, G33)), % delete
      ?_assertMatch(R73, gen_server:call(SI, G43)), % not_found
      ?_assertMatch(R93, gen_server:call(SI, G47)), % not_found
      ?_assertMatch(R95, gen_server:call(SI, G49)), % not_found
      ?_assertMatch(R25, gen_server:call(SI, G24)), % append
      ?_assertMatch(R74, gen_server:call(SI, G44)), % find -> fail
      ?_assertMatch(R84, gen_server:call(SI, G49)), % not_found
      ?_assertMatch(R34, gen_server:call(SI, G34)), % delete
      ?_assertMatch(R45, gen_server:call(SI, G44)), % find
      ?_assertMatch(R11, gen_server:call(SI, G11)), % delete table si001
      ?_assertMatch(ok,  b3s:stop())
     ]};
hft_site(_, _) ->
    [].

%% 
%% @doc Find or append a mapping for a string from the string-id
%% table.
%% 
%% @spec hc_get_id(string(), si_state()) -> {reply, term(),
%% si_state()}
%% 
hc_get_id(StrDat, State) ->
    case ?DB_IMPLEMENTATION of
	epgsql ->
	    hc_get_id_epgsql(StrDat, State)
    end.

hc_get_id_epgsql(StrDat, State) when is_list(StrDat) ->
    hgie_find(hc_find_id_epgsql(StrDat, State), StrDat, State);
hc_get_id_epgsql(StrDat, State) ->
    R = {exception, {unkown_type_of_data, StrDat}},
    error_msg(hc_get_id_epgsql, [StrDat, State], R),
    {reply, R, State}.

hgie_find({reply, {found, Id}, State}, _, _) ->
    {reply, {id, Id}, State};
hgie_find(_, StrDat, State) ->
    {_, _, NewState} = hc_append_epgsql(StrDat, State),
    hgie_append(hc_find_id_epgsql(StrDat, NewState)).

hgie_append({reply, {found, Id}, State}) ->
    {reply, {id, Id}, State};
hgie_append(R) ->
    R.

hc_get_id_test_() ->
    application:load(b3s),
    {ok, TM} = application:get_env(b3s, test_mode),
    hgit_site(TM, ?DB_IMPLEMENTATION).

hgit_site(local2, epgsql) ->
    SI = string_id,
    MT = si001,
    SD1 = "http://www.yahoo.co.jp/",
    SD2 = "http://dir.yahoo.co.jp/Regional/Japanese_Regions/Kanto/Chiba/Cities/Funabashi/Entertainment/Local_Characters/Funassy/?q=Funassy",
    SD3 = "http://dir.yahoo.co.jp/Computers_and_Internet/Internet/World_Wide_Web/Weblogs/Mini_Blog/Twitter/Celebrities_Twitter/?frc=dsrp_jp0008",
    SD4 = "qwe",
    SD5 = "{unknown, []}",
    SD6 = {uri,"id_igagss_1xk_1abgrgo"},
    G01 = {create_table, MT},
    G21 = make_index,
    G31 = {get_id, SD1},
    G32 = {get_id, SD2},
    G33 = {get_id, SD3},
    G34 = {get_id, SD4},
    G35 = {get_id, SD5},
    G36 = {get_id, SD6},
    G41 = get_size,
    G51 = {delete, 1},
    R01 = {created, MT},
    R21 = {index_built, MT},
    R31 = {id, 1},
    R32 = {id, 2},
    R33 = {id, 3},
    R34 = {id, 4},
    R35 = {id, 5},
    R36 = {exception,{unkown_type_of_data,{uri,"id_igagss_1xk_1abgrgo"}}},
    R37 = {id, 6},
    R41 = {size, 0},
    R42 = {size, 5},
    R51 = {deleted, {MT, 1}},
    R71 = {not_found, SD1, epgsql_exec_failed},
    {inorder,
     [
      ?_assertMatch(ok,  b3s:start()),
      ?_assertMatch(ok,  b3s:bootstrap()),
      ?_assertMatch(R71, gen_server:call(SI, G31)), % get_id -> fail
      ?_assertMatch(R01, gen_server:call(SI, G01)), % create table
      ?_assertMatch(R41, gen_server:call(SI, G41)), % get_size
      ?_assertMatch(R31, gen_server:call(SI, G31)), % get_id
      ?_assertMatch(R21, gen_server:call(SI, G21)), % make index
      ?_assertMatch(R31, gen_server:call(SI, G31)), % get_id
      ?_assertMatch(R31, gen_server:call(SI, G31)), % get_id
      ?_assertMatch(R32, gen_server:call(SI, G32)), % get_id
      ?_assertMatch(R33, gen_server:call(SI, G33)), % get_id
      ?_assertMatch(R34, gen_server:call(SI, G34)), % get_id
      ?_assertMatch(R35, gen_server:call(SI, G35)), % get_id
      ?_assertMatch(R36, gen_server:call(SI, G36)), % get_id
      ?_assertMatch(R32, gen_server:call(SI, G32)), % get_id
      ?_assertMatch(R33, gen_server:call(SI, G33)), % get_id
      ?_assertMatch(R34, gen_server:call(SI, G34)), % get_id
      ?_assertMatch(R42, gen_server:call(SI, G41)), % get_size
      ?_assertMatch(R51, gen_server:call(SI, G51)), % delete
      ?_assertMatch(R37, gen_server:call(SI, G31)), % get_id
      ?_assertMatch(R42, gen_server:call(SI, G41)), % get_size
      ?_assertMatch(ok,  b3s:stop())
     ]};
hgit_site(_, _) ->
    [].

%% 
%% @doc Count the size of the string-id table.
%% 
%% @spec hc_get_size(si_state()) -> {reply, term(), si_state()}
%% 
hc_get_size(State) ->
    case ?DB_IMPLEMENTATION of
	epgsql ->
	    hc_get_size_epgsql(State)
    end.

hc_get_size_epgsql(State) ->
    hgse_perform(db_interface:dip_get_connection(), State).

hgse_perform(fail, State) ->
    R = {aborted, epgsql_open_failed},
    error_msg(hc_get_size_epgsql, [State], R),
    {reply, R, State};

hgse_perform(C, State) ->
    TabName = get(sid_table_name),
    case is_atom(TabName) of
	true ->
	    Tab = atom_to_list(TabName);
	false ->
	    Tab = TabName
    end,

    Fmt = "SELECT COUNT(*) FROM ~s;",
    Sql = lists:flatten(io_lib:format(Fmt, [Tab])),
    QL  = [Sql],
    A   = [State],

    case hcte_exec_sqls(QL, C) of
	{size, Size} ->
	    hcte_exec_sqls(["COMMIT;"], C),

	    Rep = {size, Size},
	    Res = [{sql, QL}, Rep],
	    info_msg(hc_get_size_epgsql, A, Res, 50),

	    {reply, Rep, State};
	ok ->
	    hcte_exec_sqls(["COMMIT;"], C),

	    Rep = {size, 0},
	    Res = [{sql, QL}, Rep],
	    info_msg(hc_get_size_epgsql, A, Res, 50),

	    {reply, Rep, State};
	E ->
	    hcte_exec_sqls(["ROLLBACK;"], C),
	    R = {aborted, epgsql_exec_failed},
	    error_msg(hc_get_size_epgsql, A, {R, E}),
	    {reply, R, State}
    end.

%% 
%% @doc Replace strings with ids in input triple and send it to the
%% subsequent process.
%% 
%% @spec hc_stream(term(), si_str_triple(), pid(), pid(), si_state())
%% -> {noreply, si_state()}
%% 
hc_stream(Cmd, {_, _, _, _, _, eof}, DestProc, NotifyProc, State) ->
    M = {Cmd, {eof, eof, eof, eof, eof, eof}, NotifyProc},
    gen_server:cast(DestProc, M),
    {noreply, State};
hc_stream(Cmd, StrT, DestProc, NotifyProc, State) ->
    {TrpId, StrSbj, StrPrd, StrObj, ObjTyp, ObjNum} = StrT,
    SL = [TrpId, StrSbj, StrPrd, StrObj, ObjTyp],
    {IL, NewState} = hcs_convert(SL, State, []),
    [IdOT, IdObj, IdPrd, IdSbj, IdTI] = IL, % note for making reverse order
    Res = {Cmd, {IdTI, IdSbj, IdPrd, IdObj, IdOT, ObjNum}, NotifyProc},
    gen_server:cast(DestProc, Res),

    A = [Cmd, StrT, DestProc, NotifyProc, State],
    R = {performed_cast, DestProc, Res},
    info_msg(hc_stream, A, R, 80),
    {noreply, NewState}.

hcs_convert([], State, Result) ->
    {Result, State};
hcs_convert([String | Rest], State, Result) ->
    {_, {id, Id}, NewState} = hc_get_id(String, State),
    hcs_convert(Rest, NewState, [Id | Result]).

hc_stream_test_() ->
    application:load(b3s),
    {ok, TM} = application:get_env(b3s, test_mode),
    hst_site(TM, ?DB_IMPLEMENTATION).

hst_site(local2, epgsql) ->
    {inorder,
     [
     ]};
hst_site(_, _) ->
    [].

%% ======================================================================
%% 
%% API
%% 

%% 
%% @doc This function finds or appends a mapping for a string from the
%% string-id table. It performs the same functionality with {@section
%% @{get_id, StrDat@}} synchronous message. It returns fail if error
%% occurred. It uses 2 keys of process dictionary: sid_table_name and
%% sid_max_id. These keys must be erased before the first call of this
%% function from a process. While they can be erased everytime before
%% calling this function subsequently, leaving them as they are will
%% increase the performance.
%% 
%% @spec get_id(string()) -> integer() | fail
%% 
get_id(StrDat) ->
    %% info_msg(get_id, [StrDat], entered, 50),
    gi_check_pd(get(sid_table_name), get(sid_max_id), StrDat).

gi_check_pd(undefined, _, StrDat) ->
    BS = gen_server:call(node_state, {get, b3s_state_pid}),
    Tab = gen_server:call(BS, {get, name_of_string_id_table}),
    put(sid_table_name, Tab),
    gi_check_pd(get(sid_table_name), get(sid_max_id), StrDat);
gi_check_pd(_, undefined, StrDat) ->
    put(sid_max_id, 0),
    gi_check_pd(get(sid_table_name), get(sid_max_id), StrDat);
gi_check_pd(_Tab, _Max, StrDat) ->
    {reply, R, _} = hc_get_id(StrDat, #{}),
    %% info_msg(get_id, [StrDat], {R, _Tab, _Max, hc_save_pd()}, 50),
    gi_return(R, StrDat).

gi_return({id, Id}, _) ->
    Id;
gi_return(E, StrDat) ->
    error_msg(get_id, [StrDat], {error, E}),
    fail.

get_id_test_() ->
    application:load(b3s),
    {ok, TM} = application:get_env(b3s, test_mode),
    git_site(TM, ?DB_IMPLEMENTATION).

git_site(local2, epgsql) ->
    SD1 = "http://www.yahoo.co.jp/",
    SD2 = "http://dir.yahoo.co.jp/Regional/Japanese_Regions/Kanto/Chiba/Cities/Funabashi/Entertainment/Local_Characters/Funassy/?q=Funassy",
    SD3 = "http://dir.yahoo.co.jp/Computers_and_Internet/Internet/World_Wide_Web/Weblogs/Mini_Blog/Twitter/Celebrities_Twitter/?frc=dsrp_jp0008",
    SD4 = "qwe",
    SD5 = "{unknown, []}",
    SD6 = {uri,"id_igagss_1xk_1abgrgo"},
    SD7 = "\"Paris\"",
    SI  = string_id,
    Tab = string_id_2,
    R01 = {created, Tab},
    %% R11 = {deleted, Tab},
    R21 = {index_built, Tab},
    R99 = fail,

    erase(sid_table_name),
    erase(sid_max_id),
    {inorder,
     [
      ?_assertMatch(ok,  b3s:start()),
      ?_assertMatch(ok,  b3s:bootstrap()),
      ?_assertMatch(R01, gen_server:call(SI, {create_table, Tab})),
      ?_assertMatch(R21, gen_server:call(SI, make_index)),

      ?_assertMatch(1,   get_id(SD1)),
      ?_assertMatch(2,   get_id(SD2)),
      ?_assertMatch(3,   get_id(SD3)),
      ?_assertMatch(Tab, erase(sid_table_name)),
      ?_assertMatch(3,   erase(sid_max_id)),
      ?_assertMatch(4,   get_id(SD4)),
      ?_assertMatch(5,   get_id(SD5)),
      ?_assertMatch(R99, get_id(SD6)),
      ?_assertMatch(Tab, erase(sid_table_name)),
      ?_assertMatch(2,   erase(sid_max_id)),
      ?_assertMatch(R99, get_id(SD6)),
      ?_assertMatch(5,   get_id(SD5)),
      ?_assertMatch(4,   get_id(SD4)),
      ?_assertMatch(3,   get_id(SD3)),
      ?_assertMatch(2,   get_id(SD2)),
      ?_assertMatch(1,   get_id(SD1)),
      ?_assertMatch(6,   get_id(SD7)),

      %% ?_assertMatch(R11, gen_server:call(SI, delete_table)),
      ?_assertMatch(ok,  timer:sleep(100)),
      ?_assertMatch(ok,  b3s:stop())
     ]};
git_site(_, _) ->
    [].

%% 
%% @doc This function finds corresponding id or string from
%% Data::integer() | string(). It performs the same functionality with
%% {@section @{find, Data@}} synchronous message. It returns fail if
%% not found or error occurred. It uses a key of process dictionary
%% sid_table_name. This key must be erased before the first call of
%% this function from a process. While it can be erased everytime
%% before calling this function subsequently, leaving it as it is will
%% increase the performance.
%% 
%% @spec find(integer() | string()) -> string() | interger()
%% 
find(Data) ->
%    info_msg(find, [Data], entered, 50),
    fi_check_pd(get(sid_table_name), Data).

fi_check_pd(undefined, Data) ->
    BS = gen_server:call(node_state, {get, b3s_state_pid}),
    Tab = gen_server:call(BS, {get, name_of_string_id_table}),
    put(sid_table_name, Tab),
    fi_check_pd(get(sid_table_name), Data);
fi_check_pd(_Tab, Data) ->
    {reply, R, _} = hc_find(Data, #{}),
%    info_msg(find, [Data], {R, _Tab}, 50),
    fi_return(R, Data).

fi_return({found, R}, _) ->
    R;
fi_return(E, Data) ->
    error_msg(find, [Data], {error, E}),
    fail.

find_test_() ->
    application:load(b3s),
    {ok, TM} = application:get_env(b3s, test_mode),
    ft_site(TM, ?DB_IMPLEMENTATION).

ft_site(local2, epgsql) ->
    {inorder,
     [
      ?_assertMatch(ok,  b3s:start()),
      ?_assertMatch(ok,  b3s:bootstrap()),
      ?_assertMatch(ok,  b3s:stop())
     ]};
ft_site(_, _) ->
    [].

%% 
%% @doc This function encodes a triple. The implementation of this
%% function can be replaced by changing STRING_ID_CODING_METHOD
%% constant definition. The string_id implementation of this function
%% uses process dictionary key "__string_id_mp" that stores several
%% re:mp() matching patterns.
%% 
%% @spec encode_triple(query_node:qn_triple()) ->
%% query_node:qn_triple() | fail
%% 
encode_triple(Triple) ->
    encode_triple_method(?STRING_ID_CODING_METHOD, Triple).

encode_triple_method(string_integer, Triple) ->
    etsi_prepare_mp(),
    IntMP = maps:get(integer,  get('__string_id_mp')),
    ReaMP = maps:get(real,     get('__string_id_mp')),
    DatMP = maps:get(datetime, get('__string_id_mp')),
    Obj   = element(4, Triple),
    IntRR = re:run(Obj, IntMP),
    ReaRR = re:run(Obj, ReaMP),
    DatRR = re:run(Obj, DatMP),

    R = [{int, IntRR}, {real, ReaRR}, {date, DatRR}],
    info_msg(encode_triple_method, [string_integer, Triple], R, 80),

    IdId  = get_id(element(1, Triple)),
    SbjId = get_id(element(2, Triple)),
    PrdId = get_id(element(3, Triple)),
    {IdId, SbjId, PrdId, etsi_conv_obj(IntRR, ReaRR, DatRR, Obj)};
encode_triple_method(M, Triple) ->
    error_msg(encode_triple_method, [M, Triple], unknown_method),
    fail.

etsi_conv_obj({match, [_, {S, L}]}, _, _, Obj) ->
    ObjSt = string:substr(Obj, S + 1, L),
    {list_to_integer(ObjSt), integer};
etsi_conv_obj(_, {match, ML}, _, Obj) ->
    {S, L} = lists:nth(2, ML),
    ObjSt = string:substr(Obj, S + 1, L),
    {list_to_float(ObjSt), real};
etsi_conv_obj(_, _, {match, ML}, Obj) ->
    {etsi_parse_datetime(ML, Obj), datetime};
etsi_conv_obj(_, _, _, Obj) ->
    {get_id(Obj), code}.

etsi_parse_datetime([_, {Y, YL}, _, {M, ML}, _, {D, DL}], Str) ->
    YS = string:substr(Str, Y + 1, YL),
    MS = string:substr(Str, M + 1, ML),
    DS = string:substr(Str, D + 1, DL),

    YY = list_to_integer(YS),
    case string:chr(MS, $#) of
	0 -> MM = list_to_integer(MS);
	_ -> MM = 1
    end,
    case string:chr(DS, $#) of
	0 -> DD = list_to_integer(DS);
	_ -> DD = 1
    end,

    {{YY, MM, DD}, {0, 0, 0}};
etsi_parse_datetime([_, {Y, YL}, _, {M, ML}, _, {D, DL},
		     _, {H, HL}, {N, NL}], Str) ->
    YS = string:substr(Str, Y + 1, YL),
    MS = string:substr(Str, M + 1, ML),
    DS = string:substr(Str, D + 1, DL),
    HS = string:substr(Str, H + 1, HL),
    NS = string:substr(Str, N + 1, NL),

    YY = list_to_integer(YS),
    case string:chr(MS, $#) of
	0 -> MM = list_to_integer(MS);
	_ -> MM = 1
    end,
    case string:chr(DS, $#) of
	0 -> DD = list_to_integer(DS);
	_ -> DD = 1
    end,
    HH = list_to_integer(HS),
    NN = list_to_integer(NS),

    {{YY, MM, DD}, {HH, NN, 0}};
etsi_parse_datetime([_, {Y, YL}, _, {M, ML}, _, {D, DL},
		     _, {H, HL}, {N, NL}, _, {S, SL}], Str) ->
    YStr = string:substr(Str, Y + 1, YL),
    MStr = string:substr(Str, M + 1, ML),
    DStr = string:substr(Str, D + 1, DL),
    HStr = string:substr(Str, H + 1, HL),
    NStr = string:substr(Str, N + 1, NL),
    SStr = string:substr(Str, S + 1, SL),

    YY = list_to_integer(YStr),
    case string:chr(MStr, $#) of
	0 -> MM = list_to_integer(MStr);
	_ -> MM = 1
    end,
    case string:chr(DStr, $#) of
	0 -> DD = list_to_integer(DStr);
	_ -> DD = 1
    end,
    HH = list_to_integer(HStr),
    NN = list_to_integer(NStr),
    SS = list_to_integer(SStr),

    {{YY, MM, DD}, {HH, NN, SS}}.

%% 
%% @doc This function prepares re:mp() matching patterns into process
%% dictionary key "__string_id_mp". It is a #{} object that takes
%% following atom() keys for accessing matching patterns.
%% 
%% <table bgcolor="lemonchiffon">
%% <tr><th>key</th> <th>mapching pattern</th></tr>
%% <tr><td>integer</td>  <td>-?[0-9]+</td></tr>
%% <tr><td>real</td>
%%     <td>-?[0-9]+\\\.[0-9]+([eE][\+\-]?[0-9]+)?</td></tr>
%% <tr><td>datetime</td>
%%     <td>([0-9]+)[-/]+([0-9#]+)[-/]+([0-9#]+)</td></tr>
%% <tr><td></td>
%%     <td>(\s+([0-9]+):([0-9]+)(:([0-9]+))?)?</td></tr>
%% </table>
%% 
etsi_prepare_mp() ->
    etsipm_perform(get('__string_id_mp')).

etsipm_perform(NonMap) when not is_map(NonMap) ->
    H   = "^[\s\t\\\"]*",
    F   = "[\s\t\\\"]*$",
    REI = H ++ "(-?[0-9]+)" ++ F,
    RER = H ++ "(-?[0-9]+\\\.[0-9]+([eE][\+\-]?[0-9]+)?)" ++ F,
    RED = H ++ "([0-9]+)(-|/)([0-9#]+)(-|/)([0-9#]+)" ++
	   "(\s+([0-9]+):([0-9]+)(:([0-9]+))?)?" ++ F,
    {ok, MPI} = re:compile(REI),
    {ok, MPR} = re:compile(RER),
    {ok, MPD} = re:compile(RED),

    K = '__string_id_mp',
    put(K, maps:new()),
    put(K, maps:put(integer,  MPI, get(K))),
    put(K, maps:put(real,     MPR, get(K))),
    put(K, maps:put(datetime, MPD, get(K))),
    R = {prepared, get(K)},
    info_msg(etsi_prepare_mp, [], R, 50);
etsipm_perform(_) ->
    ok.

%% 
%% @doc This function decodes a triple. The implementation of this
%% function can be replaced by changing STRING_ID_CODING_METHOD
%% constant definition.
%% 
%% @spec decode_triple(query_node:qn_triple()) ->
%% query_node:qn_triple() | fail
%% 
decode_triple(Triple) ->
    decode_triple_method(?STRING_ID_CODING_METHOD, Triple).

decode_triple_method(string_integer, {Id, Sbj, Prd, {ObjV, code}}) ->
    IdStr  = find(Id),
    SbjStr = find(Sbj),
    PrdStr = find(Prd),
    ObjStr = find(ObjV),
    {IdStr, SbjStr, PrdStr, ObjStr};
decode_triple_method(string_integer, {Id, Sbj, Prd, {ObjV, integer}}) ->
    IdStr  = find(Id),
    SbjStr = find(Sbj),
    PrdStr = find(Prd),
    ObjStr = integer_to_list(ObjV),
    {IdStr, SbjStr, PrdStr, ObjStr};
decode_triple_method(string_integer, {Id, Sbj, Prd, {ObjV, real}}) ->
    IdStr  = find(Id),
    SbjStr = find(Sbj),
    PrdStr = find(Prd),
    ObjStr = float_to_list(ObjV, [{decimals, 4}, compact]),
    {IdStr, SbjStr, PrdStr, ObjStr};
decode_triple_method(string_integer, {Id, Sbj, Prd, {ObjV, datetime}}) ->
    IdStr  = find(Id),
    SbjStr = find(Sbj),
    PrdStr = find(Prd),
    F      = fun(X) -> tuple_to_list(X) end,
    DTFlat = lists:flatten(lists:map(F, F(ObjV))),
    DTForm = "~4..0w-~2..0w-~2..0w ~2..0w:~2..0w:~2..0w",
    ObjStr = lists:flatten(io_lib:format(DTForm, DTFlat)),
    {IdStr, SbjStr, PrdStr, ObjStr};
decode_triple_method(string_integer, {Id, Sbj, Prd, {ObjV, _}}) ->
    IdStr  = find(Id),
    SbjStr = find(Sbj),
    PrdStr = find(Prd),
    ObjStr = ObjV,
    {IdStr, SbjStr, PrdStr, ObjStr};
decode_triple_method(M, Triple) ->
    error_msg(decode_triple_method, [M, Triple], unknown_method),
    fail.

%% 
%% @doc This function encodes a triple pattern. The implementation of
%% this function can be replaced by changing STRING_ID_CODING_METHOD
%% constant definition. Variable elements are left not encoded. The
%% string_id implementation of this function uses process dictionary
%% key "__string_id_mp" that stores several re:mp() matching patterns.
%% 
%% @spec encode_triple_pattern( query_node:qn_triple_pattern()) ->
%% {integer() | string(), integer() | string(), integer() | string(),
%% integer() | string()} | fail
%% 
encode_triple_pattern(Triple) ->
    encode_triple_pattern_method(?STRING_ID_CODING_METHOD, Triple).

encode_triple_pattern_method(string_integer, Triple) ->
    IdId  = etpm_string_integer(1, Triple),
    SbjId = etpm_string_integer(2, Triple),
    PrdId = etpm_string_integer(3, Triple),
    ObjId = etpm_string_integer(4, Triple),
    {IdId, SbjId, PrdId, ObjId};
encode_triple_pattern_method(M, Triple) ->
    error_msg(encode_triple_pattern_method, [M, Triple], unknown_method),
    fail.

etpm_string_integer(Pos, Triple) ->
    DCV = db_interface:dttq_conv_var(Pos, Triple),
    etpmsi_perform(DCV, Pos, Triple).

etpmsi_perform(false, Pos, Triple) when Pos == 4 ->
    etsi_prepare_mp(),
    IntMP = maps:get(integer,  get('__string_id_mp')),
    ReaMP = maps:get(real,     get('__string_id_mp')),
    DatMP = maps:get(datetime, get('__string_id_mp')),
    Obj   = element(Pos, Triple),
    IntRR = re:run(Obj, IntMP),
    ReaRR = re:run(Obj, ReaMP),
    DatRR = re:run(Obj, DatMP),
    etsi_conv_obj(IntRR, ReaRR, DatRR, Obj);
etpmsi_perform(false, Pos, Triple) ->
    get_id(element(Pos, Triple));
etpmsi_perform(true, Pos, Triple) ->
    element(Pos, Triple).

%% 
%% @doc This function decodes a triple pattern. The implementation of
%% this function can be replaced by changing STRING_ID_CODING_METHOD
%% constant definition.
%% 
%% @spec decode_triple_pattern({integer() | string(), integer() |
%% string(), integer() | string(), integer() | string()}) ->
%% query_node:qn_triple_pattern() | fail
%% 
decode_triple_pattern(Triple) ->
    decode_triple_pattern_method(?STRING_ID_CODING_METHOD, Triple).

decode_triple_pattern_method(string_integer, Triple) ->
    IdStr  = dtpm_string_integer(1, Triple),
    SbjStr = dtpm_string_integer(2, Triple),
    PrdStr = dtpm_string_integer(3, Triple),
    ObjStr = dtpm_string_integer(4, Triple),
    {IdStr, SbjStr, PrdStr, ObjStr};
decode_triple_pattern_method(M, Triple) ->
    error_msg(decode_triple_pattern_method, [M, Triple], unknown_method),
    fail.

dtpm_string_integer(Pos, Triple) ->
    case is_list(element(Pos, Triple)) of
	true ->
	    DCV = db_interface:dttq_conv_var(Pos, Triple);
	false ->
	    DCV = false
    end,
    dtpmsi_perform(DCV, Pos, Triple).

dtpmsi_perform(false, Pos, Triple) ->
    find(element(Pos, Triple));
dtpmsi_perform(true, Pos, Triple) ->
    element(Pos, Triple).

encode_test_() ->
    application:load(b3s),
    {ok, TM} = application:get_env(b3s, test_mode),
    et_site(TM, ?DB_IMPLEMENTATION).

et_site(local2, epgsql) ->
    {inorder,
     [
      ?_assertMatch(ok,  b3s:start()),
      ?_assertMatch(ok,  b3s:bootstrap()),
      ?_assertMatch(ok,  b3s:stop())
     ]};
et_site(_, _) ->
    [].

%% 
%% @doc Return child spec for this process. It can be used in
%% supervisor:init/0 callback implementation.
%% 
%% @spec child_spec() -> supervisor:child_spec()
%% 
child_spec() ->
    Id = string_id,
    GSOpt = [{local, Id}, Id, [], []],
    StartFunc = {gen_server, start_link, GSOpt},
    Restart = permanent,
    Shutdwon = 1000,
    Type = worker,
    Modules = [string_id],

    {Id, StartFunc, Restart, Shutdwon, Type, Modules}.

%% ====> END OF LINE <====

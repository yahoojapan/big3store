%%
%% Perform database access
%%
%% @copyright 2015-2016 UP FAMNIT and Yahoo Japan Corporation
%% @version 0.3
%% @since July, 2015
%% @author Kiyoshi Nitta <knitta@yahoo-corp.jp>
%% 
%% @see b3s
%% @see b3s_state
%% @see node_state
%% @see triple_distributor
%% @see db_interface
%% 
%% @doc This module performs database access operations. It provides a
%% server function for a {@link triple_distributor} process to write
%% data to a triple table. This is a gen_server process running on
%% each node that belongs to the distributed big3store system.
%% 
%% <table bgcolor="lemonchiffon">
%% <tr><th>Section Index</th></tr>
%% <tr><td>{@section property list}</td></tr>
%% <tr><td>{@section handle_call (synchronous) message API}</td></tr>
%% <tr><td>{@section handle_cast (asynchronous) message API}</td></tr>
%% </table>
%% 
%% == property list ==
%% 
%% (LINK: {@section property list})
%% 
%% <table border="3">
%% <tr><th>Name</th><th>Type</th><th>Description</th></tr>
%% 
%% <tr> <td>created</td> <td>boolean()</td> <td>true denotes that
%% process dictionary was created and used. false denotes that
%% completely new process.</td> </tr>
%% 
%% <tr> <td>pid</td> <td>pid()</td> <td>local process id of the
%% process.</td> </tr>
%% 
%% <tr> <td>start_date_time</td> <td>calendar:datetime()</td>
%% <td>started date and time of the process.</td> </tr>
%% 
%% <tr> <td>tablename</td> <td>string()</td> <td>name of triple
%% table.</td> </tr>
%% 
%% <tr> <td>opened_db</td> <td>boolean()</td> <td>true if database
%% triple table was already opened.</td> </tr>
%% 
%% </table>
%% 
%% == handle_call (synchronous) message API ==
%% 
%% (LINK: {@section handle_call (synchronous) message API})
%% 
%% <table border="3">
%% 
%% <tr> <th>Message</th> <th>Args</th> <th>Returns</th>
%% <th>Description</th> </tr>
%% 
%% <tr> <td>{@section db_init}</td> <td></td> <td>ok | fail</td>
%% <td>call {@link db_interface:db_init/0}</td> </tr>
%% 
%% <tr> <td>{@section db_open}</td> <td></td> <td>ok | fail</td>
%% <td>call {@link db_interface:db_open_tp/1}</td> </tr>
%% 
%% <tr> <td>{@section @{db_write, Triple@}}</td> <td>{@type
%% Triple::query_node:qn_triple()}</td> <td>ok | fail</td>
%% <td>call {@link db_interface:db_write/1}</td> </tr>
%% 
%% <tr> <td>{@section db_close}</td> <td></td> <td>ok | fail</td>
%% <td>call {@link db_interface:db_close/0}</td> </tr>
%% 
%% </table>
%% 
%% === db_init ===
%% 
%% This request initializes and opens a triple table. All data will be
%% deleted. It is implemented by {@link hc_db_init/0}. These requests
%% are typically sent from {@link triple_distributor} for executing
%% store_stream messages. (LINK: {@section db_init})
%% 
%% === db_open ===
%% 
%% This request opens a triple table. It is implemented by {@link
%% hc_db_open/0}. These requests are typically sent from {@link
%% triple_distributor} for executing store_stream messages. (LINK:
%% {@section db_open})
%% 
%% === {db_write, Triple} ===
%% 
%% This request writes a triple to the opened triple table. It is
%% implemented by {@link hc_db_write/1}. These requests are typically
%% sent from {@link triple_distributor} for executing store_stream
%% messages. (LINK: {@section db_open})
%% 
%% === db_close ===
%% 
%% This request closes the opened triple table. It is implemented by
%% {@link hc_db_close/0}. These requests are typically sent from
%% {@link triple_distributor} for executing store_stream
%% messages. (LINK: {@section db_open})
%% 
%% == handle_cast (asynchronous) message API ==
%% 
%% (LINK: {@section handle_cast (asynchronous) message API})
%% 
%% <table border="3">
%% 
%% <tr> <th>Message</th> <th>Args</th> <th>Returns</th>
%% <th>Description</th> </tr>
%% 
%% <tr> <td></td> <td></td> <td></td> <td></td> </tr>
%% 
%% </table>
%% 
%% @type dw_state() = maps:map().
%% 
-module(db_writer).
-behavior(gen_server).
-include_lib("eunit/include/eunit.hrl").
-export(
   [
    child_spec/0,
    init/1, handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3
   ]).

%% ======================================================================
%% 
%% api
%% 

%% 
%% @doc Return child spec for this process. It can be used in
%% supervisor:init/0 callback implementation.
%% 
%% @spec child_spec() -> supervisor:child_spec()
%% 
child_spec() ->
    Id        = db_writer,
    GSOpt     = [{local, Id}, Id, [], []],
    StartFunc = {gen_server, start_link, GSOpt},
    Restart   = permanent,
    Shutdwon  = 1000,
    Type      = worker,
    Modules   = [db_writer],

    {Id, StartFunc, Restart, Shutdwon, Type, Modules}.

%% ======================================================================
%% 
%% gen_server behavior
%% 

%% 
%% init/1
%% 
%% @doc Initialize a triple_distributor process.
%% 
%% @spec init([]) -> {ok, dw_state()}
%% 
init([]) ->
    process_flag(trap_exit, true),

    State = #{
      created          => true,
      pid              => self(),
      self             => {db_writer, node()},
      opened_db        => false,
      start_date_time  => calendar:local_time()
     },

    info_msg(init, [], State, -1),
    {ok, State}.

%% 
%% handle_call/3
%% 
%% @doc Handle synchronous query requests.
%% 
%% @spec handle_call(term(), pid(), dw_state()) -> {reply, term(),
%% dw_state()}
%% 
handle_call({db_write, Triple}, _, State) ->
    hc_restore_pd(erlang:get(created), State),
    {reply, hc_db_write(Triple), State};

handle_call(db_init, _, State) ->
    hc_restore_pd(erlang:get(created), State),
    {reply, hc_db_init(), hc_save_pd()};

handle_call(db_open, _, State) ->
    hc_restore_pd(erlang:get(created), State),
    {reply, hc_db_open(erlang:get(opened_db)), hc_save_pd()};

handle_call(db_close, _, State) ->
    {reply, hc_db_close(), State};

%% default
handle_call(Request, From, State) ->
    R = {unknown_request, Request},
    error_msg(handle_call, [Request, From, State], R),
    {reply, R, State}.

%% 
%% handle_cast/2
%% 
%% @doc Handle asynchronous query requests. 
%% 
%% @spec handle_cast(term(), dw_state()) -> {noreply, dw_state()}
%% 

%% default
handle_cast(Request, State) ->
    R = {unknown_request, Request},
    error_msg(handle_cast, [Request, State], R),
    {noreply, State}.

%% 
%% @doc Restore process dictionaries from state map structure.
%% 
%% @spec hc_restore_pd([{atom(), term()}] | undefined, dw_state()) -> ok
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
%% @spec hc_save_pd() -> dw_state()
%% 
hc_save_pd() ->
    maps:from_list(get()).

%% 
%% handle_info/2
%% 
%% @doc Handle exceptional query requests. 
%% 
%% @spec handle_info(term(), dw_state()) -> {noreply, dw_state()}
%% 
handle_info(_Info, State) ->
    {noreply, State}.

%% 
%% terminate/2
%% 
%% @doc Process termination. 
%% 
%% @spec terminate(term(), dw_state()) -> none()
%% 
terminate(Reason, State) ->
    info_msg(terminate, [Reason, State], terminate_normal, 0),
    ok.

%% 
%% code_change/3
%% 
%% @doc Process code change action. 
%% 
%% @spec code_change(term(), dw_state(), term()) -> {ok, dw_state()}
%% 
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ======================================================================
%% 
%% handle call/cast implementation
%% 

%% 
%% @doc This function initializes and opens a triple table.
%% 
%% @spec hc_db_init() -> ok | fail
%% 
hc_db_init() ->
    info_msg(hc_db_init, [], entered, 50),

    Tab = db_interface:dot_get_tn(),
    put(tablename, Tab),

    R1 = db_interface:db_init(),
    R2 = db_interface:db_add_index(),
    case {R1, R2} of
	{ok, ok} -> ok;
	_        -> fail
    end.

%% 
%% @doc This function opens a triple table.
%% 
%% @spec hc_db_open(boolean()) -> ok | fail
%% 
hc_db_open(true) ->
    info_msg(hc_db_open, [true], already_opened_db, 50);

hc_db_open(A) ->
    info_msg(hc_db_open, [A], entered, 50),

    Tab = db_interface:dot_get_tn(),
    put(tablename, Tab),
    put(opened_db, true),

    TP = {"_:",  "_:sbj", "_:prd", "_:obj"},
    db_interface:db_open_tp(TP).

%% 
%% @doc This function writes a triple to a triple table.
%% 
%% @spec hc_db_write(Triple::query_node:qn_triple()) -> ok | fail
%% 
hc_db_write(Triple) ->
    Id  = element(1, Triple),
    Sbj = element(2, Triple),
    Prd = element(3, Triple),
    Obj = element(4, Triple),
    TR  = {get(tablename), Id, Sbj, Prd, Obj},

    info_msg(hc_db_write, [Triple], {db_interface_db_write, TR}, 80),

    db_interface:db_write(TR).

%% 
%% @doc This function closes a triple table.
%% 
%% @spec hc_db_close() -> ok | fail
%% 
hc_db_close() ->
    info_msg(hc_db_close, [], closing, 50),

    put(opened_db, false),
    db_interface:db_disconnect().

%% ======================================================================
%% 
%% utility
%% 

%% 
%% @doc Report an error issue to the error_logger.
%% 
%% @spec error_msg(atom(), term(), term()) -> ok
%% 
error_msg(FunName, Argument, Result) ->
    node_state:error_msg(?MODULE, FunName, Argument, Result).

%% 
%% @doc Report an information issue to the error_logger if current
%% debug level is greater than ThresholdDL.
%% 
%% @spec info_msg(atom(), term(), term(), integer()) -> ok
%% 
info_msg(FunName, Argument, Result, ThresholdDL) ->
    node_state:info_msg(?MODULE, FunName, Argument, Result, ThresholdDL).

%% ======================================================================
%% 
%% @doc Unit tests.
%% 
dw_test_() ->
    application:load(b3s),
    dwt_site(b3s_state:get(test_mode)).

dwt_site(local1) ->
    {inorder,
     [
      ?_assertMatch(ok,        b3s:start()),
      ?_assertMatch(ok,        b3s:stop())
     ]};

dwt_site(_) ->
    [].

%% ====> END OF LINE <====

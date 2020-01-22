%%
%% Manage global resources of one node.
%%
%% @copyright 2014-2019 UP FAMNIT and Yahoo Japan Corporation
%% @version 0.3
%% @since August, 2014
%% @author Kiyoshi Nitta <knitta@yahoo-corp.jp>
%% 
%% @see b3s
%% @see b3s_state
%% @see user_interface
%% 
%% @doc Manage global resources of one node. This is a gen_server
%% process running on each node that belongs to the distributed
%% big3store system.
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
%% <tr> <td>b3s_state_pid</td> <td>{@type ns_pid()}</td> <td>global id
%% of b3s state process. (not cloned)</td> </tr>
%% 
%% <tr> <td>clm_row_conf</td> <td>maps:map()</td> <td>mapping from
%% {@type node_state:ns_column_id()} to {@type
%% node_state:ns_rows()}.</td> </tr>
%% 
%% <tr> <td>pred_clm</td> <td>maps:map()</td> <td>mapping from {@link
%% tp_query_node:qn_predicate()} to {@type
%% node_state:ns_column_id()}.</td> </tr>
%% 
%% <tr> <td>tree_id_format</td> <td>string()</td> <td>format string
%% for generating {@link query_tree:qt_id()}.</td> </tr>
%% 
%% <tr> <td>tree_id_current</td> <td>integer()</td> <td>seed integer
%% for generating {@link query_tree:qt_id()}.</td> </tr>
%% 
%% <tr> <td>start_date_time</td> <td>calendar:datetime()</td>
%% <td>started date and time of the process.</td> </tr>
%% 
%% <tr> <td>update_date_time</td> <td>calendar:datetime()</td>
%% <td>updated date and time of process properties.</td> </tr>
%% 
%% <tr> <td>db_interface_cursor</td> <td>{@type
%% db_interface:di_bdbnif_cursor()}</td> <td>session information for a
%% table.</td> </tr>
%% 
%% <tr> <td>ui_queued_jobs</td> <td>[{@type
%% user_interface:ui_job_tuple()}]</td> <td>a list of running
%% jobs.</td> </tr>
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
%% <tr> <td>{@section @{get, PropertyName@}}</td> <td>atom()</td>
%% <td>term() | undefined</td> <td>get value</td> </tr>
%% 
%% <tr> <td>{@section @{put, PropertyName, Value@}}</td> <td>atom(),
%% term()</td> <td>ok | {error, term()}</td> <td>put value</td> </tr>
%% 
%% <tr> <td>{@section @{perform_ui, UIStatement@}}</td>
%% <td>[string()]</td> <td>string()</td> <td>perform a statement</td>
%% </tr>
%% 
%% <tr> <td>{@section next_message}</td> <td></td> <td>string() |
%% no_message</td> <td>obtain a result message</td> </tr>
%% 
%% </table>
%% 
%% === {get, PropertyName} ===
%% 
%% This message gets a value of the specified property. It takes
%% PropertyName::atom() as an argument and returns the value
%% Result::term() of the specified global property managed by the
%% node_state process. It returns undefined, if the property was not
%% defined yet. (LINK: {@section @{get, PropertyName@}})
%% 
%% === {put, PropertyName, Value} ===
%% 
%% This message puts the specified value into the specified
%% property. It takes PropertyName::atom() and Value::term() as
%% arguments. If it successfully puts the value to the property, it
%% returns ok. Otherwise, it returns {error, Reason::term()}. (LINK:
%% {@section @{put, PropertyName, Value@}})
%% 
%% === {perform_ui, UIStatement} ===
%% 
%% This message performs the specified {@link user_interface}
%% statement. It takes UIStatement::[string()] as arguments. While the
%% user interface usually takes a space delimited string as an input
%% statement, this message takes a list of strings splitted by
%% spaces. It returns a result string if the statement was
%% successfully executed within 5 seconds. Otherwise, it returns the
%% message: 'performing job. try later: gc &lt;node&gt; NS
%% next_message'. The result could be obtained calling {@section
%% next_message} API later in the latter case. (LINK: {@section
%% @{perform_ui, UIStatement@}})
%% 
%% === next_message ===
%% 
%% This message obtains the next result message of the {@link
%% user_interface} statement that had been executed by calling
%% {@section @{perform_ui, UIStatement@}} API. It returns the result
%% string if the result message arrived within 1 seconds. Otherwise,
%% it returns an atom 'no_message'. (LINK: {@section next_message})
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
%% @type ns_column_id() = integer(). A column identifier of natual
%% number.
%% 
%% @type ns_rows() = maps:map(). A mapping object from {@type
%% ns_row_id()} to node().
%% 
%% @type ns_row_id() = integer(). A row identifier of natual number.
%% 
%% @type ns_node_location() = {ns_column_id(), ns_row_id()}. Column
%% and row representation of a node location. It can be resolved to
%% node() using the map structure obtained from clm_row_conf
%% property. (See {@section property list})
%% 
%% @type ns_pid() = {atom(), node()}. A process identifier in
%% distributed environment. The first element atom() must be a locally
%% registered name of pid().
%% 
%% @type ns_pid_list() = [{atom(), node()}]. List of {@type ns_pid()}.
%% 
%% @type ns_state() = maps:map().
%% 
-module(node_state).
-behavior(gen_server).
-include_lib("eunit/include/eunit.hrl").
-export(
   [
    child_spec/0,
    init/1, handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3,
    error_msg/4, info_msg/5,
    hcuqj_perform_ui/1
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
    Id        = node_state,
    GSOpt     = [{local, Id}, Id, [], []],
    StartFunc = {gen_server, start_link, GSOpt},
    Restart   = permanent,
    Shutdwon  = 1000,
    Type      = worker,
    Modules   = [node_state],

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
%% @spec init([]) -> {ok, ns_state()}
%% 
init([]) ->
    process_flag(trap_exit, true),

    NS = list_to_atom(lists:nth(1, string:split(atom_to_list(node()), "@"))),
    State = #{
	      created          => true,
	      pid              => self(),
	      node_state_pid   => {node_state, node()},
	      start_date_time  => calendar:local_time(),
	      update_date_time => calendar:local_time(),
	      tree_id_current  => 0,
	      ui_queued_jobs   => [{NS, 'boot-fs'}]
     },

    info_msg(?MODULE, init, [], State, -1),
    {ok, State}.

%% 
%% handle_call/3
%% 
%% @doc Handle synchronous query requests.
%% 
%% @spec handle_call(term(), pid(), ns_state()) -> {reply, term(),
%% ns_state()}
%% 

handle_call({get, all}, _, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    {reply, erlang:get(), State};

handle_call({get, PropertyName}, _, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    {reply, erlang:get(PropertyName), State};

handle_call({put, PropertyName, Value}, From, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hcp_check_ui_queued_jobs(PropertyName),
    erlang:put(PropertyName, Value),
    erlang:put(update_date_time, calendar:local_time()),
    NewState = hc_save_pd(),
    A = [{put, PropertyName, Value}, From, State],
    info_msg(?MODULE, handle_call, A, NewState, 50),
    {reply, ok, NewState};

handle_call(next_message, _, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    R = receive
	    M -> M
	after
	    1000 -> no_message
	end,
    {reply, R, State};

handle_call({perform_ui, UICommands}, _, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    case erlang:get(front_server_nodes) of
	undefined ->
	    {ok, BSN} = application:get_env(b3s, b3s_state_nodes);
	FSN ->
	    BSN = [lists:nth(1, FSN)]
    end,
    [C1 | CR] = UICommands,
    C1A = list_to_atom(C1),
    SF = self(),
    FI = fun () ->
		 erlang:put(cmd_params, CR),
		 erlang:put(b3s_state_nodes, BSN),
		 SF ! user_interface:ui_interpret(C1A)
	 end,
    spawn(FI),
    PJ = "performing job. try later: gc <node> NS next_message\n",
    FR = fun F(S) ->
		 receive
		     {'$gen_call', From, Mes} ->
			 {reply, R, NS} = handle_call(Mes, From, S),
			 gen_server:reply(From, R),
			 F(NS);
		     R ->
			 {R, S}
		 after
		     5000 ->
			 {PJ, S}
		 end
	 end,
    {Result, NewState} = FR(State),
    {reply, Result, NewState};

%% default
handle_call(Request, From, State) ->
    R = {unknown_request, Request},
    error_msg(?MODULE, handle_call, [Request, From, State], R),
    {reply, R, State}.

hcp_check_ui_queued_jobs(ui_queued_jobs) ->
    JL = ['boot-fs', 'boot-ds', 'load-ds', 'terminate-ds'],
    case erlang:get(front_server_nodes) of
	undefined ->
	    {ok, BSN} = application:get_env(b3s, b3s_state_nodes);
	FSN ->
	    BSN = [lists:nth(1, FSN)]
    end,
    BS = {b3s_state, lists:nth(1, BSN)},
    NS = {node_state, node()},
    F = fun (J) -> 
		erlang:put(b3s_state_nodes, BSN),
		erlang:put(b3s_state_pid, BS),
		erlang:put(node_state_pid, NS),
		case user_interface:ui_batch_complete_queue(J) of
		    completed ->
			hcuqj_perform(J);
		    _ ->
			ok
		end
	end,
    spawn(lists, map, [F, JL]);
hcp_check_ui_queued_jobs(_) ->
    ok.

hcuqj_perform('boot-fs') ->
    GC = {get, ui_run_command_boot_fs},
    SL = gen_server:call(erlang:get(b3s_state_pid), GC),
    lists:map(fun hcuqj_perform_ui/1, SL);
hcuqj_perform('boot-ds') ->
    GC = {get, ui_run_command_boot_ds},
    SL = gen_server:call(erlang:get(b3s_state_pid), GC),
    lists:map(fun hcuqj_perform_ui/1, SL);
hcuqj_perform('load-ds') ->
    GC = {get, ui_run_command_load_ds},
    SL = gen_server:call(erlang:get(b3s_state_pid), GC),
    lists:map(fun hcuqj_perform_ui/1, SL);
hcuqj_perform('terminate-ds') ->
    GC = {get, ui_run_command_terminate_ds},
    SL = gen_server:call(erlang:get(b3s_state_pid), GC),
    lists:map(fun hcuqj_perform_ui/1, SL);
hcuqj_perform(_) ->
    unknown_job.

hcuqj_perform_ui(Statement) ->
    NS = erlang:get(node_state_pid),
    S = string:split(Statement, " ", all),
    case (catch gen_server:call(NS, {perform_ui, S})) of
	{'EXIT', E} ->
	    error_msg(node_state, hcuqj_perform_ui, [Statement], E),
	    R = io_lib:format("process ~p is busy.\n", [NS]);
	R -> R
    end,
    info_msg(node_state, hcuqj_perform_ui, [Statement], R, 50).

%% 
%% handle_cast/2
%% 
%% @doc Handle asynchronous query requests. 
%% 
%% @spec handle_cast(term(), ns_state()) -> {noreply, ns_state()}
%% 

%% default
handle_cast(Request, State) ->
    R = {unknown_request, Request},
    error_msg(?MODULE, handle_cast, [Request, State], R),
    {noreply, State}.

%% 
%% @doc Restore process dictionaries from state map structure.
%% 
%% @spec hc_restore_pd([{atom(), term()}] | undefined, ns_state()) -> ok
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
%% @spec hc_save_pd() -> ns_state()
%% 
hc_save_pd() ->
    maps:from_list(get()).

%% 
%% handle_info/2
%% 
%% @doc Handle exceptional query requests. 
%% 
%% @spec handle_info(term(), ns_state()) -> {noreply, ns_state()}
%% 
handle_info(_Info, State) ->
    {noreply, State}.

%% 
%% terminate/2
%% 
%% @doc Process termination. 
%% 
%% @spec terminate(term(), ns_state()) -> none()
%% 
terminate(Reason, State) ->
    info_msg(?MODULE, terminate, [Reason, State], terminate_normal, 0),
    ok.

%% 
%% code_change/3
%% 
%% @doc Process code change action. 
%% 
%% @spec code_change(term(), ns_state(), term()) -> {ok, ns_state()}
%% 
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ======================================================================
%% 
%% handle call/cast implementation
%% 

%% ======================================================================
%% 
%% utility
%% 

%% 
%% @doc Report an error issue to the error_logger.
%% 
%% @spec error_msg(atom(), atom(), term(), term()) -> ok
%% 
error_msg(ModName, FunName, Argument, Result) ->
    Arg = [ModName, FunName, Argument, Result],
    Fmt = "~w:~w(~p): ~p.~n",
    error_logger:error_msg(Fmt, Arg).

%% 
%% @doc Report an information issue to the error_logger if current
%% debug level is greater than ThresholdDL.
%% 
%% @spec info_msg(atom(), atom(), term(), term(), integer()) -> ok
%% 
info_msg(ModName, FunName, Argument, Result, ThresholdDL) ->
    Arg = [os:timestamp(), ModName, FunName, Argument, Result],
    Fmt = "timestamp: ~w~n~w:~w(~p): ~p.~n",
    im_cond(Fmt, Arg, ThresholdDL).

%% 
%% @doc Report an information issue to the error_logger with specified
%% format and argument if current debug level is greater than
%% ThresholdDL.
%% 
%% @spec im_cond(atom(), term(), integer()) -> ok
%% 
im_cond(Format, Argument, ThresholdDL) ->
    {ok, DL} = application:get_env(b3s, debug_level),
    im_cond(Format, Argument, DL, ThresholdDL).

im_cond(Format, Argument, DL, TDL) when DL > TDL ->
    error_logger:info_msg(Format, Argument);
im_cond(_, _, _, _) ->
    ok.

%% ======================================================================
%% 
%% @doc Unit tests.
%% 
ns_test_() ->
    application:load(b3s),
    nt_site(b3s_state:get(test_mode)).

nt_site(local2) ->
    NDS    = node(),
    NodStr = atom_to_list(NDS),
    NDC    = list_to_atom("b3ss02" ++ string:sub_string(NodStr, 7)),
    NSS    = node_state,
    NSC    = {node_state, NDC},
    BSS    = {b3s_state, NDS},
    BSC    = {b3s_state, NDC},
    CRC    = clm_row_conf,
    BSP    = b3s_state_pid,
    UND    = undefined,

    RMS = #{1 => NDS},
    RMC = #{1 => NDC},
    CM1 = #{1 => RMS, 2 => RMC},
    R01 = [NDS, NDC],

    {inorder,
     [
      ?_assertMatch(ok,  b3s:start()),
      ?_assertMatch(ok,  b3s:bootstrap()),
      ?_assertMatch(UND, gen_server:call(NSS, {get, qwe})),
      ?_assertMatch(ok,  gen_server:call(NSS, {put, qwe, asd})),
      ?_assertMatch(asd, gen_server:call(NSS, {get, qwe})),
      ?_assertMatch(UND, gen_server:call(NSC, {get, qwe})),
      ?_assertMatch(ok,  gen_server:call(NSC, {put, qwe, asd})),
      ?_assertMatch(asd, gen_server:call(NSC, {get, qwe})),
      ?_assertMatch(ok,  gen_server:call(NSC, {put, BSP, UND})),

      ?_assertMatch(UND, gen_server:call(NSC, {get, BSP})),
      ?_assertMatch(ok,  gen_server:call(BSS, {put, CRC, CM1})),
      ?_assertMatch(R01, gen_server:call(BSS, propagate)),
      ?_assertMatch(BSS, gen_server:call(NSS, {get, BSP})),
      ?_assertMatch(BSS, gen_server:call(NSC, {get, BSP})),

      ?_assertMatch(ok,  gen_server:call(BSS, {clone, NDC})),
      ?_assertMatch(R01, gen_server:call(BSC, propagate)),
      ?_assertMatch(BSC, gen_server:call(NSS, {get, BSP})),
      ?_assertMatch(BSC, gen_server:call(NSC, {get, BSP})),

      ?_assertMatch(ok,        b3s:stop())
     ]};

nt_site(_) ->
    [].

%% ====> END OF LINE <====

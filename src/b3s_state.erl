%%
%% Manage global resources of distributed b3s system.
%%
%% @copyright 2014-2019 UP FAMNIT and Yahoo Japan Corporation
%% @version 0.3
%% @since August, 2014
%% @author Kiyoshi Nitta <knitta@yahoo-corp.jp>
%% 
%% @see b3s
%% @see node_state
%% @see user_interface
%% 
%% @doc Manage global resources of distributed b3s system. This is a
%% gen_server process that runs one instance in a distributed b3s
%% system. Typically, this process will be invoked on a front server
%% node of the distributed big3store system.
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
%% See <a href='b3s.html#environment_variables'> b3s application
%% configuration properties </a>. Some of the definitions are copied
%% to this process. Following properties includes them.
%% 
%% <table border="3">
%% <tr><th>Name</th><th>Type</th><th>Description</th></tr>
%% 
%% <tr> <td>created</td> <td>boolean()</td> <td>true denotes that
%% process dictionary was created and used. false denotes that
%% completely new process.</td> </tr>
%% 
%% <tr> <td>pid</td> <td>pid()</td> <td>local id of this process. (not
%% cloned)</td> </tr>
%% 
%% <tr> <td>b3s_state_pid</td> <td>{@type node_state:ns_pid()}</td>
%% <td>global id of this process. (not cloned) (propagated)</td> </tr>
%% 
%% <tr> <td>start_date_time</td> <td>calendar:datetime()</td>
%% <td>started date and time of the process. (not cloned)</td> </tr>
%% 
%% <tr> <td>clone_date_time</td> <td>calendar:datetime()</td>
%% <td>cloned date and time of the process.</td> </tr>
%% 
%% <tr> <td>update_date_time</td> <td>calendar:datetime()</td>
%% <td>updated date and time of process properties.</td> </tr>
%% 
%% <tr> <td>push_prop_list</td> <td>[atom()]</td> <td>properties to be
%% distributed to {@link node_state}.</td> </tr>
%% 
%% <tr> <td>clm_row_conf</td> <td>maps:map()</td> <td>mapping from
%% {@type node_state:ns_column_id()} to {@type
%% node_state:ns_rows()}. (propagated)</td> </tr>
%% 
%% <tr> <td>num_of_empty_msgs</td> <td>integer()</td> <td>the number
%% of empty messages that can be sent at once.</td> </tr>
%%
%% <tr> <td>name_of_triple_tables</td> <td>[{node(), atom()}]</td>
%% <td>names of triple tables for data server columns.</td> </tr>
%%
%% <tr> <td>name_of_pred_clm_table</td> <td>atom()</td> <td>name of
%% pred_clm table.</td> </tr>
%%
%% <tr> <td>name_of_pred_freq_table</td> <td>atom()</td> <td>name of
%% pred_freq table.</td> </tr>
%%
%% <tr> <td>name_of_string_id_table</td> <td>atom()</td> <td>name of
%% string id conversion table.</td> </tr>
%%
%% <tr> <td>store_report_frequency</td> <td>integer()</td>
%% <td>report frequency of storing process.</td> </tr>
%%
%% <tr> <td>triple_id_skel</td> <td>string()</td> <td>skeleton string
%% for generating triple id.</td> </tr>
%%
%% <tr> <td>triple_distributor_pid</td> <td>{@type
%% node_state:ns_pid()} | undefined</td> <td>id of the
%% triple_distributor process. (propagated)</td> </tr>
%% 
%% <tr> <td>pred_clm</td> <td>maps:map()</td> <td>mapping from {@link
%% tp_query_node:qn_predicate()} to {@type
%% node_state:ns_column_id()}. (propagated)</td> </tr>
%% 
%% <tr> <td>pred_freq</td> <td>maps:map()</td> <td>mapping from {@link
%% tp_query_node:qn_predicate()} to Frequency::integer().</td> </tr>
%% 
%% <tr> <td>front_server_nodes</td> <td>[atom()]</td> <td>list of
%% front server node names. (propagated)</td> </tr>
%% 
%% <tr> <td>benchmark_task</td> <td>atom()</td> <td>name of executing
%% benchmark task.</td> </tr>
%% 
%% <tr> <td>epgsql_host</td> <td>string()</td> <td>host name of
%% postgres used in {@link db_interface}.</td> </tr>
%% 
%% <tr> <td>epgsql_user</td> <td>string()</td> <td>user name to access
%% postgres used in {@link db_interface}.</td> </tr>
%% 
%% <tr> <td>epgsql_pass</td> <td>string()</td> <td>password to access
%% postgres used in {@link db_interface}.</td> </tr>
%% 
%% <tr> <td>result_record_max</td> <td>integer()</td> <td>max number
%% of records to be reported.</td> </tr>
%%
%% <tr> <td>aws_node_instance_map</td> <td>maps:map()</td> <td>mapping
%% from node to <a
%% href='https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-lifecycle.html'>AWS
%% EC2 instance</a> id.</td> </tr>
%%
%% <tr> <td>aws_node_fleet_map</td> <td>maps:map()</td> <td>mapping
%% from node to <a
%% href='https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-spot-instances.html'>AWS
%% EC2 spot fleet</a> id.</td> </tr>
%%
%% <tr> <td>ui_run_command_boot_fs</td> <td>[{@type
%% user_interface:ui_statement()}]</td> <td>user interface statements
%% called after booting the front server.</td> </tr>
%%
%% <tr> <td>ui_run_command_boot_ds</td> <td>[{@type
%% user_interface:ui_statement()}]</td> <td>user interface statements
%% called after booting all data servers.</td> </tr>
%%
%% <tr> <td>ui_run_command_load_ds</td> <td>[{@type
%% user_interface:ui_statement()}]</td> <td>user interface statements
%% called after loading table data on all data servers.</td> </tr>
%%
%% <tr> <td>ui_run_command_finish_benchmark</td> <td>[{@type
%% user_interface:ui_statement()}]</td> <td>user interface statements
%% called after finishing a benchmark batch invoked by batch_benchmark
%% command.</td> </tr>
%%
%% <tr> <td>lock_node</td> <td>[{@type pid()}]</td> <td>lock
%% state.</td> </tr>
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
%% <tr> <td>{@section @{clone, Node@}}</td> <td>node()</td> <td>ok |
%% {error, term()}</td> <td>clone the process</td> </tr>
%% 
%% <tr> <td>{@section propagate}</td> <td></td> <td>[node()] | {error,
%% term()}</td> <td>propergate some properties</td> </tr>
%% 
%% <tr> <td>{@section lock}</td> <td></td> <td>ok | {locked_by,
%% pid()}</td> <td>lock all accesses</td> </tr>
%% 
%% <tr> <td>{@section unlock}</td> <td></td> <td>ok | {locked_by,
%% pid()} | no_lock</td> <td>unlock all accesses</td> </tr>
%% 
%% </table>
%% 
%% === {get, PropertyName} ===
%% 
%% This message takes PropertyName::atom() as an argument and returns
%% the value Result::term() of the specified global property managed
%% by the b3s_state process. It returns undefined, if the property was
%% not defined yet. (LINK: {@section @{get, PropertyName@}})
%% 
%% === {put, PropertyName, Value} ===
%% 
%% This message takes PropertyName::atom() and Value::term() as
%% arguments. If it successfully puts the value to the property, it
%% returns ok. Otherwise, it returns {error, Reason::term()}. (LINK:
%% {@section @{put, PropertyName, Value@}})
%% 
%% === {clone, Node} ===
%% 
%% This message takes Node::node() as an argument. If it successfully
%% invoke a clone process on the Node, it returns ok. Otherwise, it
%% returns {error, Reason::term()}. Because it always use 'b3s_state'
%% as local process id, Node must be different from the node, on which
%% currently running b3s_state process exists. If a b3s_state process
%% was already running on the node, it simply copies properties. Some
%% properties will not copied because of its locality (See {{@section
%% property list}}). (LINK: {@section @{clone, Node@}})
%% 
%% === propagate ===
%% 
%% This message propagates properties listed in 'push_prop_list'
%% property to nodes obtained from 'clm_row_conf' and
%% 'front_server_nodes' property. It returns a list of nodes, to which
%% it succeeded copying the properties.  (LINK: {@section propagate})
%% 
%% === lock ===
%% 
%% This message locks all accesses to the calling node. (LINK:
%% {@section lock} {@link hc_lock/1})
%% 
%% === unlock ===
%% 
%% This message locks all accesses to the calling node. (LINK:
%% {@section unlock} {@link hc_unlock/1})
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
%% @type bs_state() = maps:map().
%% 
-module(b3s_state).
-behavior(gen_server).
-include_lib("eunit/include/eunit.hrl").
-export(
   [
    get/1, put/2, sync_app_env/0, hpp_get_nodes/1,
    child_spec/0,
    init/1, handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3,
    hc_monitor_mq/1
   ]).

%% ======================================================================
%% 
%% api
%% 

%% 
%% @doc Get property.
%% 
%% @deprecated Please use {@section handle_call (synchronous) message API}.
%% 
%% @spec get(PropName::atom()) -> PropValue::term() | undefined
%% 
get(PropName) ->
    get_perform(application:get_env(b3s, PropName)).

get_perform({ok, PropValue}) ->
    PropValue;
get_perform(undefined) ->
    undefined.

%% 
%% @doc Put a value to a property.
%% 
%% @deprecated Please use {@section handle_call (synchronous) message API}.
%% 
%% @spec put(PropName::atom(), PropValue::term()) -> ok
%% 
put(PropName, PropValue) ->
    application:set_env(b3s, PropName, PropValue).

%% 
%% @doc Synchronize all application environment properties on
%% configured nodes.
%% 
%% @deprecated Please use {@section handle_call (synchronous) message API}.
%% 
%% @spec sync_app_env() -> ok | {error, Reason}
%% 
sync_app_env() ->
    sae_find_tp(b3s_state:get(triple_distributor)).

sae_find_tp(undefined) ->
    E = triple_distributor_must_be_invoked,
    error_msg(sae_find_tp, [undefined], E),
    {error, E};
sae_find_tp(TP) ->
    CRCMap = gen_server:call(TP, {get_property, clm_row_conf}),
    sae_crc(maps:size(CRCMap), CRCMap).

sae_crc(0, CRCMap) ->
    E = {no_column_configured, CRCMap},
    error_msg(sae_crc, [0, CRCMap], E),
    {error, E};
sae_crc(_, CRCMap) ->
    F1 =
	fun (X) ->
		TRMap = maps:get(X, CRCMap),
		F2 = fun (Y) -> maps:get(Y, TRMap) end,
		L = lists:map(F2, maps:keys(TRMap)),
		lists:map(fun sae_update_node/1, L)
	end,
    sae_confirm(lists:flatten(lists:map(F1, maps:keys(CRCMap)))).

sae_update_node(Node) ->
    F = fun ({P, V}) ->
		rpc:call(Node, application, set_env, [b3s, P, V]),
		{updated, P, V}
	end,
    R = lists:map(F, application:get_all_env(b3s)),
    info_msg(sae_update_node, [Node], R, 80),
    {updated, Node}.

sae_confirm([]) ->
    {error, no_node_updated};
sae_confirm(A) ->
    info_msg(sae_confirm, [A], successfully_updated, 10).

%% 
%% @doc Return child spec for this process. It can be used in
%% supervisor:init/0 callback implementation.
%% 
%% @spec child_spec() -> supervisor:child_spec()
%% 
child_spec() ->
    Id        = b3s_state,
    GSOpt     = [{local, Id}, Id, [], []],
    StartFunc = {gen_server, start_link, GSOpt},
    Restart   = permanent,
    Shutdwon  = 1000,
    Type      = worker,
    Modules   = [b3s_state],

    {Id, StartFunc, Restart, Shutdwon, Type, Modules}.

%% ======================================================================
%% 
%% gen_server behavior
%% 

%% 
%% init/1
%% 
%% @doc Initialize a b3s_state process.
%% 
%% @spec init([]) -> {ok, bs_state()}
%% 
init([]) ->
    process_flag(trap_exit, true),

    SklMap = #{
      created          => true,
      pid              => self(),
      b3s_state_pid    => {b3s_state, node()},
      start_date_time  => calendar:local_time(),
      clone_date_time  => calendar:local_time(),
      update_date_time => calendar:local_time(),
      lock_node        => [],
      push_prop_list   => [
			   b3s_state_pid,
			   triple_distributor_pid,
			   clm_row_conf,
			   pred_clm,
			   front_server_nodes
			  ]
     },
    OmitKeys =
	[
	 column_id,
	 debug_level,
	 b3s_mode,
	 front_server_node,
	 included_applications,
	 test_mode
	],
    AppMap = maps:from_list(application:get_all_env(b3s)),
    State  = maps:without(OmitKeys, maps:merge(AppMap, SklMap)),

    info_msg(init, [], State, -1),
    {ok, State}.

%% 
%% handle_call/3
%% 
%% @doc Handle synchronous query requests.
%% 
%% @spec handle_call(term(), pid(), bs_state()) -> {reply, term(),
%% bs_state()}
%% 

handle_call({get, all}, _, State) ->
    hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    {reply, erlang:get(), State};

handle_call({get, PropertyName}, _, State) ->
    hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    {reply, erlang:get(PropertyName), State};

handle_call({put, PropertyName, Value}, _, State) ->
    hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    erlang:put(PropertyName, Value),
    erlang:put(update_date_time, calendar:local_time()),
    {reply, ok, hc_save_pd()};

handle_call({clone, Node}, _, State) ->
    hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    {reply, hc_clone_process(Node), hc_save_pd()};

handle_call(propagate, _, State) ->
    hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    {reply, hc_propagate_properties(), hc_save_pd()};

handle_call(lock, From, State) ->
    hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    {reply, hc_lock(From), hc_save_pd()};

handle_call(unlock, From, State) ->
    hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    {reply, hc_unlock(From), hc_save_pd()};

%% default
handle_call(Request, From, State) ->
    R = {unknown_request, Request},
    error_msg(handle_call, [Request, From, State], R),
    {reply, R, State}.

%% 
%% hc_monitor_mq/1
%% 
%% @doc This function monitors message queue activities. It updates
%% process dictionary properties 'mq_count' and 'mq_maxlen' if true
%% was given to argument FL. Otherwise, it does nothing. Typically,
%% "erlang:get(mq_debug)" should be used as its argument for calling
%% it.
%% 
%% @spec hc_monitor_mq(FL::boolean()) -> ok
%% 
hc_monitor_mq(true) ->
    MQL = message_queue_len,
    MQC = mq_count,
    MQM = mq_maxlen,
    case erlang:get(MQC) of
	undefined ->
	    erlang:put(MQC, 0);
	C ->
	    erlang:put(MQC, C + 1)
    end,
    case ({erlang:get(MQM),
	   erlang:process_info(self(), [MQL])}) of
    	{M, [{MQL, L}]} when M < L ->
    	    erlang:put(MQM, L);
    	{undefined, [{MQL, L}]} ->
    	    erlang:put(MQM, L);
	_ -> ok
    end;
hc_monitor_mq(_) ->
    ok.

%% 
%% handle_cast/2
%% 
%% @doc Handle asynchronous query requests. 
%% 
%% @spec handle_cast(term(), bs_state()) -> {noreply, bs_state()}
%% 

%% default
handle_cast(Request, State) ->
    R = {unknown_request, Request},
    error_msg(handle_cast, [Request, State], R),
    {noreply, State}.

%% 
%% @doc Restore process dictionaries from state map structure.
%% 
%% @spec hc_restore_pd([{atom(), term()}] | undefined, bs_state()) -> ok
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
%% @spec hc_save_pd() -> bs_state()
%% 
hc_save_pd() ->
    maps:from_list(get()).

%% 
%% handle_info/2
%% 
%% @doc Handle exceptional query requests. 
%% 
%% @spec handle_info(term(), bs_state()) -> {noreply, bs_state()}
%% 
handle_info(_Info, State) ->
    {noreply, State}.

%% 
%% terminate/2
%% 
%% @doc Process termination. 
%% 
%% @spec terminate(term(), bs_state()) -> none()
%% 
terminate(Reason, State) ->
    info_msg(terminate, [Reason, State], terminate_normal, 0),
    ok.

%% 
%% code_change/3
%% 
%% @doc Process code change action. 
%% 
%% @spec code_change(term(), bs_state(), term()) -> {ok, bs_state()}
%% 
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ======================================================================
%% 
%% handle call/cast implementation
%% 

%% 
%% @doc This function invokes a clone process on the specified node.
%% 
%% @spec hc_clone_process(Node::node()) -> ok | {error, Reason::term()}
%% 
hc_clone_process(Node) ->
    B3S = {b3s, Node},
    R = supervisor:start_child(B3S, b3s_state:child_spec()),
    hcp_invoke(R, Node).

hcp_invoke({error, {already_started, _}}, Node) ->
    hcp_copy_property(Node);
hcp_invoke({error, Reason}, Node) ->
    E = {error, Reason},
    error_msg(hcp_invoke, [E, Node], E);
hcp_invoke(_, Node) ->
    hcp_copy_property(Node).

hcp_copy_one_property({'$ancestors', X}, _) ->
    {not_copied, {'$ancestors', X}};
hcp_copy_one_property({'$initial_call', X}, _) ->
    {not_copied, {'$initial_call', X}};
hcp_copy_one_property({pid, X}, _) ->
    {not_copied, {pid, X}};
hcp_copy_one_property({b3s_state_pid, X}, _) ->
    {not_copied, {b3s_state_pid, X}};
hcp_copy_one_property({start_date_time, X}, _) ->
    {not_copied, {start_date_time, X}};
hcp_copy_one_property({P, V}, F) ->

    {copied, F(P, V)}.

hcp_copy_property(Node) ->
    BS = {b3s_state, Node},
    ExecCopy =
	fun(P, V) ->
		M = {put, P, V},
		gen_server:call(BS, M)
	end,
    CopyAProp = fun(X) -> hcp_copy_one_property(X, ExecCopy) end,
    R = lists:map(CopyAProp, erlang:get()),
    info_msg(hcp_copy_property, [Node], R, 80).

%% 
%% @doc This function propagates properties listed in 'push_prop_list'
%% property to nodes obtained from 'clm_row_conf' and
%% 'front_server_nodes' property.
%% 
%% @spec hc_propagate_properties() -> [node()]
%% 
hc_propagate_properties() ->
    NLD = hpp_get_nodes(erlang:get(clm_row_conf)),
    NLS = erlang:get(front_server_nodes),
    NL  = sets:to_list(sets:from_list(lists:append(NLD, NLS))),
    hpp_copy_properties(erlang:get(push_prop_list), NL).

hpp_get_nodes(undefined) ->
    error_msg(hpp_get_nodes, [undefined], no_column_defined),
    [];
hpp_get_nodes(MapCRC) ->
    hgn_proc_column(maps:values(MapCRC), sets:new()).

hgn_proc_column([], NodeSet) ->
    sets:to_list(NodeSet);
hgn_proc_column([MapRow | Rest], NodeSet) ->
    S = sets:from_list(maps:values(MapRow)),
    hgn_proc_column(Rest, sets:union(S, NodeSet)).

hpp_get_nodes_test_() ->
    RMap01 = #{1 => node1, 2 => node2},
    RMap02 = #{1 => node3, 2 => node4},
    RMap03 = #{1 => node5, 2 => node6},
    CMap01 = #{1 => RMap01, 2 => RMap02, 3 => RMap03},
    R01    = [node1, node2, node3, node4, node5, node6],
    CMap02 = #{1 => RMap01, 3 => RMap03},
    R02    = [node1, node2, node5, node6],
    CMap03 = #{1 => RMap02, 2 => RMap02, 3 => RMap02},
    R03    = [node3, node4],

    [
     ?_assertMatch([],  hpp_get_nodes(#{})),
     ?_assertMatch(R01, hpp_get_nodes(CMap01)),
     ?_assertMatch(R02, hpp_get_nodes(CMap02)),
     ?_assertMatch(R03, hpp_get_nodes(CMap03))
    ].

%% Unit tests of hpp_copy_properties will be included in node_state
%% module.

hpp_copy_properties(undefined, NL) ->
    A = [undefined, NL],
    E = push_prop_list_undefined,
    error_msg(hpp_copy_properties, A, E),
    [];
hpp_copy_properties([], NL) ->
    A = [[], NL],
    E = push_prop_list_empty,
    error_msg(hpp_copy_properties, A, E),
    [];
hpp_copy_properties(PPL, NL) ->
    hcp_node(NL, PPL, []).

hcp_node([], PL, Succeeded) ->
    A  = [[], PL, Succeeded],
    info_msg(hcp_node, A, updated, 10),
    Succeeded;
hcp_node([Node | Rest], PL, Succeeded) ->
    A  = [[Node | Rest], PL, Succeeded],
    NS = {node_state, Node},
    UpdateOneProp =
	fun(X) ->
		case (catch gen_server:call(NS, {put, X, erlang:get(X)})) of
		    {'EXIT', E} ->
			error_msg(hcp_node, [Node | Rest], E),
			error;
		    R -> R
		end
	end,
    R = lists:map(UpdateOneProp, PL),
    info_msg(hcp_node, A, {updated, R}, 80),
    hcp_node(Rest, PL, [Node | Succeeded]).

%% 
%% @doc This message locks all accesses to the calling node if no
%% process has already obtained the lock. The locking feature is only
%% applied to processes that use this message. Calling process must
%% wait and retry if this message didn't return 'ok'. After performing
%% a sequence of accesses, the calling process must release the lock
%% by 'unlock'.
%% 
%% @spec hc_lock({pid(), Tag::atom()}) -> ok | {locked_by, pid()}
%% 
hc_lock(From) ->
    hl_check(From, erlang:get(lock_node)).

hl_check({Pid, Tag}, []) ->
    erlang:put(lock_node, [Pid]),
    info_msg(hl_check, [{Pid, Tag}, []], {locked, ok}, 50);
hl_check(A, [Pid]) ->
    R = {locked_by, Pid},
    info_msg(hl_check, [A, [Pid]], R, 80),
    R;
hl_check(From, LockNode) ->
    error_msg(hl_check, [From, LockNode], illegal_state).

%% 
%% @doc This message unlocks all accesses if the calling process has
%% already obtained the lock. It returns 'ok' if succeeded. It returns
%% '{locked_by, Pid}' where Pid is an identifier of currently locking
%% process. It returns 'no_lock' if no process obtained the lock.
%% 
%% @spec hc_unlock({pid(), Tag::atom()}) -> ok | {locked_by, pid()} |
%% no_lock
%% 
hc_unlock(From) ->
    hu_check(From, erlang:get(lock_node)).

hu_check(A, []) ->
    info_msg(hl_check, [A, []], no_lock, 80),
    no_lock;
hu_check({Pid, Tag}, [Pid]) ->
    info_msg(hl_check, [{Pid, Tag}, [Pid]], {unlocked, ok}, 50),
    erlang:put(lock_node, []),
    ok;
hu_check(From, [Node]) ->
    R = {locked_by, Node},
    info_msg(hl_check, [From, [Node]], R, 80),
    R;
hu_check(From, LockNode) ->
    error_msg(hu_check, [From, LockNode], illegal_state).

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
bs_test_() ->
    application:load(b3s),
    bt_site(b3s_state:get(test_mode)).

bt_site(local1) ->
    {inorder,
     [
      ?_assertMatch(ok, b3s:start()),
      ?_assertMatch(ok, b3s:bootstrap()),
      {generator, fun()-> btl_propagate() end},
      {generator, fun()-> btl_lock() end},
      ?_assertMatch(ok, b3s:stop())
     ]};

bt_site(local2) ->
    {inorder,
     [
      ?_assertMatch(ok, b3s:start()),
      ?_assertMatch(ok, b3s:bootstrap()),
      {generator, fun()-> btl_propagate() end},
      {generator, fun()-> btl_clone() end},
      {generator, fun()-> btl_lock() end},
      ?_assertMatch(ok, b3s:stop())
     ]};

bt_site(_) ->
    [].

btl_propagate() ->
    NDS = node(),
    BSS = {b3s_state, NDS},
    NSS = {node_state, NDS},
    CRC = clm_row_conf,

    RMS = #{1 => NDS},
    CM1 = #{1 => RMS, 2 => RMS},
    R01 = [NDS],
    FSS = front_server_nodes,

    {inorder,
     [
      ?_assertMatch(ok,  gen_server:call(BSS, {put, CRC, CM1})),
      ?_assertMatch(ok,  gen_server:call(BSS, {put, FSS, [NDS]})),
      ?_assertMatch(R01, gen_server:call(BSS, propagate)),
      ?_assertMatch(BSS, gen_server:call(NSS, {get, b3s_state_pid})),
      ?_assertMatch(NSS, gen_server:call(NSS, {get, node_state_pid})),
      ?_assertMatch(CM1, gen_server:call(NSS, {get, CRC})),
      ?_assertMatch([NDS], gen_server:call(NSS, {get, FSS}))
     ]}.

btl_clone() ->
    NDS    = node(),
    NodStr = atom_to_list(NDS),
    NDC    = list_to_atom("b3ss02" ++ string:sub_string(NodStr, 7)),
    BSS    = b3s_state,
    BSC    = {b3s_state, NDC},

    {inorder,
     [
      ?_assertMatch(undefined, gen_server:call(BSS, {get, qwe})),
      ?_assertMatch(ok,        gen_server:call(BSS, {put, qwe, asd})),
      ?_assertMatch(asd,       gen_server:call(BSS, {get, qwe})),
      ?_assertMatch(ok,        gen_server:call(BSS, {clone, NDC})),
      ?_assertMatch(asd,       gen_server:call(BSC, {get, qwe})),
      ?_assertMatch(ok,        gen_server:call(BSC, {put, qwe, zxc})),
      ?_assertMatch(zxc,       gen_server:call(BSC, {get, qwe})),
      ?_assertMatch(ok,        gen_server:call(BSC, {clone, NDS})),
      ?_assertMatch(zxc,       gen_server:call(BSC, {get, qwe}))
     ]}.

btl_lock() ->
    NDS = node(),
    BSS = {b3s_state, NDS},

    R01 = {locked_by, self()},
    R02 = no_lock,
    R03 = {locked_by, qweasd},
    {inorder,
     [
      ?_assertMatch(R02, gen_server:call(BSS, unlock)),
      ?_assertMatch(ok,  gen_server:call(BSS, lock)),
      ?_assertMatch(ok,  gen_server:call(BSS, unlock)),
      ?_assertMatch(R02, gen_server:call(BSS, unlock)),
      ?_assertMatch(ok,  gen_server:call(BSS, {put, lock_node, [qweasd]})),
      ?_assertMatch(R03, gen_server:call(BSS, unlock)),
      ?_assertMatch(ok,  gen_server:call(BSS, {put, lock_node, []})),
      ?_assertMatch(ok,  gen_server:call(BSS, lock)),
      ?_assertMatch(R01, gen_server:call(BSS, lock))
     ]}.

%% ====> END OF LINE <====

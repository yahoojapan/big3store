%%
%% Join Query Node processes
%%
%% @copyright 2014-2019 UP FAMNIT and Yahoo Japan Corporation
%% @version 0.3
%% @since May, 2014
%% @author Iztok Savnik <iztok.savnik@famnit.upr.si>
%% @author Kiyoshi Nitta <knitta@yahoo-corp.jp>
%% 
%% @doc Join query node is implemented as independent gen_process. Join query node is 
%% a state-machine realizing protocol that has incoming and outcoming messages. 
%% Each message is implemented as co-routine. 
%%
%% State-machine has the following states: inactive, active, wait_next_outer, wait_next_inner, 
%% and eos. Message start set state of protocol to active. Message eval moves state
%% to wait_next_outer. After this, state alternates between wait_next_outer and wait_next_inner. 
%% State moves to eos after end of outer streams is detected. 
%%
%% Communication with other query nodes forming query tree is realized by input and output 
%% streams. Join query node has input queues for inner and outer query nodes, and, input and 
%% output queues to comminicate with parent query node. Input queue stores messages from 
%% a child query node to be processed by join query node. Output queue stores messages to 
%% be sent to the parent. Detailed presentation of protocol used between two query nodes is 
%% given in {@link query_node}.
%%
%% <table bgcolor="lemonchiffon">
%% <tr><th>Section Index</th></tr>
%% <tr><td>{@section join algorithm}</td></tr>
%% <tr><td>{@section property list}</td></tr>
%% <tr><td>{@section handle_call (synchronous) message API}</td></tr>
%% <tr><td>{@section handle_cast (asynchronous) message API}</td></tr>
%% </table>
%% 
%% == join algorithm ==
%% 
%% (LINK: {@section join algorithm})
%% 
%% Join query node implements <i>join method</i> which is a variant of indexed nested-loop 
%% join algorithm. Join query node is independent gen_server process that can have multiple 
%% outer query nodes as well as multiple inner query nodes--each of them is implemented as 
%% separate gen_server process.
%% Since we suppose that every local triple-store indexes triple table on all possible
%% subsets of SPO, all possible access methods are supported by indexes. 
%% 
%% Algorithm of join method is defined as follows. Each graph 
%% obtained from outer query nodes causes initialization of inner query nodes using message
%% eval. Inicialization of inner query nodes uses the values of join variables obtained 
%% from outer graph. Only those graphs are retrieved from inner query nodes that match previously 
%% obtained outer graph. Each outer and inner graphs are merged into one graph which is  
%% then sent to parent query node.
%%
%% Join algorithm is implemented as join protocol that can be best descibed in terms of 
%% main loops of protocol. It includes three main loops: outer-loop, inner-loop and 
%% transport-loop. All three loops are triggered by appropriate message: data-outer, data-inner,
%% and empty messages, respectively. Each of the loops is composed of three main phases:
%% (1) storing the message in appropriate queue, (2) processing the message, and (3) 
%% checking the conditions and moving control to other loops if needed.
%%
%% == property list ==
%% 
%% (LINK: {@section property list})
%% 
%% The gen_server process uses following properties holded by {@link
%% jqn_state()}.
%% 
%% <table border="3">
%% <tr><th>Name</th><th>Type</th><th>Description</th></tr>
%% 
%% <tr> <td>created</td> <td>boolean()</td> <td>true denotes that
%% process dictionary was created and used. false denotes that
%% completely new process.</td> </tr>
%% 
%% <tr> <td>id</td> <td>string()</td> <td>query node identifier</td> </tr>
%% 
%% <tr> <td>pid</td> <td>pid()</td> <td>process id</td> </tr>
%% 
%% <tr> <td>state</td> <td>atom()</td> <td>active | inactive | wait_next_outer | 
%% wait_next_inner | eos</td> </tr>
%% 
%% <tr> <td>gp</td> <td>maps:map()</td> <td>graph represented as 
%% mapping from {@type query_node:qn_id()} to {@type query_node:qn_triple_pattern()}</td> </tr>
%% 
%% <tr> <td>select_pred</td> <td>query_node:qn_select_predicate()</td> <td>selection 
%% predicate in the form of abstract syntax tree of type {@type query_node:qn_select_predicate()}
%% </td> </tr>
%% 
%% <tr> <td>project_list</td> <td>query_node:qn_project_list()</td> <td>list of 
%% variables to be projected</td> </tr>
%% 
%% <tr> <td>project_out</td> <td>[query_node::qn_id()]</td> <td>list of 
%% query node id-s identifying triples to be projected out of resulting graph
%% </td> </tr>
%% 
%% <tr> <td>column_row</td> <td>{@link
%% triple_distributor:td_node_location()}</td> <td>location of query
%% node process</td> </tr>
%% 
%% <tr> <td>parent</td> <td>pid()</td> <td>process id of parent query
%% node</td> </tr>
%% 
%% <tr> <td>outer</td> <td>[pid()]</td> <td>process ids of outer
%% children query nodes</td> </tr>
%% 
%% <tr> <td>inner</td> <td>[pid()]</td> <td>process ids of inner
%% children query nodes</td> </tr>
%% 
%% <tr> <td>join_vars</td> <td>[{@link query_node:qn_var()}]</td> <td>List of
%% variables used for joining.</td> </tr>
%% 
%% <tr> <td>vars_pos</td> <td>maps:map()</td> <td>mapping from {@link
%% query_node:qn_var()} to {@link jqn_var_position()}</td> </tr>
%% 
%% <tr> <td>vars_values</td> <td>maps:map()</td> <td>mapping from
%% {@link query_node:qn_var()} to string() (not used)</td> </tr>
%% 
%% <tr> <td>wait</td> <td>boolean()</td> <td>indicate whether the
%% process is in wait state or not.</td> </tr>
%% 
%% <tr> <td>inner_outer</td> <td>inner | outer</td> <td> Position to
%% its parent query node.</td> </tr>
%% 
%% <tr> <td>inner_graph</td> <td>{@link query_node:qn_graph()}</td> <td>current
%% graph data from inner child</td> </tr>
%% 
%% <tr> <td>outer_graph</td> <td>{@link query_node:qn_graph()}</td> <td>current
%% graph data from outer child</td> </tr>
%% 
%% <tr> <td>state_of_outer_streams</td> <td>maps:map()</td> <td> Map
%% structure from outer child pid() to atom() (alive | eos).</td> </tr>
%% 
%% <tr> <td>empty_outer_sent</td> <td>boolean()</td> <td>N empty messages 
%% are sent to each of outer processes when eval message of join_query_node
%% is processed.</td> </tr>
%% 
%% <tr> <td>state_of_inner_streams</td> <td>maps:map()</td> <td> Map
%% structure from inner child pid() to atom() (alive | eos).</td> </tr>
%% 
%% <tr> <td>empty_inner_sent</td> <td>boolean()</td> <td>N empty messages 
%% are sent to each of inner processes after first eval message is sent 
%% to them.
%% </td> </tr>
%% 
%% <tr> <td>queue_from_outer</td> <td>queue:queue()</td> <td> Queue storing 
%% graphs from outer child query node while processing one of previous 
%% outer graphs.</td> </tr>
%% 
%% <tr> <td>queue_from_parpent</td> <td>queue:queue()</td> <td> Queue storing 
%% empty messages from parent when graph to be sent to parent is not 
%% yet available.</td> </tr>
%% 
%% <tr> <td>queue_to_parent</td> <td>queue:queue()</td> <td> Queue storing 
%% graphs (complete messages) to be sent to parent but there is no empty message 
%% available.</td> </tr>
%% 
%% <tr> <td>pause</td> <td>boolean()</td> <td>query stops evaluating
%% if true and evaluates normally if false</td> </tr>
%% 
%% <tr> <td>start_date_time</td> <td>calendar:datetime()</td>
%% <td>started date and time of the process.</td> </tr>
%% 
%% <tr> <td>b3s_state_pid</td> <td>{@type node_state:ns_pid()}</td>
%% <td>process id of b3s_state.</td> </tr>
%% 
%% <tr> <td>benchmark_task_pid</td> <td>{@type
%% node_state:ns_pid()}</td> <td>process id of executing benchmark
%% task.</td> </tr>
%% 
%% <tr> <td>result_record_max</td> <td>integer()</td> <td>Max number
%% of records to be reported.</td> </tr>
%%
%% </table>
%% 
%% == handle_call (synchronous) message API ==
%% 
%% (LINK: {@section handle_call (synchronous) message API})
%% 
%% === {start, QueryNodeId, QueryId, SessionId, Self, GraphPattern, SelectPred, ProjectList, ParentPid, OuterPids, InnerPids, VarsPositions, JoinVars} ===
%% 
%% Initialization of join query node process. All parameters are 
%% saved to process dictionary. 
%% (LINK: {@section @{start, QueryNodeId, QueryId, SessionId, Self, GraphPattern, SelectPred, ProjectList, ParentPid, OuterPids, InnerPids, VarsPositions, JoinVars@}})
%%
%% QueryNodeId is {@link query_node:qn_id()}, QueryId is string(), SessionId is string(), 
%% Self is {@link node_state:ns_pid()}, GraphPattern is {@link query_node:qn_graph_pattern()}, 
%% SelectPred is {@link query_node:qn_select_predicate()}, 
%% ProjectList is {@link query_node:qn_project_list()}, ParentPid is pid(),
%% OuterPids is [pid()], InnerPids is [pid()], VarsPositions is {@link
%% jqn_var_position()}, JoinVars is [{@link query_node:qn_var()}].
%% 
%% This request is implemented by {@link hc_start/10}. 
%% 
%% === {eval, VarsValues} ===
%%
%% Initiate evaluation of query node. The state of
%% query node must be either active or eos so that eval message is executed.
%% 
%% Firstly, initiate evaluation in all children, and, then send N empty 
%% messages to each child so that they can begin sending results.
%% Note than message passing for eval message is synchronous. This means that 
%% complete query tree is locked while evaluation is initiated.
%%
%% VarsValues is query_node:qn_var_val_map().
%% It includes variables and values to be set in graph pattern
%% of query node. In the case value of VarsValues is [] then graph pattern 
%% of query node is not changed.
%%
%% Message eval can be sent to query node multiple times. In each instance,
%% process dictionary is initialized to the initial state. After eval is executed
%% query node can expect empty messages from parent.
%% (LINK: {@section @{eval, VarsValues@}})
%% 
%% VarsValues is {@link query_node:qn_var_val_map()}. This request is implemented by {@link hc_eval/1}. 
%%
%% === {get_property, Name} ===
%% 
%% Return the value of specified property name. Variable Name is an
%% atom(). This request is implemented by {@link
%% hc_get_property/2}.
%% 
%% == handle_cast (asynchronous) message API ==
%% 
%% (LINK: {@section handle_cast (asynchronous) message API})
%% 
%% === {data_outer, ParentPid, Graph} ===
%% 
%% Processing data message from outer child. In the case join query node is in state
%% wait_next_inner, complete data message is stored in queue_from_outer to be processed 
%% later. 
%%
%% In the case join query node is in state wait_next_outer then data message from outer 
%% children is set as current outer message. Inner query nodes are reset using join values
%% of common variables that are set in graph-pattern of outer query nodes. 
%%
%% When all outer query nodes are in state eos (end of stream) then end_of_stream is sent
%% to parent and state of this query node is set to eos. 
%% (LINK: {@section @{data_outer, Pid, Graph@}})
%%
%% Pid is pid() and Graph is query_node:qn_graph(). This request is implemented by {@link hc_data_outer/1}. 
%%
%% === {data_inner, Pid, Graph} ===
%% 
%% Processing data message from inner children. Inner graph is joined with
%% current outer graph stored as outer_graph in process dictionary. Resulted graph
%% is sent to parent as outer data message. While inner graphs are comming from 
%% inner children, query node is in state wait_next_inner. 
%%
%% More graphs from outer child may be stored in queue_from_outer. State may change to 
%% wait_next_outer in the case all inner streams are terminated and queue_from_outer 
%% is not empty. In this case function hc_data_outer is called (from hc_data_inner)
%% for outer graph from queue. (LINK: {@section @{data_inner, Pid, Graph@}})
%%
%% Pid is pid(), Graph is query_node:qn_graph(). This request is implemented by {@link hc_data_inner/3}. 
%%
%% === {empty, ParentPid} ===
%% 
%% (LINK: {@section @{empty, ParentPid@}}).
%%
%% Processing empty message from parent. If state of query node is 
%% eos or inactive then simply ignore empty message. If queue_to_parent 
%% does not include any data message prepared for parent then empty 
%% message is stored in queue_from_parent and used later. Finally, if 
%% there is a message in queue_to_parent then send it to parent.
%%
%% ParentPid is pid().
%% 
%% This request is implemented by {@link hc_empty/1}. 
%% 
%% @type jqn_state() = maps:map(). Map
%% structure that manages properties for operating the gen_server
%% process.
%%
%% @type jqn_var_position() = maps:map(). Mapping from {@link query_node:qn_var()} 
%% to [{{@link query_node:qn_id()}, integer()}]. List of pairs represent 
%% positions of some variable in a triple pattern of given query. Pairs include
%% query node id of triple pattern, and, position of variable in triple pattern
%% (1: id, 2:sub, 3:prd, 4:obj).
%% 
-module(join_query_node).
-behavior(gen_server).
-export(
   [
    child_spec/1, spawn_process/2, receive_empty/0, hcst_sne/0,
    init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3
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
%% @doc Initialize a join_query_node process.
%% 
%% @spec init([]) -> {ok, jqn_state()}
%% 
init([]) ->
    process_flag(trap_exit, true),

    %% set main pd keys
    put(wait, true),
    put(pid, self()),
    put(start_date_time, calendar:local_time()),

    %% init queues
    query_node:queue_init(from_parent, plain, empty),
    query_node:queue_init(to_parent, output, data_outer),
    query_node:queue_init(from_inner, input, data_inner),
    query_node:queue_init(from_outer, input, data_outer),

    info_msg(init, [{state,hc_save_pd()}], done, -1),
    {ok, hc_save_pd()}.

%% 
%% handle_call/3
%% 
%% @doc Handle synchronous query requests.
%% 
%% @spec handle_call(term(), {pid(), term()}, jqn_state()) -> {reply, term(), jqn_state()}
%% 
handle_call({start, QueryNodeId, QueryId, SessionId, Self, GraphPattern, SelectPred, ProjectList, ParentPid, OuterPid, InnerPid, 
             VarsPositions, JoinVars}, _, State) ->
    hc_restore_pd(get(created), State),
    info_msg(handle_call, [Self, {message,start}, {all,get()}, get(state)], message_received, 10),
    hc_start(QueryNodeId, QueryId, SessionId, Self, GraphPattern, SelectPred, ProjectList, ParentPid, OuterPid, InnerPid, VarsPositions, JoinVars),
    {reply, ok, hc_save_pd()};

handle_call({get_property, all}, _, State) ->
    hc_restore_pd(get(created), State),
    info_msg(handle_call, [get(self), {message,get_property}, {name,all}, {value,get()}, get(state)], message_received, 10),
    {reply, get(), hc_save_pd()};

handle_call({get_property, Name}, _, State) ->
    hc_restore_pd(get(created), State),
    info_msg(handle_call, [get(self), {message,get_property}, {name,Name}, {value,get(Name)}, get(state)], message_received, 10),
    {reply, get(Name), hc_save_pd()};

handle_call({eval, VarsValues}, _, State) ->
    hc_restore_pd(get(created), State),
    info_msg(handle_call, [get(self), {message,eval}, {vars_values,VarsValues}, {all,get()}, get(state)], message_received, 10),
    hc_eval(VarsValues, get(state)),
    {reply, ok, hc_save_pd()};

%% default
handle_call(Request, From, State) ->
    R = {unknown_request, Request},
    error_msg(handle_call, [get(self), Request, From, get()], R),
    {reply, R, State}.

%% 
%% handle_cast/2
%%
%% @doc Handle asynchronous query requests. 
%% 
%% @spec handle_cast(term(), jqn_state()) -> {noreply, jqn_state()}
%% 
handle_cast({empty, From}, State) ->
    hc_restore_pd(get(created), State),

    %% insert into queue
    query_node:queue_write(from_parent, {empty, From}), 

    %% process empty message
    info_msg(handle_cast, [get(self), {message,empty}, {from,From}, {queue_from_parent,get(queue_from_parent)}, get(state)], message_received, 30),
    hc_empty(get(state)),
    {noreply, hc_save_pd()};

handle_cast({data_inner, From, Block}, State) ->
    hc_restore_pd(get(created), State),
    info_msg(handle_cast, [get(self), {message,data_inner}, {from,From}, {block,Block}, get(state)], message_received, 30),

    %% insert into queue
    query_node:queue_write(from_inner, {data_inner, From, Block}), 

    hc_data_inner(get(state)),
    {noreply, hc_save_pd()};

handle_cast({data_outer, From, Block}, State) ->
    hc_restore_pd(get(created), State),
    info_msg(handle_cast, [get(self), {message,data_outer}, {from,From}, {block,Block}, get(state)], message_received, 30),

    %% insert into queue
    query_node:queue_write(from_outer, {data_outer, From, Block}), 

    %% process outer block
    hc_data_outer(get(state)),
    {noreply, hc_save_pd()};

handle_cast({stop, From}, State) ->
    hc_restore_pd(get(created), State),
    info_msg(handle_cast, [get(self), {message,stop}, {from,From}, get(state)], message_received, 10),
    %% erase complete PD
    erase(),                               
    {noreply, hc_save_pd()};

%% default
handle_cast(Request, State) ->
    R = {unknown_request, Request},
    error_msg(handle_cast, [get(self), {request,Request}, {state,State}, get()], R),
    {noreply, hc_save_pd()}.

%% 
%% @doc Restore process dictionaries from state map structure.
%% 
%% @spec hc_restore_pd([{atom(), term()}] | undefined, jqn_state()) -> ok
%% 
hc_restore_pd(undefined, State) ->
    hc_restore_pd_1(maps:to_list(State));
hc_restore_pd(_, _) ->
    ok.

hc_restore_pd_1([]) ->
    ok;
hc_restore_pd_1([{K, V} | T]) ->
    put(K, V),
    hc_restore_pd_1(T).

%% 
%% @doc Save process all dictionary contents into state map structure.
%% 
%% @spec hc_save_pd() -> jqn_state()
%% 
hc_save_pd() ->
    maps:from_list(get()).

%% 
%% handle_info/2
%% 
%% @doc Handle exceptional query requests. 
%% 
%% @spec handle_info(term(), jqn_state()) -> {noreply, jqn_state()}
%% 
handle_info(_Info, State) ->
    {noreply, State}.

%% 
%% terminate/2
%% 
%% @doc Process termination. 
%% 
%% @spec terminate(term(), jqn_state()) -> none()
%% 
terminate(Reason, State) ->
    P = pid_to_list(self()),
    info_msg(terminate, [get(self), {reason,Reason}, {state,State}, {pid,P}, get(state)], done, -1),
    ok.

%% 
%% code_change/3
%% 
%% @doc Process code change action. 
%% 
%% @spec code_change(term(), jqn_state(), term()) -> {ok, jqn_state()}
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
%% api
%% 

%% 
%% @doc Return child spec for this process. It can be used in
%% supervisor:init/0 callback implementation.
%% 
%% @spec child_spec(Id::atom()) -> supervisor:child_spec()
%% 
child_spec(Id) ->
    GSOpt = [{local, Id}, join_query_node, [], []],
    StartFunc = {gen_server, start_link, GSOpt},
    Restart = permanent,
    Shutdwon = 1000,
    Type = worker,
    Modules = [join_query_node],

    {Id, StartFunc, Restart, Shutdwon, Type, Modules}.

%% 
%% @doc Spawn tp_query_node process with given local identifier at given node.
%% 
%% @spec spawn_process( Id::atom(), Node::node() ) -> node_state:ns_pid()
%% 
spawn_process(Id, Node ) -> 
    ChildSpec = join_query_node:child_spec(Id),
    supervisor:start_child({b3s, Node}, ChildSpec),
    {Id, Node}.

%% ======================================================================
%% 
%% handle call/cast implementation
%% 

%% 
%% hc_start/10
%%
%% @doc Initialize join query node process. 
%% 
%% @spec hc_start(query_node:qn_id(), string(), string(), node_state:ns_pid(), [query_node:qn_triple_pattern()], 
%%                query_node:qn_select_predicate(), query_node:qn_project_list(), 
%%                node_state:ns_pid(), [node_state:ns_pid()], [node_state:ns_pid()], 
%%                jqn_var_position(), [query_node:qn_var()]) -> ok
%% 
hc_start(QueryNodeId, QueryId, SessionId, Self, GraphPattern, SelectPred, ProjectList, ParentPid, OuterPids, InnerPids, VarsPositions, JoinVars) ->

    put(created,      true),
    put(qnode,        join),
    put(node_id,      QueryNodeId),
    put(query_id,     QueryId),
    put(session_id,   SessionId),
    put(self,         Self),
    put(state,        active),
    put(gp,           GraphPattern),
    put(select_pred,  SelectPred),
    put(project_list, ProjectList),
    put(parent,       ParentPid),
    put(outer,      OuterPids),
    put(inner,      InnerPids),
    put(vars_pos,   VarsPositions),
    put(join_vars,  JoinVars),
    put(empty_outer_sent, false),
    put(empty_inner_sent, false),
    put(wait,       false),
    put(pause,      false),

    erase(sid_table_name),
    erase(sid_max_id),
    erase(di_cursor__),
    erase(di_ets__),

    %% benchmark stuff 
    BSP = b3s_state_pid,
    BMT = benchmark_task,
    BTP = benchmark_task_pid,
    put(BSP, gen_server:call(node_state, {get, BSP})),
    {_, FSN} = get(BSP),
    put(BTP, {gen_server:call(get(BSP), {get, BMT}), FSN}),

    %% store num-of-empty-msgs in PD
    {ok, N} = application:get_env(b3s, num_of_empty_msgs),
    put(num_of_empty_msgs, N),

    %% store block-size in PD
    BSZ = block_size,
    put(BSZ, gen_server:call(get(BSP), {get, BSZ})).

%% @doc Send N empty messages to Pid. N is stored in config.
send_N_empty(Pid) ->
    N = get(num_of_empty_msgs),
    send_N_empty_1(Pid, N),
    info_msg(send_N_empty, [get(self), {send_to, Pid}, {num, N}], done, 50).

send_N_empty_1(_, 0) ->
    ok;
send_N_empty_1(Pid, N) ->
    gen_server:cast(Pid, {empty, get(self)}),
    info_msg(send_cast, [get(self), {message,empty}, {to,Pid}, {invoker,send_N_empty}, get(state)], message_sent, 30),
    send_N_empty_1(Pid, N-1).

receive_empty() ->
    receive 
        {_, M} -> M 
    end,
    info_msg(receive_empty, [get(self), {message, M}], done, 50),
    M.
    
%%
%% @doc Test function for hc_start.
%%
hc_start_test_() ->
    b3s:start(),
    b3s:stop(),
    b3s:start(),
    b3s:bootstrap(),
    {inorder,
     [
%      ?_assertMatch(ok, b3s:start()),
%      {generator, fun()-> hcst_sne() end},
      {generator, fun()-> hcst_q01() end},
      ?_assertMatch(ok, b3s:stop())
     ]}.

hcst_sne() ->
    info_msg(hcst_sne, [get(self)], start, 50),
    S = self(),
    {inorder,
     [
      ?_assertMatch(ok,         send_N_empty(S)),
      ?_assertMatch({empty, S}, receive_empty()),
      ?_assertMatch({empty, S}, receive_empty())
     ]}.

hcst_q01() ->
    info_msg(hcst_q01, [get(self)], start, 50),

    QueryNodeId   = "3",
    QueryId       = "1",
    SessionId     = "1",
    Id            = list_to_atom(SessionId++"-"++QueryId++"-"++QueryNodeId),
    JQN1          = join_query_node:spawn_process(Id, node()),

    QNState       = active,
    GraphPattern  = #{"1" => {"?id1", "<Japanese>",  "?prd", "?obj1"},
		      "2" => {"?id2", "<Slovenian>", "?prd", "?obj2"}},
    SelectPred    = none,
    ProjectList   = none,
    ParentPid     = self(),
    OuterPids     = [self()],
    InnerPids     = [self()],
    VarsPositions = #{"?id1"  => [{"1", 1}],
		      "?id2"  => [{"2", 1}],
		      "?prd"  => [{"1", 3}, {"2", 3}],
		      "?obj1" => [{"1", 4}],
		      "?obj2" => [{"2", 4}]},
    JoinVars      = ["?prd"],

    M1            = {start, QueryNodeId, QueryId, SessionId, JQN1,
		     GraphPattern, SelectPred, ProjectList,
		     ParentPid, OuterPids, InnerPids, VarsPositions, JoinVars},
    GP            = get_property,

    {inorder,
     [
      ?_assertMatch(true,          gen_server:call(JQN1, {GP, wait})),
      ?_assertMatch(ok,            gen_server:call(JQN1, M1)),
      ?_assertMatch(QueryNodeId,   gen_server:call(JQN1, {GP, node_id})),
      ?_assertMatch(GraphPattern,  gen_server:call(JQN1, {GP, gp})),
      ?_assertMatch(QNState,       gen_server:call(JQN1, {GP, state})),
      ?_assertMatch(ParentPid,     gen_server:call(JQN1, {GP, parent})),
      ?_assertMatch(OuterPids,     gen_server:call(JQN1, {GP, outer})),
      ?_assertMatch(InnerPids,     gen_server:call(JQN1, {GP, inner})),
      ?_assertMatch(VarsPositions, gen_server:call(JQN1, {GP, vars_pos})),
      ?_assertMatch(JoinVars,      gen_server:call(JQN1, {GP, join_vars})),
      ?_assertMatch(false,         gen_server:call(JQN1, {GP, wait})),
      ?_assertMatch(undefined,     gen_server:call(JQN1, {GP, inner_outer}))
     ]}.

%% 
%% hc_eval/2
%%
%% @doc Initiate evaluation of join query node. 
%%
%% @spec hc_eval(jqn_var_position(), atom()) -> ok
%% 

% ignoring VarsValues since we have currently only left-deep trees [TODO]
hc_eval(_, State) 
    when (State =:= eos) or (State =:= active) ->

    %% send eval to each pid in outer children and mark state of outer stream 'alive'.
    put(state_of_outer_streams, #{}),
    F1 = fun (Pid) ->
		 gen_server:call(Pid, {eval, []}),
                 info_msg(send_call, [get(self), {message,eval}, {to,Pid}, {gp,get(gp)}, {vars_values,[]}, {invoker,hc_eval}, get(state)], message_sent, 30),
		 M = get(state_of_outer_streams),
		 put(state_of_outer_streams, maps:put(Pid, alive, M))
	 end,
    lists:map(F1, get(outer)),

    %% send empty messages
    case get(empty_outer_sent) of 
       false -> lists:map(fun send_N_empty/1, get(outer));
       true -> ok
    end,
    put(empty_outer_sent, true),

    %% compute list of qn id-s to be projected out 
    query_node:project_prepare(get(project_list)),

    %% update parameters
    BSP = b3s_state_pid,
    BSZ = block_size,
    put(BSZ, gen_server:call(get(BSP), {get, BSZ})),

    %% waiting for data from outer
    put(state, wait_next_outer);

hc_eval(_, State) -> 
    error_msg(hc_eval, [get(self), {all,get()}, State], wrong_state),
    ok.

%% 
%% hc_empty/2
%%  
%% @doc Co-routine for processing empty message from parent. It is expected that there is 
%% at least one empty message in queue from_parent, either the one that has just arrived 
%% as message, or, some other procedure has checked that the message is in the queue. 
%% 
%% @spec hc_empty( State::atom() ) -> ok
%% 
hc_empty(undefined) ->

    %% leave empty message in queue from parent
    info_msg(hc_empty, [get(self), {from,get(parent)}, get(state)], empty_before_start, 50);
    
hc_empty(active) ->

    %% leave empty message in queue from parent
    info_msg(hc_empty, [get(self), {from,get(parent)}, get(state)], empty_before_eval, 50);
    
hc_empty(State) 
    when (State =:= wait_next_outer) or (State =:= wait_next_inner) or
         (State =:= eos) ->

    %% check if there are messages to parent and send data message to parent
    case query_node:queue_prepared(to_parent) of 
       true ->
           %% read empty message from queue (there must be at least one msg)
           {empty, _} = query_node:queue_read(from_parent),

           %% get data message from queue_to_parent and send it
           Msg = query_node:queue_read(to_parent),

           gen_server:cast(get(parent), Msg),
    	   info_msg(send_cast, [get(self), {message,Msg}, {to,get(parent)}, {invoker,hc_empty}, get(state)], message_sent, 30);

       false -> 
    	   info_msg(hc_empty, [get(self), {to,get(parent)}, get(state)], no_messages_to_parent, 50)
    end,

    %% next actions to be done

    %% state==wait_next_outer
    QEFO = query_node:queue_empty(from_outer),
    case {get(state), QEFO} of
    {wait_next_outer, false} ->
        %% if state is wait_next_outer and there are messages waiting from outer query nodes 
        %% than empty message wakes up processing of outer graph
        hc_data_outer(get(state));
        _ -> ok 
    end,

    %% state==wait_next_inner
    QEFI = query_node:queue_empty(from_inner),
    case {get(state), QEFI} of 
    {wait_next_inner, false} ->
         %% if state is wait_next_inner and there are messages waiting from inner query nodes 
         %% than empty message wakes up processing of inner graphs
         hc_data_inner(get(state));
         _ -> ok 
    end,

    %% if there is another pair of empty-data messages run hc_empty again
    case {query_node:queue_prepared(from_parent), query_node:queue_prepared(to_parent)} of
         {true,true} -> hc_empty(get(state));
         _ -> ok
    end;

hc_empty(State) -> 
    error_msg(hc_empty, [get(self), {all,get()}, State], wrong_state).

%% 
%% hc_data_outer/3
%%
%% @doc Co-routine for processing data message from outer child.
%% 
%% @spec hc_data_outer(atom()) -> ok
%% 
hc_data_outer(State) when State == wait_next_inner -> 
    %% data message left in queue from_outer to be processed later
    info_msg(hc_data_outer, [get(self), get(state)], leaving_data_outer_in_queue, 50);

hc_data_outer(State) when State == wait_next_outer -> 

    %% get outer graph from queue_from_outer 
    {From, Graph} = query_node:queue_get(from_outer),
 
    %% get status for end of block
    BE = query_node:queue_block_end(from_outer),

    %% send empty if at the end of block
    case BE of 
    true ->  %% send empty message back to outer
             EMsg = {empty, get(self)},
             gen_server:cast(From, EMsg),
             info_msg(send_cast, [get(self), {message,EMsg}, {to,From}, {invoker,hc_data_outer}, get(state)], message_sent, 30);

    _    -> ok
    end,

    %% store graph for inner loop
    put(outer_graph, Graph),

    %% outer loop actions for Fraph read from outer queue 
    case Graph of 
    end_of_stream -> 
         hcdo_process_eos(From),
         info_msg(hc_data_outer, [get(self), {from,From}, {graph,Graph}, get(state)], outer_eos_processed, 50);

    _ -> hcdo_process_graph(Graph),
         info_msg(hc_data_outer, [get(self), {from,From}, {graph,Graph}, get(state)], outer_graph_processed, 50)
    end,

    %% next protocol actions

    %% state = wait_next_inner?
    QPFP = query_node:queue_prepared(from_parent), 
    QEFI = query_node:queue_empty(from_inner),

    case {get(state), QPFP, QEFI} of
    {wait_next_inner, true, false} ->
 
          %% state changed from wait_next_outer to wait_next_inner. 
          %% outer graph has been processed and eval has been initiated for all inner qns.
          %% QEFI=false should not happen since inner messages could not be received. (?)
          hc_data_inner(get(state)),
          info_msg(hc_data_outer, [get(self), get(state)], call_inner_loop, 50);
   
    _ -> ok 
    end,

    %% state = wait_next_outer?
    QEFO = query_node:queue_empty(from_outer),

    case {get(state), QEFO} of 
    {wait_next_outer, false} ->

         %% eos has been processed but not all streams are finished.
         %% therefore, state did not change (from wait_next_outer) and next outer 
         %% message needs to be processed. we do not wait for empty message since inner 
         %% streams have to be processed before.
         hc_data_outer(get(state)),
         info_msg(hc_data_outer, [get(self), get(state)], call_outer_loop, 50);

    _ -> ok
    end;

hc_data_outer(State) -> 
    error_msg(hc_data_outer, [get(self), {all,get()}, {state,State}, get(state)], wrong_state ).

hcdo_process_eos(From) ->

    %% mark end_of_stream of outer process
    M = get(state_of_outer_streams),
    put(state_of_outer_streams, maps:put(From, eos, M)),
	
    %% count finished streams and send eos to parent if 0
    F3 = fun (alive) -> true;		 
             (eos)   -> false
         end,
    NumAlive = length(lists:filter(F3, maps:values(get(state_of_outer_streams)))),
    case NumAlive of 
    0 -> %% send parent eos and set state eos
         hcdo_send_parent_eos(),
         info_msg(hc_data_outer, [get(self), get(state)], query_evaluation_completed, 50);

    _ -> ok
    end.

hcdo_process_graph(Graph) ->

    %% make qn_var_val_map()
    F1 = fun (V) ->
       	     {V, hce_get_var_value(V, Graph)}
         end,
    JoinVarValues = lists:map(F1, get(join_vars)),
    %%info_msg(hcdo_process_graph, [get(self), {graph,Graph}, {join_vars,get(join_vars)}, {join_var_values,JoinVarValues}, get(state)], debug_join_var_values, 50),
    
    %% send eval and empty messages to inner nodes and update state of inner streams
    put(state_of_inner_streams, #{}),
    F2 = fun (Pid) ->
             %% reset inner child
 	     gen_server:call(Pid, {eval, JoinVarValues}),
             info_msg(send_call, [get(self), {message,eval}, {invoker,hcdo_process_outer}, {to,Pid}, {gp,get(gp)}, 
                                  {join_var_values,JoinVarValues}, get(state)], message_sent, 30),

             %% first time? send empty messages
             case get(empty_inner_sent) of 
                false -> send_N_empty(Pid);
                true -> ok
             end,
           
             %% remember state of inner child
	     M = get(state_of_inner_streams),
	     put(state_of_inner_streams, maps:put(Pid, alive, M))
	 end,
    lists:map(F2, get(inner)),
    put(empty_inner_sent, true),

    % mark state as waiting for inner messages
    put(state, wait_next_inner).

hcdo_send_parent_eos() ->

    %% store eos in queue to_parent and flush it
    query_node:queue_put(to_parent, end_of_stream),
    query_node:queue_flush(to_parent),

    % check if queue_from_parent includes empty messages
    case query_node:queue_prepared(from_parent) of
       
    true  -> %% there is empty message from parent
             {empty, _} = query_node:queue_read(from_parent),
             Msg = query_node:queue_read(to_parent),

             %% send parent last block of to_parent
             gen_server:cast(get(parent), Msg),
             info_msg(send_cast, [get(self), {message,Msg}, {to,get(parent)}, {invoker,hcdo_send_parent_eos}, get(state)], message_sent, 30);

    false -> %% empty queue from_parent, so leave message in queue to_parent.
             %% msg will be processed when the first empty message comes from_parent.
             ok
    end,

    %% move state to eos 
    put(state, eos).

hce_get_var_value(Variable, Graph) ->
    VP = get(vars_pos),

    %% get position and tuple
    %% LVP is [{NodeId, Pos}|_]
    LVP = maps:get(Variable, VP),
    {NodeId,Pos} = hce_get_node_id(LVP, Graph),
    Tuple = maps:get(NodeId, Graph),

    %% Pos+1 since first component is table-name
    element(Pos+1, Tuple).   

hce_get_node_id([{NID,Pos}|Rest], Graph) -> 
    case maps:is_key(NID, Graph) of
    true -> {NID,Pos};
    false -> hce_get_node_id(Rest, Graph)
    end;

hce_get_node_id([], Graph) -> 
    error_msg(hce_get_node_id, [get(self), {graph,Graph}, {all,get()}, get(state)], cant_find_var_val).

%% 
%% hc_data_inner/3
%%
%% @doc Co-routine for processing data block from inner children. 
%% 
%% @spec hc_data_inner(State::atom()) -> ok|fail
%% 
hc_data_inner(State) when State =/= wait_next_inner ->
    error_msg(hc_data_inner, {all,get()}, wrong_state);

hc_data_inner(State) when State =:= wait_next_inner ->

    %% retrieve inner graph from queue from_inner and save it in PD
    {From, Graph} = query_node:queue_get(from_inner),
    put(inner_graph, Graph),

    %% get status for end of block
    BE = query_node:queue_block_end(from_inner),

    %% send empty if at the end of block
    case BE of 
    true ->  %% send empty message back to inner
             EMsg = {empty, get(self)},
             gen_server:cast(From, EMsg),
             info_msg(send_cast, [get(self), {message,EMsg}, {to,From}, {invoker,hc_data_inner}, get(state)], message_sent, 30);

    _    -> ok
    end,

    %% do action for outer graph read from queue
    case Graph of
    end_of_stream ->
        %% we are at end of some inner stream
        hcdi_process_eos(From);
        %%info_msg(hc_data_inner, [get(self), {from,From}, {graph,Graph}, get(state)], inner_eos_processed, 50)

    _ -> %% join inner graph with outer and send it to parent
        hcdi_process_graph(Graph)
        %%info_msg(hc_data_inner, [get(self), {from,From}, {graph,Graph}, get(state)], inner_graph_processedd, 50)
    end,

    %% next protocol actions for inner loop

    %% state = wait_next_inner?
    QPFP = query_node:queue_prepared(from_parent), 
    QEFI = query_node:queue_empty(from_inner),

    case {get(state), QPFP, QEFI} of
    {wait_next_inner, true, false} ->
 
          %% continue inner loop if we stayed in wait_next_inner and 
          %% there is another inner graph to process and there is empty message
          %% to be used
          hc_data_inner(get(state)),
          info_msg(hc_data_inner, [get(self), get(state)], recursive_loop_inner, 50);
   
    _ -> ok 
    end,

    %% state = wait_net_outer?
    QEFO = query_node:queue_empty(from_outer),

    %% do we need to restrict calling outer loop only when empty message is ready (?)
    %%case {get(state), QPFP, QEFO} of 
    case {get(state), QEFO} of 

    %%{wait_next_outer, true, false} ->
    {wait_next_outer, false} ->

         %% end of all inner streams detected (and we moved to wait_next_outer) 
         %% and queue from outer includes some messages. we don't wait for empty message 
         %% since inner stream has to be started before.
         hc_data_outer(get(state)),
         info_msg(hc_data_inner, [get(self), get(state)], recursive_loop_outer, 50);

    _ -> ok
    end.

hcdi_process_eos(From) ->

    %% mark inner stream not alive
    M = get(state_of_inner_streams),
    put(state_of_inner_streams, maps:put(From, eos, M)),

    %% check if all inner streams are dead
    F1 = fun (alive) -> true;		 
             (eos)   -> false
         end,
    NumAlive = length(lists:filter(F1, maps:values(get(state_of_inner_streams)))),

    %% all inner streams dead?
    if NumAlive == 0 -> 
           put(state, wait_next_outer);
       true -> ok
    end.
    %%info_msg(hc_data_inner, [get(self), {from,From}, {graph,Graph}, {numAlive,NumAlive}, get(state)], eos_inner_processed, 50).
            
hcdi_process_graph(Graph) ->

    %% compute join
    OG = get(outer_graph),
    G = maps:merge(Graph, OG),
    %%info_msg(hc_data_inner, [get(self), Graph, OG, G, get(state)], join_computed, 50),

    %% set current graph G and compute val of select predicate
    put(gp_val, G),
    SP = query_node:eval_select(get(select_pred)),
    info_msg(hc_data_inner, [get(self), {graph,G}, {select_pred,get(select_pred)}, {select_pred_value,SP}, get(state)], select_pred_computed, 50),

    %% skip graph G if SP==false
    case SP of 
    true -> 
        %% compute projection and put in queue to_parent
        query_node:eval_project(get(project_list)),
        G1 = get(gp_val),
        query_node:queue_put(to_parent, G1),
        info_msg(hc_data_inner, [get(self), {graph_in,G}, {project_list,get(project_list)}, {graph_out,G1}, get(state)], project_computed, 50),

        %% send block to parent if evrythng preped
        case query_node:queue_prepared(from_parent) and query_node:queue_prepared(to_parent) of 
        true ->
            %% get empty message from queue from_parent
            {empty, _} = query_node:queue_read(from_parent),

            %% get block and create message
            Msg = query_node:queue_read(to_parent),

            %% send it to parent
            gen_server:cast(get(parent), Msg),
            info_msg(send_cast, [get(self), {message,Msg}, {to,get(parent)}, {invoker,hcdi_process_graph}, get(state)], message_sent, 30);
        false-> ok
        end;

    false -> ok
    end.

%% 
%% hc_eval_test_/0
%%
%% @doc Main test function of module.
%% 
hc_eval_test_() ->
    hcet_site(b3s_state:get(test_mode)).

hcet_site(local1) ->
    Attrs = {attributes, record_info(fields, triple_store)},
    TabDef = [Attrs, {disc_copies, [node()]}],
    info_msg(hcet_load_db, [get(self), TabDef], display_table, 50),

    NDS    = node(),
    BSS    = {b3s_state, NDS},
    CRC    = clm_row_conf,

    RMS = #{1 => NDS},
    CM1 = #{1 => RMS, 2 => RMS},
    R01 = [NDS],                       %%, NDC],

    put(self, {'1-1-1', node()}),
    {inorder,
     [
      ?_assertMatch(ok, b3s:start()),
      ?_assertMatch(ok, b3s:bootstrap()),
      ?_assertMatch(ok,  gen_server:call(BSS, {put, CRC, CM1})),
      ?_assertMatch(R01, gen_server:call(BSS, propagate)),
      {generator, fun()-> tp_query_node:hcet_load_db() end},
      {generator, fun()-> hcet_q02() end},
      ?_assertMatch(ok, b3s:stop()),

      ?_assertMatch(ok, b3s:start()),
      ?_assertMatch(ok, b3s:bootstrap()),
      ?_assertMatch(ok,  gen_server:call(BSS, {put, CRC, CM1})),
      ?_assertMatch(R01, gen_server:call(BSS, propagate)),
      {generator, fun()-> hcet_load_db() end},
      {generator, fun()-> hcet_q03() end},
      {generator, fun()-> hcet_q05() end},
      {generator, fun()-> hcet_q06() end},
      %% finish
      ?_assertMatch(ok, timer:sleep(1000)),
      ?_assertMatch(stopped, mnesia:stop()),
      ?_assertMatch(ok, b3s:stop())
%      ?_assertMatch(ok, mnesia:start()),
%      ?_assertMatch({atomic, ok}, mnesia:create_table(triple_store, TabDef)),
%      ?_assertMatch({atomic, ok}, mnesia:delete_table(triple_store)),
%      ?_assertMatch(stopped, mnesia:stop())
    ]};

hcet_site(local_two) ->
    [];

hcet_site(_) ->
    [].

hcet_q02() ->
    info_msg(hcet_q02, [get(self)], start, 50),

    BS  = gen_server:call(node_state, {get, b3s_state_pid}),
    Tab = gen_server:call(BS, {get, name_of_triple_table}),

    QueryNodeId   = "3",
    QueryId       = "2",
    SessionId     = "1",
    Id3           = list_to_atom(SessionId++"-"++QueryId++"-"++QueryNodeId),
    JQN3          = join_query_node:spawn_process(Id3, node()),
    TPQN1         = hcet_tpqn1(JQN3),
    TPQN2         = hcet_tpqn2(JQN3),
    GraphPattern  = maps:from_list(
		      [{"1", {"?id1", eI("<Japanese>"),  "?prd", "?obj1"}},
		       {"2", {"?id2", eI("<Slovenian>"), "?prd", "?obj2"}}]),
    SelectPred    = none,
    ProjectList   = none,
    ParentPid     = self(),
    OuterPids     = [TPQN1],
    InnerPids     = [TPQN2],
    VarsPositions = #{"?id1"  => [{"1", 1}],
		      "?id2"  => [{"2", 1}],
		      "?prd"  => [{"1", 3}, {"2", 3}],
		      "?obj1" => [{"1", 4}],
		      "?obj2" => [{"2", 4}]},
    JoinVars      = ["?prd"],

    GP            = get_property,
    DFO           = data_outer,
    EOS           = end_of_stream,

    T1 = eT({Tab,"<triple_id_0002>","<Japanese>","<eat>","<fishes>"}),
    T2 = eT({Tab,"<triple_id_0003>","<Slovenian>","<eat>","<potatoes>"}),

    TP1 = {"?id2", eI("<Slovenian>"), eI("<eat>"), "?obj2"},

    M1            = {start, QueryNodeId, QueryId, SessionId, JQN3, GraphPattern,
		     SelectPred, ProjectList, ParentPid, OuterPids, InnerPids, 
                     VarsPositions, JoinVars},
    M2            = {eval, []},

    R1Map         = maps:put("1", T1, maps:new()),
    R2Map         = maps:put("2", T2, R1Map),
    R             = {DFO, JQN3, [R2Map,EOS]},

    {inorder,
     [
      ?_assertMatch(true,          gen_server:call(JQN3, {GP, wait})),
      ?_assertMatch(ok,            gen_server:call(JQN3, M1)),
      ?_assertMatch(false,         gen_server:call(JQN3, {GP, wait})),
      ?_assertMatch(undefined,     gen_server:call(JQN3, {GP, inner_outer})),
      ?_assertMatch(ok,            gen_server:call(JQN3, M2)),
      ?_assertMatch({_, R},        hcet_send_empty(JQN3, R)),

      ?_assertMatch(TP1,           gen_server:call(TPQN2, {GP, tp})),
      ?_assertMatch(OuterPids,     gen_server:call(JQN3, {GP, outer})),
      ?_assertMatch(InnerPids,     gen_server:call(JQN3, {GP, inner})),
      ?_assertMatch(GraphPattern,  gen_server:call(JQN3, {GP, gp})),

      ?_assertMatch(ok,            gen_server:cast(TPQN1, {stop, self()})),
      ?_assertMatch(ok,            gen_server:cast(TPQN2, {stop, self()})),
      ?_assertMatch(ok,            gen_server:cast(JQN3, {stop, self()}))
     ]}.

hcet_tpqn1(Pid) ->
    QueryNodeId   = "1",
    QueryId       = "2",
    SessionId     = "1",
    Id            = list_to_atom(SessionId++"-"++QueryId++"-"++QueryNodeId),
    TPQN1         = tp_query_node:spawn_process(Id, node()),
    TriplePattern = {"?id1", eI("<Japanese>"),  "?prd", "?obj1"},
    SelectPred    = none,
    ProjectList   = none,
    ParentPid     = Pid,
    VarsPositions = #{"?id1" => 1, "?prd" => 3, "?obj1" => 4},
    M             = {start, QueryNodeId, QueryId, SessionId, TPQN1,
		     TriplePattern, SelectPred, ProjectList,
		     ParentPid, VarsPositions, outer},
    gen_server:call(TPQN1, M),
    TPQN1.

hcet_tpqn2(Pid) ->
    QueryNodeId   = "2",
    QueryId       = "2",
    SessionId     = "1",
    Id            = list_to_atom(SessionId++"-"++QueryId++"-"++QueryNodeId),
    TPQN2         = tp_query_node:spawn_process(Id, node()),
    TriplePattern = {"?id2", eI("<Slovenian>"), "?prd", "?obj2"},
    SelectPred    = none,
    ProjectList   = none,
    ParentPid     = Pid,
    VarsPositions = #{"?id2" => 1, "?prd" => 3, "?obj2" => 4},
    M             = {start, QueryNodeId, QueryId, SessionId, TPQN2,
		     TriplePattern, SelectPred, ProjectList,
		     ParentPid, VarsPositions, inner},
    gen_server:call(TPQN2, M),
    TPQN2.

          %%
%% @doc Creation of triple-store used in examples.
%%
example_table() ->
    [%% country
     {triple_store, "id1",  "japan",	"type",	"country"},
     {triple_store, "id2",  "slovenia",	"type",	"country"},
     %% cities
     {triple_store, "id3",  "koper",	"type",	"city"},
     {triple_store, "id4",  "ljubljana","type", "city"},
     {triple_store, "id5",  "tokyo", 	"type", "city"},
     {triple_store, "id6",  "kyoto", 	"type", "city"},
     {triple_store, "id7",  "osaka", 	"type", "city"},
     %% organizations
     {triple_store, "id8",  "up",	"type",	"university"},   % uni primorska
     {triple_store, "id9",  "ul",	"type",	"university"},   % uni ljubljana
     {triple_store, "id10", "ijs", 	"type",	"institute"},    % institute jozef stefan
     {triple_store, "id11", "yj", 	"type",	"corporation"},  % yahoo ! japan
     {triple_store, "id12", "tu", 	"type", "university"},   % tokyo uni
     {triple_store, "id13", "ku", 	"type", "university"},   % kyoto uni
     {triple_store, "id14", "ou", 	"type", "university"},   % osaka uni
     %% persons
     {triple_store, "id15", "shou",	"type",	"person"},
     {triple_store, "id16", "yoshio", 	"type", "person"},
     {triple_store, "id17", "sakura", 	"type", "person"},
     {triple_store, "id18", "luka", 	"type", "person"},
     {triple_store, "id19", "jan", 	"type", "person"},
     {triple_store, "id20", "nika", 	"type", "person"},
     {triple_store, "id57", "marko", 	"type", "person"},
     %% hasCapital
     {triple_store, "id21", "japan",	"hasCapital",	"tokyo"},
     {triple_store, "id22", "slovenia", "hasCapital", 	"ljubljana"},
     %% isLocatedIn
     {triple_store, "id23", "tokyo",	"isLocatedIn",	"japan"},
     {triple_store, "id24", "kyoto", 	"isLocatedIn",	"japan"},
     {triple_store, "id25", "osaka", 	"isLocatedIn",	"japan"},
     {triple_store, "id26", "koper", 	"isLocatedIn",	"slovenia"},
     {triple_store, "id27", "ljubljana","isLocatedIn",	"slovenia"},
     {triple_store, "id28", "up",	"isLocatedIn",	"koper"},
     {triple_store, "id29", "ul", 	"isLocatedIn",	"ljubljana"},
     {triple_store, "id30", "ijs", 	"isLocatedIn",	"ljubljana"},
     {triple_store, "id31", "yj", 	"isLocatedIn",	"tokyo"},
     {triple_store, "id32", "ku", 	"isLocatedIn",	"kyoto"},
     {triple_store, "id33", "ou", 	"isLocatedIn",	"osaka"},
     {triple_store, "id34", "tu", 	"isLocatedIn",	"tokyo"},
     %% livesIn
     {triple_store, "id35", "shou",	"livesIn",	"tokyo"},
     {triple_store, "id36", "yoshio",	"livesIn", 	"tokyo"},
     {triple_store, "id37", "sakura", 	"livesIn", 	"kyoto"},
     {triple_store, "id38", "luka", 	"livesIn", 	"ljubljana"},
     {triple_store, "id39", "jan", 	"livesIn", 	"koper"},
     {triple_store, "id40", "nika", 	"livesIn", 	"ljubljana"},
     {triple_store, "id41", "marko", 	"livesIn", 	"ljubljana"},
     %% worksAt
     {triple_store, "id42", "shou",	"worksAt",	"yj"},
     {triple_store, "id43", "shou",	"worksAt",	"ku"},
     {triple_store, "id44", "yoshio", 	"worksAt", 	"yj"},
     {triple_store, "id45", "sakura", 	"worksAt", 	"ku"},
     {triple_store, "id46", "luka", 	"worksAt", 	"up"},
     {triple_store, "id47", "luka", 	"worksAt", 	"ijs"},
     {triple_store, "id48", "jan", 	"worksAt", 	"up"},
     {triple_store, "id49", "nika", 	"worksAt", 	"ijs"},
     {triple_store, "id50", "marko", 	"worksAt", 	"ijs"},
     %% graduatedFrom
     {triple_store, "id51", "shou",	"graduatedFrom",	"ou"},
     {triple_store, "id52", "yoshio", 	"graduatedFrom", 	"tu"},
     {triple_store, "id53", "sakura", 	"graduatedFrom", 	"ku"},
     {triple_store, "id54", "luka", 	"graduatedFrom", 	"ul"},
     {triple_store, "id55", "jan", 	"graduatedFrom", 	"up"},
     {triple_store, "id56", "nika", 	"graduatedFrom", 	"ul"},
     %% age 
     {triple_store, "id58", "shou",	"age", "25"},
     {triple_store, "id59", "yoshio", 	"age", "36"},
     {triple_store, "id60", "sakura", 	"age", "27"},
     {triple_store, "id61", "luka", 	"age", "38"},
     {triple_store, "id62", "jan", 	"age", "45"},
     {triple_store, "id63", "nika", 	"age", "22"},
     {triple_store, "id64", "marko", 	"age", "30"}].

hcet_load_db() ->
    case 2 of
	2 -> hcet_load_db_postgres()
    end.

hcet_load_db_postgres() ->
    info_msg(hcet_load_db_postgres, [], {start, get(self)}, 50),

    BS  = gen_server:call(node_state, {get, b3s_state_pid}),
    Tab = db_interface:dot_get_tn(),
    F1 = fun (X) ->
  	     {_, Tid, Sbj, Prd, Obj} = X,
	     D = eT({Tab, Tid, Sbj, Prd, Obj}),
	     db_interface:db_write(D)
	 end,

    SI  = string_id,
    SIT = gen_server:call(BS, {get, name_of_string_id_table}),
    gen_server:call(SI, {put, sid_table_name, SIT}),
    gen_server:call(SI, {put, di_cursor__, undefined}),
    gen_server:call(SI, delete_table),
    gen_server:call(SI, {create_table, SIT}),
    gen_server:call(SI, make_index),
    erase(sid_table_name),
    erase(sid_max_id),

    %% ok = db_interface:db_close(),
    ok = db_interface:db_init(),
    info_msg(hcet_load_db_postgres, [], {di_cursor__, get(di_cursor__)}, 50),
    %% ok = db_interface:db_close(),
    ok = lists:foreach(F1, example_table()),
    ok = db_interface:db_add_index(),
    %% ok = db_interface:db_close(),

    TP01 = eTP({"id1", "?s", "?p", "?o"}),
    TP02 = eTP({"id11", "?s", "?p", "?o"}),
    TP03 = eTP({"id56", "?s", "?p", "?o"}),
    R01  = eT({Tab, "id1", "japan", "type", "country"}),
    R02  = eT({Tab, "id11", "yj", "type", "corporation"}),
    R03  = eT({Tab, "id56", "nika", "graduatedFrom", "ul"}),
    EOS  = end_of_stream,

    {inorder,
     [
      %% ?_assertMatch(ok,  db_interface:db_close()),
      %% ?_assertMatch(ok,  db_interface:db_init()),
      %% ?_assertMatch(ok,  lists:foreach(F1, example_table())),
      %% ?_assertMatch(ok,  db_interface:db_add_index()),
      %% ?_assertMatch(ok,  db_interface:db_close()),

      ?_assertMatch(ok,  db_interface:db_open_tp(TP01)),
      ?_assertMatch(R01, db_interface:db_next()),
      %% ?_assertMatch(EOS, db_interface:db_next()),
      ?_assertMatch(ok,  db_interface:db_open_tp(TP02)),
      ?_assertMatch(R02, db_interface:db_next()),
      ?_assertMatch(EOS, db_interface:db_next()),
      ?_assertMatch(ok,  db_interface:db_open_tp(TP03)),
      ?_assertMatch(R03, db_interface:db_next()),
      ?_assertMatch(EOS, db_interface:db_next()),
      ?_assertMatch(ok,  db_interface:db_close())
     ]}.

hcet_send_empty(QN, R) ->
    gen_server:cast(QN, {empty, self()}),
    receive
	M -> M
    end,
    info_msg(hcet_send_empty, [get(self), {from,QN}, {received, M}, {expected,R}, get(state)], data_received, 50),
    M.

hcet_get_PD(QN) ->
    M = gen_server:call(QN, {get_property, all}),
    info_msg(hcet_get_PD, [get(self), {pid,QN}, {all,M}, length(M)], response_property_all_received, 50),
    M.

eI(X) -> string_id:get_id(X).

eT({T,I,S,P,O}) ->
    ET = string_id:encode_triple({I, S, P, O}),
    list_to_tuple([T | tuple_to_list(ET)]).

eTP(X) -> string_id:encode_triple_pattern(X).

hcet_q03() ->
    %%
    %%  query: using single tp query nodes
    %% 
    %%  slovenia hasCapital ?x
    %%  ?y livesIn ?x
    %%  ?y worksAt ijs
    %%  
    info_msg(hcet_q03, [get(self)], start, 50),

    Tab = db_interface:dot_get_tn(),

    %% creating processes
    QueryNodeId3  = "3",
    QueryId       = "3",
    SessionId     = "1",
    Id3           = list_to_atom(SessionId++"-"++QueryId++"-"++QueryNodeId3),
    JQN3          = join_query_node:spawn_process(Id3, node()),

    QueryNodeId5  = "5",
    Id5           = list_to_atom(SessionId++"-"++QueryId++"-"++QueryNodeId5),
    JQN5          = join_query_node:spawn_process(Id5, node()),

    TPQN1         = hcet_tpqn3(JQN3),
    TPQN2         = hcet_tpqn4(JQN3),
    TPQN4         = hcet_tpqn5(JQN5),

    %% first join 
    GraphPattern3  = maps:from_list(
    		       [{"1", {"?i1", eI("slovenia"),  eI("hasCapital"), "?x"}},
			{"2", {"?i2", "?y", eI("livesIn"), "?x"}}]),
    SelectPred3    = none,
    ProjectList3   = none,
    ParentPid3     = JQN5,
    OuterPids3     = [TPQN1],
    InnerPids3     = [TPQN2],
    VarsPositions3 = #{"?i1"  => [{"1", 1}],
		       "?i2"  => [{"2", 1}],
		       "?i4"  => [{"4", 1}],
		       "?x"   => [{"1", 4}, {"2", 4}],
		       "?y"   => [{"2", 2}]},
    JoinVars3      = ["?x"],

    %% second join 
    GraphPattern5  = maps:from_list(
		       [{"1", {"?i1", eI("slovenia"),  eI("hasCapital"), "?x"}},
			{"2", {"?i2", "?y", eI("livesIn"), "?x"}},
			{"4", {"?i4", "?y", eI("worksAt"), eI("ijs")}}]),
    SelectPred5    = {lnot, {"?y", equal, eI("luka")}},
    ProjectList5   = ["?y"],
    ParentPid5     = self(),
    OuterPids5     = [JQN3],
    InnerPids5     = [TPQN4],
    VarsPositions5 = #{"?i1"  => [{"1", 1}],
		       "?i2"  => [{"2", 1}],
 		       "?i4"  => [{"4", 1}],
		       "?x"   => [{"1", 4}, {"2", 4}],
		       "?y"   => [{"2", 2}, {"4", 2}]},
    JoinVars5      = ["?y"],

    %% data to be returned
    T4 = eT({Tab, "id40", "nika", "livesIn", "ljubljana"}),
    T5 = eT({Tab, "id49", "nika", "worksAt", "ijs"}),
    T6 = eT({Tab, "id41", "marko", "livesIn", "ljubljana"}),
    T7 = eT({Tab, "id50", "marko", "worksAt", "ijs"}),

    %% messages for JQN3 and JQN5
    DFO           = data_outer,
    EOS           = end_of_stream,

    S3            = {start, QueryNodeId3, QueryId, SessionId, JQN3, GraphPattern3,
		     SelectPred3, ProjectList3, ParentPid3, OuterPids3, InnerPids3, 
                     VarsPositions3, JoinVars3},
    S5            = {start, QueryNodeId5, QueryId, SessionId, JQN5, GraphPattern5,
		     SelectPred5, ProjectList5, ParentPid5, OuterPids5, InnerPids5, 
                     VarsPositions5, JoinVars5},
    E5            = {eval, []},

    %% tuples to be returned
    R2Map         = maps:put("4", T5, maps:put("2", T4, maps:new())),
    R3Map         = maps:put("4", T7, maps:put("2", T6, maps:new())),
    R1            = {DFO, JQN5, [R3Map,R2Map,EOS]},
%   info_msg(hcet_q03, [get(self), R1, RE], before_tests, 50),

    {inorder,
     [
      ?_assertMatch(ok, mnesia:start()),
      ?_assertMatch(ok, timer:sleep(1000)),
      ?_assertMatch(ok, gen_server:call(JQN3, S3)),
      ?_assertMatch(ok, gen_server:call(JQN5, S5)),

      %% check state of qn-s
      ?_assertMatch(35,     length(hcet_get_PD(TPQN1))),
      ?_assertMatch(35,     length(hcet_get_PD(TPQN2))),
      ?_assertMatch(42,     length(hcet_get_PD(JQN3))),
      ?_assertMatch(35,     length(hcet_get_PD(TPQN4))),
      ?_assertMatch(42,     length(hcet_get_PD(JQN5))),

      %% start evaluation 
      ?_assertMatch(ok,     gen_server:call(JQN5, E5)),

      %% send empty messages to JQN5
      ?_assertMatch({'$gen_cast', R1}, hcet_send_empty(JQN5, R1))
      ]}.

hcet_tpqn3(Pid) ->
    QueryNodeId   = "1",
    QueryId       = "3",
    SessionId     = "1",
    Id            = list_to_atom(SessionId++"-"++QueryId++"-"++QueryNodeId),
    TPQN1         = tp_query_node:spawn_process(Id, node()),
    TriplePattern = {"?i1", eI("slovenia"),  eI("hasCapital"), "?x"},
    SelectPred    = none,
    ProjectList   = none,
    ParentPid     = Pid,
    VarsPositions = #{"?i1" => 1, "?x" => 4},
    M             = {start, QueryNodeId, QueryId, SessionId, TPQN1, TriplePattern,
		     SelectPred, ProjectList, ParentPid, VarsPositions, outer},
    gen_server:call(TPQN1, M),
    TPQN1.

hcet_tpqn4(Pid) ->
    QueryNodeId   = "2",
    QueryId       = "3",
    SessionId     = "1",
    Id            = list_to_atom(SessionId++"-"++QueryId++"-"++QueryNodeId),
    TPQN2         = tp_query_node:spawn_process(Id, node()),
    TriplePattern = {"?i2", "?y", eI("livesIn"), "?x"},
    SelectPred    = none,
    ProjectList   = none,
    ParentPid     = Pid,
    VarsPositions = #{"?i2" => 1, "?y" => 2, "?x" => 4},
    M             = {start, QueryNodeId, QueryId, SessionId, TPQN2, TriplePattern,
		     SelectPred, ProjectList, ParentPid, VarsPositions, inner},

    gen_server:call(TPQN2, M),
    TPQN2.

hcet_tpqn5(Pid) ->
    QueryNodeId   = "4",
    QueryId       = "3",
    SessionId     = "1",
    Id            = list_to_atom(SessionId++"-"++QueryId++"-"++QueryNodeId),
    TPQN4         = tp_query_node:spawn_process(Id, node()),
    TriplePattern = {"?i4", "?y", eI("worksAt"), eI("ijs")},
    SelectPred    = none,
    ProjectList   = none,
    ParentPid     = Pid,
    VarsPositions = #{"?i4" => 1, "?y" => 2},
    M             = {start, QueryNodeId, QueryId, SessionId, TPQN4, TriplePattern,
		     SelectPred, ProjectList, ParentPid, VarsPositions, inner},
    gen_server:call(TPQN4, M),
    TPQN4.

hcet_q05() ->
    %%
    %%  query: using two query nodes for each tp
    %% 
    %%  slovenia hasCapital ?x
    %%  ?y livesIn ?x
    %%  ?y worksAt ijs
    %%  
    info_msg(hcet_q05, [get(self)], start, 50),

    Tab = db_interface:dot_get_tn(),

    %% creating processes
    QueryNodeId3  = "3",
    QueryId       = "5",
    SessionId     = "1",
    Id3           = list_to_atom(SessionId++"-"++QueryId++"-"++QueryNodeId3),
    JQN3          = join_query_node:spawn_process(Id3, node()),

    QueryNodeId5  = "5",
    Id5           = list_to_atom(SessionId++"-"++QueryId++"-"++QueryNodeId5),
    JQN5          = join_query_node:spawn_process(Id5, node()),

    TPQN1         = hcet_tp5qn3(JQN3),
    TPQN1a        = hcet_tp5qn3a(JQN3),
    TPQN2         = hcet_tp5qn4(JQN3),
    TPQN2a        = hcet_tp5qn4a(JQN3),
    TPQN4         = hcet_tp5qn5(JQN5),
    TPQN4a        = hcet_tp5qn5a(JQN5),

    %% first join 
    GraphPattern3  = maps:from_list(
		       [{"1", {"?i1", eI("slovenia"),  eI("hasCapital"), "?x"}},
			{"2", {"?i2", "?y", eI("livesIn"), "?x"}}]),
    SelectPred     = none,
    ProjectList    = none,
    ParentPid3     = JQN5,
    OuterPids3     = [TPQN1,TPQN1a],
    InnerPids3     = [TPQN2,TPQN2a],
    VarsPositions3 = #{"?i1"  => [{"1", 1}],
		       "?i2"  => [{"2", 1}],
		       "?i4"  => [{"4", 1}],
		       "?x"   => [{"1", 4}, {"2", 4}],
		       "?y"   => [{"2", 2}]},
    JoinVars3      = ["?x"],

    %% second join 
    GraphPattern5  = maps:from_list(
		       [{"1", {"?i1", eI("slovenia"),  eI("hasCapital"), "?x"}},
			{"2", {"?i2", "?y", eI("livesIn"), "?x"}},
			{"4", {"?i4", "?y", eI("worksAt"), eI("ijs")}}]),
    ParentPid5     = self(),
    OuterPids5     = [JQN3],
    InnerPids5     = [TPQN4,TPQN4a],
    VarsPositions5 = #{"?i1"  => [{"1", 1}],
		       "?i2"  => [{"2", 1}],
 		       "?i4"  => [{"4", 1}],
		       "?x"   => [{"1", 4}, {"2", 4}],
		       "?y"   => [{"2", 2}, {"4", 2}]},
    JoinVars5      = ["?y"],

    %% data to be returned
    T1 = eT({Tab, "id22", "slovenia", "hasCapital", "ljubljana"}),
    T2 = eT({Tab, "id38", "luka", "livesIn", "ljubljana"}),
    T3 = eT({Tab, "id47", "luka", "worksAt", "ijs"}),
    T4 = eT({Tab, "id40", "nika", "livesIn", "ljubljana"}),
    T5 = eT({Tab, "id49", "nika", "worksAt", "ijs"}),
    T6 = eT({Tab, "id41", "marko", "livesIn", "ljubljana"}),
    T7 = eT({Tab, "id50", "marko", "worksAt", "ijs"}),

    %% messages for JQN3 and JQN5
    DFO           = data_outer,
    EOS           = end_of_stream,

    S3            = {start, QueryNodeId3, QueryId, SessionId, JQN3, GraphPattern3,
		     SelectPred, ProjectList, ParentPid3, OuterPids3, InnerPids3, 
                     VarsPositions3, JoinVars3},
    S5            = {start, QueryNodeId5, QueryId, SessionId, JQN5, GraphPattern5,
		     SelectPred, ProjectList, ParentPid5, OuterPids5, InnerPids5, 
                     VarsPositions5, JoinVars5},
    E5            = {eval, []},

    %% tuples to be returned
    R1Map         = maps:put("4", T3, maps:put("2", T2, maps:put("1", T1, maps:new()))),
    R2Map         = maps:put("4", T5, maps:put("2", T4, maps:put("1", T1, maps:new()))),
    R3Map         = maps:put("4", T7, maps:put("2", T6, maps:put("1", T1, maps:new()))),
    R1            = {DFO, JQN5, [R3Map,R3Map,R2Map,R2Map,R1Map]},
    R2            = {DFO, JQN5, [R1Map,R3Map,R3Map,R2Map,R2Map]},
    R3            = {DFO, JQN5, [R1Map,R1Map,R3Map,R3Map,R2Map]},
    R4            = {DFO, JQN5, [R2Map,R1Map,R1Map,R3Map,R3Map]},
    R5            = {DFO, JQN5, [R2Map,R2Map,R1Map,R1Map,EOS]},
%   info_msg(hcet_q05, [get(self), R1, R2, RE], before_tests, 50),

    {inorder,
     [
      ?_assertMatch(ok, mnesia:start()),
      ?_assertMatch(ok, timer:sleep(1000)),
      ?_assertMatch(ok, gen_server:call(JQN3, S3)),
      ?_assertMatch(ok, gen_server:call(JQN5, S5)),

      %% check state of qn-s
      ?_assertMatch(35,     length(hcet_get_PD(TPQN1))),
      ?_assertMatch(35,     length(hcet_get_PD(TPQN2))),
      ?_assertMatch(42,     length(hcet_get_PD(JQN3))),
      ?_assertMatch(35,     length(hcet_get_PD(TPQN4))),
      ?_assertMatch(42,     length(hcet_get_PD(JQN5))),

      %% start evaluation 
      ?_assertMatch(ok,            gen_server:call(JQN5, E5)),

      %% send empty messages to JQN5
      %% works only with block_size=5 !!! (iztok,2016/01/31)
      ?_assertMatch({'$gen_cast', R1},       hcet_send_empty(JQN5, R1)),
      ?_assertMatch({'$gen_cast', R2},       hcet_send_empty(JQN5, R2)),
      ?_assertMatch({'$gen_cast', R3},       hcet_send_empty(JQN5, R3)),
      ?_assertMatch({'$gen_cast', R4},       hcet_send_empty(JQN5, R4)),
      ?_assertMatch({'$gen_cast', R5},       hcet_send_empty(JQN5, R5))
     ]}.

hcet_tp5qn3(Pid) ->
    QueryNodeId   = "1",
    QueryId       = "5",
    SessionId     = "1",
    Id            = list_to_atom(SessionId++"-"++QueryId++"-"++QueryNodeId),
    TPQN1         = tp_query_node:spawn_process(Id, node()),
    TriplePattern = {"?i1", eI("slovenia"),  eI("hasCapital"), "?x"},
    SelectPred    = none,
    ProjectList   = none,
    ParentPid     = Pid,
    VarsPositions = #{"?i1" => 1, "?x" => 4},
    M             = {start, QueryNodeId, QueryId, SessionId, TPQN1, TriplePattern,
		     SelectPred, ProjectList, ParentPid, VarsPositions, outer},
    gen_server:call(TPQN1, M),
    TPQN1.

hcet_tp5qn3a(Pid) ->
    QueryNodeId   = "1",
    QueryId       = "5a",
    SessionId     = "1",
    Id            = list_to_atom(SessionId++"-"++QueryId++"-"++QueryNodeId),
    TPQN1a        = tp_query_node:spawn_process(Id, node()),
    TriplePattern = {"?i1", eI("slovenia"),  eI("hasCapital"), "?x"},
    SelectPred    = none,
    ProjectList   = none,
    ParentPid     = Pid,
    VarsPositions = #{"?i1" => 1, "?x" => 4},
    M             = {start, QueryNodeId, QueryId, SessionId, TPQN1a, TriplePattern,
		     SelectPred, ProjectList, ParentPid, VarsPositions, outer},
    gen_server:call(TPQN1a, M),
    TPQN1a.

hcet_tp5qn4(Pid) ->
    QueryNodeId   = "2",
    QueryId       = "5",
    SessionId     = "1",
    Id            = list_to_atom(SessionId++"-"++QueryId++"-"++QueryNodeId),
    TPQN2         = tp_query_node:spawn_process(Id, node()),
    TriplePattern = {"?i2", "?y", eI("livesIn"), "?x"},
    SelectPred    = none,
    ProjectList   = none,
    ParentPid     = Pid,
    VarsPositions = #{"?i2" => 1, "?y" => 2, "?x" => 4},
    M             = {start, QueryNodeId, QueryId, SessionId, TPQN2, TriplePattern,
		     SelectPred, ProjectList, ParentPid, VarsPositions, inner},
    gen_server:call(TPQN2, M),
    TPQN2.

hcet_tp5qn4a(Pid) ->
    QueryNodeId   = "2",
    QueryId       = "5a",
    SessionId     = "1",
    Id            = list_to_atom(SessionId++"-"++QueryId++"-"++QueryNodeId),
    TPQN2a        = tp_query_node:spawn_process(Id, node()),
    TriplePattern = {"?i2", "?y", eI("livesIn"), "?x"},
    SelectPred    = none,
    ProjectList   = none,
    ParentPid     = Pid,
    VarsPositions = #{"?i2" => 1, "?y" => 2, "?x" => 4},
    M             = {start, QueryNodeId, QueryId, SessionId, TPQN2a, TriplePattern,
		     SelectPred, ProjectList, ParentPid, VarsPositions, inner},
    gen_server:call(TPQN2a, M),
    TPQN2a.

hcet_tp5qn5(Pid) ->
    QueryNodeId   = "4",
    QueryId       = "5",
    SessionId     = "1",
    Id            = list_to_atom(SessionId++"-"++QueryId++"-"++QueryNodeId),
    TPQN4         = tp_query_node:spawn_process(Id, node()),
    TriplePattern = {"?i4", "?y", eI("worksAt"), eI("ijs")},
    SelectPred    = none,
    ProjectList   = none,
    ParentPid     = Pid,
    VarsPositions = #{"?i4" => 1, "?y" => 2},
    M             = {start, QueryNodeId, QueryId, SessionId, TPQN4, TriplePattern,
		     SelectPred, ProjectList, ParentPid, VarsPositions, inner},
    gen_server:call(TPQN4, M),
    TPQN4.

hcet_tp5qn5a(Pid) ->
    QueryNodeId   = "4",
    QueryId       = "5a",
    SessionId     = "1",
    Id            = list_to_atom(SessionId++"-"++QueryId++"-"++QueryNodeId),
    TPQN4a        = tp_query_node:spawn_process(Id, node()),
    TriplePattern = {"?i4", "?y", eI("worksAt"), eI("ijs")},
    SelectPred    = none,
    ProjectList   = none,
    ParentPid     = Pid,
    VarsPositions = #{"?i4" => 1, "?y" => 2},
    M             = {start, QueryNodeId, QueryId, SessionId, TPQN4a, TriplePattern,
		     SelectPred, ProjectList, ParentPid, VarsPositions, inner},
    gen_server:call(TPQN4a, M),
    TPQN4a.

hcet_q06() ->
    %%
    %%  query: using three query nodes for each tp
    %%
    %%  slovenia hasCapital ?x
    %%  ?y livesIn ?x
    %%  ?y worksAt ijs
    %%  
    info_msg(hcet_q06, [get(self)], start, 50),

    Tab = db_interface:dot_get_tn(),

    %% creating processes
    QueryNodeId3  = "3",
    QueryId       = "6",
    SessionId     = "1",
    Id3           = list_to_atom(SessionId++"-"++QueryId++"-"++QueryNodeId3),
    JQN3          = join_query_node:spawn_process(Id3, node()),

    QueryNodeId5  = "5",
    Id5           = list_to_atom(SessionId++"-"++QueryId++"-"++QueryNodeId5),
    JQN5          = join_query_node:spawn_process(Id5, node()),

    TPQN1         = hcet_tp6qn3(JQN3),
    TPQN1a        = hcet_tp6qn3a(JQN3),
    TPQN1b        = hcet_tp6qn3b(JQN3),
    TPQN2         = hcet_tp6qn4(JQN3),
    TPQN2a        = hcet_tp6qn4a(JQN3),
    TPQN2b        = hcet_tp6qn4b(JQN3),
    TPQN4         = hcet_tp6qn5(JQN5),
    TPQN4a        = hcet_tp6qn5a(JQN5),
    TPQN4b        = hcet_tp6qn5b(JQN5),

    %% first join 
    GraphPattern3  = maps:from_list(
		       [{"1", {"?i1", eI("slovenia"), eI("hasCapital"), "?x"}},
			{"2", {"?i2", "?y", eI("livesIn"), "?x"}}]),
    SelectPred     = none,
    ProjectList    = none,
    ParentPid3     = JQN5,
    OuterPids3     = [TPQN1,TPQN1a,TPQN1b],
    InnerPids3     = [TPQN2,TPQN2a,TPQN2b],
    VarsPositions3 = #{"?i1"  => [{"1", 1}],
		       "?i2"  => [{"2", 1}],
		       "?i4"  => [{"4", 1}],
		       "?x"   => [{"1", 4}, {"2", 4}],
		       "?y"   => [{"2", 2}]},
    JoinVars3      = ["?x"],

    %% second join 
    GraphPattern5  = maps:from_list(
		       [{"1", {"?i1", eI("slovenia"), eI("hasCapital"), "?x"}},
			{"2", {"?i2", "?y", eI("livesIn"), "?x"}},
			{"4", {"?i4", "?y", eI("worksAt"), eI("ijs")}}]),
    ParentPid5     = self(),
    OuterPids5     = [JQN3],
    InnerPids5     = [TPQN4,TPQN4a,TPQN4b],
    VarsPositions5 = #{"?i1"  => [{"1", 1}],
		       "?i2"  => [{"2", 1}],
 		       "?i4"  => [{"4", 1}],
		       "?x"   => [{"1", 4}, {"2", 4}],
		       "?y"   => [{"2", 2}, {"4", 2}]},
    JoinVars5      = ["?y"],

    %% data to be returned
    T1 = eT({Tab, "id22", "slovenia", "hasCapital", "ljubljana"}),
    T2 = eT({Tab, "id38", "luka", "livesIn", "ljubljana"}),
    T3 = eT({Tab, "id47", "luka", "worksAt", "ijs"}),
    T4 = eT({Tab, "id40", "nika", "livesIn", "ljubljana"}),
    T5 = eT({Tab, "id49", "nika", "worksAt", "ijs"}),
    T6 = eT({Tab, "id41", "marko", "livesIn", "ljubljana"}),
    T7 = eT({Tab, "id50", "marko", "worksAt", "ijs"}),

    %% messages for JQN3 and JQN5
    DFO           = data_outer,
    EOS           = end_of_stream,

    S3            = {start, QueryNodeId3, QueryId, SessionId, JQN3, GraphPattern3,
		     SelectPred, ProjectList, ParentPid3, OuterPids3, InnerPids3, 
                     VarsPositions3, JoinVars3},
    S5            = {start, QueryNodeId5, QueryId, SessionId, JQN5, GraphPattern5,
		     SelectPred, ProjectList, ParentPid5, OuterPids5, InnerPids5, 
                     VarsPositions5, JoinVars5},
    E5            = {eval, []},

    %% tuples to be returned
    R1Map         = maps:put("4", T3, maps:put("2", T2, maps:put("1", T1, maps:new()))),
    R2Map         = maps:put("4", T5, maps:put("2", T4, maps:put("1", T1, maps:new()))),
    R3Map         = maps:put("4", T7, maps:put("2", T6, maps:put("1", T1, maps:new()))),
    R1            = {DFO, JQN5, [R3Map,R3Map,R3Map,R2Map,R2Map]},
    R2            = {DFO, JQN5, [R2Map,R1Map,R1Map,R1Map,R3Map]},
    R3            = {DFO, JQN5, [R3Map,R3Map,R2Map,R2Map,R2Map]},
    R4            = {DFO, JQN5, [R1Map,R1Map,R1Map,R3Map,R3Map]},
    R5            = {DFO, JQN5, [R3Map,R2Map,R2Map,R2Map,R1Map]},
    R6            = {DFO, JQN5, [R1Map,R1Map,R3Map,R3Map,R3Map]},
    R7            = {DFO, JQN5, [R2Map,R2Map,R2Map,R1Map,R1Map]},
    R8            = {DFO, JQN5, [R1Map,R3Map,R3Map,R3Map,R2Map]},
    R9            = {DFO, JQN5, [R2Map,R2Map,R1Map,R1Map,R1Map]},
    RE            = {DFO, JQN5, [R1Map,EOS]},
%   info_msg(hcet_q06, [get(self), R1, RE], before_tests, 50),

    {inorder,
     [
      ?_assertMatch(ok, mnesia:start()),
      ?_assertMatch(ok, timer:sleep(1000)),
      ?_assertMatch(ok, gen_server:call(JQN3, S3)),
      ?_assertMatch(ok, gen_server:call(JQN5, S5)),

      %% check state of qn-s
      ?_assertMatch(35,     length(hcet_get_PD(TPQN1))),
      ?_assertMatch(35,     length(hcet_get_PD(TPQN2))),
      ?_assertMatch(42,     length(hcet_get_PD(JQN3))),
      ?_assertMatch(35,     length(hcet_get_PD(TPQN4))),
      ?_assertMatch(42,     length(hcet_get_PD(JQN5))),

      %% start evaluation 
      ?_assertMatch(ok,            gen_server:call(JQN5, E5)),

      %% send empty messages to JQN5
      ?_assertMatch({'$gen_cast', R1},       hcet_send_empty(JQN5, R1)),
      ?_assertMatch({'$gen_cast', R2},       hcet_send_empty(JQN5, R2)),
      ?_assertMatch({'$gen_cast', R3},       hcet_send_empty(JQN5, R3)),
      ?_assertMatch({'$gen_cast', R4},       hcet_send_empty(JQN5, R4)),
      ?_assertMatch({'$gen_cast', R5},       hcet_send_empty(JQN5, R5)),
      ?_assertMatch({'$gen_cast', R6},       hcet_send_empty(JQN5, R6)),
      ?_assertMatch({'$gen_cast', R7},       hcet_send_empty(JQN5, R7)),
      ?_assertMatch({'$gen_cast', R8},       hcet_send_empty(JQN5, R8)),
      ?_assertMatch({'$gen_cast', R9},       hcet_send_empty(JQN5, R9)),
      ?_assertMatch({'$gen_cast', R1},       hcet_send_empty(JQN5, R1)),
      ?_assertMatch({'$gen_cast', R2},       hcet_send_empty(JQN5, R2)),
      ?_assertMatch({'$gen_cast', R3},       hcet_send_empty(JQN5, R3)),
      ?_assertMatch({'$gen_cast', R4},       hcet_send_empty(JQN5, R4)),
      ?_assertMatch({'$gen_cast', R5},       hcet_send_empty(JQN5, R5)),
      ?_assertMatch({'$gen_cast', R6},       hcet_send_empty(JQN5, R6)),
      ?_assertMatch({'$gen_cast', R7},       hcet_send_empty(JQN5, R7)),
      ?_assertMatch({'$gen_cast', RE},       hcet_send_empty(JQN5, RE))
     ]}.

hcet_tp6qn3(Pid) ->
    QueryNodeId   = "1",
    QueryId       = "6",
    SessionId     = "1",
    Id            = list_to_atom(SessionId++"-"++QueryId++"-"++QueryNodeId),
    TPQN1         = tp_query_node:spawn_process(Id, node()),
    TriplePattern = {"?i1", eI("slovenia"),  eI("hasCapital"), "?x"},
    SelectPred    = none,
    ProjectList   = none,
    ParentPid     = Pid,
    VarsPositions = #{"?i1" => 1, "?x" => 4},
    M             = {start, QueryNodeId, QueryId, SessionId, TPQN1, TriplePattern,
		     SelectPred, ProjectList, ParentPid, VarsPositions, outer},
    gen_server:call(TPQN1, M),
    TPQN1.

hcet_tp6qn3a(Pid) ->
    QueryNodeId   = "1",
    QueryId       = "6a",
    SessionId     = "1",
    Id            = list_to_atom(SessionId++"-"++QueryId++"-"++QueryNodeId),
    TPQN1        = tp_query_node:spawn_process(Id, node()),
    TriplePattern = {"?i1", eI("slovenia"),  eI("hasCapital"), "?x"},
    SelectPred    = none,
    ProjectList   = none,
    ParentPid     = Pid,
    VarsPositions = #{"?i1" => 1, "?x" => 4},
    M             = {start, QueryNodeId, QueryId, SessionId, TPQN1, TriplePattern,
		     SelectPred, ProjectList, ParentPid, VarsPositions, outer},
    gen_server:call(TPQN1, M),
    TPQN1.

hcet_tp6qn3b(Pid) ->
    QueryNodeId   = "1",
    QueryId       = "6b",
    SessionId     = "1",
    Id            = list_to_atom(SessionId++"-"++QueryId++"-"++QueryNodeId),
    TPQN1        = tp_query_node:spawn_process(Id, node()),
    TriplePattern = {"?i1", eI("slovenia"),  eI("hasCapital"), "?x"},
    SelectPred    = none,
    ProjectList   = none,
    ParentPid     = Pid,
    VarsPositions = #{"?i1" => 1, "?x" => 4},
    M             = {start, QueryNodeId, QueryId, SessionId, TPQN1, TriplePattern,
		     SelectPred, ProjectList, ParentPid, VarsPositions, outer},
    gen_server:call(TPQN1, M),
    TPQN1.

hcet_tp6qn4(Pid) ->
    QueryNodeId   = "2",
    QueryId       = "6",
    SessionId     = "1",
    Id            = list_to_atom(SessionId++"-"++QueryId++"-"++QueryNodeId),
    TPQN2         = tp_query_node:spawn_process(Id, node()),
    TriplePattern = {"?i2", "?y", eI("livesIn"), "?x"},
    SelectPred    = none,
    ProjectList   = none,
    ParentPid     = Pid,
    VarsPositions = #{"?i2" => 1, "?y" => 2, "?x" => 4},
    M             = {start, QueryNodeId, QueryId, SessionId, TPQN2, TriplePattern,
		     SelectPred, ProjectList, ParentPid, VarsPositions, inner},
    gen_server:call(TPQN2, M),
    TPQN2.

hcet_tp6qn4a(Pid) ->
    QueryNodeId   = "2",
    QueryId       = "6a",
    SessionId     = "1",
    Id            = list_to_atom(SessionId++"-"++QueryId++"-"++QueryNodeId),
    TPQN2        = tp_query_node:spawn_process(Id, node()),
    TriplePattern = {"?i2", "?y", eI("livesIn"), "?x"},
    SelectPred    = none,
    ProjectList   = none,
    ParentPid     = Pid,
    VarsPositions = #{"?i2" => 1, "?y" => 2, "?x" => 4},
    M             = {start, QueryNodeId, QueryId, SessionId, TPQN2, TriplePattern,
		     SelectPred, ProjectList, ParentPid, VarsPositions, inner},
    gen_server:call(TPQN2, M),
    TPQN2.

hcet_tp6qn4b(Pid) ->
    QueryNodeId   = "2",
    QueryId       = "6b",
    SessionId     = "1",
    Id            = list_to_atom(SessionId++"-"++QueryId++"-"++QueryNodeId),
    TPQN2        = tp_query_node:spawn_process(Id, node()),
    TriplePattern = {"?i2", "?y", eI("livesIn"), "?x"},
    SelectPred    = none,
    ProjectList   = none,
    ParentPid     = Pid,
    VarsPositions = #{"?i2" => 1, "?y" => 2, "?x" => 4},
    M             = {start, QueryNodeId, QueryId, SessionId, TPQN2, TriplePattern,
		     SelectPred, ProjectList, ParentPid, VarsPositions, inner},
    gen_server:call(TPQN2, M),
    TPQN2.

hcet_tp6qn5(Pid) ->
    QueryNodeId   = "4",
    QueryId       = "6",
    SessionId     = "1",
    Id            = list_to_atom(SessionId++"-"++QueryId++"-"++QueryNodeId),
    TPQN4         = tp_query_node:spawn_process(Id, node()),
    TriplePattern = {"?i4", "?y", eI("worksAt"), eI("ijs")},
    SelectPred    = none,
    ProjectList   = none,
    ParentPid     = Pid,
    VarsPositions = #{"?i4" => 1, "?y" => 2},
    M             = {start, QueryNodeId, QueryId, SessionId, TPQN4, TriplePattern,
		     SelectPred, ProjectList, ParentPid, VarsPositions, inner},
    gen_server:call(TPQN4, M),
    TPQN4.

hcet_tp6qn5a(Pid) ->
    QueryNodeId   = "4",
    QueryId       = "6a",
    SessionId     = "1",
    Id            = list_to_atom(SessionId++"-"++QueryId++"-"++QueryNodeId),
    TPQN4        = tp_query_node:spawn_process(Id, node()),
    TriplePattern = {"?i4", "?y", eI("worksAt"), eI("ijs")},
    SelectPred    = none,
    ProjectList   = none,
    ParentPid     = Pid,
    VarsPositions = #{"?i4" => 1, "?y" => 2},
    M             = {start, QueryNodeId, QueryId, SessionId, TPQN4, TriplePattern,
		     SelectPred, ProjectList, ParentPid, VarsPositions, inner},
    gen_server:call(TPQN4, M),
    TPQN4.

hcet_tp6qn5b(Pid) ->
    QueryNodeId   = "4",
    QueryId       = "6b",
    SessionId     = "1",
    Id            = list_to_atom(SessionId++"-"++QueryId++"-"++QueryNodeId),
    TPQN4        = tp_query_node:spawn_process(Id, node()),
    TriplePattern = {"?i4", "?y", eI("worksAt"), eI("ijs")},
    SelectPred    = none,
    ProjectList   = none,
    ParentPid     = Pid,
    VarsPositions = #{"?i4" => 1, "?y" => 2},
    M             = {start, QueryNodeId, QueryId, SessionId, TPQN4, TriplePattern,
		     SelectPred, ProjectList, ParentPid, VarsPositions, inner},
    gen_server:call(TPQN4, M),
    TPQN4.
 
%% ====> END OF LINE <====

%%
%% Query Tree process
%%
%% @copyright 2014-2019 UP FAMNIT and Yahoo Japan Corporation
%% @version 0.3
%% @since May, 2014
%% @author Iztok Savnik <iztok.savnik@famnit.upr.si>
%% @author Kiyoshi Nitta <knitta@yahoo-corp.jp>
%% 
%% @doc Query tree module implements process that serves as front-end 
%% of query tree represented as tree of inter-connected processes running 
%% on array of servers. Message start initializes query tree. Message 
%% eval starts evaluation of query tree.
%%
%% Query tree process determines the location of each query node in terms
%% of column and row in array of servers. Each query node is executed on 
%% location determined by query tree process. 
%% Firstly, the column of query node is computed by using distribution function
%% that translates triple-pattern to columns in array of servers. 
%% Secondly, rows in given columns are selected dynamically based on 
%% CPU load of servers in columns.
%% 
%% Query of type qt_query() presented to query tree process as parameter of 
%% message start is converted into tree data structure stored as process 
%% dictionary entry query_tree. First element of 
%% list representing qt_query() is triple-pattern of the left-most query
%% node. Last element of list is triple-pattern of the upper-most and 
%% right-most query node. All other triple-patterns are placed as inner 
%% query nodes in order between left-most and upper-right-most.
%%
%%
%% <table bgcolor="lemonchiffon">
%% <tr><th>Section Index</th></tr>
%% <tr><td>{@section property list}</td></tr>
%% <tr><td>{@section type qt_query_node()}</td></tr>
%% <tr><td>{@section handle_call (synchronous) message API}</td></tr>
%% <tr><td>{@section handle_cast (asynchronous) message API}</td></tr>
%% <tr><td>{@section existent shortcuts}</td></tr>
%% </table>
%% 
%% == property list ==
%% 
%% (LINK: {@section property list})
%% 
%% The gen_server process uses following properties holded by {@link
%% qt_state()}.
%% 
%% <table border="3">
%% <tr><th>Name</th><th>Type</th><th>Description</th></tr>
%% 
%% <tr> <td>created</td> <td>boolean()</td> <td>true denotes that
%% process dictionary was created and used. false denotes that
%% completely new process.</td> </tr>
%% 
%% <tr> <td>tree_id</td> <td>string()</td> <td>query tree identifier</td> </tr>
%% 
%% <tr> <td>state</td> <td>atom()</td> <td>active | inactive | eos</td> </tr>
%% 
%% <tr> <td>gp</td> <td>maps:map()</td> <td>graph pattern of query tree
%% represented as mapping from {@type query_node:qn_id()} to 
%% {@type query_node:qn_triple_pattern()}</td> </tr>
%% 
%% <tr> <td>node_pids</td> <td>maps:map()</td> <td>mapping from {@type query_node:qn_id()} to 
%% [{@type node_state:ns_pid()}]</td> </tr>
%% 
%% <tr> <td>self</td> <td>{@link
%% node_state:ns_pid()}</td> <td>location of query
%% tree process</td> </tr>
%% 
%% <tr> <td>session_id</td> <td>integer()</td> <td>session identifier</td> </tr>
%% 
%% <tr> <td>invoker</td> <td>{@link
%% node_state:ns_pid()}</td> <td>location of parent query
%% node process</td> </tr>
%% 
%% <tr> <td>query</td> <td>{@link qt_query()}</td> <td>current
%% query in flat form</td> </tr>
%% 
%% <tr> <td>query_tree</td> <td>{@link qt_query_node()}</td> <td>query 
%% tree of input query of type {@section qt_query()}</td> </tr>
%% 
%% <tr> <td>roots</td> <td>{@link node_state:ns_pid_list()}</td> <td>long pids of
%% roots of query tree</td> </tr>
%% 
%% <tr> <td>queue_result</td> <td>queue()</td> <td>temporary storage for
%% results of query evaluation</td> </tr>
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
%% <tr> <td>pid_start</td> <td>maps:map()</td> <td>mapping from term()
%% to erlang:timestamp().</td> </tr>
%% 
%% <tr> <td>pid_elapse</td> <td>maps:map()</td> <td>mapping from
%% term() to integer() (in microseconds).</td> </tr>
%% 
%% <tr> <td>result_freq</td> <td>maps:map()</td> <td>mapping from
%% pid() to integer().</td> </tr>
%% 
%% <tr> <td>start_date_time</td> <td>calendar:datetime()</td>
%% <td>started date and time of the process.</td> </tr>
%% 
%% </table>
%%
%%
%% == type qt_query_node() ==
%% 
%% (LINK: {@section type qt_query_node()})
%% 
%% Query tree used for internal representation of query is based on maps.
%% Each node of query tree is a map storing all properties of query node to 
%% be run as (tp|join)_query_node process. The properties of query nodes 
%% follow structure and properties of (tp|join)_query_node processes.
%% 
%% <table border="3">
%% <tr><th>Name</th><th>Type</th><th>Description</th></tr>
%% 
%% <tr> <td>node_id</td> <td>string()</td> <td>query node identifier</td> </tr>
%% 
%% <tr> <td>state</td> <td>atom()</td> <td>active | inactive | eos</td> </tr>
%% 
%% <tr> <td>gp</td> <td>maps:map()</td> <td>graph pattern of query tree
%% represented as mapping from {@type query_node:qn_id()} to 
%% {@type query_node:qn_triple_pattern()}</td> </tr>
%% 
%% <tr> <td>select_pred</td> <td>{@link query_node:qn_select_predicate()}</td> 
%% <td>select expression represented as abstract syntax tree including 
%% operations on strings, booleans and integers</td> </tr>
%% 
%% <tr> <td>project_list</td> <td>{@link query_node:qn_project_list()}</td> 
%% <td>list of variables to be included in graphs computed in 
%% a given query node. Only the triples that include values of specified 
%% variables are included in result. </td> </tr>
%% 
%% <tr> <td>location</td> <td>[{@link node_state:ns_pid()}]</td> 
%% <td>list of locations of query node processes</td> </tr>
%% 
%% <tr> <td>invoker</td> <td>{@link
%% node_state:ns_pid()}</td> <td>process id of parent query
%% node</td> </tr>
%% 
%% <tr> <td>query</td> <td>{@link qt_query()}</td> <td>query represented 
%% by given qt_query_node()</td> </tr>
%% 
%% <tr> <td>outer</td> <td>qt_query_node()</td> <td>outer sub-tree of 
%% given query tree</td> </tr>
%% 
%% <tr> <td>inner</td> <td>qt_query_node()</td> <td>inner sub-tree of 
%% given query tree</td> </tr>
%% 
%% <tr> <td>outer_pids</td> <td>[{@link
%% node_state:ns_pid()}]</td> <td>process ids of outer
%% children query nodes</td> </tr>
%% 
%% <tr> <td>inner_pids</td> <td>[{@link
%% node_state:ns_pid()}]</td> <td>process ids of inner
%% children query nodes</td> </tr>
%% 
%% <tr> <td>join_vars</td> <td>[{@link query_node:qn_var()}]</td> <td>list of
%% variables used for joining</td> </tr>
%% 
%% <tr> <td>var_pos</td> <td>maps:map()</td> <td>mapping from {@link
%% query_node:qn_var()} to {@link join_query_node:jqn_var_position()}; 
%% general form, to be used only in gp</td> </tr>
%% 
%% <tr> <td>vars</td> <td>maps:map()</td> <td>mapping from {@link
%% query_node:qn_var()} to integer()</td>; to be used only in tp</tr>
%% 
%% <tr> <td>var_values</td> <td>maps:map()</td> <td>mapping from
%% {@link query_node:qn_var()} to string() (not used)</td> </tr>
%% 
%% <tr> <td>inner_outer</td> <td>inner | outer</td> <td>Position to
%% its parent query node.</td> </tr>
%% 
%% </table>
%%
%% 
%% == handle_call (synchronous) message API ==
%% 
%% (LINK: {@section handle_call (synchronous) message API})
%% 
%% === {start, Query, Self, Invoker, TreeId, SessionId} ===
%% 
%% Initialization of join query node process. All parameters are 
%% saved to process dictionary. 
%% (LINK: {@section @{start, Query, Self, Invoker, TreeId, SessionId @}})
%%
%% Query is {@link qt_query()}, Self is {@link
%% node_state:ns_pid()}. Invoker is {@link
%% node_state:ns_pid()}, TreeId is integer(), SessionId is integer().
%% This request is implemented by {@link hc_start/5}. 
%% 
%% === {eval} ===
%%
%% Initiate evaluation of query tree by sending root query node message eval.
%% After sending eval, N empty messages are sent to the root of query tree, and
%% results are expected in the form of data_outer messages from root of 
%% query tree. 
%% (LINK: {@section @{eval@}})
%%
%% This request is implemented by {@link hc_eval/0}. 
%%
%% === {get, Name} ===
%% 
%% Return the value of specified property name. If Name=all then return complete 
%% process dictionary. Variable Name is an atom(). 
%% (LINK: {@section @{get_property, Name@}}).
%%
%% This request is implemented by {@link hc_get_property/1}. 
%% 
%% == handle_cast (asynchronous) message API ==
%% 
%% (LINK: {@section handle_cast (asynchronous) message API})
%% 
%% === {data_outer, Pid, Graph} ===
%% 
%% Root of query tree is sending results to its initiator query_tree process.
%% Each graph received from root of query tree as data_outer message 
%% is presented to caller process.
%% (LINK: {@section @{data_outer, Pid, Graph@}})
%% 
%% Pid is node_state:ns_pid() and Graph is query_node:qn_graph().
%% This request is implemented by {@link hc_data_outer/3}. 
%%
%% == existent shortcuts ==
%% 
%% (LINK: {@section existent shortcuts})
%% 
%% Existent shortcuts in the implementation of query_tree. 
%% 
%% <table border="3">
%% <tr><th>Num</th><th>Function</th><th>Description</th></tr>
%% 
%% <tr> <td>1.</td> <td>hc_start()</td> <td>We suppose that prop_clm returns 
%% single column for a given predicate. However, data structures as well as
%% modules tp_query_node and join_query_node are implemented to support
%% accessing multiple tp_query_node access methods.</td> </tr>
%% 
%% <tr> <td>2.</td> <td>hc_start()</td> <td>Load balancing among the row nodes
%% for a given column is currently implemented by selecting random row. Next 
%% version will dynamically gather statistics about running query nodes and 
%% relations among sessions and locations of runnning query nodes.</td> </tr>
%% 
%% </table>
%%
%% 
%% @type qt_state() = maps:map(). Map
%% structure that manages properties for operating the gen_server
%% process.
%% 
%% @type qt_query() = qt_bin_query() | qt_tp_query(). 
%% Query provided to query_tree module is built from algebra operations
%% tp (triple pattern), join, leftjoin, union or differ. 
%% Query is either binary query or triple pattern query. 
%%
%% @type qt_bin_query() = {query_node:qn_opsyn(), qt_query(), qt_query(), query_node:qn_select_predicate(), query_node:qn_project_list()}. 
%% Binary query is built from algebra operations join, leftjoin, union 
%% or differ. Query operation is described by tuple including operation symbol, 
%% two input streams of graphs which are results of qt_query() 
%% select predicate and project list. 
%%
%% @type qt_tp_query() = {query_node:qn_opsyn(), query_node:qn_triple_pattern(), query_node:qn_select_predicate(), query_node:qn_project_list()}. 
%% Triple pattern query describes access method defined for given 
%% triple pattern, select predicate and project list. 
%%
%% @type qt_query_node() = maps:map(). Query node is represented as map 
%% storing properties that resemble those of {tp|join}_query_node 
%% processes. All properties must be prepared before creating the 
%% tree of {tp|join}_query_node processes.
%% qt_query_node() map is presented in detail in 
%% {@section Type qt_query_node()}.
%% 
%%
-module(query_tree).
-behavior(gen_server).
-export(
   [

    eI/1, eT/1, eTP/1, dT/1, dTMap/1, dTML/1, dTMLL/1,

    child_spec/1, init/1, handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3, spawn_process/2

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
%% @spec init([]) -> {ok, qt_state()}
%% 
init([]) ->
    process_flag(trap_exit, true),
    {ok, BSN} = application:get_env(b3s, b3s_state_nodes),
    BS  = {b3s_state, lists:nth(1, BSN)},
    MQD = gen_server:call(BS, {get, mq_debug}),
    State = #{wait            => true,
	      start_date_time => calendar:local_time(),
	      mq_debug        => MQD,
	      pid             => self()},

    %% init result queue
    query_node:queue_init(result, plain, data_block),

    info_msg(init, [], State, -1),
    {ok, State}.

%% 
%% handle_call/3
%% 
%% @doc Handle synchronous query requests.
%% 
%% @spec handle_call(term(), {pid(), term()}, qt_state()) -> {reply,
%% term(), qt_state()}
%% 
handle_call({start, Query, Self, Invoker, TreeId, SessionId}, _, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(get(created), State),
    info_msg(handle_call, [Self, {message,start}, {query,Query}, {self,Self}, 
                           {invoker,Invoker}, {tree_id,TreeId}, {session_id,SessionId}, get(state)], message_received, 10),
    hc_start(Query, Self, Invoker, TreeId, SessionId),
    {reply, ok, hc_save_pd()};

handle_call({eval}, _, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(get(created), State),
    info_msg(handle_call, [get(self), {message,eval}, get(state)], message_received, 10),
    hc_eval(),
    {reply, ok, hc_save_pd()};

handle_call({get, all}, _, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(get(created), State),
    info_msg(handle_call, [get(self), {message,get}, {get,all}, {value,get()}, get(state)], message_received, 10),
    {reply, get(), hc_save_pd()};

handle_call({get, Name}, _, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(get(created), State),
    info_msg(handle_call, [get(self), {message,get}, {get,Name}, {value,get(Name)}, get(state)], message_received, 10),
    {reply, get(Name), hc_save_pd()};

handle_call({put, Name, Value}, _, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(get(created), State),
    put(Name, Value),
    info_msg(handle_call, [get(self), {message,put}, {put,Name}, {value,Value}, get(state)], message_received, 10),
    {reply, get(Name), hc_save_pd()};

handle_call({read}, _, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(get(created), State),
    info_msg(handle_call, [get(self), {message,read}, {invoker,get(invoker)}, get(state)], message_received,  30),
    Block = hc_read(),
    {reply, Block, hc_save_pd()};

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
%% @spec handle_cast(term(), qt_state()) -> {noreply, qt_state()}
%% 
handle_cast({data_outer, From, Block}, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(get(created), State),
    info_msg(handle_cast, [get(self), {message,data_outer}, {from,From}, {block,Block}, get(state)], message_received,  30),
    hc_data_outer(From, Block),
    {noreply, hc_save_pd()};

%% just for suppressing errors on benchmark task executions
handle_cast({empty, From}, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    info_msg(handle_cast, [get(self), {message,empty}, {from,From}, get(state)], message_received,  30),
    {noreply, State};

%% default
handle_cast(Request, State) ->
    R = {unknown_request, Request},
    error_msg(handle_cast, [get(self), Request, State], R),
    {noreply, hc_save_pd()}.

%% 
%% @doc Restore process dictionaries from state map structure.
%% 
%% @spec hc_restore_pd([{atom(), term()}] | undefined, qt_state()) -> ok
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
%% @spec hc_save_pd() -> qt_state()
%% 
hc_save_pd() ->
    maps:from_list(get()).

%% 
%% handle_info/2
%% 
%% @doc Handle exceptional query requests. 
%% 
%% @spec handle_info(term(), qt_state()) -> {noreply, qt_state()}
%% 
handle_info(_Info, State) ->
    {noreply, State}.

%% 
%% terminate/2
%% 
%% @doc Process termination. 
%% 
%% @spec terminate(term(), qt_state()) -> none()
%% 
terminate(Reason, State) ->
    P = "pid: " ++ pid_to_list(self()),
    info_msg(terminate, [Reason, State], P, -1),
    ok.

%% 
%% code_change/3
%% 
%% @doc Process code change action. 
%% 
%% @spec code_change(term(), qt_state(), term()) -> {ok, qt_state()}
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
%% @spec child_spec( Id::atom() ) -> supervisor:child_spec()
%% 
child_spec(Id) ->
    GSOpt = [{local, Id}, query_tree, [], []],
    StartFunc = {gen_server, start_link, GSOpt},
    Restart = permanent,
    Shutdwon = 1000,
    Type = worker,
    Modules = [query_tree],

    {Id, StartFunc, Restart, Shutdwon, Type, Modules}.

%% 
%% @doc Spawns process which is an instance of given module with given identifier 
%% at given node.
%% 
%% @spec spawn_process( Id::atom(), Node::node() ) -> node_state:ns_pid()
%% 
spawn_process(Id, Node) -> 
    ChildSpec = query_tree:child_spec(Id),
    supervisor:start_child({b3s, Node}, ChildSpec),
    {Id, Node}.

%% ======================================================================
%% 
%% handle call/cast implementation
%% 

%% 
%% hc_start/5
%%
%% @doc Initialize query tree process. Convert query represented as a list of 
%% triple-patterns, filters, option statements and set operations into query tree
%% data structure internal to query_tree process. Determine node locations for 
%% triple-patterns, and other operations. Compute all properties needed to construct
%% query tree by means of distributed processes. Finally, create processes for 
%% query nodes and send start messages to all processes comprising query.
%% 
%% @spec hc_start( Query::qt_query(), Self::node_state:ns_pid(), 
%%                 Invoker::node_state:ns_pid(), TreeId::integer(), SessionId::integer() ) -> ok
%% 
hc_start( Query, Self, Invoker, TreeId, SessionId ) ->

    %% store patrameters in PD
    put(created, true),
    put(state, active),
    put(session_id, SessionId),
    put(tree_id, TreeId),
    put(self, Self),
    put(invoker, Invoker),
    put(query, Query),
    put(node_pids, maps:new()),
    %% queue_result is in init()
    %% reset here for executing restart successfully (1/22 knitta)
    put(queue_result, queue:new()),
    put(pause, false),

    erase(sid_table_name),
    erase(sid_max_id),
    erase(di_cursor__),
    erase(di_ets__),

    info_msg(hc_start, [get(self), {query,Query}, {invoker,Invoker}, 
                        {tree_id,TreeId}, {session_id,SessionId}], entered, 10),

    %% benchmark stuff
    BSP = b3s_state_pid,
    BMT = benchmark_task,
    BTP = benchmark_task_pid,
    RRM = result_record_max,
    TDP = triple_distributor_pid,
    DA  = distribution_algorithm,
    put(BSP, gen_server:call(node_state, {get, BSP})),
    {_, FSN} = get(BSP),
    put(BTP, {gen_server:call(get(BSP), {get, BMT}), FSN}),
    put(RRM, gen_server:call(get(BSP), {get, RRM})),
    put(TDP, gen_server:call(get(BSP), {get, TDP})),
    put(DA, gen_server:call(get(TDP), {get_property, DA})),

    %% retrieve pred_clm and clm_row_conf in PD (kiyoshi) 
    PredClm = gen_server:call(node_state, {get, pred_clm}),
    ClmRowConf = gen_server:call(node_state, {get, clm_row_conf}),
    put(pred_clm, PredClm),
    put(clm_row_conf, ClmRowConf),
    %%info_msg(hc_start, [get(self), PredClm, ClmRowConf], debug_b3s_configuration_read, 60),

    %% convert query to query tree
    QT = hcs_query_to_tree(Query),

    %% add ids to query nodes
    case maps:get(type, QT) of
	tp ->
	    QT1A = hcs_ids_to_nodes(tp, QT, 1),
	    QT1  = maps:put(side, outer, QT1A);
	QueryTreeType ->
	    QT1 = hcs_ids_to_nodes(QueryTreeType, QT, 1)
    end,

    %% compute all neccessary components of query nodes
    QT2 = hcs_comp_qn_entries(maps:get(type, QT1), QT1),
    info_msg(hc_start, [get(self), {query,Query}, {query_tree,QT2}], query_compiled, 10),
    
    %% create processes for given query nodes
    QT3 = hcs_create_processes( maps:get(type, QT2), QT2 ),
    info_msg(hc_start, [get(self), {query_tree,QT3}], processes_created, 10),

    %% set invoker of root to qt
    QT4 = maps:put(invoker, get(self), QT3),
    put(query_tree, QT4),

    %% set roots of query tree
    Loc = maps:get(location, QT4),
    put(roots, Loc),

    %% set gp of query in PD
    put(gp, maps:get(gp, QT4)),

    %% start individual query nodes of query
    hcs_start_query( maps:get(type, QT4), QT4 ),
    info_msg(hc_start, [get(self), {query,get(query)}, {node_pids,get(node_pids)}], processes_started, 10),

    ok.

%%
%% hcs_query_to_tree/1
%%
%% @doc Transforms graph pattern expressed as list of triple patterns
%% (stored in PD as 'query')
%% to left-deep query tree. The last triple pattern in the list is
%% transformed to the leftmost outer triple pattern in tree. The first
%% triple pattern in list is transformed to the highest inner triple
%% pattern of query tree.
%%
%% <i>Note</i>: triple patterns will be augmented with projection and 
%% selection lists attached to particular triple pattern.
%%
%% @spec hcs_query_to_tree(qt_query()) -> qt_query_node()
%%
hcs_query_to_tree({tp, TP, SL, PL}) ->
    ETP = string_id:encode_triple_pattern(TP),
    ESL = encode_select_expr(SL),
    info_msg(hcs_query_to_tree, [get(self), {query,{tp,TP,SL,PL}}, {encoded_tp,ETP}, {encoded_select_pred,ESL}, get(state)], tp_encoded, 50), 

    #{ type         => tp, 
       query        => ETP,
       select_pred  => ESL,
       project_list => PL };

hcs_query_to_tree({OP, QO, QI, SL, PL}) 
    when (OP =:= join) or (OP =:= hjoin) or (OP =:= mjoin) ->

    %% transform outer and inner query to tree
    Outer = hcs_query_to_tree(QO),
    Inner = hcs_query_to_tree(QI),
               
    %% encode select predicate
    ESL = encode_select_expr(SL),
    info_msg(hcs_query_to_tree, [get(self), {query,{OP,QO,QI,SL,PL}}, {encoded_select_pred,ESL}, get(state)], join_encoded, 50), 

    %% construct join query node
    #{ type         => OP,
       query        => {OP, QO, QI, SL, PL},
       select_pred  => ESL,
       project_list => PL,
       outer        => Outer,
       inner        => Inner }.

%%
%% encode_select_predicate/1
%%
%% @doc Encodes all URIs and strings (identifiers) in selection predicate 
%% of a given query node to integer keys. Identifiers of selection predicate 
%% parse tree are encoded in left depth-first order, recursively.
%%
%% @spec encode_select_expr(SP::query_node:qn_select_predicate()) -> query_node:qn_select_predicate()
%%
encode_select_expr(S) when is_atom(S) -> 
    case S of 
    none -> info_msg(encode_select_pred, [get(self), {expr,none}, {type,atom}, get(state)], encode_atom_expr, 50), 
            none;
    _    -> error_msg(encode_select_expr, [get(self), {expr,S}, {type,atom}, {all,get()}, get(state)], illegal_select_expression),
            fail
    end;

encode_select_expr(S) when is_integer(S) -> 
    info_msg(encode_select_pred, [get(self), {expr,S}, {type,integer}, get(state)], encode_integer_expr, 50), 
    {S, integer};

encode_select_expr(S) when is_float(S) -> 
    info_msg(encode_select_pred, [get(self), {expr,S}, {type,float}, get(state)], encode_float_expr, 50), 
    {S, real};

%% selection predicate is a string S.
encode_select_expr(S) when is_list(S) -> 
    info_msg(encode_select_pred, [get(self), {expr,S}, {type,string}, get(state)], encode_string_expr, 50), 

    %% check if variable
    IsVar = string:chr(S, $?) =:= 1, 
    case IsVar of 
         %% don't do anything with variables
         true  -> S;

         %% encode string to integer key
         false -> {string_id:get_id(S), code}
    end;

%% selection predicate includes comparison ops
encode_select_expr({S1, equal, S2}) -> 
    {encode_select_expr(S1), equal, encode_select_expr(S2)};

encode_select_expr({S1, less, S2}) -> 
    {encode_select_expr(S1), less, encode_select_expr(S2)};

encode_select_expr({S1, lesseq, S2}) -> 
    {encode_select_expr(S1), lesseq, encode_select_expr(S2)};

encode_select_expr({S1, greater, S2}) -> 
    {encode_select_expr(S1), greater, encode_select_expr(S2)};

encode_select_expr({S1, greatereq, S2}) -> 
    {encode_select_expr(S1), greatereq, encode_select_expr(S2)};

encode_select_expr({S1, land, S2}) -> 
    {encode_select_expr(S1), land, encode_select_expr(S2)}; 

encode_select_expr({S1, lor, S2}) -> 
    {encode_select_expr(S1), lor, encode_select_expr(S2)};

encode_select_expr({lnot, S1}) -> 
    {lnot, encode_select_expr(S1)};

encode_select_expr(Expr) ->
    error_msg(encode_select_expr, [get(self), {expr,Expr}, {all,get()}, get(state)], illegal_select_expression),
    fail.

%%
%% hcs_ids_to_nodes/3
%%
%% @doc Define ids for each node of parameter query tree QT. Nodes are
%% numbered in depth-first post-order i.e. number first left sub-tree, then 
%% number right sub-tree, and, finally root, recursively. 
%%
%% @spec hcs_ids_to_nodes(tp | gp, QT::qt_query_node(), N::integer())
%% -> qt_query_node()
%%
hcs_ids_to_nodes(tp, QT, N) -> 
    %% save id
    maps:put(node_id, integer_to_list(N), QT);

hcs_ids_to_nodes(OP, QT, N)
    when (OP =:= join) or (OP =:= hjoin) or (OP =:= mjoin) ->

    Outer = maps:get(outer, QT),
    Inner = maps:get(inner, QT),

    %% tag nodes in outer sub-tree
    NO = hcs_ids_to_nodes(maps:get(type, Outer), Outer, N),
    NO1 = maps:put(side, outer, NO),
    IDO = list_to_integer(maps:get(node_id, NO)),

    %% tag nodes in inner sub-tree
    NI = hcs_ids_to_nodes(maps:get(type, Inner), Inner, IDO+1),
    NI1 = maps:put(side, inner, NI),
    IDI = list_to_integer(maps:get(node_id, NI)),
    
    %% update QT with new inner and outer and tag with id
    QT1 = maps:put(outer, NO1, QT),
    QT2 = maps:put(inner, NI1, QT1),
    QT3 = maps:put(side, outer, QT2),
    maps:put(node_id, integer_to_list(IDI+1), QT3).
    
%%
%% hcs_comp_qn_entries/2
%%
%% @doc Given input query tree: 
%% 1) construct graph patterns for all query nodes and store it in gp
%%    (single triple pattern is also stored in gp to simplify merging gp-s);
%% 2) extract variables and their positions in triple patterns to construct
%%    var_pos; and
%% 3) extract common variables of joins and prepare join_vars.
%%
%% hcs_comp_qn_entries(Type::atom(), QT::qt_query_node()) -> qt_query_node()
%%
hcs_comp_qn_entries(tp, QT) -> 
    %% convert triple pattern to list
    TP = maps:get(query, QT),
    TL = tuple_to_list(TP),

    %% convert elements to pairs {element, index}
    put(count, 1),
    F = fun (E) ->
            N = get(count),
            put(count, N+1),
            { E, N }
        end,
    TL1 = lists:map(F, TL),
    info_msg(hcs_comp_qn_entries, [get(self), {tp,TP}, {tp_list_pair,TL1}, get(state)], debug_comp_qn_entries_1, 50), 

    %% filter list of pairs to retain vars
    F1 = fun (P) ->
             E = element(1,P),
             case is_list(E) of 
                true  -> string:chr(E,$?) =:= 1;
	        false -> false
             end
         end,
    TL2 = lists:filter(F1, TL1),
    info_msg(hcs_comp_qn_entries, [get(self), {tp,TP}, {tp_var_list_pair,TL2}, get(state)], debug_comp_qn_entries_2, 50), 

    %% construct map : vars -> {node-id, inx}
    NID = maps:get(node_id, QT),
    F2 = fun (E, M) ->
             { V, I } = E,
             maps:put(V, [{NID, I}], M)
         end,
    VP = lists:foldl(F2, #{}, TL2),

    %% construct variable positions for tp
    F3 = fun (_, V1) ->
             [{ _, I1 }] = V1,
             I1
         end,
    VP1 = maps:map(F3, VP),

    %% save graph pattern and var_pos map in query node
    QT1 = maps:put(gp, maps:put(NID, TP, #{}), QT),
    QT2 = maps:put(var_pos, VP, QT1),
    maps:put(vars, VP1, QT2);
    
hcs_comp_qn_entries(OP, QT)
    when (OP =:= join) or (OP =:= hjoin) or (OP =:= mjoin) ->

    Inner = maps:get(inner, QT),
    Outer = maps:get(outer, QT),

    %% get gp and var_pos from outer sub-tree
    NO = hcs_comp_qn_entries(maps:get(type, Outer), Outer),
    GPO = maps:get(gp, NO),
    VPO = maps:get(var_pos, NO),

    %% get gp and var_pos from inner sub-tree
    NI = hcs_comp_qn_entries(maps:get(type, Inner), Inner),
    GPI = maps:get(gp, NI),
    VPI = maps:get(var_pos, NI),
    
    %% update QT with new inner and outer
    QT1 = maps:put(outer, NO, QT),
    QT2 = maps:put(inner, NI, QT1),

    %% compute gp, var_pos and join_vars and store them in query node
    QT3 = maps:put(var_pos, merge_maps(VPO, VPI), QT2),
    QT4 = maps:put(gp, maps:merge(GPO, GPI), QT3),
    L1 = maps:keys(VPO),
    L2 = maps:keys(VPI),
    maps:put(join_vars, intersect_lists(L1, L2), QT4).

%%
%% hcs_create_processes/2
%%
%% @doc For each query node of QT select optimal rows of columns
%% associated with given predicate of triple-pattern. Spawn query node
%% at selected node. Join node is assigned to one of it's inner
%% query nodes. Therefore, current physical structure of query tree is
%% pipeline.
%%
%% @spec hcs_create_processes( OP::query_node:qn_opsym(), QT::qt_query_node()) -> qt_query_node()
%%
hcs_create_processes( tp, QT ) ->
    %% get optimal rows in columns
    Nodes = hcs_select_nodes( maps:get(type, QT), QT ),   
    info_msg(hcs_create_processes, [get(self), {tp,maps:get(gp, QT)}, 
                                    {nodes,Nodes}], tp_nodes_selected, 50),

    %% generic Id for all nodes
    Id = generate_long_qnid( maps:get(node_id, QT)),

    %% spawn tp processes at Nodes
    put(count, 1),
    F = fun (Node) ->
            %% add instance number to generic Id
            NId = list_to_atom(Id++"-"++integer_to_list(get(count))),
            put(count,get(count)+1),

            %% spawn tp query node process at node
            tp_query_node:spawn_process(NId, Node)
            
        end,
    Pids = lists:map(F, Nodes),
    info_msg(hcs_create_processes, [get(self), {pids,Pids}], tp_nodes_created, 50),

    %% save pids for node_id in node_pids
    NP = get(node_pids),
    NP1 = maps:put(maps:get(node_id, QT), Pids, NP),
    put(node_pids, NP1),

    %% store ns_pid in PD
    maps:put(location, Pids, QT);

hcs_create_processes(OP, QT) 
    when (OP =:= join) or (OP =:= hjoin) or (OP =:= mjoin) ->

    %% create processes for nodes of subtrees
    Outer = maps:get(outer, QT),
    Inner = maps:get(inner, QT),
    ONod = hcs_create_processes( maps:get(type, Outer), Outer ),
    INod = hcs_create_processes( maps:get(type, Inner), Inner ),
    
    %% store locs of inner and outer qn-s!
    OPids = maps:get(location, ONod),
    IPids = maps:get(location, INod),
    QT1 = maps:put(inner_pids, IPids, QT),
    QT2 = maps:put(outer_pids, OPids, QT1),

    %% determine node and id, spawn it, and store
    %% [NOTE] join query nodes are not parallelised! 
    [InnerPid] = hcs_select_nodes( maps:get(type, QT2), QT2 ),
    Node = element(2, InnerPid), 
    Id = list_to_atom(generate_long_qnid( maps:get(node_id, QT2))),
    info_msg(hcs_create_processes, [get(self), {gp, maps:get(gp, QT2)}, {location, {Id, Node}}], gp_nodes_selected, 50),

    %% spawn process and save pid as location
    case maps:get(type, QT) of
       join  -> Pid = join_query_node:spawn_process(Id, Node);
       hjoin -> Pid = hj_query_node:spawn_process(Id, Node);
       mjoin -> Pid = mj_query_node:spawn_process(Id, Node)
    end,
    QT3 = maps:put(location, [Pid], QT2),

    %% save pid for node_id in node_pids
    NP = get(node_pids),
    NP1 = maps:put(maps:get(node_id, QT), [Pid], NP),
    put(node_pids, NP1),

    %% re-link query tree children
    ONod1 = maps:put(invoker, Pid, ONod),
    INod1 = maps:put(invoker, Pid, INod),
    QT4 = maps:put(outer, ONod1, QT3),
    maps:put(inner, INod1, QT4).

%%
%% @doc Generate long ID of query node. 
%%
generate_long_qnid( Id ) ->
    QTid = get(tree_id),
    SSid = get(session_id), 
    "qn-"++SSid++"-"++QTid++"-"++Id.
 
%%    
%% hcs_select_node/2
%%
%% @doc Select concrete node (data server) for a given root of query
%% tree. Given triple-pattern query node
%% determine first column and then choose the row by means of selected
%% scheduler.  
%% 
%% Available schedulers are: random, optimal based on
%% statistical data about executed query nodes (not yet), and affinity 
%% scheduler with two levels of scheduling tending to use the same
%% nodes (data servers) for query nodes from the same session (not yet).
%% 
%% Join query node is, for the time being, executed at the
%% same node as inner triple-pattern. 
%%
%% @spec hcs_select_nodes( gp | tp, QT::qt_query_node ) -> [node()]
%%
hcs_select_nodes( OP, QT ) 
    when (OP =:= join) or (OP =:= hjoin) or (OP =:= mjoin) ->

    %% take first pid of inner_pids
    %% join_qn will then be at same location as first inner_qn.
    %% [TODO] what are alternatives? random selection of inner qn?
    IP = lists:nth(1, maps:get(inner_pids, QT)),
    info_msg(hcs_select_nodes, [get(self), {gp,maps:get(gp,QT)}, {location,[IP]}], locs_selected_for_gp, 50),
    [IP];

hcs_select_nodes( tp, QT ) -> 
    DA = distribution_algorithm,
    case get(DA) of
	predicate_based ->
	    hcssn_predicate_based(tp, QT);
	random ->
	    hcssn_random(tp, QT);
	_ ->
	    E = {unknown_distribution_algorithm, get(DA)},
	    error_msg(hcs_select_nodes, [tp, QT], E),
	    []
    end.

hcssn_predicate_based( tp, QT ) -> 
    %% extract predicate from query
    TP = maps:get(query, QT),
    %% S = element(2, TP),
    P = element(3, TP),
    %% O = element(4, TP),
    
    %% read pred_clm and clm_row_conf
    PredClm = get(pred_clm),
    ClmRowConf = get(clm_row_conf),

    %% obtain columns for P
    %% [NOTE] at this point distribution function can be changed.
    %% [NOTE] subset of SPO may be used for selection.
    case is_list(P) of 
       true  -> IsVarP = string:chr(P, $?) =:= 1;
       false -> IsVarP = false
    end,
    case is_integer(P) of 
       true  -> PS = string_id:find(P);
       false -> PS = undefined
    end,
    IsUndefP = PredClm =:= undefined,

    %% A = [tp, QT],
    %% M = [get(self), {tp,TP}, {predicate,P}],
    %% info_msg(hcs_select_nodes, A, M, 50),

    if 
       IsVarP   -> Clms = maps:keys(ClmRowConf);
       IsUndefP -> Clms = [1];
       true     -> Clms = [maps:get(PS, PredClm)]
    end,
    info_msg(hcs_select_nodes, [get(self), {tp,TP}, {predicate,P}, {clms,Clms}], clms_selected_for_tp, 50),

    %% F maps column ids to selected row of given column
    F = fun (C) ->
            Rows = maps:get(C, ClmRowConf),
            Rids = maps:keys(Rows),
            %%info_msg(hcs_select_nodes, [get(self), Clms, Rows, Rids, length(Rids)], debug_show_rids, 60),

            %% [NOTE] at this point other type of row selection can be used
            %% [NOTE] based on SPO of TP
 	    %% currently: select row randomly
	    Rid = rand:uniform(length(Rids)),
            %% [TODO] check random generator
    	    maps:get(Rid, Rows)
        end,
    Locs = lists:map(F, Clms),
    info_msg(hcs_select_nodes, [get(self), {tp,TP}, {location,Locs}], locs_selected_for_tp, 50), 
    Locs.

hcssn_random(tp, TP) -> 
    ClmRowConf = get(clm_row_conf),
    F = fun (C) ->
		Rows = maps:get(C, ClmRowConf),
		Rids = maps:keys(Rows),
		Rid = rand:uniform(length(Rids)),
		maps:get(Rid, Rows)
        end,
    Clms = maps:keys(ClmRowConf),
    Locs = lists:map(F, Clms),

    info_msg(hcs_select_nodes, [get(self), {tp,TP}, {location,Locs}], locs_selected_for_tp, 50), 
    Locs.

%%
%% hcs_start_query/2
%%
%% @doc Initiate all query nodes of a given query tree by sending
%% start message beginning with leafs and progressing towards the 
%% root of query tree.  
%% 
%% @spec hcs_start_query( Type::atom(), QT::qt_query_node() ) -> ok
%%
hcs_start_query( tp, QT ) ->
    %% gather data and start tp query node
    QueryNodeId = maps:get(node_id, QT),
    QueryId = get(tree_id),
    SessionId = get(session_id),
    [TriplePattern] = maps:values(maps:get(gp, QT)),
    SelectPred = maps:get(select_pred, QT),
    ProjectList = maps:get(project_list, QT),
    ParentPid = maps:get(invoker, QT),
    VarsPositions = maps:get(vars, QT),
    Side = maps:get(side, QT),

    %% send start to all tp query nodes
    F = fun (Loc) ->
            ST = {start, QueryNodeId, QueryId, SessionId, Loc, TriplePattern, SelectPred, ProjectList, ParentPid, VarsPositions, Side},
            info_msg(hcs_start_query, [get(self), {type,tp}, {location,Loc}, {start,ST}], sending_start, 50),
            gen_server:call(Loc, ST)
        end,
    lists:map(F, maps:get(location, QT)),

    ok;

hcs_start_query(OP, QT) 
    when (OP =:= join) or (OP =:= hjoin) or (OP =:= mjoin) ->

    %% start query nodes in sub-tree first
    Outer = maps:get(outer, QT),
    Inner = maps:get(inner, QT),
    hcs_start_query( maps:get(type, Outer), Outer ),
    hcs_start_query( maps:get(type, Inner), Inner ),

    %% gather data and start this query node
    QueryNodeId = maps:get(node_id, QT),
    QueryId = get(tree_id),
    SessionId = get(session_id),
    GraphPattern = maps:get(gp, QT),
    SelectPred = maps:get(select_pred, QT),
    ProjectList = maps:get(project_list, QT),
    ParentPid = maps:get(invoker, QT),
    OuterPids = maps:get(outer_pids, QT),
    InnerPids = maps:get(inner_pids, QT),
    VarsPositions = maps:get(var_pos, QT),
    JoinVars = maps:get(join_vars, QT),

    %% send start message to join query node 
    [Loc] = maps:get(location, QT),
    ST = {start, QueryNodeId, QueryId, SessionId, Loc, GraphPattern, SelectPred, ProjectList, ParentPid, OuterPids, InnerPids, VarsPositions, JoinVars},
    info_msg(hcs_start_query, [get(self), {type,gp}, {location,Loc}, {start,ST}], sending_start, 50),

    gen_server:call(Loc, ST),
    ok.

%%
%% hc_start_test/0
%%
%% @doc Test function for hc_start.
%%
hc_start_test_() ->
    application:load(b3s),
    {ok, TM} = application:get_env(b3s, test_mode),
    hcst_site(TM).

hcst_site(local1) ->
    b3s:start(),
    b3s:stop(),
    b3s:start(),
    b3s:bootstrap(),

    NDS    = node(),
    BSS    = {b3s_state, NDS},
    CRC    = clm_row_conf,

    RMS = #{1 => NDS},
    CM1 = #{1 => RMS, 2 => RMS},
    R01 = [NDS],                       %%, NDC],

    put(self, {'1-1-1', node()}),
    {inorder,
     [
     ?_assertMatch(ok,  gen_server:call(BSS, {put, CRC, CM1})),
     ?_assertMatch(R01, gen_server:call(BSS, propagate)),
     {generator, fun()-> tp_query_node:hcet_load_db() end},
%      {generator, fun()-> join_query_node:hcet_q02() end},
      {generator, fun()-> hcst_t01() end},
      {generator, fun()-> join_query_node:hcet_load_db() end},
      {generator, fun()-> hcst_t02() end},
      ?_assertMatch(ok, b3s:stop())
     ]};

hcst_site(_) ->
    [].


%% 
%% hcst_t01/0
%%
%% @doc Test 01 of hc_start.
%% 
hcst_t01() ->
    info_msg(hcst_t01, [], start, 50),

    Q = {hjoin, {hjoin, {tp, { "?i1", "?y", "livesIn", "?x" }, none, none}, 
                      {tp, { "?i2", "slovenia", "hasCapital", "?x" }, none, none},
                      none, none},
               {tp, { "?i3", "?y", "worksAt", "ijs" }, none, none},
               none, none},

    T = hcs_query_to_tree(Q),
    info_msg(hcst_t01, [{query,Q},{tree,T}], after_query_to_tree, 50),
    T1 = hcs_ids_to_nodes(maps:get(type, T), T, 1),
    info_msg(hcst_t01, [{query,Q},{tree,T1}], after_ids_to_nodes, 50),
    T2 = hcs_comp_qn_entries(maps:get(type, T1), T1),
    info_msg(hcst_t01, [{query,Q},{tree,T2}], after_comp_qn_entries, 50),

    {inorder,
     [
      ?_assertMatch(11,          hcst_log_cnt(T2))
     ]}.

%% 
%% hcst_t02/0
%%
%% @doc Test 02 of hc_start.
%% 
hcst_t02() ->
    info_msg(hcst_t02, [get()], start, 50),
  
    % tree ids will be handled by session
    TreeId = "0",
    SessionId = "1",

    % spawn query tree process
    ProcId = list_to_atom("qt-"++SessionId++"-"++TreeId),
    info_msg(hcst_t02, [{ProcId,node()}], query_tree_pid, 50),

    % spawn qt process locally
    QT = query_tree:spawn_process(ProcId, node()),

    % define query
    Q = {hjoin, {hjoin, {tp, { "?i1", "?y", "livesIn", "?x" }, none, none}, 
                      {tp, { "?i2", "slovenia", "hasCapital", "?x" }, none, none},
                      none, none},
               {tp, { "?i3", "?y", "worksAt", "ijs" }, none, none},
               none, none},

    % node_pids of query tree QT process
    F1 = fun (NP) ->
            Pids = lists:flatten(maps:values(NP)),
            put(temp, Pids),
            length(Pids)
         end,

    S1 = {start, Q, QT, self(), TreeId, SessionId},
    info_msg(hcst_t02, [QT, S1], before_test, 50),
  
    {inorder,
     [
      ?_assertMatch(ok, mnesia:start()),
      ?_assertMatch(ok, timer:sleep(1000)),
      ?_assertMatch(ok, gen_server:call(QT, S1)),

      % check properties of query_tree
      ?_assertMatch(33,  length(gen_server:call(QT, {get, all}))),

      % check properties by qnodes
      ?_assertMatch(5,   F1(gen_server:call(QT, {get, node_pids}))),
      ?_assertMatch(35,  length(gen_server:call(lists:nth(1,get(temp)), {get_property, all}))),
      ?_assertMatch(35,  length(gen_server:call(lists:nth(2,get(temp)), {get_property, all}))),
      ?_assertMatch(43,  length(gen_server:call(lists:nth(3,get(temp)), {get_property, all}))),
      ?_assertMatch(35,  length(gen_server:call(lists:nth(4,get(temp)), {get_property, all}))),
      ?_assertMatch(43,  length(gen_server:call(lists:nth(5,get(temp)), {get_property, all}))),
      ?_assertMatch(ok,  timer:sleep(2000))
     ]}.

%%
%% hcst_log_cnt/1
%%
%% @doc Log wathever you get as parameter and count number of entries.
%%
hcst_log_cnt(Val) when is_list(Val) -> 
    info_msg(hcst_log_cnt, [Val], logging_test, 60),
    length(Val);

hcst_log_cnt(Val) when is_map(Val) -> 
    info_msg(hcst_log_cnt, [Val], logging_test, 60),
    length(maps:to_list(Val)).
    
%% 
%% hc_eval/0
%%
%% @doc Initiate evaluation of query tree by sending eval message
%% to all roots of query tree. In the case query has 
%% two or more triple patterns then query tree has one root. 
%% In the case that query is one triple pattern then query can
%% have more roots.
%%
%% @spec hc_eval() -> ok
%% 
hc_eval() ->

    %% initialize process dictionary properties
    put(result_freq, #{}),
    put(pid_start,   #{}),
    put(pid_elapse,  #{}),

    %% send eval to all root query nodes 
    F = fun (Pid) ->

		%% send eval to one pid
		gen_server:call(Pid, {eval, []}),

		%% record starting time
		PS = pid_start,
		put(PS, maps:put(Pid, os:timestamp(), get(PS))),

		%% send N empty messages to root of query tree
		send_N_empty(Pid)

        end,
    lists:map(F, get(roots)),

    info_msg(hc_eval, [get(self), {query,get(query)}, {roots,get(roots)}, 
                       {node_pids,get(node_pids)}], eval_done, 10),
    ok.

%%hc_eval() -> 
%%    error_msg(hc_eval, [get()], forbiden_state),
%%    ok.

%% 
%% hc_read/0
%%
%% @doc Processing request to read block from resut queue and send it back to caller. 
%% 
%% @spec hc_read() -> [query_node:qn_graph()] | end_of_stream
%% 
hc_read() ->

    %% check queue_result
    case query_node:queue_prepared(result) of 
       true  -> Block = query_node:queue_read(result);
       false -> Block = no_data
    end,

    %% report and return
    info_msg(hc_read, [get(self), {reply,Block}, {invoker,get(invoker)}, get(state)], block_read, 50),
    {data_block,Block}.

%% 
%% hc_data_outer/2
%%
%% @doc Processing data message from root of query tree process.
%% 
%% @spec hc_data_outer(From::pid(), Block::[query_node:qn_graph()]) -> ok
%% 
hc_data_outer(Pid, end_of_stream) ->
    A  = [Pid, end_of_stream],

    PS   = pid_start,
    PE   = pid_elapse,
    S    = maps:get(Pid, get(PS)),
    E    = timer:now_diff(os:timestamp(), S),
    put(PE, maps:put(Pid, E, get(PE))),

    db_interface:db_disconnect(),

    R  = [get(self), {from, Pid}, {outer_data_stream_terminated, E, Pid}, get(state)],
    SW = stop_watch,
    RS = gen_server:call(SW, {record, R}),
    info_msg(hc_data_outer, A, RS, 10);

hc_data_outer(From, Block) when is_list(Block) ->
    F = fun(X) -> hc_data_outer(From, X) end,
    R = lists:foreach(F, Block),
    %%A = [From, Block],
    info_msg(hc_data_outer, [get(self), {from,From}, {block,Block}, {process_each_element,R}, get(state)], block_received, 80),

    %% store block in queue
    %% [TODO] what to do with result exactly?
    %% [TODO] block will be sent directly to session
    Q = get(queue_result),
    case maps:values(get(result_freq)) of
	[] -> ok;
	VRF ->
	    RRM = get(result_record_max),
	    case lists:max(VRF) of
		MV when MV < RRM ->
		    Q1 = queue:in(Block, Q),
		    put(queue_result, Q1);
		_ -> ok
	    end
    end,

    %% pass the stream to invoker
%   case get(invoker) of 
%   undefined -> error_msg(hc_data_outer, [get(self), {all,get()}, get(state)], data_before_start);
%   _         -> gen_server:cast(get(invoker), {data_outer, get(self), Block})
%   end,

    %% send empty back to 
    gen_server:cast(From, {empty, get(self)}),

    info_msg(hc_data_outer, [get(self), {from,From}, {invoker,get(invoker)}, {result_freq,get(result_freq)},
                             {result_record_max,get(result_record_max)}, {queue_result,get(queue_result)}, get(state)], block_stored, 80),
    ok;

hc_data_outer(Pid, Graph) ->
    %%A = [Pid, Graph],

    MapRF = get(result_freq),
    case maps:is_key(Pid, MapRF) of
	true  -> Freq = maps:get(Pid, MapRF);
	false -> Freq = 0
    end,
    NewRF = maps:put(Pid, Freq + 1, MapRF),
    put(result_freq, NewRF),

    info_msg(hc_data_outer, [get(self), {from,Pid}, {graph,Graph}, {result_freq, NewRF}, get(state)], graph_processed, 80).

%%
%% send_N_empty/1
%%
%% @doc Send N empty messages to Pid. N is stored in config.
%%
send_N_empty(Pid) ->
    {ok, N} = application:get_env(b3s, num_of_empty_msgs),  % [TODO] store id PD
    send_N_empty_1(Pid, N),
    info_msg(send_N_empty, [Pid, N], done, 100).

send_N_empty_1(_, 0) ->
    ok;
send_N_empty_1(Pid, N) ->
    QTpid = get(self),
    gen_server:cast(Pid, {empty, QTpid}),
    send_N_empty_1(Pid, N-1).

%%
%% Merges maps M1 and M2 into a single map. Keys of resulting map include 
%% union of keys from M1 and M2. If key is in both maps M1 and M2 then
%% then the value of key includes union of values from M1 and M2. Values 
%% are stored as lists of pairs (query node, and, index of key in triple).
%%
merge_maps(M1, M2) -> 
    F = fun (K, V, Acc) ->
            MakeUnion = maps:is_key(K, M1),
            if 
               MakeUnion ->
                   maps:put(K, lists:append(maps:get(K, M1), V), Acc);
               true -> 
                   maps:put(K, V, Acc)
            end
        end,
    maps:fold(F, M1, M2).

%% 
%% Compute intersection of two lists.
%%
intersect_lists(_, []) -> [];

intersect_lists(L1, [X|L]) -> 
    XInIntersection = lists:member(X, L1),
    if 
        XInIntersection -> 
            L2 = intersect_lists(L1, L), 
            [X|L2];
        true -> 
            intersect_lists(L1, L)
    end.

%%
%% hc_eval_test/0
%%
%% @doc Test function for hc_eval.
%%
hc_eval_test_() ->
    application:load(b3s),
    {ok, TM} = application:get_env(b3s, test_mode),
    hcet_site(TM).

hcet_site(local1) ->
    NDS    = node(),
    BSS    = {b3s_state, NDS},
    CRC    = clm_row_conf,

    RMS = #{1 => NDS},
    CM1 = #{1 => RMS, 2 => RMS},
    R01 = [NDS],                       %%, NDC],

    {inorder,
     [
      ?_assertMatch(ok, b3s:start()),
      ?_assertMatch(ok, b3s:bootstrap()),
      ?_assertMatch(ok,  gen_server:call(BSS, {put, CRC, CM1})),
      ?_assertMatch(R01, gen_server:call(BSS, propagate)),
      {generator, fun()-> tp_query_node:hcet_load_db() end},
      {generator, fun()-> join_query_node:hcet_load_db() end},
      {generator, fun()-> hcet_t01() end},
      {generator, fun()-> hcet_t01h() end},
      {generator, fun()-> hcet_t02() end},
      {generator, fun()-> hcet_t03() end},
      ?_assertMatch(ok, b3s:stop())
     ]};

hcet_site(_) ->
    [].

%% 
%% eI/1, eT/1, eTP/1, dT/1m dTMap/1, dTML/1, dTMLL/1
%%
%% @doc Encoding / decoding string, triple and triple pattern.
%% 
eI(X) -> string_id:get_id(X).

eT({T,I,S,P,O}) ->
    ET = string_id:encode_triple({I, S, P, O}),
    list_to_tuple([T | tuple_to_list(ET)]).

eTP(X) -> string_id:encode_triple_pattern(X).

dT({T,I,S,P,O}) ->
    DT = string_id:decode_triple({I, S, P, O}),
    list_to_tuple([T | tuple_to_list(DT)]).

dTMap(M) when is_map(M) ->
    F = fun ({K, T}) -> {K, dT(T)} end,
    maps:from_list(lists:map(F, maps:to_list(M)));
dTMap(T) ->
    T.

dTML(L) when is_list(L) ->
    lists:map(fun dTMap/1, L).

dTMLL(L) when is_list(L) ->
    lists:map(fun dTML/1, L).

%% 
%% hcet_t01/0
%%
%% @doc Test 01 of hc_eval.
%% 
hcet_t01() ->
    info_msg(hcet_t01, [get()], start, 50),
  
    % tree ids will be handled by session
    TreeId = "1",
    SessionId = "1",

    % spawn query tree process
    ProcId = list_to_atom("qt-"++SessionId++"-"++TreeId),
    info_msg(hcst_t01, [{ProcId,node()}], query_tree_pid, 50),

    % spawn qt process locally
    QT = query_tree:spawn_process(ProcId, node()),

    % define query
    Q = {join, {hjoin, {tp, { "?i1", "?y", "livesIn", "?x" }, none, none}, 
                      {tp, { "?i2", "slovenia", "hasCapital", "?x" }, none, none},
                      none, none},
               {tp, { "?i3", "?y", "worksAt", "ijs" }, none, none},
               none, none},

    %% data to be returned
    T1 = eT({triple_store, "id22", "slovenia", "hasCapital", "ljubljana"}),
    T2 = eT({triple_store, "id38", "luka", "livesIn", "ljubljana"}),
    T3 = eT({triple_store, "id47", "luka", "worksAt", "ijs"}),
    T4 = eT({triple_store, "id40", "nika", "livesIn", "ljubljana"}),
    T5 = eT({triple_store, "id49", "nika", "worksAt", "ijs"}),
    T6 = eT({triple_store, "id41", "marko", "livesIn", "ljubljana"}),
    T7 = eT({triple_store, "id50", "marko", "worksAt", "ijs"}),
    R1Map         = maps:put("4", T3, maps:put("2", T1, maps:put("1", T2, maps:new()))),
    R2Map         = maps:put("4", T5, maps:put("2", T1, maps:put("1", T4, maps:new()))),
    R3Map         = maps:put("4", T7, maps:put("2", T1, maps:put("1", T6, maps:new()))),
    QR = [[R3Map, R2Map, R1Map, end_of_stream]],

    %% F1 = fun (RQ) -> 
    %%           info_msg(hcst_t01, [QT, {received, dTMLL(RQ)}, {expected, dTMLL(QR)}], compare_results, 20),
    %%           RQ
    %%      end, 
    F2 = fun ([Rec], [Exp]) -> 
		 A = [QT, {received, dTML(Rec)}, {expected, dTML(Exp)}],
		 info_msg(hcst_t01, A, compare_results, 20),
		 sets:from_list(Rec) == sets:from_list(Exp)
         end,

    S1 = {start, Q, QT, self(), TreeId, SessionId},
    info_msg(hcst_t01, [QT, S1, QR], before_test, 50),
  
    {inorder,
     [
      ?_assertMatch(ok, mnesia:start()),
      ?_assertMatch(ok, timer:sleep(1000)),
      ?_assertMatch(ok, gen_server:call(QT, S1)),

      % check properties by qnodes
      ?_assertMatch(33,  length(gen_server:call(QT, {get, all}))),
      ?_assertMatch(ok,  gen_server:call(QT, {eval})),
      ?_assertMatch(ok,  timer:sleep(1000)),
      %% ?_assertMatch(QR,  F1(queue:to_list(load_queue_result(QT)))),
      ?_assertMatch(true, F2(queue:to_list(load_queue_result(QT)), QR)),
      ?_assertMatch(ok,  timer:sleep(1000))
     ]}.

%% 
%% hcet_t01h/0
%%
%% @doc Test 01h of hc_eval.
%% 
hcet_t01h() ->
    info_msg(hcet_t01, [get()], start, 50),
  
    % tree ids will be handled by session
    TreeId = "1h",
    SessionId = "1",

    % spawn query tree process
    ProcId = list_to_atom("qt-"++SessionId++"-"++TreeId),
    info_msg(hcst_t01h, [{ProcId,node()}], query_tree_pid, 50),

    % spawn qt process locally
    QT = query_tree:spawn_process(ProcId, node()),

    % define query
    Q = {join, {hjoin, {tp, { "?i1", "?y", "livesIn", "?x" }, none, none}, 
                        {tp, { "?i2", "slovenia", "hasCapital", "?x" }, none, none},
                        none, none},
                {tp, { "?i3", "?y", "worksAt", "ijs" }, none, none},
                none, none},

    %% data to be returned
    T1 = eT({triple_store, "id22", "slovenia", "hasCapital", "ljubljana"}),
    T2 = eT({triple_store, "id38", "luka", "livesIn", "ljubljana"}),
    T3 = eT({triple_store, "id47", "luka", "worksAt", "ijs"}),
    T4 = eT({triple_store, "id40", "nika", "livesIn", "ljubljana"}),
    T5 = eT({triple_store, "id49", "nika", "worksAt", "ijs"}),
    T6 = eT({triple_store, "id41", "marko", "livesIn", "ljubljana"}),
    T7 = eT({triple_store, "id50", "marko", "worksAt", "ijs"}),
    R1Map         = maps:put("4", T3, maps:put("2", T1, maps:put("1", T2, maps:new()))),
    R2Map         = maps:put("4", T5, maps:put("2", T1, maps:put("1", T4, maps:new()))),
    R3Map         = maps:put("4", T7, maps:put("2", T1, maps:put("1", T6, maps:new()))),
    QR = [[R3Map, R2Map, R1Map, end_of_stream]],

    %% F1 = fun (RQ) -> 
    %%           info_msg(hcst_t01, [QT, {received, dTMLL(RQ)}, {expected, dTMLL(QR)}], compare_results, 20),
    %%           RQ
    %%      end, 
    F2 = fun ([Rec], [Exp]) -> 
		 A = [QT, {received, dTML(Rec)}, {expected, dTML(Exp)}],
		 info_msg(hcst_t01, A, compare_results, 20),
		 sets:from_list(Rec) == sets:from_list(Exp)
         end,

    S1 = {start, Q, QT, self(), TreeId, SessionId},
    info_msg(hcst_t01, [QT, S1, QR], before_test, 50),
  
    {inorder,
     [
      ?_assertMatch(ok, mnesia:start()),
      ?_assertMatch(ok, timer:sleep(1000)),
      ?_assertMatch(ok, gen_server:call(QT, S1)),

      % check properties by qnodes
      ?_assertMatch(33,  length(gen_server:call(QT, {get, all}))),
      ?_assertMatch(ok,  gen_server:call(QT, {eval})),
      ?_assertMatch(ok,  timer:sleep(1000)),
      %% ?_assertMatch(QR,  F1(queue:to_list(load_queue_result(QT)))),
      ?_assertMatch(true, F2(queue:to_list(load_queue_result(QT)), QR)),
      ?_assertMatch(ok,  timer:sleep(1000))
     ]}.

%% 
%% load_queue_result/1
%%
%% @doc Load queue_result from query tree QT and check if there is end_of_stram 
%% at the end of queue. If end_of_stream found return complete queue queue_result,
%% otherwise wait for more data. 
%% 
load_queue_result(QT) ->
    RQ = gen_server:call(QT, {get, queue_result}),
    Last = queue:peek_r(RQ),
    case Last of 
       empty -> timer:sleep(300),
                load_queue_result(QT);
       _ -> ok
    end,
    {value, B} = Last,
    case lists:member(end_of_stream, B) of 
       true  -> RQ;
       false -> timer:sleep(300), load_queue_result(QT)
    end.

%% 
%% hcet_t02/0
%%
%% @doc Test 01 of hc_eval.
%% 
hcet_t02() ->
    info_msg(hcst_t02, [get()], start, 50),

    % tree ids will be handled by session
    TreeId = "2",
    SessionId = "1",

    % spawn query tree process localy
    ProcId = list_to_atom("qt-"++SessionId++"-"++TreeId),
    info_msg(hcst_t02, [{ProcId,node()}], query_tree_pid, 50),
    QT = query_tree:spawn_process(ProcId, node()),

    % define query
    Q = {hjoin, {tp, { "?i1", "nika", "?x", "ijs" }, none, none},
               {tp, { "?i2", "?y", "?x", "yj" }, none, none},
               none, none},

    %% data to be returned
    T1 = eT({triple_store, "id49", "nika", "worksAt", "ijs"}),
    T2 = eT({triple_store, "id44", "yoshio", "worksAt", "yj"}),
    T3 = eT({triple_store, "id42", "shou",	"worksAt", "yj"}),
    R1Map         = maps:put("2", T3, maps:put("1", T1, maps:new())),
    R2Map         = maps:put("2", T2, maps:put("1", T1, maps:new())),
    QR = [[R2Map, R2Map, R1Map, R1Map, R2Map], 
	  [R2Map, R1Map, R1Map, end_of_stream]],

    F1 = fun (RQ) -> 
              info_msg(hcst_t02, [QT, {received, dTMLL(RQ)}, {expected, dTMLL(QR)}], compare_results, 20),
              RQ
         end, 

    S1 = {start, Q, QT, self(), TreeId, SessionId},
    info_msg(hcst_t02, [QT, S1], before_test, 50),
  
    {inorder,
     [
      ?_assertMatch(ok, mnesia:start()),
      ?_assertMatch(ok, timer:sleep(1000)),
      ?_assertMatch(ok, gen_server:call(QT, S1)),

      % check properties by qnodes
      ?_assertMatch(33,  length(gen_server:call(QT, {get, all}))),
      ?_assertMatch(ok,  gen_server:call(QT, {eval})),
      ?_assertMatch(ok,  timer:sleep(1000)),
      ?_assertMatch(QR,  F1(queue:to_list(load_queue_result(QT)))),
      ?_assertMatch(ok,  timer:sleep(1000))
     ]}.

%% 
%% hcet_t03/0
%%
%% @doc Test 03 of hc_eval.
%% 
hcet_t03() ->
    info_msg(hcst_t03, [get()], start, 50),

    % tree ids will be handled by session
    TreeId = "3",
    SessionId = "1",

    % spawn query tree process localy
    ProcId = list_to_atom("qt-"++SessionId++"-"++TreeId),
    info_msg(hcst_t03, [{ProcId,node()}], query_tree_pid, 50),
    QT = query_tree:spawn_process(ProcId, node()),

    % define query
    Q = {join, {hjoin, {tp, { "?i1", "?x", "livesIn", "ljubljana" }, none, none},
                      {tp, { "?i2", "?x", "graduatedFrom", "ul" }, none, none},
                      none, none },
               {tp, {"?i3", "?x", "age", "?y"}, none, none},
               {{"?y", less, 30}, land, {"?y", greatereq, 20}}, ["?y"]},

    %% data to be returned
%    T1 = {triple_store, "id40", "nika", "livesIn", "ljubljana"},
%    T2 = {triple_store, "id56", "nika", "graduatedFrom", "ul"},
    T3 = eT({triple_store, "id63", "nika", "age", "22"}),
    R1Map = maps:put("4", T3, maps:new()),
    QR = [[R1Map, end_of_stream]],

    F1 = fun (RQ) -> 
              info_msg(hcst_t03, [QT, {received, dTMLL(RQ)}, {expected, dTMLL(QR)}], compare_results, 20),
              RQ
         end, 

    S1 = {start, Q, QT, self(), TreeId, SessionId},
%    info_msg(hcst_t03, [QT, S1], before_test, 50),
  
    {inorder,
     [
      ?_assertMatch(ok, mnesia:start()),
      ?_assertMatch(ok, timer:sleep(1000)),
      ?_assertMatch(ok, gen_server:call(QT, S1)),

      % check properties by qnodes
      ?_assertMatch(33,  length(gen_server:call(QT, {get, all}))),
      ?_assertMatch(ok,  gen_server:call(QT, {eval})),
      ?_assertMatch(ok,  timer:sleep(1000)),
      ?_assertMatch(QR,  F1(queue:to_list(load_queue_result(QT)))),
      ?_assertMatch(ok,  timer:sleep(1000))
     ]}.

%% ====> END OF LINE <====

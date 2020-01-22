%%
%% Triple Pattern Query Node processes
%%
%% @copyright 2014-2019 UP FAMNIT and Yahoo Japan Corporation
%% @version 0.3
%% @since May, 2014
%% @author Iztok Savnik <iztok.savnik@famnit.upr.si>
%% @author Kiyoshi Nitta <knitta@yahoo-corp.jp>
%% 
%% @doc Triple pattern query node is implemented as gen-process. It realizes 
%% access method to local triple-store based on triple patterns. Access method
%% supports index based access to triple-table.
%%
%% TP query node is implemented as state machine. Input and output messages 
%% trigger co-routines that comprise protocol. The states of TP query node are:
%% active, db_access, eos, and inactive. Message start inits state to active. 
%% Eval message moves state to read_db. Subsequent empty messages retain state 
%% read_db. After end_of_stream is obtained from
%% accessing triple-store state moves to eos. Eval can be received multiple times 
%% if in state active or eos. Finally, stop message puts 
%% TP query node to state inactive.
%%
%% <table bgcolor="lemonchiffon">
%% <tr><th>Section Index</th></tr>
%% <tr><td>{@section triple-pattern access method}</td></tr>
%% <tr><td>{@section property list}</td></tr>
%% <tr><td>{@section handle_call (synchronous) message API}</td></tr>
%% <tr><td>{@section handle_cast (asynchronous) message API}</td></tr>
%% </table>
%%
%% == triple-pattern access method ==
%%
%% (LINK: {@section triple-pattern access method})
%%
%% Triple-pattern access method is a query node implemented as independent gen_process.
%% Process communicates with local database system via module {@link db_interface} that
%% converts triple-pattern into internal data access method. Triples from database
%% are retrieved in the form of block including N triples where N is application 
%% variable block_size.
%% 
%% Triple-pattern access method protocol is simple. Each time empty message is received 
%% block of triples (possibly filtered against the selection predicate) is first collected 
%% from local database and then sent to parent query node. In the case triple-pattern query 
%% node has the role of inner query node of it is expected that it will receive 
%% multiple messages {@section @{eval, VarsValues@}}.
%%
%% == property list ==
%% 
%% (LINK: {@section property list})
%% 
%% The gen_server process uses following properties holded by {@link
%% tpqn_state()}.
%% 
%% <table border="3">
%% <tr><th>Name</th><th>Type</th><th>Description</th></tr>
%% 
%% <tr> <td>created</td> <td>boolean()</td> <td>true denotes that
%% process dictionary was created and used. false denotes that
%% completely new process.</td> </tr>
%% 
%% <tr> <td>node_id</td> <td>string()</td> <td>query node identifier</td> </tr>
%% 
%% <tr> <td>query_id</td> <td>string()</td> <td>query tree identifier</td> </tr>
%%
%% <tr> <td>session_id</td> <td>string()</td> <td>session identifier</td> </tr>
%%
%% <tr> <td>self</td> <td>state_node:ns_pid()</td> <td>pid of self</td> </tr>
%% 
%% <tr> <td>state</td> <td>atom()</td> <td>inactive | active | db_access | 
%% eos</td> </tr>
%% 
%% <tr> <td>orig_tp</td> <td>query_node:qn_triple_pattern()</td> <td>original 
%% triple pattern set by hc_start()</td> </tr>
%% 
%% <tr> <td>tp</td> <td>query_node:qn_triple_pattern()</td> <td>triple pattern of 
%% query node</td> </tr>
%% 
%% <tr> <td>select_pred</td> <td>query_node:qn_select_predicate()</td> <td>selection 
%% predicate in the form of abstract syntax tree of type {@type query_node:qn_select_predicate()}
%% </td> </tr>
%% 
%% <tr> <td>project_list</td> <td>query_node:qn_project_list()</td> <td>list of 
%% variables to be projected</td> </tr>
%% 
%% <tr> <td>parent</td> <td>node_state:ns_pid()</td> <td>process id of query node
%% process</td> </tr>
%% 
%% <tr> <td>vars</td> <td>maps:map()</td> <td>mapping from {@link
%% query_node:qn_var()} to integer()</td> </tr>
%% 
%% <tr> <td>wait</td> <td>boolean()</td> <td>indicate whether the
%% process is in wait state or not.</td> </tr>
%% 
%% <tr> <td>inner_outer</td> <td>inner | outer</td> <td> Position to
%% its parent query node.</td> </tr>
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
%% <tr> <td>queue_from_parent</td> <td>queue:queue()</td> <td> Queue 
%% storing empty messages from parent if tp_query_node in state undefined, 
%% eos, or, active.</td> </tr>
%% 
%% <tr> <td>benchmark_task_pid</td> <td>{@type
%% node_state:ns_pid()}</td> <td>process id of executing benchmark
%% task.</td> </tr>
%% 
%% <tr> <td>table_name</td> <td>string()</td> <td>name of triple table
%% used by {@link db_interface}.</td> </tr>
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
%% === {start, QueryNodeId, QueryId, SessionId, Self, TriplePattern, SelectPred, ProjectList, ParentPid, VarsPositions} ===
%% 
%% Initialize all parameters and save them to the state structure.
%%
%% QueryNodeId is {@link query_node:qn_id()}, QueryId is string(), SessionId is string(), 
%% Self is {@link node_state:ns_pid()}, Triplepattern is {@link query_node:qn_triple_pattern()}, 
%% SelectPred is {@type query_node:qn_select_predicate()}, ProjectList is 
%% {@type query_node:qn_project_list()}, ParentPid is node_state:ns_pid(), and
%% VarsPositions is {@link tpqn_var_position()}.
%%  
%% This request is implemented by {@link hc_start/6}. (LINK: {@section
%% @{start, QueryNodeId, QueryId, SessionId, Self, TriplePattern, SelectPred, ProjectList, ParentPid, VarsPositions@}}).
%% 
%% === {eval, VarsValues} ===
%% 
%% Eval initiates evaluation of query node state machine. It opens cursor-based 
%% access to triple-table. Indexes are used to access subset of keys from SPO. 
%%
%% Protocol is driven by empty messages received by TP query node from parent 
%% query node. After receiving empty message TP query node sends next graph to 
%% parent. End of stream message is sent to parent when there are no more 
%% triples from triple-store.
%%
%% VarsValues is {@link query_tree:qn_var_val_map()}. This request is implemented 
%% by {@link hc_eval/1}. (LINK: {@section @{eval, VarsValues@}}).
%% 
%% === {get_property, Name} ===
 %% 
%% Return the value of specified property name. Variable Name is an
%% atom(). In the case Name=all complete PD is returned. This request is 
%% implemented by {@link hc_get_property/2}. 
%% (LINK: {@section @{get_property, Name@}}).
%% 
%% == handle_cast (asynchronous) message API ==
%% 
%% (LINK: {@section handle_cast (asynchronous) message API})
%% 
%% === {empty, From} ===
%% 
%% Retrieve next triple from triple-store and send triple as graph
%% to parent. Variable From is an node_state:ns_pid(). This request is implemented by {@link
%% hc_empty/2}. (LINK: {@section @{empty, From@}}).
%%
%% === {stop, From} ===
%% 
%% Stop this tp_query_node process. PD of process is erased. 
%% Variable From is node_state:ns_pid(). 
%% (LINK: {@section @{stop, From@}}).
%%
%%
%% @type tpqn_state() = maps:map(). Map structure
%% that manages properties for operating the gen_server process.
%% 
%% @type tpqn_var_position() = maps:map(). Mapping from 
%% {@link query_node:qn_var()} to {@link query_node:qn_id()}. 
%% 
-module(tp_query_node).
-behavior(gen_server).
-export(
   [
    child_spec/1, spawn_process/2,
    init/1, handle_call/3, handle_cast/2,
    handle_info/2, terminate/2, code_change/3,
    hcet_load_db/0
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
%% @doc Initialize a tp_query_node process.
%% 
%% @spec init([]) -> {ok, tpqn_state()}
%% 
init([]) ->
    process_flag(trap_exit, true),

    %% set main pd keys
    put(wait, true),
    put(pid, self()),
    put(start_date_time, calendar:local_time()),
    put(mq_debug, gen_server:call(node_state, {get, mq_debug})),

    %% init queues and counters
    query_node:queue_init(from_parent, plain, empty),
    query_node:queue_init(to_parent, output, undefined),
    query_node:queue_init(from_db, input, db_block),
    put(count_eval_msg, 0),

    info_msg(init, [{state,hc_save_pd()}], done, -1),
    {ok, hc_save_pd()}.

%% 
%% handle_call/3
%% 
%% @doc Handle synchronous query requests.
%% 
%% @spec handle_call(term(), {pid(), term()}, tpqn_state()) -> {reply, term(), tpqn_state()}
%% 

handle_call({start, QueryNodeId, QueryId, SessionId, Self, TriplePattern, SelectPred, ProjectList, ParentPid, VarsPositions, Side}, _, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(get(created), State),
    info_msg(handle_call, [get(self), {message,start}, {all,get()}, get(state)], message_received, 10),
    hc_start(QueryNodeId, QueryId, SessionId, Self, TriplePattern, SelectPred, ProjectList, ParentPid, VarsPositions, Side),
    {reply, ok, hc_save_pd()};

handle_call({get_property, all}, _, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(get(created), State),
    info_msg(handle_call, [get(self), {message,get_property}, {name,all}, {value,get()}, get(state)], done, 10),
    {reply, get(), State};

handle_call({get_property, Name}, _, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(get(created), State),
    info_msg(handle_call, [get(self),  {message,get_property}, {name,Name}, {value,get(Name)}, get(state)], done, 10),
    {reply, get(Name), State};

handle_call({get, Name}, _, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(get(created), State),
    info_msg(handle_call, [get(self), {message,get}, {get,Name}, {value,get(Name)}, get(state)], message_received, 10),
    {reply, get(Name), State};

handle_call({eval, VarsValues}, _, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(get(created), State),

    %% count number of eval message and log 
    CEM = get(count_eval_msg),
    case (CEM rem 1000 == 0) of
    true -> info_msg(handle_call, [get(self), {message,eval}, {vars_values,VarsValues}, {count_eval_msg,CEM}, {all,get()}, get(state)], message_received, 30);
    false -> ok
    end,
    put(count_eval_msg,CEM+1),

    hc_eval(VarsValues, get(state)),
    {reply, ok, hc_save_pd()};

%% default
handle_call(Request, From, State) ->
    R = {unknown_request, Request},
    error_msg(handle_call, [get(self), Request, From, State, get()], R),
    {reply, R, State}.

%% 
%% handle_cast/2
%% 
%% @doc Handle asynchronous query requests. 
%% 
%% @spec handle_cast(term(), tpqn_state()) -> {noreply, tpqn_state()}
%% 

handle_cast({empty, From}, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(get(created), State),

    %% insert into queue from parent
    query_node:queue_write(from_parent, {empty, From}),

    %% process empty message
    info_msg(handle_cast, [get(self), {message,empty}, {from,From}, {queue_from_parent, get(queue_from_parent)}, get(state)], message_received, 30),
    hc_empty(get(state)),
    {noreply, hc_save_pd()};

handle_cast({stop, From}, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(get(created), State),
    info_msg(handle_cast, [get(self), {message,stop}, {from,From}, get(state)], message_received, 10),
    %% erase complete PD
    erase(),                               
    {noreply, hc_save_pd()};

%% default
handle_cast(Request, State) ->
    R = {unknown_request, Request},
    error_msg(handle_cast, [Request, State], R),
    {noreply, hc_save_pd()}.

%% 
%% @doc Restore process dictionaries from state map structure.
%% 
%% @spec hc_restore_pd([{atom(), term()}] | undefined, tpqn_state()) -> ok
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
%% @spec hc_save_pd() -> tpqn_state()
%% 
hc_save_pd() ->
    maps:from_list(get()).

%% 
%% handle_info/2
%% 
%% @doc Handle exceptional query requests. 
%% 
%% @spec handle_info(term(), tpqn_state()) -> {noreply, tpqn_state()}
%% 
handle_info(_Info, State) ->
    {noreply, State}.

%% 
%% terminate/2
%% 
%% @doc Process termination. 
%% 
%% @spec terminate(term(), tpqn_state()) -> none()
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
%% @spec code_change(term(), tpqn_state(), term()) -> {ok, tpqn_state()}
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
    GSOpt = [{local, Id}, tp_query_node, [], []],
    StartFunc = {gen_server, start_link, GSOpt},
    Restart = permanent,
    Shutdwon = 1000,
    Type = worker,
    Modules = [tp_query_node],

    {Id, StartFunc, Restart, Shutdwon, Type, Modules}.

%% 
%% @doc Spawn tp_query_node process with given local identifier at given node.
%% 
%% @spec spawn_process( Id::atom(), Node::node() ) -> node_state:ns_pid()
%% 
spawn_process(Id, Node ) -> 
    ChildSpec = tp_query_node:child_spec(Id),
    supervisor:start_child({b3s, Node}, ChildSpec),   
    {Id, Node}.

%% ======================================================================
%% 
%% handle call/cast implementation
%% 

%% 
%% @doc Initialize the query node process.
%% 
%% @spec hc_start(QueryNodeId::query_node:qn_id(), QueryId::string(), SessionId::string(), Self::node_state:ns_pid(),
%% TriplePattern::query_node:qn_triple_pattern(), SelectPred::query_node:qn_select_predicate(), 
%% ProjectList::query_node:qn_project_list(), ParentPid::node_state:ns_pid(), 
%% VarsPositions::tpqn_var_position(), Side::qn_side()) -> ok
%% 
hc_start(QueryNodeId, QueryId, SessionId, Self, TriplePattern, SelectPred, ProjectList, ParentPid, VarsPositions, Side) ->

    %% set process dictionary
    put(created,      true),
    put(qnode,        tp),
    put(node_id,      QueryNodeId),
    put(query_id,     QueryId),
    put(session_id,   SessionId),
    put(self,         Self),
    put(state,        active),
    put(orig_tp,      TriplePattern),
    put(tp,           TriplePattern),
    put(select_pred,  SelectPred),
    put(project_list, ProjectList),
    put(parent,       ParentPid),
    put(vars,         VarsPositions),
    put(inner_outer,  Side),
    put(wait,       false),
    put(pause,      false),
    %%put(count_eval_msg, 0),

    erase(sid_table_name),
    erase(sid_max_id),
    erase(di_cursor__),
    erase(di_ets__),

    %% init queues
%    query_node:queue_init(from_parent, plain, empty),
%    query_node:queue_init(to_parent, output, undefined),
%    query_node:queue_init(from_db, input, db_block),

    %% store benchmark info
    BSP = b3s_state_pid,
    BMT = benchmark_task,
    BTP = benchmark_task_pid,
    put(BSP, gen_server:call(node_state, {get, BSP})),
    {_, FSN} = get(BSP),
    put(BTP, {gen_server:call(get(BSP), {get, BMT}), FSN}),

    %% save block size in PD
    BSZ = block_size,
    put(BSZ, gen_server:call(get(BSP), {get, BSZ})).

hc_start_test_() ->
    b3s:start(),
    b3s:stop(),
    hst_site(b3s_state:get(test_mode)).

hst_site(local1) ->
    NDS    = node(),
    NodStr = atom_to_list(NDS),
    NDC    = list_to_atom("b3ss01" ++ string:sub_string(NodStr, 7)),
    BSS    = {b3s_state, NDS},
    CRC    = clm_row_conf,

    RMS = #{1 => NDS},
    RMC = #{1 => NDC},
    CM1 = #{1 => RMS, 2 => RMC},
    R01 = [NDC],

    {inorder,
     [
      ?_assertMatch(ok, b3s:start()),
      ?_assertMatch(ok, b3s:bootstrap()),
      ?_assertMatch(ok,  gen_server:call(BSS, {put, CRC, CM1})),
      ?_assertMatch(R01, gen_server:call(BSS, propagate)),
      {generator, fun()-> hcst_q01() end},
      ?_assertMatch(ok, b3s:stop())
     ]};

hst_site(local_two) ->
    [];

hst_site(_) ->
    [].

hcst_q01() ->
    info_msg(hcst_q01, [get(self), get()], start, 50),
    SessionId = "1",
    QueryId = "1",
    QueryNodeId   = "2",

    %% init queues
    query_node:queue_init(from_parent, plain, empty),
    query_node:queue_init(to_parent, output, undefined),
    query_node:queue_init(from_db, input, db_block),

    Id = list_to_atom(SessionId++"-"++QueryId++"-"++QueryNodeId),
    TPQN1 = spawn_process(Id, node()),
    TriplePattern = {"?id", "<Chinese>", "<eat>", "?obj"},
    SelectPred = none,
    ProjectList = none,
    ParentPid     = self(),
    VarsPositions = #{"?id" => 1, "?obj" => 4},
    M1            = {start, QueryNodeId, QueryId, SessionId, TPQN1, TriplePattern,
		     SelectPred, ProjectList, ParentPid, VarsPositions, outer},
    GP            = get_property,

    info_msg(hcst_q01, [get(self), get()], testing, 50),
    {inorder,
     [
      ?_assertMatch(true,          gen_server:call(TPQN1, {GP, wait})),
      ?_assertMatch(ok,            gen_server:call(TPQN1, M1)),
      ?_assertMatch(QueryNodeId,   gen_server:call(TPQN1, {GP, node_id})),
      ?_assertMatch(QueryId, 	   gen_server:call(TPQN1, {GP, query_id})),
      ?_assertMatch(SessionId, 	   gen_server:call(TPQN1, {GP, session_id})),
      ?_assertMatch(TriplePattern, gen_server:call(TPQN1, {GP, orig_tp})),
      ?_assertMatch(TriplePattern, gen_server:call(TPQN1, {GP, tp})),
      ?_assertMatch(ParentPid,     gen_server:call(TPQN1, {GP, parent})),
      ?_assertMatch(VarsPositions, gen_server:call(TPQN1, {GP, vars})),
      ?_assertMatch(false,         gen_server:call(TPQN1, {GP, wait})),
      ?_assertMatch(outer,         gen_server:call(TPQN1, {GP, inner_outer})),
      ?_assertMatch(ok,            gen_server:cast(TPQN1, {stop, self()}))
     ]}.

%% 
%% @doc Start query execution. If argument VarsValues is given, the tp
%% query node assume to eval inner node for its parent
%% node. Otherwise, outer node.
%% 
%% @spec hc_eval(VarsValues::query_node:qn_var_val_map() | [], State::atom()) -> ok
%% 
hc_eval([], State) when (State == active) or (State == eos) ->

    %% set main variables
    %%put(inner_outer, outer),  %% 14/2/16: iztok: inner_outer is now explicit
    put(tp, get(orig_tp)),
    put(state, db_access),

    %% set msg head for queue to_parent
    MTO = list_to_atom("data_"++atom_to_list(get(inner_outer))),
    put(msg_to_parent, MTO),

    %% for benchmark
    BTP = get(benchmark_task_pid),
    gen_server:cast(BTP, {restart_record, get(self), eval}),
    %% BSP = b3s_state_pid,
    %% block_size read in hc_start (iztok,2016/01/31)
    %%BSZ = block_size,
    %%put(BSZ, gen_server:call(get(BSP), {get, BSZ})),

    %% start iterator
    hc_eval_1(db_interface:db_open_tp(get(tp))),
    info_msg(hc_eval, [get(self), {tp,get(tp)}, {vars_values,[]}, get(state)], eval_started_direct, 50),

    %% any empty messages to process?
    case query_node:queue_prepared(from_parent) of
       true  -> hc_empty(get(state));
       false -> ok
    end;
    
hc_eval(VarsValues, State) when (State == active) or (State == eos) ->

    %% set main variables
    %%put(inner_outer, inner),  %% 14/2/16: iztok: inner_outer is now explicit
    put(tp, get(orig_tp)),
    put(state, db_access),

    %% set msg head for queue to_parent
    MTO = list_to_atom("data_" ++ atom_to_list(get(inner_outer))),
    put(msg_to_parent, MTO), 

    %% for benchmark
    BTP = get(benchmark_task_pid),
    gen_server:cast(BTP, {restart_record, get(self), eval}),
    BSP = b3s_state_pid,
    BSZ = block_size,
    put(BSZ, gen_server:call(get(BSP), {get, BSZ})),

    %% setting new variable values in tp
    Vars = get(vars),
    F = fun ({Var, Value}) ->
		Pos   = maps:get(Var, Vars),
		TP    = get(tp),
		NewTP = setelement(Pos, TP, Value),
		put(tp, NewTP)
	end,
    lists:map(F, VarsValues),

    %% start iteration loop of tp_query_node
    hc_eval_1(db_interface:db_open_tp(get(tp))),
    info_msg(hc_eval, [get(self), {tp,get(tp)}, {vars_values,VarsValues}, get(state)], eval_started_setvar, 50),

    %% any empty messages to process?
    case query_node:queue_prepared(from_parent) of
       true  -> hc_empty(get(state));
       false -> ok
    end;

hc_eval(VarsValues, State) ->
    %% tp query node can not accept eval
    error_msg(hc_eval, [get(self), {vars_values,VarsValues}, {state,State}, get()], wrong_state).

hc_eval_1(fail) ->
    error_msg(hc_eval, [get(self), {all,get()}, get(state)], db_open_tp_failed);
    %% some protocol actions will be required here. [TODO]

hc_eval_1(ok) ->
    info_msg(hc_eval, [get(self), {tp,get(tp)}, {select_pred,get(select_pred)}, get(state)], db_open_tp_succeeded, 50).

%% 
%% @doc Empty message from parent received. Get next triple from 
%% cursor for tp and send it in a message to parent. 
%% Send end_of_stream if no more triples exist. 
%% 
%% @spec hc_empty(State::atom()) -> ok
%% 

hc_empty(State) when (State =:= undefined) -> 
    %% empty message stays in queue
    info_msg(hc_empty, [get(self), {from,get(parent)}, get(state)], empty_stays, 50);

hc_empty(db_access) ->

    %% process and send next block
    hce_process(query_node:queue_empty(from_db)),

    %% more empty messages to process?
    case query_node:queue_prepared(from_parent) of
       true ->  hc_empty(get(state));
       false -> ok
    end;

hc_empty(eos) ->

    %% send message to parent if block prepared
    case query_node:queue_prepared(to_parent) of
    true ->  
        %% get empty message from queue
        {empty, _} = query_node:queue_read(from_parent),

        %% read and send msg to parent 
        Msg = query_node:queue_read(to_parent),
        gen_server:cast(get(parent), Msg),
        info_msg(send_cast, [get(self), {message,Msg}, {to,get(parent)}, {invoker,hcep_process_triple}, get(state)], message_sent, 30);

    false -> 
        %% nothing to do. leave empty message in queue.
        ok
    end;
        
hc_empty(State) ->

    %% wrong state, report error 
    error_msg(hc_empty, [get(self), {from,get(parent)}, {state,State}, {all,get()}, get(state)], wrong_state).

%%
%% hce_process/1
%% 
%% @doc Iterate through the triples from queue from_db and process them. Each triple is 
%% tested for selection predicate and then stored to queue to_parent. 
%%
%% hce_process_iterate(FromDbEmpty::boolean()) -> ok
%%
hce_process(false) -> 

    %% get one triple from queue from_db
    {_, Triple} = query_node:queue_get(from_db), 

    %% process triple
    hcep_process_triple(Triple);

hce_process(true) -> 

    %% obtain next block
    TList = db_interface:db_next_block(get(block_size)),
    info_msg(hce_process, [get(self), {block_value,TList}, get(state)], next_block_read, 50),

    %% store list in queue from_db 
    query_node:queue_write(from_db, {db_block, get(self), TList}),

    %% loop
    hce_process(query_node:queue_empty(from_db)). 

%%
%% hcep_process_triple/2
%% 
%% @doc Process one triple and then continue to iterate through the triples 
%% from queue from_db and process them. Each triple is 
%% tested against selection predicate and then stored to queue to_parent. 
%%
%% hce_process_iterate(Triple::query_node:qn_triple()) -> ok
%%
hcep_process_triple(end_of_stream) -> 

    db_interface:db_close_tp(),

    %% for benchmark
    BTP = get(benchmark_task_pid),
    gen_server:cast(BTP, {stop_record, get(self), eval}),

    %% store eos to queue to_parent
    query_node:queue_put(to_parent, end_of_stream),

    %% flush buffer to_parent and send the message
    query_node:queue_flush(to_parent),
    %%info_msg(hcep_process_triple, [get(self), {buff_to_parent,get(buff_to_parent)}, {queue_to_parent,get(queue_to_parent)}, get(state)], debug_after_flush, 50),

    case query_node:queue_prepared(to_parent) of
    true -> %% get empty message from queue
            {empty, _} = query_node:queue_read(from_parent),

            %% get msg from queue and send it
            Msg = query_node:queue_read(to_parent),
            gen_server:cast(get(parent), Msg),
            info_msg(send_cast, [get(self), {to,get(parent)}, {message,Msg}, {invoker,hcep_process_triple}, get(state)], message_sent, 30);

    false -> ok
    end,

    %% new state is eos
    put(state, eos);

hcep_process_triple(Triple) -> 

    %% set current triple and compute val of select predicate
    put(tp_val, Triple),
    SP = query_node:eval_select(get(select_pred)),
    info_msg(hcep_process_triple, [get(self), {triple,Triple}, {select_pred,get(select_pred)}, {select_pred_value,SP}, get(state)], select_pred_computed, 50),

    %% send to Parent only if SP==true
    case SP of 
    true  -> 
       %% store triple to queue to_parent
       query_node:queue_put(to_parent, maps:put(get(node_id), Triple, maps:new())),

       %% send message to parent if block prepared
       case query_node:queue_prepared(to_parent) of
       true ->  
          %% get empty message from queue
          {empty, _} = query_node:queue_read(from_parent),

          %% read and send msg to parent and then stop since empty has been used
          Msg = query_node:queue_read(to_parent),
          gen_server:cast(get(parent), Msg),
          info_msg(send_cast, [get(self), {message,Msg}, {to,get(parent)}, {invoker,hcep_process_triple}, get(state)], message_sent, 30);

       false -> 
          %% iterate: get next triple and process it
          hce_process(query_node:queue_empty(from_db))
       end;
        
    false -> 
       %% skip this triple and get next
       hce_process(query_node:queue_empty(from_db))
    end.

%%
%% hc_eval_test_/0
%%
%% @doc Test function of eval protocol guided by empty messages.
%%
hc_eval_test_() ->
    hcet_site(b3s_state:get(test_mode)).

hcet_site(local1) ->
    NDS    = node(),
    BSS    = {b3s_state, NDS},
    CRC    = clm_row_conf,

    RMS = #{1 => NDS},
    CM1 = #{1 => RMS, 2 => RMS},
    R01 = [NDS],

    put(self, {'1-1-1', node()}),
    {inorder,
     [
      ?_assertMatch(ok, b3s:start()),
      ?_assertMatch(ok, b3s:bootstrap()),
      ?_assertMatch(ok,  gen_server:call(BSS, {put, CRC, CM1})),
      ?_assertMatch(R01, gen_server:call(BSS, propagate)),
      {generator, fun()-> hcet_load_db() end},
      {generator, fun()-> hcet_q01() end},
      {generator, fun()-> hcet_q02() end},
      {generator, fun()-> hcet_q03() end},
      ?_assertMatch(ok, b3s:stop())
     ]};

hcet_site(local_two) ->
    [];

hcet_site(_) ->
    [].

hcet_load_db() ->
    case ?STRING_ID_CODING_METHOD of
	string_integer -> hcet_load_db_epgsql_string_integer()
    end.

hcet_load_db_epgsql_string_integer() ->
    BS  = gen_server:call(node_state, {get, b3s_state_pid}),
    Tab = db_interface:dot_get_tn(),
    FE  = fun ({T, I, S, P, O}) ->
		  ET = string_id:encode_triple({I, S, P, O}),
		  list_to_tuple([T | tuple_to_list(ET)])
	  end,
    FET = fun (X) -> string_id:encode_triple_pattern(X) end,

    SI  = string_id,
    SIT = gen_server:call(BS, {get, name_of_string_id_table}),
    gen_server:call(SI, {put, sid_table_name, SIT}),
    gen_server:call(SI, delete_table),
    gen_server:call(SI, {create_table, SIT}),
    gen_server:call(SI, make_index),
    erase(sid_table_name),
    erase(sid_max_id),

    I01 = "<triple_id_0001>",
    S01 = "<Chinese>",
    P01 = "<eat>",
    O01 = "<vegetables>",
    T01 = FE({Tab, I01, S01, P01, O01}),

    I02 = "<triple_id_0002>",
    S02 = "<Japanese>",
    O02 = "<fishes>",
    T02 = FE({Tab, I02, S02, P01, O02}),

    I03 = "<triple_id_0003>",
    S03 = "<Slovenian>",
    O03 = "<potatoes>",
    T03 = FE({Tab, I03, S03, P01, O03}),

    TP01 = FET({I01,  "?s", "?p", "?o"}),
    TP02 = FET({I02,  "?s", "?p", "?o"}),
    TP03 = FET({I03,  "?s", "?p", "?o"}),
    TP11 = FET({"?id", "<Chinese>", "<eat>", "<vegetables>"}),
    TP12 = FET({"?id", "?sbj",      "<eat>", "<fishes>"}),
    TP13 = FET({"?id", "<Chinese>", "?prd",  "<vegetables>"}),
    TP14 = FET({"?id", "<Chinese>", "<eat>", "?obj"}),
    TP15 = FET({"?id", "<Chinese>", "?prd",  "?obj"}),
    TP16 = FET({"?id", "?sbj",      "<eat>", "?obj"}),
    TP17 = FET({"?id", "?sbj",      "?prd",  "<potatoes>"}),
    TP18 = FET({"?id", "?sbj",      "?prd",  "?obj"}),
    TP19 = FET({"_:",  "_:sbj",     "_:prd", "_:obj"}),
    TP20 = FET({"?id", "<Chinese>", "<eat>", "<fishes>"}),
    TP21 = FET({"?id", "<Chinese>", "?prd",  "<fished>"}),

    EOS = end_of_stream,

    {inorder,
     [
      ?_assertMatch(ok,  db_interface:db_init()),
      ?_assertMatch(ok,  db_interface:db_add_index()),
      ?_assertMatch(ok,  db_interface:db_write(T01)),
      ?_assertMatch(ok,  db_interface:db_write(T02)),
      ?_assertMatch(ok,  db_interface:db_write(T03)),
      ?_assertMatch(ok,  db_interface:db_close()),

      ?_assertMatch(ok,  db_interface:db_open_tp(TP01)),
      ?_assertMatch(T01, db_interface:db_next()),
      ?_assertMatch(EOS, db_interface:db_next()),
      ?_assertMatch(EOS, db_interface:db_next()),
      ?_assertMatch(ok,  db_interface:db_open_tp(TP02)),
      ?_assertMatch(T02, db_interface:db_next()),
      ?_assertMatch(EOS, db_interface:db_next()),
      ?_assertMatch(ok,  db_interface:db_open_tp(TP03)),
      ?_assertMatch(T03, db_interface:db_next()),
      ?_assertMatch(EOS, db_interface:db_next()),
      ?_assertMatch(ok,  db_interface:db_open_tp(TP12)),
      ?_assertMatch(T02, db_interface:db_next()),
      ?_assertMatch(EOS, db_interface:db_next()),
      ?_assertMatch(ok,  db_interface:db_open_tp(TP13)),
      ?_assertMatch(T01, db_interface:db_next()),
      ?_assertMatch(EOS, db_interface:db_next()),
      ?_assertMatch(ok,  db_interface:db_open_tp(TP14)),
      ?_assertMatch(T01, db_interface:db_next()),
      ?_assertMatch(EOS, db_interface:db_next()),
      ?_assertMatch(ok,  db_interface:db_open_tp(TP15)),
      ?_assertMatch(T01, db_interface:db_next()),
      ?_assertMatch(EOS, db_interface:db_next()),
      ?_assertMatch(ok,  db_interface:db_open_tp(TP16)),
      ?_assertMatch(T01, db_interface:db_next()),
      ?_assertMatch(T02, db_interface:db_next()),
      ?_assertMatch(T03, db_interface:db_next()),
      ?_assertMatch(EOS, db_interface:db_next()),
      ?_assertMatch(ok,  db_interface:db_open_tp(TP17)),
      ?_assertMatch(T03, db_interface:db_next()),
      ?_assertMatch(EOS, db_interface:db_next()),
      ?_assertMatch(ok,  db_interface:db_open_tp(TP18)),
      ?_assertMatch(T01, db_interface:db_next()),
      ?_assertMatch(T02, db_interface:db_next()),
      ?_assertMatch(T03, db_interface:db_next()),
      ?_assertMatch(EOS, db_interface:db_next()),
      ?_assertMatch(ok,  db_interface:db_open_tp(TP19)),
      ?_assertMatch(T01, db_interface:db_next()),
      ?_assertMatch(T02, db_interface:db_next()),
      ?_assertMatch(T03, db_interface:db_next()),
      ?_assertMatch(EOS, db_interface:db_next()),
      ?_assertMatch(ok,  db_interface:db_open_tp(TP21)),
      ?_assertMatch(EOS, db_interface:db_next()),
      ?_assertMatch(ok,  db_interface:db_open_tp(TP11)),
      ?_assertMatch(T01, db_interface:db_next()),
      ?_assertMatch(EOS, db_interface:db_next()),
      ?_assertMatch(ok,  db_interface:db_open_tp(TP20)),
      ?_assertMatch(EOS, db_interface:db_next()),
      ?_assertMatch(ok,  db_interface:db_close())
     ]}.

hcet_send_empty(TPQN, R) ->
    gen_server:cast(TPQN, {empty, self()}),
    receive
	M -> M
    end,
    info_msg(hcet_send_empty, [get(self), {from,TPQN}, {received, M}, {expected,R}, get(state)], data_received, 50),
    M.


hcet_q01() ->
    case ?STRING_ID_CODING_METHOD of
	string_integer -> hcet_q01_string_integer()
    end.

hcet_q01_string_integer() ->
    info_msg(hcet_q01_string_integer, [get(self), get()], start, 50),
    SessionId = "1",
    QueryId = "1",
    QueryNodeId   = "2",
    Id = list_to_atom(SessionId++"-"++QueryId++"-"++QueryNodeId),
    TPQN1 = spawn_process(Id, node()),
    TPOrig = {"?id", "<Chinese>", "<eat>", "?obj"},
    TriplePattern = string_id:encode_triple_pattern(TPOrig),
    SelectPred    = none,
    ProjectList   = none,
    ParentPid     = self(),
    VarsPositions = #{"?id" => 1, "?obj" => 4},
    M1            = {start, QueryNodeId, QueryId, SessionId, TPQN1, TriplePattern,
		     SelectPred, ProjectList, ParentPid, VarsPositions, outer},
    M2            = {eval, []},

    FE  = fun ({T, I, S, P, O}) ->
		  ET = string_id:encode_triple({I, S, P, O}),
		  list_to_tuple([T | tuple_to_list(ET)])
	  end,
    T1Orig = #triple_store{id = "<triple_id_0001>",
			   s  = "<Chinese>",
			   p  = "<eat>",
			   o  = "<vegetables>"},
    T1            = FE(T1Orig),

    GP            = get_property,
    DFO           = data_outer,
    EOS           = end_of_stream,

    R1Map         = maps:put(QueryNodeId, T1, maps:new()),
    R1            = {DFO, {Id, node()}, [R1Map,EOS]},
%    R2            = {DFO, {Id, node()}, EOS},

    info_msg(hcet_q01_string_integer,
	     [get(self), get(), {'T1', T1}, {'R1', R1}], testing, 50),
    {inorder,
     [
      ?_assertMatch(ok,            gen_server:call(TPQN1, M1)),
      ?_assertMatch(active,        gen_server:call(TPQN1, {GP, state})),
      ?_assertMatch(ok,            gen_server:call(TPQN1, M2)),
      ?_assertMatch(db_access,     gen_server:call(TPQN1, {GP, state})),
      ?_assertMatch({_, R1},       hcet_send_empty(TPQN1, R1)),
      ?_assertMatch(outer,         gen_server:call(TPQN1, {GP, inner_outer})),
      ?_assertMatch(ok,            gen_server:cast(TPQN1, {stop, self()})),
      ?_assertMatch(ok,            timer:sleep(1000))
     ]}.

hcet_q02() ->
    info_msg(hcet_q02, [get(self), get()], start, 50),
    SessionId = "1",
    QueryId = "2",
    QueryNodeId   = "2",

    Id = list_to_atom(SessionId++"-"++QueryId++"-"++QueryNodeId),
    TPQN1 = spawn_process(Id, node()),
    TPOrig = {"?id", "?sbj", "<eat>", "?obj"},
    TriplePattern = string_id:encode_triple_pattern(TPOrig),
    SelectPred = none,
    ProjectList = none,
    ParentPid     = self(),
    VarsPositions = #{"?id" => 1, "?sbj" => 2, "?obj" => 4},
    M1            = {start, QueryNodeId, QueryId, SessionId, TPQN1, TriplePattern,
		     SelectPred, ProjectList, ParentPid, VarsPositions, inner},
    M2            = {eval, [{"?sbj", string_id:get_id("<Chinese>")}]},

    FE  = fun ({T, I, S, P, O}) ->
		  ET = string_id:encode_triple({I, S, P, O}),
		  list_to_tuple([T | tuple_to_list(ET)])
	  end,
    T1Orig = #triple_store{id = "<triple_id_0001>",
			   s  = "<Chinese>",
			   p  = "<eat>",
			   o  = "<vegetables>"},
    T1            = FE(T1Orig),

    GP            = get_property,
    DFI           = data_inner,
    EOS           = end_of_stream,

    R1Map         = maps:put(QueryNodeId, T1, maps:new()),
    R1            = {DFI, TPQN1, [R1Map,EOS]},
%    R2            = {DFI, TPQN1, EOS},

    info_msg(hcet_q02, [get(self), get()], testing, 50),
    {inorder,
     [
      ?_assertMatch(ok,            gen_server:call(TPQN1, M1)),
      ?_assertMatch(ok,            gen_server:call(TPQN1, M2)),
      ?_assertMatch({_, R1},       hcet_send_empty(TPQN1, R1)),
      ?_assertMatch(inner,         gen_server:call(TPQN1, {GP, inner_outer})),
      ?_assertMatch(ok,            gen_server:cast(TPQN1, {stop, self()})),
      ?_assertMatch(ok,            timer:sleep(1000))
     ]}.

hcet_q03() ->
    info_msg(hcet_q03, [get(self), get()], start, 50),
    SessionId = "1",
    QueryId = "2",
    QueryNodeId   = "3",
    Id = list_to_atom(SessionId++"-"++QueryId++"-"++QueryNodeId),
    TPQN1 = spawn_process(Id, node()),
    TPOrig = {"?id", "?sbj", "<eat>", "?obj"},
    TriplePattern = string_id:encode_triple_pattern(TPOrig),
    SelectPred = {{"?sbj", equal, string_id:get_id("<Chinese>")}, lor,
		  {"?obj", equal, string_id:get_id("<fishes>")}},
    ProjectList = none,
    ParentPid     = self(),
    VarsPositions = #{"?id" => 1, "?sbj" => 2, "?obj" => 4},
    M1            = {start, QueryNodeId, QueryId, SessionId, TPQN1, TriplePattern,
		     SelectPred, ProjectList, ParentPid, VarsPositions, outer},
    M2            = {eval, []},

    T1Orig = #triple_store{id = "<triple_id_0001>",
			   s  = "<Chinese>",
			   p  = "<eat>",
			   o  = "<vegetables>"},

    T2Orig = #triple_store{id = "<triple_id_0002>",
			   s  = "<Japanese>",
			   p  = "<eat>",
			   o  = "<fishes>"},

%    T3 = #triple_store{id = "<triple_id_0003>",
%    		       s  = "<Slovenian>",
%    		       p  = "<eat>",
%    		       o  = "<potatoes>"},

    FE  = fun ({T, I, S, P, O}) ->
		  ET = string_id:encode_triple({I, S, P, O}),
		  list_to_tuple([T | tuple_to_list(ET)])
	  end,
    T1            = FE(T1Orig),
    T2            = FE(T2Orig),

    GP            = get_property,
    DFI           = data_outer,
    EOS           = end_of_stream,

    R1Map         = maps:put(QueryNodeId, T1, maps:new()),
    R2Map         = maps:put(QueryNodeId, T2, maps:new()),
%    R3Map         = maps:put(QueryNodeId, T3, maps:new()),
    R1            = {DFI, TPQN1, [R2Map,R1Map,EOS]},
%    R2            = {DFI, TPQN1, R2Map},
%    R3            = {DFI, TPQN1, R3Map},
%    R4            = {DFI, TPQN1, EOS},

    info_msg(hcet_q03, [get(self), get()], testing, 50),
    {inorder,
     [
      ?_assertMatch(ok,            gen_server:call(TPQN1, M1)),
      ?_assertMatch(ok,            gen_server:call(TPQN1, M2)),
      ?_assertMatch({_, R1},       hcet_send_empty(TPQN1, R1)),
      ?_assertMatch(outer,         gen_server:call(TPQN1, {GP, inner_outer})),
      ?_assertMatch(ok,            gen_server:cast(TPQN1, {stop, self()})),
      ?_assertMatch(ok,            timer:sleep(1000))
     ]}.

%% ====> END OF LINE <====

%%
%% Distribute triples and triple patterns to columns.
%%
%% @copyright 2014-2016 UP FAMNIT and Yahoo Japan Corporation
%% @version 0.3
%% @since March, 2014
%% @author Kiyoshi Nitta <knitta@yahoo-corp.jp>
%% @author Iztok Savnik <iztok.savnik@famnit.upr.si>
%% 
%% @see b3s
%% @see db_interface
%% @see file_reader
%% @see tp_query_node
%% @see join_query_node
%% 
%% @doc A gen_server process for distributing given triples and triple
%% patterns to columns of available data servers and managing schema
%% information. This module is implemented as an erlang <A
%% href="http://www.erlang.org/doc/man/gen_server.html"> gen_server
%% </A> process. For performing the distribution, the triple
%% distributor process must have schema information. This module
%% provides means for investigating the schema from given data
%% set. The triple distributor process holds a mnesia table for
%% storing the schema.
%% 
%% <table bgcolor="lemonchiffon">
%% <tr><th>Section Index</th></tr>
%% <tr><td>{@section bootstrap procedure}</td></tr>
%% <tr><td>{@section property list}</td></tr>
%% <tr><td>{@section handle_call (synchronous) message API}</td></tr>
%% <tr><td>{@section handle_cast (asynchronous) message API}</td></tr>
%% </table>
%% 
%% == bootstrap procedure ==
%% 
%% (LINK: {@section bootstrap procedure})
%% 
%% The triple_distibutor process must be invoked by following
%% bootstrap procedure (maybe coded at {@link b3s}?).
%% 
%% <ol>
%% 
%% <li>Invoke a front server b3s application on a server.</li>
%% 
%% <li>Invoke a triple_distibutor process.</li>
%% 
%% <li>Invoke client b3s applications on distributed servers.</li>
%% 
%% <li>Define column / row configuration using {@section
%% @{register_node, Column, NodeName@}} synchronous messages.</li>
%% 
%% <li>Send {@section @{investigate_stream, Triple, NotifyProc@}}
%% asynchronous messages to the triple_distibutor process for
%% determing the distribution strategy.</li>
%% 
%% <li>Send {@section @{store_stream, Triple, NotifyProc@}}
%% asynchronous messages to the triple_distibutor process for storing
%% triples to column tables.</li>
%% 
%% </ol>
%% 
%% == property list ==
%% 
%% (LINK: {@section property list})
%% 
%% The gen_server process uses following properties holded by {@link
%% td_state()}. These properties can be accessed using {@section
%% @{get_property, Name@}} synchronous messages.
%% 
%% <table border="3">
%% <tr><th>Name</th><th>Type</th><th>Description</th></tr>
%% 
%% <tr> <td>created</td> <td>boolean()</td> <td>true denotes that
%% process dictionary was created and used. false denotes that
%% completely new process.</td> </tr>
%% 
%% <tr> <td>pid</td> <td>pid()</td> <td>id of the triple_distributor
%% process.</td> </tr>
%% 
%% <tr> <td>node</td> <td>node()</td> <td>node name that runs the
%% triple_distributor process.</td> </tr>
%% 
%% <tr> <td>clm_row_conf</td> <td>maps:map()</td> <td>mapping from
%% {@type node_state:ns_column_id()} to {@type
%% node_state:ns_rows()}.</td> </tr>
%% 
%% <tr> <td>clm_filename</td> <td>maps:map()</td> <td>mapping from
%% {@type node_state:ns_column_id()} to string().</td> </tr>
%% 
%% <tr> <td>clm_count</td> <td>maps:map()</td> <td>mapping from {@type
%% node_state:ns_column_id()} to integer().</td> </tr>
%% 
%% <tr> <td>clm_iodev</td> <td>maps:map()</td> <td>mapping from {@type
%% node_state:ns_column_id()} to io_device().</td> </tr>
%% 
%% <tr> <td>pred_clm</td> <td>maps:map()</td> <td>mapping from {@link
%% tp_query_node:qn_predicate()} to {@type
%% node_state:ns_column_id()}.</td> </tr>
%% 
%% <tr> <td>pred_freq</td> <td>maps:map()</td> <td>mapping from {@link
%% tp_query_node:qn_predicate()} to Frequency::integer().</td> </tr>
%% 
%% <tr> <td>last_triple</td> <td>{@type
%% tp_query_node:qn_triple()}</td> <td>last triple received.</td>
%% </tr>
%% 
%% <tr> <td>investigate_processes</td> <td>[{@type
%% node_state:ns_pid()}]</td> <td>list of invoked {@link file_reader}
%% processes.</td> </tr>
%% 
%% <tr> <td>store_processes</td> <td>[{@type
%% node_state:ns_pid()}]</td> <td>list of invoked {@link file_reader}
%% processes.</td> </tr>
%% 
%% <tr> <td>sender_processes</td> <td>[{@type
%% node_state:ns_pid()}]</td> <td>list of processes that sent start
%% messages.</td> </tr>
%% 
%% <tr> <td>distribution_algorithm</td> <td>atom()</td> <td>specifies
%% a triple distribution algorithm.</td> </tr>
%% 
%% <tr> <td>max_id</td> <td>integer()</td> <td>max id integer for
%% no-id triples.</td> </tr>
%% 
%% <tr> <td>ti_skel</td> <td>string()</td> <td>skeleton string for
%% generating triple id.</td> </tr>
%% 
%% <tr> <td>column_file_path</td> <td>string()</td> <td>skeleton
%% path string for column dump files.</td> </tr>
%% 
%% <tr> <td>start_date_time</td> <td>calendar:datetime()</td>
%% <td>started date and time of the process.</td> </tr>
%% 
%% <tr> <td>update_date_time</td> <td>calendar:datetime()</td>
%% <td>updated date and time of process properties.</td> </tr>
%% 
%% <tr> <td>write_mode</td> <td>on_the_fly | postgres_bulk_load</td>
%% <td>specify actual behavior for write requests.</td> </tr>
%% 
%% <tr> <td>encode_mode</td> <td>boolean()</td>
%% <td>perform encoding when it is set to true.</td> </tr>
%% 
%% <tr> <td>initialized_string_id_table</td> <td>boolean()</td>
%% <td>default: false. becomes true if initialization performed.</td>
%% </tr>
%% 
%% </table>
%% 
%% === implemented triple distribution algorithm ===
%% 
%% ==== random ====
%% 
%% Assign a column number at random.
%% 
%% ==== predicate_based ====
%% 
%% Assign a column number according to the predicate of the triple. A
%% mapping table from predicate string to column number integer will
%% be constructed by the investigate_stream process.
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
%% <tr> <td>{@section @{register_node, Column, NodeName@}}</td>
%% <td>{@type node_state:ns_column_id()}, node()</td> <td>{registered,
%% {{@link node_state:ns_node_location()}, node()}} | {failed, Column,
%% NodeName}</td> <td>register a data server node</td> </tr>
%% 
%% <tr> <td>{@section @{get_property, Name@}}</td> <td>atom()</td>
%% <td>term()</td> <td>get property value</td> </tr>
%% 
%% <tr> <td>{@section @{put_property, Name, Value@}}</td> <td>atom(),
%% term()</td> <td>ok</td> <td>put property value</td> </tr>
%% 
%% <tr> <td>{@section build_distribution_function}</td> <td></td>
%% <td>ok | {error, term()}</td> <td>build distribution function</td>
%% </tr>
%% 
%% <tr> <td>{@section @{save_property, Name@}}</td> <td>atom()</td>
%% <td>ok | {error, term()}</td> <td>save property value</td> </tr>
%% 
%% <tr> <td>{@section @{load_property, Name@}}</td> <td>atom()</td>
%% <td>ok | {error, term()}</td> <td>load property value</td> </tr>
%% 
%% </table>
%% 
%% === {register_node, Column, NodeName} ===
%% 
%% This message registers an Erlang node NodeName::node() as one of
%% alive nodes of column label Column::{@type
%% node_state:ns_column_id()}. It returns {registered, {{@link
%% node_state:ns_node_location()}, node()}} if successfully
%% registered. Otherwise, it returns {failed, Column, NodeName}. This
%% request is implemented by {@link hc_rn/2}. (LINK: {@section
%% @{register_node, Column, NodeName@}})
%% 
%% === {get_property, Name} ===
%% 
%% This message returns the value of specified property name. Variable
%% Name is an atom(). (LINK: {@section @{get_property, Name@}}).
%% 
%% === {put_property, Name, Value} ===
%% 
%% This message puts the value of specified property name to process
%% dictionary. Variable Name is an atom(). Variable Value is a
%% term(). (LINK: {@section @{put_property, Name, Value@}}).
%% 
%% === {save_property, Name} ===
%% 
%% This message saves the value of specified property name to
%% permanent storage. Variable Name is an atom(). (LINK: {@section
%% @{save_property, Name@}}). This request is implemented by {@link
%% hc_save_property/1}.
%% 
%% === {load_property, Name} ===
%% 
%% This message loads the value of specified property name from
%% permanent storage. Variable Name is an atom(). (LINK: {@section
%% @{load_property, Name@}}). This request is implemented by {@link
%% hc_load_property/1}.
%% 
%% === build_distribution_function ===
%% 
%% This message builds a distribution function. It returns ok if the
%% function was successfully built. Otherwise, it returns {error,
%% Reason:term()}. This request is implemented by {@link
%% hc_build_df/0}. (LINK: {@section build_distribution_function}).
%% 
%% == handle_cast (asynchronous) message API ==
%% 
%% (LINK: {@section handle_cast (asynchronous) message API})
%% 
%% <table border="3">
%% 
%% <tr> <th>Message</th> <th>Args</th>
%% <th>Description</th> </tr>
%% 
%% <tr> <td>{@section @{start, From, investigate_stream |
%% store_stream, FileName, Proc, Node@}}</td> <td>{@type
%% node_state:ns_pid()}, atom(), string(), atom(), atom()</td>
%% <td>start a file reading process</td> </tr>
%% 
%% <tr> <td>{@section @{investigate_stream, From, Triple@}}</td>
%% <td>{@type node_state:ns_pid()}, end_of_stream | {@type
%% tp_query_node:qn_triple()}</td> <td>investigate a triple</td> </tr>
%% 
%% <tr> <td>{@section @{store_stream, From, Triple@}}</td> <td>{@type
%% node_state:ns_pid()}, end_of_stream | {@type
%% tp_query_node:qn_triple()}</td> <td>store a triple</td> </tr>
%% 
%% </table>
%% 
%% === {start, From, investigate_stream | store_stream, FileName, Proc, Node} ===
%% 
%% This message starts invetigate or store process by invoking a
%% {@link file_reader} process. Parameter From is {@type
%% node_state:ns_pid()} that indicates the sender process in
%% distributed environment. Parameter FileName is a string() that
%% shows a path of file to be read. Parameter Proc and Node are
%% process id and node id of invoking {@link file_reader}
%% process. This request is implemented by {@link hc_start/5}. (LINK:
%% {@section @{start, From, investigate_stream | store_stream,
%% FileName, Proc, Node@}})
%% 
%% === {investigate_stream, From, Triple} ===
%% 
%% This message investigates a triple record Triple::{@link
%% tp_query_node:qn_triple()} for determining the distribution
%% strategy. From is {@type node_state:ns_pid()} that indicates the
%% sender process in distributed environment. While this message will
%% be used similar to {@section @{store_stream, From, Triple@}}
%% message, the investigate_stream message does not store anything. If
%% Triple is end_of_stream, it calculates the structure for assigining
%% column numbers to triples and sends a {triple_distributor_finished,
%% {Pid::atom(), NodeId::node()}} asynchronous message to the
%% processes that sent {@section @{start, From, investigate_stream |
%% store_stream, FileName, Proc, Node@}} messages. This request is
%% implemented by {@link hc_investigate_stream/2}. (LINK: {@section
%% @{investigate_stream, From, Triple@}})
%% 
%% === {store_stream, From, Triple} ===
%% 
%% This message appends a triple record Triple::{@link
%% tp_query_node:qn_triple()} into tables of an appropreate
%% clomun. From is {@type node_state:ns_pid()} that indicates the
%% sender process in distributed environment. If Triple is
%% end_of_stream, it sends a {triple_distributor_finished,
%% {Pid::atom(), NodeId::node()}} asynchronous message to the
%% processes that sent {@section @{start, From, investigate_stream |
%% store_stream, FileName, Proc, Node@}} messages. (NOTE) The actual
%% write rpc calls will be performed on the first row servers of each
%% column. This request is implemented by {@link
%% hc_store_stream/2}. (LINK: {@section @{store_stream, From,
%% Triple@}})
%% 
%% @type td_state() = maps:map(). A Reference pointer of the map
%% object that manages properties for operating the gen_server
%% process.
%% 
-module(triple_distributor).
-behavior(gen_server).
-export(
   [
    child_spec/0, register_column/2, register_columns/1,

    tdt_register_local/0, tdt_investigate/0, tdt_store/0,
    tdt_store_pbl/0, tdt_flush_local/0,

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
%% @doc Initialize a triple_distributor process.
%% 
%% @spec init([]) -> {ok, td_state()}
%% 
init([]) ->
    process_flag(trap_exit, true),

    %% {Dt, Tm}  = calendar:local_time(),
    %% DtArray   = [element(1, Dt), element(2, Dt), element(3, Dt)],
    %% TmArray   = [element(1, Tm), element(2, Tm), element(3, Tm)],
    %% DtString  = io_lib:format("~4..0B~2..0B~2..0B", DtArray),
    %% TmString  = io_lib:format("~2..0B~2..0B~2..0B", TmArray),
    %% {ok, TIS} = application:get_env(b3s, triple_id_skel),
    %% TISkel    = lists:flatten(TIS ++ "_"
    %% 			      ++ DtString ++ "_"
    %% 			      ++ TmString ++ "_"),

    %% for ty6s unit tests to work well
    %% {Dt, _}   = calendar:local_time(),
    %% DtArray   = [element(1, Dt), element(2, Dt), element(3, Dt)],
    %% DtString  = io_lib:format("~4..0B~2..0B~2..0B", DtArray),
    %% {ok, TIS} = application:get_env(b3s, triple_id_skel),
    %% TISkel    = lists:flatten(TIS ++ "_" ++ DtString ++ "_"),

    %% for reducing the size of string_id table
    {ok, TISkel} = application:get_env(b3s, triple_id_skel),

    State = #{
      created => true,
      pid => self(),
      node => node(),
      clm_row_conf => #{},
      pred_clm => #{},
      pred_freq => #{},
      clm_filename => #{},
      clm_count => #{},
      clm_iodev => #{},
      investigate_processes => [],
      store_processes => [],
      sender_processes => [],
      distribution_algorithm => predicate_based,
      max_id => 1,
      ti_skel => TISkel ++ "_",
      column_file_path => "bak/column-~w.tsv",
      start_date_time  => calendar:local_time(),
      write_mode => on_the_fly,
      encode_mode => true,
      initialized_string_id_table => false
     },
    info_msg(init, [], State, -1),
    {ok, State}.

%% 
%% handle_call/3
%% 
%% @doc Handle synchronous query requests.
%% 
%% @spec handle_call(term(), {pid(), term()}, td_state()) -> {reply,
%% term(), td_state()}
%% 
handle_call({register_node, Column, NodeName}, _, State) ->
    hc_restore_pd(get(created), State),
    {reply, hc_rn(Column, NodeName), hc_save_pd()};

handle_call({get_property, all}, _, State) ->
    hc_restore_pd(get(created), State),
    {reply, get(), State};

handle_call({get_property, Name}, _, State) ->
    hc_restore_pd(get(created), State),
    {reply, get(Name), State};

handle_call({put_property, Name, Value}, _, State) ->
    hc_restore_pd(get(created), State),
    put(Name, Value),
    put(update_date_time, calendar:local_time()),
    {reply, ok, hc_save_pd()};

handle_call({save_property, Name}, _, State) ->
    hc_restore_pd(get(created), State),
    {reply, hc_save_property(Name), hc_save_pd()};

handle_call({load_property, Name}, _, State) ->
    hc_restore_pd(get(created), State),
    {reply, hc_load_property(Name), hc_save_pd()};

handle_call(build_distribution_function, _, State) ->
    hc_restore_pd(get(created), State),
    {reply, hc_build_df(), hc_save_pd()};

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
%% @spec handle_cast(term(), td_state()) -> {noreply, td_state()}
%% 
handle_cast({start, From, Role, FileName, Proc, Node}, State) ->
    hc_restore_pd(get(created), State),
    hc_start(From, Role, FileName, Proc, Node),
    {noreply, hc_save_pd()};

handle_cast({investigate_stream, From, Triple}, State) ->
    hc_restore_pd(get(created), State),
    hc_investigate_stream(From, Triple),
    {noreply, hc_save_pd()};

handle_cast({store_stream, From, Triple}, State) ->
    hc_restore_pd(get(created), State),
    hc_store_stream(From, Triple),
    {noreply, hc_save_pd()};

%% default
handle_cast(Request, State) ->
    R = {unknown_request, Request},
    error_msg(handle_cast, [Request, State], R),
    {noreply, State}.

%% 
%% @doc Restore process dictionaries from state map structure.
%% 
%% @spec hc_restore_pd([{atom(), term()}] | undefined, td_state()) -> ok
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
%% @spec hc_save_pd() -> td_state()
%% 
hc_save_pd() ->
    maps:from_list(get()).

%% 
%% handle_info/2
%% 
%% @doc Handle exceptional query requests. 
%% 
%% @spec handle_info(term(), td_state()) -> {noreply, td_state()}
%% 
handle_info(_Info, State) ->
    {noreply, State}.

%% 
%% terminate/2
%% 
%% @doc Process termination. 
%% 
%% @spec terminate(term(), td_state()) -> none()
%% 
terminate(Reason, State) ->
    info_msg(terminate, [Reason, State], terminate_normal, 0),
    ok.

%% 
%% code_change/3
%% 
%% @doc Process code change action. 
%% 
%% @spec code_change(term(), td_state(), term()) -> {ok, td_state()}
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
%% @spec child_spec() -> supervisor:child_spec()
%% 
child_spec() ->
    Id = triple_distributor,
    GSOpt = [{local, Id}, Id, [], []],
    StartFunc = {gen_server, start_link, GSOpt},
    Restart = permanent,
    Shutdwon = 1000,
    Type = worker,
    Modules = [triple_distributor],

    {Id, StartFunc, Restart, Shutdwon, Type, Modules}.

%% 
%% @doc Register an Erlang node to a column of triple_distributor
%% process that runs on a front server node. This function should be
%% called from the data server. It sends {@section @{register_node,
%% Column, NodeName@}} synchronous request ({@link hc_rn/2})
%% internally.
%% 
%% @spec register_column(Column::node_state:ns_column_id(),
%% FrontServerNode::node()) -> {registered,
%% {node_state:ns_node_location(), node()}} | {failed,
%% node_state:ns_column_id(), node()}
%% 
register_column(Column, FrontServerNode) ->
    S = {triple_distributor, FrontServerNode},
    M = {register_node, Column, node()},
    R = gen_server:call(S, M),
    A = [Column, FrontServerNode],
    info_msg(register_column, A, R, 50),
    R.

%% 
%% @doc Register Erlang nodes to columns of triple_distributor process
%% that runs on a fron server node. It takes a list of
%% {DataServerNode::node(), Column::{@type node_state:ns_column_id()}}
%% tuples. This function should be called from the front server. It
%% sends {@section @{register_node, Column, NodeName@}} synchronous
%% request ({@link hc_rn/2}) from remote nodes.
%% 
%% @spec register_columns([{DataServerNode::node(),
%% Column::node_state:ns_column_id()}]) -> [{registered,
%% {node_state:ns_node_location(), node()}} | {failed,
%% node_state:ns_column_id(), node()} | {badrpc, Reason::term(),
%% node()}]
%% 
register_columns(List) ->
    rc_perform(List, []).

rc_perform([], Result) ->
    Result;
rc_perform([{DataServerNode, Column} | List], Result) ->
    TD = triple_distributor,
    RC = register_column,
    A = [Column, node()],
    R = rpc:call(DataServerNode, TD, RC, A),
    rcp_rpc(R, DataServerNode, List, Result).

rcp_rpc({badrpc, Reason}, DataServerNode, List, Result) ->
    rc_perform(List, [{badrpc, Reason, DataServerNode} | Result]);
rcp_rpc(R, _, List, Result) ->
    rc_perform(List, [R | Result]).

%% ======================================================================
%% 
%% handle call/cast implementation
%% 

%% 
%% @doc This function saves the value of specified property name to
%% permanent storage. The property value must be maps:map() data type.
%% 
%% @spec hc_save_property(atom()) -> ok | {error, Reason::term()}
%% 
hc_save_property(pred_clm) ->
    BS  = gen_server:call(node_state, {get, b3s_state_pid}),
    Tab = gen_server:call(BS, {get, name_of_pred_clm_table}),
    R = db_interface:db_put_map(get(pred_clm), Tab),
    info_msg(hc_save_property, [pred_clm], R, 50);
hc_save_property(pred_freq) ->
    BS  = gen_server:call(node_state, {get, b3s_state_pid}),
    Tab = gen_server:call(BS, {get, name_of_pred_freq_table}),
    R = db_interface:db_put_map(get(pred_freq), Tab),
    info_msg(hc_save_property, [pred_freq], R, 50);
hc_save_property(Name) ->
    E = {error, {table_name_not_accociated, Name}},
    error_msg(hc_save_property, [Name], E),
    E.

%% 
%% @doc This function loads the value of specified property name from
%% permanent storage.
%% 
%% @spec hc_load_property(atom()) -> ok | {error, Reason::term()}
%% 
hc_load_property(pred_clm) ->
    BS  = gen_server:call(node_state, {get, b3s_state_pid}),
    Tab = gen_server:call(BS, {get, name_of_pred_clm_table}),
    M = db_interface:db_get_map(Tab),
    hlp_perform(M, pred_clm);
hc_load_property(pred_freq) ->
    BS  = gen_server:call(node_state, {get, b3s_state_pid}),
    Tab = gen_server:call(BS, {get, name_of_pred_freq_table}),
    M = db_interface:db_get_map(Tab),
    hlp_perform(M, pred_freq);
hc_load_property(Name) ->
    E = {error, {table_name_not_accociated, Name}},
    error_msg(hc_load_property, [Name], E),
    E.

hlp_perform({error, Reason}, Name) ->
    E = {error, Reason},
    error_msg(hlp_perform, [E, Name], E),
    E;
hlp_perform(Map, Name) ->
    put(Name, Map),
    info_msg(hlp_perform, [Map, Name], successfully_loaded, 50).

%% 
%% @doc This function builds a distribution function according to
%% 'distribution_algorithm' process dictionary property. It returns ok
%% if the function was successfully built. Otherwise, it returns
%% {error, Reason:term()}.
%% 
%% @spec hc_build_df() -> ok | {error, term()}
%% 
hc_build_df() ->
    DA  = get(distribution_algorithm),
    CRC = get(clm_row_conf),
    PF  = get(pred_freq),
    hbd_perform(DA, CRC, PF).

hbd_perform(random, _, _) ->
    ok;
hbd_perform(_, undefined, _) ->
    {error, no_clm_row_conf};
hbd_perform(predicate_based, _, undefined) ->
    {error, {predicate_based, {not_defined, pred_freq}}};
hbd_perform(predicate_based, _, _) ->
    hcis_assign_column(predicate_based, 0);
hbd_perform(DA, _, _) ->
    {error, {unknown_distribution_algorithm, DA}}.

%% 
%% @doc Register an Erlang node to a column.
%% 
%% @spec hc_rn(Column::node_state:ns_column_id(), NodeName::node()) ->
%% {registered, {node_state:ns_node_location(), node()}} | {failed,
%% node_state:ns_column_id(), node()}
%% 
hc_rn(Column, NodeName) ->
    RowMap = maps:find(Column, get(clm_row_conf)),
    hcr_register(Column, NodeName, RowMap).

hcr_register(Column, NodeName, error) ->
    hcrr_perform(Column, NodeName, #{});
hcr_register(Column, NodeName, {ok, RowMap}) ->
    hcrr_perform(Column, NodeName, RowMap).

hcrr_perform(Column, NodeName, RowMap) ->
    Size   = maps:size(RowMap),
    NewMap = maps:put(Size + 1, NodeName, RowMap),
    put(clm_row_conf, maps:put(Column, NewMap, get(clm_row_conf))),

    L = {Column, Size + 1},
    R = {registered, {L, NodeName}},
    A = [Column, NodeName],
    info_msg(hc_rn, A, R, 80),
    R.
%{failed, Column, NodeName}

%% 
%% @doc Start invetigate or store process by invoking a {@link
%% file_reader} process. Parameter From is a tuple {ProcessId::pid(),
%% NodeId::node()} that indicates the sender process in distributed
%% environment. Parameter FileName is a string() that shows a path of
%% file to be read. Parameter Proc and Node are process id and node id
%% of invoking {@link file_reader} process.
%% 
%% @spec hc_start(From::{ProcId::pid(), NodeId::node()},
%% Role::investigate_stream | store_stream, FileName::string(),
%% Proc::atom(), Node::atom()) -> ok
%% 
hc_start(From, Role, FileName, Proc, Node) ->
    B3S          = {b3s, Node},
    CS           = file_reader:child_spec(Proc),
    SSC          = supervisor:start_child(B3S, CS),
    ChiPid       = hcs_start_process(SSC),
    TD           = {triple_distributor, node()},
    MesStart     = {start, FileName, TD, Role},
    ok           = gen_server:call({Proc, Node}, MesStart),
    MesEmpty     = {empty, TD},
    ok           = gen_server:cast({Proc, Node}, MesEmpty),

    P = hcs_remind_process(Role, Proc, Node),
    hcs_start_stop_watch(P),
    hcs_remind_from(From),
    hcs_open_db(get(write_mode), Role),
    hcs_initialize_string_id(get(initialized_string_id_table),
			     get(encode_mode), get(sid_table_name)),
    R = {started, {ChiPid, Node}, P},
    A = [From, Role, FileName, Proc, Node],
    info_msg(hc_start, A, R, 50).

hcs_start_process({ok, ChiPid}) ->
    ChiPid;
hcs_start_process({error, {already_started, ChiPid}}) ->
    A = {error, {already_started, ChiPid}},
    R = ChiPid,
    error_msg(hcs_start_process, A, R),
    R.

hcs_remind_from(From) ->
    SP     = sender_processes,
    SPList = get(SP),
    SPSet  = sets:from_list(SPList),
    SPS    = sets:add_element(From, SPSet),
    put(SP, sets:to_list(SPS)).

hcs_remind_process(investigate_stream, Proc, Node) ->
    K = investigate_processes,
    ProcList = lists:append(get(K), [{Proc, Node}]),
    put(K, ProcList),
    ProcList;
hcs_remind_process(store_stream, Proc, Node) ->
    K = store_processes,
    ProcList = lists:append(get(K), [{Proc, Node}]),
    put(K, ProcList),
    ProcList.

hcs_start_stop_watch([{P, N}]) ->
    M = triple_distributor_hc_start,
    RR = gen_server:call(stop_watch, report),
    RS = gen_server:call(stop_watch, {start, M}),
    info_msg(hcs_start_stop_watch, [{P, N}], {RR, RS}, 50);
hcs_start_stop_watch(_) ->
    ok.

hcs_open_db(postgres_bulk_load, _) ->
    ok;
hcs_open_db(WM, investigate_stream) ->
    info_msg(hcs_open_db, [WM, investigate_stream], do_nothing, 50);
hcs_open_db(WM, store_stream) ->
    CRC = get(clm_row_conf),
    NL = lists:flatten(lists:map(fun maps:values/1, maps:values(CRC))),
    NodLst = lists:usort(NL),
    F = fun (X) ->
		RC = gen_server:call({db_writer, X}, db_open),
		{X, RC}
	end,
    R = {db_open, lists:map(F, NodLst)},
    info_msg(hcs_open_db, [WM, store_stream], R, 50).

hcs_initialize_string_id(true, _, _) ->
    ok;
hcs_initialize_string_id(false, false, _) ->
    ok;
hcs_initialize_string_id(false, true, undefined) ->
    BS  = gen_server:call(node_state, {get, b3s_state_pid}),
    Tab = gen_server:call(BS, {get, name_of_string_id_table}),
    RCT = gen_server:call(string_id, {create_table, Tab}),
    RMI = gen_server:call(string_id, make_index),
    R   = {{create_table, RCT}, {make_index, RMI}},
%    mnesia:wait_for_tables([Tab], 1000),
    put(initialized_string_id_table, true),
    info_msg(hcs_initialize_string_id, [false, true, undefined], R, 50);
hcs_initialize_string_id(false, true, _) ->
    ok;
hcs_initialize_string_id(false, M, T) ->
    error_msg(hcs_initialize_string_id, [false, M, T], unknown_encode_mode).

%% 
%% @doc Investigate a triple for determining an appropriate column to
%% be stored.
%% 
%% @spec hc_investigate_stream(From::{ProcId::pid(), NodeId::node()},
%% Triple::tp_query_node:qn_triple()) -> ok
%% 
hc_investigate_stream(From, end_of_stream) ->
    K = investigate_processes,
    ProcList = lists:delete(From, get(K)),
    put(K, ProcList),
    DA = get(distribution_algorithm),
    hcis_assign_column(DA, length(ProcList)),

    A = [From, end_of_stream],
    F = gen_server:call(From, {get_property, file_name}),
    M = {investigate_stream_finished, F, ProcList},
    gen_server:cast(stop_watch, {record, M}),
    info_msg(hc_investigate_stream, A, {F, ProcList}, 10);

hc_investigate_stream(From, Triple) ->
    put(last_triple, Triple),
    DA = get(distribution_algorithm),
    hcis_accumulate_statistics(DA),
    gen_server:cast(From, {empty, {triple_distributor, node()}}),

    A = [From, Triple],
    M = get(pred_freq),
    info_msg(hc_investigate_stream, A, M, 80).

hcis_notify_finish() ->
    SP     = sender_processes,
    SPList = get(SP),
    TDF    = triple_distributor_finished,
    PN     = {triple_distributor, node()},
    F = fun (X) -> gen_server:cast(X, {TDF, PN}) end,
    put(SP, []),
    put(update_date_time, calendar:local_time()),
    lists:map(F, SPList).

hcis_assign_column(predicate_based, 0) ->
    MapPF  = get(pred_freq),
    F = fun ({_, A}, {_, B}) when A > B -> true; (_, _) -> false end,
    LstPrd = lists:sort(F, maps:to_list(MapPF)),
    MapCRC = get(clm_row_conf),
    LstCol = maps:keys(MapCRC),
    MapCol = hacpb_prep_colmap(LstCol, #{}),
    hacpb_perform(LstPrd, MapCol),
    hcis_notify_finish(),

    M = {MapPF, LstPrd, MapCRC, LstCol, MapCol},
    info_msg(hcis_assign_column, [predicate_based, 0], M, 80);
hcis_assign_column(predicate_based_p, 0) ->
    MapCRC = get(clm_row_conf),
    LstCol = maps:keys(MapCRC),
    NumCol = maps:size(MapCRC),
    MapPF  = get(pred_freq),
    F = fun ({_, A}, {_, B}) when A > B -> true; (_, _) -> false end,
    LstPrd = lists:sort(F, maps:to_list(MapPF)),
    hac_pbp(LstPrd, 1, NumCol, LstCol),
    hcis_notify_finish(),

    M = {MapCRC, LstCol, NumCol, LstPrd},
    info_msg(hcis_assign_column, [predicate_based_p, 0], M, 80);
hcis_assign_column(Algorithm, NumAliveProc) when NumAliveProc > 0 ->
    A = [Algorithm, NumAliveProc],
    M = file_reader_still_running,
    info_msg(hcis_assign_column, A, M, 50);
hcis_assign_column(random, _) ->
    ok;
hcis_assign_column(Algorithm, NumAliveProc) ->
    A = [Algorithm, NumAliveProc],
    M = unknown_distribution_algorithm,
    error_msg(hcis_assign_column, A, M).

hacpb_prep_colmap([], Map) ->
    Map;
hacpb_prep_colmap([X | L], M) ->
    hacpb_prep_colmap(L, maps:put(X, 0, M)).

hacpb_perform([], MapCol) ->
    A = [[], MapCol],
    M = {finish, get(pred_clm), get(pred_freq)},
    info_msg(hacpb_perform, A, M, 50);
hacpb_perform([Prd | LstPrd], MapCol) ->
    F = fun ({_, A}, {_, B}) when A < B -> true; (_, _) -> false end,
    [{Column, _} | _] = lists:sort(F, maps:to_list(MapCol)),
    {P, Freq} = Prd,
    PC = pred_clm,
    put(PC, maps:put(P, Column, get(PC))),
    NewCC = maps:get(Column, MapCol) + Freq,
    NewMC = maps:update(Column, NewCC, MapCol),

    A = [[Prd | LstPrd], MapCol],
    M = {processing, P, Freq, Column, NewCC, NewMC, get(pred_clm)},
    info_msg(hacpb_perform, A, M, 80),
    hacpb_perform(LstPrd, NewMC).

hac_pbp([], Column, MaxCol, LstCol) ->
    A = [[], Column, MaxCol, LstCol],
    M = {finish, get(pred_clm), get(pred_freq)},
    info_msg(hac_pbp, A, M, 50);
hac_pbp([Prd | LstPrd], Column, MaxCol, LstCol) ->
    {P, _} = Prd,
    PC = pred_clm,
    put(PC, maps:put(P, Column, get(PC))),

    A = [[Prd | LstPrd], Column, MaxCol, LstCol],
    M = {processing, Prd, get(pred_clm)},
    info_msg(hac_pbp, A, M, 80),
    hac_pbp(LstPrd, Column rem MaxCol + 1, MaxCol, LstCol).

hcis_accumulate_statistics(random) ->
    ok;
hcis_accumulate_statistics(predicate_based) ->
    {_Id, _Sbj, Prd, _Obj} = get(last_triple),
    MapOld = get(pred_freq),
    case maps:is_key(Prd, MapOld) of
	true ->
	    F = maps:get(Prd, MapOld) + 1;
	false ->
	    F = 1
    end,
    MapNew = maps:put(Prd, F, MapOld),
    put(pred_freq, MapNew).

%% 
%% @doc Store a triple into an appropriate column.
%% 
%% @spec hc_store_stream(From::{ProcId::pid(), NodeId::node()},
%% Triple::tp_query_node:qn_triple()) -> ok
%% 
hc_store_stream(From, end_of_stream) ->
    K = store_processes,
    ProcList = lists:delete(From, get(K)),
    put(K, ProcList),
    hcss_finish(length(ProcList)),

    A = [From, end_of_stream],
    F = gen_server:call(From, {get_property, file_name}),
    M = {store_stream_finished, F, ProcList},
    gen_server:cast(stop_watch, {record, M}),
    info_msg(hc_store_stream, A, {F, ProcList}, 10);

hc_store_stream(From, Triple) ->
    put(last_triple, Triple),
    DA = get(distribution_algorithm),
    R = hcss_remind_triple(DA, Triple),
    gen_server:cast(From, {empty, {triple_distributor, node()}}),

    A = [From, Triple],
    info_msg(hc_store_stream, A, R, 80).

hcss_finish(0) ->
    hcss_close_db(get(write_mode)),
    NF = hcis_notify_finish(),
    info_msg(hcss_finish, [0], NF, 80);
hcss_finish(NumAliveProc) ->
    A = [NumAliveProc],
    M = file_reader_still_running,
    info_msg(hcss_finish, A, M, 50).

hcss_close_db(postgres_bulk_load) ->
    ok;
hcss_close_db(WM) ->
    CRC = get(clm_row_conf),
    NL = lists:flatten(lists:map(fun maps:values/1, maps:values(CRC))),
    NodLst = lists:usort(NL),
    F = fun (X) ->
		RC = gen_server:call({db_writer, X}, db_close),
		{X, RC}
	end,
    R = {db_close, lists:map(F, NodLst)},
    info_msg(hcss_close_db, [WM], R, 50).

hcss_remind_triple(DistributionAlgorithm, Triple) ->
    hrt_switch(DistributionAlgorithm, Triple).

hrt_switch(random, Triple) ->
    ColumnNumber = maps:size(get(clm_row_conf)),
    Column       = random:uniform(ColumnNumber),

    hrt_write_a_triple(Column, Triple);
hrt_switch(predicate_based, Triple) ->
    Prd = element(3, Triple),
    MapPC = get(pred_clm),
    R = hrt_pb_perform(maps:is_key(Prd, MapPC), Triple),

    %% A = [predicate_based, Triple],
    %% info_msg(hcss_remind_triple, A, {R, Prd, MapPC}, 80),

    R.

hrt_pb_perform(false, Triple) ->
    A = {false, Triple},
    R = {store_to_new_column, get(pred_clm)},
    error_msg(hrt_pb_perform, A, R),
    R;
hrt_pb_perform(true, Triple) ->
    Prd    = element(3, Triple),
    MapPC  = get(pred_clm),
    Column = maps:get(Prd, MapPC),
    hrt_write_a_triple(Column, Triple).

hrt_encode(Triple) ->
    hrt_encode(get(encode_mode), Triple).

hrt_encode(true, Triple) ->
    case string_id:encode_triple(Triple) of
	fail ->
	    Triple;
	T ->
	    T
    end;
hrt_encode(false, Triple) ->
    Triple.

hrt_write_a_triple(Column, {[], Sbj, Prd, Obj}) ->
    TIS = get(ti_skel),
    MI  = put(max_id, get(max_id) + 1),
    MIS = lists:flatten(io_lib:format("~12..0w", [MI])),
    TId = "<" ++ TIS ++ MIS ++ ">",
    hrt_write_a_triple(Column, {TId, Sbj, Prd, Obj});
hrt_write_a_triple(Column, Triple) ->
    hrtwat_select_mode(get(write_mode), Column, hrt_encode(Triple)).

hrtwat_select_mode(on_the_fly, Column, Triple) ->
    MapCRC = get(clm_row_conf),
    hrt_rpc(maps:is_key(Column, MapCRC), Column, Triple);
hrtwat_select_mode(postgres_bulk_load, Column, Triple) ->
    MikCFN = maps:is_key(Column, get(clm_filename)),
    MikCCT = maps:is_key(Column, get(clm_count)),
    EM     = get(encode_mode),
    hrtwat_pbl(MikCFN, MikCCT, Column, Triple, ?STRING_ID_CODING_METHOD, EM);
hrtwat_select_mode(WriteMode, Column, Triple) ->
    A = [WriteMode, Column, Triple],
    E = {unknown_mode, WriteMode},
    error_msg(hrtwat_select_mode, A, E).

hrt_rpc(false, Column, Triple) ->
    A = [false, Column, Triple],
    R = no_node_configured_for_column,
    error_msg(hrt_rpc, A, R);
hrt_rpc(true, Column, Triple) ->
    MapCRC = get(clm_row_conf),
    NodLst = [lists:nth(1, maps:to_list(maps:get(Column, MapCRC)))],

    DW  = db_write,
    DWR = db_writer,

    F = fun ({_, Node}) ->
		RR = gen_server:call({DWR, Node}, {DW, Triple}),
		hrt_examine(RR, Node, Column, Triple)
	end,
    R = lists:map(F, NodLst),
    %% A = [true, Column, Triple],
    %% info_msg(hrt_rpc, A, R, 50),
    R.

hrt_examine(ok, Node, Column, Triple) ->
    {successfully_stored, Node, Column, Triple};
hrt_examine(fail, Node, Column, Triple) ->
    A = [fail, Node, Column, Triple],
    R = {db_write_failed, Node, Column, Triple},
    error_msg(hrt_examine, A, R),
    R;
hrt_examine(E, Node, Column, Triple) ->
    A = [fail, Node, Column, Triple],
    R = {rpc_failed, E},
    error_msg(hrt_examine, A, R),
    R.

hrtwat_pbl(false, MikCCT, Column, Triple, SIMode, EM) ->
    Fmt = get(column_file_path),
    FN = list_to_atom(lists:flatten(io_lib:format(Fmt, [Column]))),
    M = maps:put(Column, FN, get(clm_filename)),
    put(clm_filename, M),

    %% open file here
    HOF = hrtwat_open_file(FN, Column),
    case HOF of
	{opened, _, _, _} ->
	    A = [false, MikCCT, Column, Triple, SIMode, EM],
	    R = {open_file, HOF},
	    info_msg(hrtwat_pbl, A, R, 10),
	    hrtwat_pbl(true, MikCCT, Column, Triple, SIMode, EM);
	{open_failed, _, _, _, _} ->
	    A = [false, MikCCT, Column, Triple, SIMode, EM],
	    R = {open_file, HOF},
	    error_msg(hrtwat_pbl, A, R)
    end;
hrtwat_pbl(MikCFN, false, Column, Triple, SIMode, EM) ->
    M = maps:put(Column, 0, get(clm_count)),
    put(clm_count, M),
    hrtwat_pbl(MikCFN, true, Column, Triple, SIMode, EM);
hrtwat_pbl(true, true, Column, Triple, string_integer, true) ->
    A = [true, true, Column, Triple, string_integer, true],

    FileName = maps:get(Column, get(clm_filename)),
    Count    = maps:get(Column, get(clm_count)) + 1,
    put(clm_count, maps:put(Column, Count, get(clm_count))),

    {Id, Sbj, Prd, {ObjVal, ObjTyp}} = Triple,
    case ObjTyp of
	code ->
	    Val = "~w\t~w\t~w\t~w\t~w\t0\t1\n",
	    Rec = [Count, Id, Sbj, Prd, ObjVal];
	integer ->
	    Val = "~w\t~w\t~w\t~w\t~w\t0\t2\n",
	    Rec = [Count, Id, Sbj, Prd, ObjVal];
	real ->
	    Val = "~w\t~w\t~w\t~w\t\t ~g\t3\n",
	    Rec = [Count, Id, Sbj, Prd, ObjVal];
	datetime ->
	    case hrtwat_convert_datetime(ObjVal) of
		fail ->
		    F      = fun(X) -> tuple_to_list(X) end,
		    DTFlat = lists:flatten(lists:map(F, F(ObjVal))),
		    DTForm = "~4..0w-~2..0w-~2..0w ~2..0w:~2..0w:~2..0w",
		    ObjStr = lists:flatten(io_lib:format(DTForm, DTFlat)),
		    Val = "~w\t~w\t~w\t~w\t~ts\t0\t5\n",
		    Rec = [Count, Id, Sbj, Prd, ObjStr];
		GS ->
		    Val = "~w\t~w\t~w\t~w\t~w\t0\t4\n",
		    Rec = [Count, Id, Sbj, Prd, GS]
	    end;
	string ->
	    Val = "~w\t~w\t~w\t~w\t~ts\t0\t5\n",
	    Rec = [Count, Id, Sbj, Prd, ObjVal];
	undefined ->
	    Val = "~w\t~w\t~w\t~w\t~w\t0\t0\n",
	    Rec = [Count, Id, Sbj, Prd, ObjVal]
    end,

    %% write a record here
    io:fwrite(maps:get(Column, get(clm_iodev)), Val, Rec),

    R = {wrote, FileName, {Val, Rec}},
    info_msg(hrtwat_pbl, A, R, 80);
hrtwat_pbl(true, true, Column, Triple, string_integer, false) ->
    A = [true, true, Column, Triple, string_integer, true],

    FileName = maps:get(Column, get(clm_filename)),
    Count    = maps:get(Column, get(clm_count)) + 1,
    put(clm_count, maps:put(Column, Count, get(clm_count))),

    {Id, Sbj, Prd, Obj} = Triple,
    Val = "~w\t~ts\t~ts\t~ts\t~ts\n",
    Rec = [Count, Id, Sbj, Prd, Obj],

    %% write a record here
    io:fwrite(maps:get(Column, get(clm_iodev)), Val, Rec),

    R = {wrote, FileName, {Val, Rec}},
    info_msg(hrtwat_pbl, A, R, 80);
hrtwat_pbl(true, true, Column, Triple, no_encode, EM) ->
    FileName = maps:get(Column, get(clm_filename)),
    Count    = maps:get(Column, get(clm_count)) + 1,
    Line     = [integer_to_list(Count) | tuple_to_list(Triple)],

    M = maps:put(Column, Count, get(clm_count)),
    put(clm_count, M),

    %% write a record here
    F = "~ts\t~ts\t~ts\t~ts\t~ts\n",
    io:fwrite(maps:get(Column, get(clm_iodev)), F, Line),

    A = [true, true, Column, Triple, no_encode, EM],
    R = {wrote, FileName, Line},
    info_msg(hrtwat_pbl, A, R, 80).

hrtwat_open_file(FN, Columnn) ->
    Modes = [write, {encoding, utf8}],
    hof_perform(file:open(FN, Modes), FN, Modes, Columnn).

hof_perform({ok, IoDevice}, FN, Modes, Columnn) ->
    put(clm_iodev, maps:put(Columnn, IoDevice, get(clm_iodev))),
    {opened, Columnn, FN, Modes};
hof_perform({error, Reason}, FN, Modes, Columnn) ->
    put(clm_iodev, maps:remove(Columnn, get(clm_iodev))),
    {open_failed, Columnn, FN, Modes, Reason}.

hrtwat_convert_datetime(DateTime) ->
    R = (catch calendar:datetime_to_gregorian_seconds(DateTime)),
    hcd_perform(R, DateTime).

hcd_perform({'EXIT', E}, DateTime) ->
    A = {illegal_datetime_value, E},
    error_msg(hrtwat_convert_datetime, [DateTime], A),
    fail;
hcd_perform(R, _) ->
    R.

%% ======================================================================
%% 
%% @doc Unit tests.
%% 
td_test_() ->
    application:load(b3s),
    {ok, TM} = application:get_env(b3s, test_mode),
    tdt_site(TM).

tdt_site(local2) ->
    TUT  = td_unit_test,
    register(TUT, self()),

    ND   = node(),
    TD   = {triple_distributor, ND},
    PP   = put_property,
    CFP  = column_file_path,
    CFN  = clm_filename,
    CCT  = clm_count,
    CID  = clm_iodev,
    {inorder,
     [
      ?_assertMatch(ok, b3s:start()),
      ?_assertMatch(ok, b3s:bootstrap()),
      {generator, fun tdt_register_local/0},
      {generator, fun tdt_investigate/0},
      ?_assertMatch(ok, gen_server:call(TD, {PP, CFP, "bak/rnd-column-~w.tsv"})),
      {generator, fun tdt_store_pbl_rnd/0},
      ?_assertMatch(ok, timer:sleep(1000)),
      ?_assertMatch(ok, gen_server:call(TD, {PP, CFN, #{}})),
      ?_assertMatch(ok, gen_server:call(TD, {PP, CCT, #{}})),
      ?_assertMatch(ok, gen_server:call(TD, {PP, CID, #{}})),
      ?_assertMatch(ok, gen_server:call(TD, {PP, CFP, "bak/pd-column-~w.tsv"})),
      {generator, fun tdt_store_pbl_pd/0},
      ?_assertMatch(ok, timer:sleep(1000)),
      {generator, fun tdt_put_get_save_load/0},
      ?_assertMatch(ok, b3s:stop())
     ]};

tdt_site(local2_on_the_fly) ->
    TUT  = td_unit_test,
    register(TUT, self()),
    {inorder,
     [
      ?_assertMatch(ok, b3s:start()),
      ?_assertMatch(ok, b3s:bootstrap()),
      {generator, fun tdt_reset_string_id/0},
      {generator, fun tdt_register_local/0},
      {generator, fun tdt_investigate/0},
      {generator, fun tdt_flush_local/0},
      {generator, fun tdt_store/0},
      ?_assertMatch(ok, timer:sleep(1000)),
      {generator, fun tdt_check_data_local/0},
      %% {generator, fun tdt_store_pbl/0},
      ?_assertMatch(ok, timer:sleep(1000)),
      {generator, fun tdt_put_get_save_load/0},
      ?_assertMatch(ok, b3s:stop())
     ]};

tdt_site(yjmac) ->
    TUT  = td_unit_test,
    register(TUT, self()),
    {inorder,
     [
      ?_assertMatch(ok, b3s:start()),
      ?_assertMatch(ok, b3s:bootstrap()),
      {generator, fun tdt_node_connection/0},
      ?_assertMatch(ok, b3s:stop())
     ]};

tdt_site(yjr6) ->
    TUT  = td_unit_test,
    register(TUT, self()),
    {inorder,
     [
      ?_assertMatch(ok, b3s:start()),
      ?_assertMatch(ok, b3s:bootstrap()),
      {generator, fun tdt_register_yjr6/0},
      {generator, fun tdt_investigate/0},
      {generator, fun tdt_store/0},
      {generator, fun tdt_check_data_yjr6/0},
      {generator, fun tdt_flush_yjr6/0},
      ?_assertMatch(ok, b3s:stop())
     ]};

tdt_site(yjr6sma) ->
    TUT  = td_unit_test,
    register(TUT, self()),
    {inorder,
     [
      ?_assertMatch(ok, b3s:start()),
      ?_assertMatch(ok, b3s:bootstrap()),
      {generator, fun tdt_register_yjr6/0},
      {generator, fun tdt_investigate_store_small/0},
      {generator, fun tdt_check_data_yjr6_small/0},
      ?_assertMatch(ok, b3s:stop())
     ]};

tdt_site(yjr6mid) ->
    TUT  = td_unit_test,
    register(TUT, self()),
    {inorder,
     [
      ?_assertMatch(ok, b3s:start()),
      ?_assertMatch(ok, b3s:bootstrap()),
      {generator, fun tdt_register_yjr6/0},
      {generator, fun tdt_investigate_store_middle/0},
      {generator, fun tdt_check_data_yjr6_middle/0},
      ?_assertMatch(ok, b3s:stop())
     ]};

%% 
%% GCall/2 -- shortcut for genserver:call/2
%% GCast/2 -- shortcut for genserver:cast/2
%% RepS/0  -- report storing progress (on nightbird)
%% 
%% f().  NW1 = 'b3ss01@wild.rlab.miniy.yahoo.co.jp'.  NW = 'b3ss02@wild.rlab.miniy.yahoo.co.jp'.  NC = 'b3ss02@circus.rlab.miniy.yahoo.co.jp'.  NI = 'b3ss02@innocents.rlab.miniy.yahoo.co.jp'.  NE = 'b3ss02@erasure.rlab.miniy.yahoo.co.jp'.  NL = 'b3ss02@loveboat.rlab.miniy.yahoo.co.jp'.  NN1 = 'b3ss01@nightbird.rlab.miniy.yahoo.co.jp'.  NN = 'b3ss02@nightbird.rlab.miniy.yahoo.co.jp'.  NSW = {node_state, NW}.  NSC = {node_state, NC}.  NSI = {node_state, NI}.  NSE = {node_state, NE}.  NSL = {node_state, NL}.  NSN = {node_state, NN}.  B3S = {b3s_state, NW1}.  AllN = [NW, NC, NI, NE, NL, NN].  GCall = fun(X, Y) -> gen_server:call(X, Y) end.  GCast = fun(X, Y) -> gen_server:cast(X, Y) end.  RCall = fun(X, Y) -> rpc:call(X, Y) end.  RepS = fun() -> L = GCall({triple_distributor, NN1}, {get_property, store_processes}), {lists:map(fun(X) -> {X, GCall(X, {get_property, triple_count})} end, L), calendar:local_time()} end.  b().
%% 

tdt_site(yjr6yg2s) ->
    TUT  = td_unit_test,
    register(TUT, self()),
    {inorder,
     [
      ?_assertMatch(ok, b3s:start()),
      ?_assertMatch(ok, b3s:bootstrap()),
      {generator, fun tdt_register_yjr6/0},
      {generator, fun tdt_investigate_store_yjr6yg2s/0},
      {generator, fun tdt_check_data_yjr6_yjr6yg2s/0},
      ?_assertMatch(ok, b3s:stop())
     ]};

tdt_site(yjr6dup) ->
    TUT  = td_unit_test,
    register(TUT, self()),
    {inorder,
     [
      ?_assertMatch(ok, b3s:start()),
      ?_assertMatch(ok, b3s:bootstrap()),
      {generator, fun tdt_register_yjr2/0},
      {generator, fun tdt_investigate_store_small/0},
      {generator, fun tdt_check_data_yjr2_small/0},
      ?_assertMatch(ok, b3s:stop())
     ]};

tdt_site(_) ->
    [].

%% should be run from yj mac
tdt_node_connection() ->
    NW = 'b3ss02@wild.rlab.miniy.yahoo.co.jp',
    NC = 'b3ss02@circus.rlab.miniy.yahoo.co.jp',
    NI = 'b3ss02@innocents.rlab.miniy.yahoo.co.jp',
    NE = 'b3ss02@erasure.rlab.miniy.yahoo.co.jp',
    NL = 'b3ss02@loveboat.rlab.miniy.yahoo.co.jp',
    NN = 'b3ss02@nightbird.rlab.miniy.yahoo.co.jp',
    AllN = [NW, NC, NI, NE, NL, NN],
    {inorder,
     [
      ?_assertMatch(pong, net_adm:ping(NW)),
      ?_assertMatch(pong, net_adm:ping(NC)),
      ?_assertMatch(pong, net_adm:ping(NI)),
      ?_assertMatch(pong, net_adm:ping(NE)),
      ?_assertMatch(pong, net_adm:ping(NL)),
      ?_assertMatch(pong, net_adm:ping(NN)),
      ?_assertMatch(AllN, nodes(connected))
     ]}.

%% should be run from yj research 6 servers
tdt_register_yjr6() ->
    NW     = 'b3ss02@wild.rlab.miniy.yahoo.co.jp',
    NC     = 'b3ss02@circus.rlab.miniy.yahoo.co.jp',
    NI     = 'b3ss02@innocents.rlab.miniy.yahoo.co.jp',
    NE     = 'b3ss02@erasure.rlab.miniy.yahoo.co.jp',
    NL     = 'b3ss02@loveboat.rlab.miniy.yahoo.co.jp',
    NN     = 'b3ss02@nightbird.rlab.miniy.yahoo.co.jp',
    DBIF   = db_interface,
    DBINIT = db_init,

    {ok, S}      = application:get_env(b3s, data_server_nodes),
    {ok, T, _}   = erl_scan:string(S),
    {ok, Config} = erl_parse:parse_term(T),
    R01W   = {registered, {{1, 1}, NW}},
    R01C   = {registered, {{1, 2}, NC}},
    R01I   = {registered, {{2, 1}, NI}},
    R01E   = {registered, {{2, 2}, NE}},
    R01L   = {registered, {{3, 1}, NL}},
    R01N   = {registered, {{3, 2}, NN}},
    R01    = [R01N, R01L, R01E, R01I, R01C, R01W],

    NTT = name_of_triple_tables,
    AGE = fun(X) -> application:get_env(b3s, X) end,
    {ok, TabLst} = AGE(NTT),
    TabW = element(2, lists:keyfind(NW, 1, TabLst)),
    TabC = element(2, lists:keyfind(NC, 1, TabLst)),
    TabI = element(2, lists:keyfind(NI, 1, TabLst)),
    TabE = element(2, lists:keyfind(NE, 1, TabLst)),
    TabL = element(2, lists:keyfind(NL, 1, TabLst)),
    TabN = element(2, lists:keyfind(NN, 1, TabLst)),

    {inorder,
     [
      ?_assertMatch(ok,   trl_set_remote_env(NW, NTT, TabW)),
      ?_assertMatch(ok,   trl_set_remote_env(NC, NTT, TabC)),
      ?_assertMatch(ok,   trl_set_remote_env(NI, NTT, TabI)),
      ?_assertMatch(ok,   trl_set_remote_env(NE, NTT, TabE)),
      ?_assertMatch(ok,   trl_set_remote_env(NL, NTT, TabL)),
      ?_assertMatch(ok,   trl_set_remote_env(NN, NTT, TabN)),
      ?_assertMatch(R01,  triple_distributor:register_columns(Config)),
      ?_assertMatch(ok,   rpc:call(NW, DBIF, DBINIT, [[NW, NC]])),
      ?_assertMatch(ok,   rpc:call(NI, DBIF, DBINIT, [[NI, NE]])),
      ?_assertMatch(ok,   rpc:call(NL, DBIF, DBINIT, [[NL, NN]]))
     ]}.

tdt_register_yjr2() ->
    NW     = 'b3ss02@wild.rlab.miniy.yahoo.co.jp',
    %% NC     = 'b3ss02@circus.rlab.miniy.yahoo.co.jp',
    %% NI     = 'b3ss02@innocents.rlab.miniy.yahoo.co.jp',
    %% NE     = 'b3ss02@erasure.rlab.miniy.yahoo.co.jp',
    NL     = 'b3ss02@loveboat.rlab.miniy.yahoo.co.jp',
    %% NN     = 'b3ss02@nightbird.rlab.miniy.yahoo.co.jp',

    {ok, S}      = application:get_env(b3s, data_server_nodes),
    {ok, T, _}   = erl_scan:string(S),
    {ok, Config} = erl_parse:parse_term(T),
    R01W   = {registered, {{1, 1}, NW}},
    R01L   = {registered, {{1, 2}, NL}},
    R01    = [R01L, R01W],
    AllN   = [NW, NL],

    NTT = name_of_triple_tables,
    AGE = fun(X) -> application:get_env(b3s, X) end,
    {ok, TabLst} = AGE(NTT),
    TabW = element(2, lists:keyfind(NW, 1, TabLst)),
    TabL = element(2, lists:keyfind(NL, 1, TabLst)),

    {inorder,
     [
      ?_assertMatch(ok,   trl_set_remote_env(NW, NTT, TabW)),
      ?_assertMatch(ok,   trl_set_remote_env(NL, NTT, TabL)),
      ?_assertMatch(R01,  triple_distributor:register_columns(Config)),
      ?_assertMatch(ok,   db_interface:db_init(AllN))
     ]}.

%% make start-fs DS="\"[].\""
%% make start-ds FS='b3ss01@MBA-11Z-460.local' CI=1
%% make terminate-fs
%% make ps
tdt_register_local() ->
    NodStr = atom_to_list(node()),
    NDS    = list_to_atom("b3ss02" ++ string:sub_string(NodStr, 7)),
    TD     = triple_distributor,
    GP     = get_property,
    CRC    = clm_row_conf,
    %% DBIF   = db_interface,
    %% DBINIT = db_init,
    %% DBAI   = db_add_index,

    Config = [{NDS, 2}, {NDS, 3}, {NDS, 3}],
    %% Config = [{NDS, 1}, {NDS, 2}, {NDS, 3}, {NDS, 3}],
    %% R01DS1 = {registered, {{1, 1}, NDS}},
    R01DS2 = {registered, {{2, 1}, NDS}},
    R01DS3 = {registered, {{3, 1}, NDS}},
    R01DS4 = {registered, {{3, 2}, NDS}},
    R01    = [R01DS4, R01DS3, R01DS2],
    %% R01    = [R01DS4, R01DS3, R01DS2, R01DS1],
    R02    = [1, 2, 3],

    NTT = name_of_triple_tables,
    AGE = fun(X) -> application:get_env(b3s, X) end,
    {ok, TabLst} = AGE(NTT),
    Tab = element(2, lists:keyfind(NDS, 1, TabLst)),

    {inorder,
     [
      ?_assertMatch(ok,   trl_set_remote_env(NDS, NTT, Tab)),
      ?_assertMatch(R01,  triple_distributor:register_columns(Config)),
      ?_assertMatch(R02,  maps:keys(gen_server:call(TD, {GP, CRC})))
      %% ?_assertMatch(ok,   rpc:call(NDS, DBIF, DBINIT, [])),
      %% ?_assertMatch(ok,   rpc:call(NDS, DBIF, DBINIT, [[NDS]])),
      %% ?_assertMatch(ok,   rpc:call(NDS, DBIF, DBAI, []))
     ]}.

trl_set_remote_env(Node, Par, Val) ->
    rpc:call(Node, application, set_env, [b3s, Par, Val]).

tdt_flush_yjr6() ->
    NW     = 'b3ss02@wild.rlab.miniy.yahoo.co.jp',
    %% NC     = 'b3ss02@circus.rlab.miniy.yahoo.co.jp',
    NI     = 'b3ss02@innocents.rlab.miniy.yahoo.co.jp',
    %% NE     = 'b3ss02@erasure.rlab.miniy.yahoo.co.jp',
    NL     = 'b3ss02@loveboat.rlab.miniy.yahoo.co.jp',
    %% NN     = 'b3ss02@nightbird.rlab.miniy.yahoo.co.jp',
    MN     = mnesia,
    DT     = delete_table,

    NTT = name_of_triple_tables,
    AGE = fun(X) -> application:get_env(b3s, X) end,
    {ok, TabLst} = AGE(NTT),
    TabW = element(2, lists:keyfind(NW, 1, TabLst)),
    TabI = element(2, lists:keyfind(NI, 1, TabLst)),
    TabL = element(2, lists:keyfind(NL, 1, TabLst)),

    {inorder,
     [
      ?_assertMatch({atomic, ok}, rpc:call(NW, MN, DT, [TabW])),
      ?_assertMatch({atomic, ok}, rpc:call(NI, MN, DT, [TabI])),
      ?_assertMatch({atomic, ok}, rpc:call(NL, MN, DT, [TabL]))
     ]}.

tdt_flush_local() ->
    case 1 of
	1 -> tdt_flush_local_db_bdbnif();
	_ -> tdt_flush_local_mnesia_qlc()
    end.

tdt_flush_local_db_bdbnif() ->
    NodStr = atom_to_list(node()),
    NDS    = list_to_atom("b3ss02" ++ string:sub_string(NodStr, 7)),
    DBW    = {db_writer, NDS},

    {inorder,
     [
      ?_assertMatch(ok, gen_server:call(DBW, db_init)),
      ?_assertMatch(ok, gen_server:call(DBW, db_close))
     ]}.

tdt_flush_local_mnesia_qlc() ->
    NodStr = atom_to_list(node()),
    NDS    = list_to_atom("b3ss02" ++ string:sub_string(NodStr, 7)),
    MN     = mnesia,
    DT     = delete_table,

    NTT = name_of_triple_tables,
    AGE = fun(X) -> application:get_env(b3s, X) end,
    {ok, TabLst} = AGE(NTT),
    Tab = element(2, lists:keyfind(NDS, 1, TabLst)),

    {inorder,
     [
      ?_assertMatch({atomic, ok}, rpc:call(NDS, MN, DT, [Tab]))
     ]}.

tdt_investigate() ->
    TUT  = td_unit_test,
    ND   = node(),
    TD   = {triple_distributor, ND},
    SN   = {TUT, node()},
    IS   = investigate_stream,
    GP   = get_property,
    TC   = triple_count,
    PF   = pred_freq,
    IP   = investigate_processes,
    PC   = pred_clm,
    SP   = sender_processes,
    DA   = distribution_algorithm,
    PB   = predicate_based,
    GC   = '$gen_cast',

    %% BS  = gen_server:call(node_state, {get, b3s_state_pid}),
    %% SI  = string_id,
    %% SIT = gen_server:call(BS, {get, name_of_string_id_table}),
    %% gen_server:call(SI, {put, sid_table_name, SIT}),
    %% gen_server:call(SI, {put, di_cursor__, undefined}),
    %% gen_server:call(SI, delete_table),
    %% gen_server:call(SI, {create_table, SIT}),
    %% gen_server:call(SI, make_index),

    FR01 = file_reader_01,
    FN01 = "ygtsv/yagoGeonamesClasses-h1k.tsv",
    FR02 = file_reader_02,
    FN02 = "ygtsv/yagoLiteralFacts-h1k-a.tsv",
    M01  = {start, SN, IS, FN01, FR01, ND},
    M02  = {start, SN, IS, FN02, FR02, ND},

    R01  = #{
      "<diedOnDate>" => 105,
      "<happenedOnDate>" => 7,
      "<hasAirportCode>" => 2,
      "<hasArea>" => 39,
      "<hasDuration>" => 15,
      "<hasEconomicGrowth>" => 1,
      "<hasExpenses>" => 2,
      "<hasExport>" => 2,
      "<hasGDP>" => 3,
      "<hasGini>" => 4,
      "<hasHeight>" => 19,
      "<hasISBN>" => 4,
      "<hasImport>" => 2,
      "<hasInflation>" => 2,
      "<hasLatitude>" => 126,
      "<hasLength>" => 1,
      "<hasLongitude>" => 117,
      "<hasMotto>" => 13,
      "<hasNumberOfPeople>" => 61,
      "<hasPages>" => 4,
      "<hasPopulationDensity>" => 18,
      "<hasPoverty>" => 2,
      "<hasRevenue>" => 4,
      "<hasTLD>" => 2,
      "<hasThreeLetterLanguageCode>" => 1,
      "<hasUnemployment>" => 2,
      "<hasWeight>" => 3,
      "<wasBornOnDate>" => 216,
      "<wasCreatedOnDate>" => 209,
      "<wasDestroyedOnDate>" => 14,
      "rdfs:subClassOf" => 672
     },
    R02 = [{FR01, ND}],
    R03 = [{FR01, ND}, {FR02, ND}],
    R04 = #{
      "<diedOnDate>" => 2,
      "<happenedOnDate>" => 2,
      "<hasAirportCode>" => 1,
      "<hasArea>" => 1,
      "<hasDuration>" => 1,
      "<hasEconomicGrowth>" => 1,
      "<hasExpenses>" => 2,
      "<hasExport>" => 1,
      "<hasGDP>" => 2,
      "<hasGini>" => 2,
      "<hasHeight>" => 1,
      "<hasISBN>" => 1,
      "<hasImport>" => 2,
      "<hasInflation>" => 1,
      "<hasLatitude>" => 1,
      "<hasLength>" => 2,
      "<hasLongitude>" => 2,
      "<hasMotto>" => 1,
      "<hasNumberOfPeople>" => 1,
      "<hasPages>" => 2,
      "<hasPopulationDensity>" => 2,
      "<hasPoverty>" => 2,
      "<hasRevenue>" => 2,
      "<hasTLD>" => 1,
      "<hasThreeLetterLanguageCode>" => 2,
      "<hasUnemployment>" => 1,
      "<hasWeight>" => 1,
      "<wasBornOnDate>" => 2,
      "<wasCreatedOnDate>" => 1,
      "<wasDestroyedOnDate>" => 2,
      "rdfs:subClassOf" => 3},
      %% "<diedOnDate>" => 3,
      %% "<happenedOnDate>" => 2,
      %% "<hasAirportCode>" => 1,
      %% "<hasArea>" => 2,
      %% "<hasDuration>" => 2,
      %% "<hasEconomicGrowth>" => 1,
      %% "<hasExpenses>" => 3,
      %% "<hasExport>" => 2,
      %% "<hasGDP>" => 2,
      %% "<hasGini>" => 3,
      %% "<hasHeight>" => 3,
      %% "<hasISBN>" => 2,
      %% "<hasImport>" => 1,
      %% "<hasInflation>" => 3,
      %% "<hasLatitude>" => 1,
      %% "<hasLength>" => 3,
      %% "<hasLongitude>" => 2,
      %% "<hasMotto>" => 1,
      %% "<hasNumberOfPeople>" => 1,
      %% "<hasPages>" => 1,
      %% "<hasPopulationDensity>" => 1,
      %% "<hasPoverty>" => 2,
      %% "<hasRevenue>" => 3,
      %% "<hasTLD>" => 1,
      %% "<hasThreeLetterLanguageCode>" => 2,
      %% "<hasUnemployment>" => 3,
      %% "<hasWeight>" => 1,
      %% "<wasBornOnDate>" => 2,
      %% "<wasCreatedOnDate>" => 3,
      %% "<wasDestroyedOnDate>" => 3,
      %% "rdfs:subClassOf" => 1},
    R05 = {GC, {triple_distributor_finished, TD}},

    F01 = fun () -> receive X -> X end end,

    {timeout, 10000,
     {inorder,
      [
       ?_assertMatch([],      gen_server:call(TD,   {GP, SP})),
       ?_assertMatch(PB,      gen_server:call(TD,   {GP, DA})),

       ?_assertMatch(ok,      gen_server:cast(TD,   M01)),
       ?_assertMatch(R02,     gen_server:call(TD,   {GP, IP})),
       ?_assertMatch([SN],    gen_server:call(TD,   {GP, SP})),
       ?_assertMatch(ok,      gen_server:cast(TD,   M02)),
       ?_assertMatch(R03,     gen_server:call(TD,   {GP, IP})),
       ?_assertMatch([SN],    gen_server:call(TD,   {GP, SP})),
       ?_assertMatch(R05,     F01()),
       ?_assertMatch([],      gen_server:call(TD,   {GP, SP})),
       ?_assertMatch([],      gen_server:call(TD,   {GP, IP})),
       ?_assertMatch(1001,    gen_server:call(FR02, {GP, TC})),
       ?_assertMatch(673,     gen_server:call(FR01, {GP, TC})),
       ?_assertMatch(R01,     gen_server:call(TD,   {GP, PF})),
       ?_assertMatch(R04,     gen_server:call(TD,   {GP, PC}))
       %% ?_assertMatch(ok, timer:sleep(1000))
      ]}}.

tdt_investigate_store_small() ->
    ND   = node(),
    TD   = {triple_distributor, ND},
    GP   = get_property,
    DA   = distribution_algorithm,
    PB   = predicate_based,
    GC   = '$gen_cast',
    IS   = investigate_stream,
    SS   = store_stream,

    %% FR01 = file_reader_01,
    %% FN01 = "ygtsv/yagoGeonamesClasses-h1k.tsv",
    %% FS01 = 673,
    %% FR02 = file_reader_02,
    %% FN02 = "ygtsv/yagoLiteralFacts-h1k-a.tsv",
    %% FS02 = 1001,
    FR01 = file_reader_01,
    FN01 = "ygtsv-full/yagoStatistics.tsv",
    FS01 = 107,
    FR02 = file_reader_02,
    FN02 = "ygtsv-full/yagoSchema.tsv",
    FS02 = 350,
    FR03 = file_reader_03,
    FN03 = "ygtsv-full/yagoGeonamesGlosses.tsv",
    FS03 = 612,
    FR04 = file_reader_04,
    FN04 = "ygtsv-full/yagoGeonamesClassIds.tsv",
    FS04 = 673,
    FR05 = file_reader_05,
    FN05 = "ygtsv-full/yagoGeonamesClasses.tsv",
    FS05 = 673,
    %% FR06 = file_reader_06,
    %% FN06 = "ygtsv-full/yagoSimpleTaxonomy.tsv",
    %% FS06 = 6577,
    %% FR07 = file_reader_07,
    %% FN07 = "ygtsv-full/yagoWordnetIds.tsv",
    %% FS07 = 68863,
    %% FR08 = file_reader_08,
    %% FN08 = "ygtsv-full/yagoWordnetDomains.tsv",
    %% FS08 = 87574,
    %% FR09 = file_reader_09,
    %% FN09 = "ygtsv-full/yagoGeonamesEntityIds.tsv",
    %% FS09 = 106134,
    %% FR10 = file_reader_10,
    %% FN10 = "ygtsv-full/yagoDBpediaClasses.tsv",
    %% FS10 = 450346,
    %% FR11 = file_reader_11,
    %% FN11 = "ygtsv-full/yagoTaxonomy.tsv",
    %% FS11 = 451709,
    %% FR12 = file_reader_12,
    %% FN12 = "ygtsv-full/yagoMultilingualClassLabels.tsv",
    %% FS12 = 787651,
    %% investigate: about 1 minutes

    %% FR13 = file_reader_13,
    %% FN13 = "ygtsv-full/yagoDBpediaInstances.tsv",
    %% FS13 = 1144825,
    %% FR14 = file_reader_14,
    %% FN14 = "ygtsv-full/yagoMetaFacts.tsv",
    %% FS14 = 1347955,
    %% FR15 = file_reader_15,
    %% FN15 = "ygtsv-full/yagoImportantTypes.tsv",
    %% FS15 = 2723629,
    %% FR16 = file_reader_16,
    %% FN16 = "ygtsv-full/yagoLiteralFacts.tsv",
    %% FS16 = 3353660,
    %% %% investigate: about 6 minutes

    %% FR17 = file_reader_17,
    %% FN17 = "ygtsv-full/yagoFacts.tsv",
    %% FS17 = 4484915,
    %% FR18 = file_reader_18,
    %% FN18 = "ygtsv-full/yagoSimpleTypes.tsv",
    %% FS18 = 5437180,
    %% FR19 = file_reader_19,
    %% FN19 = "ygtsv-full/yagoMultilingualInstanceLabels.tsv",
    %% FS19 = 8164318,
    %% %% investigate: about 14 minutes

    %% FR20 = file_reader_20,
    %% FN20 = "ygtsv-full/yagoTypes.tsv",
    %% FS20 = 87574,
    %% FR21 = file_reader_21,
    %% FN21 = "ygtsv-full/yagoLabels.tsv",
    %% FS21 = 87574,
    %% FR22 = file_reader_22,
    %% FN22 = "ygtsv-full/yagoGeonamesData.tsv",
    %% FS22 = 87574,
    %% FR23 = file_reader_23,
    %% FN23 = "ygtsv-full/yagoWikipediaInfo.tsv",
    %% FS23 = 87574,
    %% FR24 = file_reader_24,
    %% FN24 = "ygtsv-full/yagoTransitiveType.tsv",
    %% FS24 = 87574,
    %% FR25 = file_reader_25,
    %% FN25 = "ygtsv-full/yagoSources.tsv",
    %% FS25 = 87574,
    %% %% investigate: about  minutes

    FSL =
      %% [{FR01, FN01}, {FR02, FN02}],
      [{FR01, FN01}, {FR02, FN02}, {FR03, FN03}, {FR04, FN04}, {FR05, FN05}],
      %% {FR06, FN06}, {FR07, FN07}, {FR08, FN08}, {FR09, FN09}, {FR10, FN10},
      %% {FR11, FN11}],
      %% {FR11, FN11}, {FR12, FN12}, {FR13, FN13}, {FR14, FN14}, {FR15, FN15},
      %% {FR16, FN16}, {FR17, FN17}, {FR18, FN18}, {FR19, FN19}, {FR20, FN20},
      %% {FR21, FN21}, {FR22, FN22}, {FR23, FN23}, {FR24, FN24}, {FR25, FN25}],
    FCL =
      %% [{FR01, FS01}, {FR02, FS02}],
      [{FR01, FS01}, {FR02, FS02}, {FR03, FS03}, {FR04, FS04}, {FR05, FS05}],
      %% {FR06, FS06}, {FR07, FS07}, {FR08, FS08}, {FR09, FS09}, {FR10, FS10},
      %% {FR11, FS11}],
      %% {FR11, FS11}, {FR12, FS12}, {FR13, FS13}, {FR14, FS14}, {FR15, FS15},
      %% {FR16, FS16}, {FR17, FS17}, {FR18, FS18}, {FR19, FS19}, {FR20, FS20},
      %% {FR21, FS21}, {FR22, FS22}, {FR23, FS23}, {FR24, FS24}, {FR25, FS25}],

    R01 = {GC, {triple_distributor_finished, TD}},
    F01 = fun () -> receive X -> X end end,
    FIS = fun ({R, N}) -> tim_start(IS, R, N) end,
    FC  = fun ({R, C}) -> tim_confirm(R, C) end,
    FSS = fun ({R, N}) -> tim_start(SS, R, N) end,

    {inorder,
     [
      ?_assertMatch(PB, gen_server:call(TD, {GP, DA})),
      lists:map(FIS, FSL),
      {timeout, 100, [?_assertMatch(R01, F01())]},
      lists:map(FC,  FCL),
      lists:map(FSS, FSL),
      {timeout, 100, [?_assertMatch(R01, F01())]},
      lists:map(FC,  FCL)
     ]}.

tdt_investigate_store_middle() ->
    ND   = node(),
    TD   = {triple_distributor, ND},
    GP   = get_property,
    DA   = distribution_algorithm,
    PB   = predicate_based,
    GC   = '$gen_cast',
    IS   = investigate_stream,
    SS   = store_stream,

    FR01 = file_reader_01,
    FN01 = "ygtsv-full/yagoStatistics.tsv",
    FS01 = 107,
    FR02 = file_reader_02,
    FN02 = "ygtsv-full/yagoSchema.tsv",
    FS02 = 350,
    FR03 = file_reader_03,
    FN03 = "ygtsv-full/yagoGeonamesGlosses.tsv",
    FS03 = 612,
    FR04 = file_reader_04,
    FN04 = "ygtsv-full/yagoGeonamesClassIds.tsv",
    FS04 = 673,
    FR05 = file_reader_05,
    FN05 = "ygtsv-full/yagoGeonamesClasses.tsv",
    FS05 = 673,
    FR06 = file_reader_06,
    FN06 = "ygtsv-full/yagoSimpleTaxonomy.tsv",
    FS06 = 6577,
    FR07 = file_reader_07,
    FN07 = "ygtsv-full/yagoWordnetIds.tsv",
    FS07 = 68863,
    FR08 = file_reader_08,
    FN08 = "ygtsv-full/yagoWordnetDomains.tsv",
    FS08 = 87574,
    FR09 = file_reader_09,
    FN09 = "ygtsv-full/yagoGeonamesEntityIds.tsv",
    FS09 = 106134,
    FR10 = file_reader_10,
    FN10 = "ygtsv-full/yagoDBpediaClasses.tsv",
    FS10 = 450346,
    FR11 = file_reader_11,
    FN11 = "ygtsv-full/yagoTaxonomy.tsv",
    FS11 = 451709,
    FR12 = file_reader_12,
    FN12 = "ygtsv-full/yagoMultilingualClassLabels.tsv",
    FS12 = 787651,
    %% investigate: about 1 minutes

    %% FR13 = file_reader_13,
    %% FN13 = "ygtsv-full/yagoDBpediaInstances.tsv",
    %% FS13 = 1144825,
    %% FR14 = file_reader_14,
    %% FN14 = "ygtsv-full/yagoMetaFacts.tsv",
    %% FS14 = 1347955,
    %% FR15 = file_reader_15,
    %% FN15 = "ygtsv-full/yagoImportantTypes.tsv",
    %% FS15 = 2723629,
    %% FR16 = file_reader_16,
    %% FN16 = "ygtsv-full/yagoLiteralFacts.tsv",
    %% FS16 = 3353660,
    %% %% investigate: about 6 minutes

    %% FR17 = file_reader_17,
    %% FN17 = "ygtsv-full/yagoFacts.tsv",
    %% FS17 = 4484915,
    %% FR18 = file_reader_18,
    %% FN18 = "ygtsv-full/yagoSimpleTypes.tsv",
    %% FS18 = 5437180,
    %% FR19 = file_reader_19,
    %% FN19 = "ygtsv-full/yagoMultilingualInstanceLabels.tsv",
    %% FS19 = 8164318,
    %% %% investigate: about 14 minutes

    %% FR20 = file_reader_20,
    %% FN20 = "ygtsv-full/yagoTypes.tsv",
    %% FS20 = 87574,
    %% FR21 = file_reader_21,
    %% FN21 = "ygtsv-full/yagoLabels.tsv",
    %% FS21 = 87574,
    %% FR22 = file_reader_22,
    %% FN22 = "ygtsv-full/yagoGeonamesData.tsv",
    %% FS22 = 87574,
    %% FR23 = file_reader_23,
    %% FN23 = "ygtsv-full/yagoWikipediaInfo.tsv",
    %% FS23 = 87574,
    %% FR24 = file_reader_24,
    %% FN24 = "ygtsv-full/yagoTransitiveType.tsv",
    %% FS24 = 87574,
    %% FR25 = file_reader_25,
    %% FN25 = "ygtsv-full/yagoSources.tsv",
    %% FS25 = 87574,
    %% %% investigate: about  minutes

    FSL =
	[{FR01, FN01}, {FR02, FN02}, {FR03, FN03}, {FR04, FN04}, {FR05, FN05},
	 {FR06, FN06}, {FR07, FN07}, {FR08, FN08}, {FR09, FN09}, {FR10, FN10},
	 {FR11, FN11}, {FR12, FN12}],
%	 {FR11, FN11}, {FR12, FN12}, {FR13, FN13}, {FR14, FN14}, {FR15, FN15},
%	 {FR16, FN16}, {FR17, FN17}, {FR18, FN18}, {FR19, FN19}, {FR20, FN20},
%	 {FR21, FN21}, {FR22, FN22}, {FR23, FN23}, {FR24, FN24}, {FR25, FN25}],
    FCL =
	[{FR01, FS01}, {FR02, FS02}, {FR03, FS03}, {FR04, FS04}, {FR05, FS05},
	 {FR06, FS06}, {FR07, FS07}, {FR08, FS08}, {FR09, FS09}, {FR10, FS10},
	 {FR11, FS11}, {FR12, FS12}],
%	 {FR11, FS11}, {FR12, FS12}, {FR13, FS13}, {FR14, FS14}, {FR15, FS15},
%	 {FR16, FS16}, {FR17, FS17}, {FR18, FS18}, {FR19, FS19}, {FR20, FS20},
%	 {FR21, FS21}, {FR22, FS22}, {FR23, FS23}, {FR24, FS24}, {FR25, FS25}],

    R01 = {GC, {triple_distributor_finished, TD}},
    F01 = fun () -> receive X -> X end end,
    FIS = fun ({R, N}) -> tim_start(IS, R, N) end,
    FC  = fun ({R, C}) -> tim_confirm(R, C) end,
    FSS = fun ({R, N}) -> tim_start(SS, R, N) end,

    {inorder,
     [
      ?_assertMatch(PB, gen_server:call(TD, {GP, DA})),
      lists:map(FIS, FSL),
      {timeout, 86400, [?_assertMatch(R01, F01())]},
      lists:map(FC,  FCL),
      lists:map(FSS, FSL),
      {timeout, 864000, [?_assertMatch(R01, F01())]},
      lists:map(FC,  FCL)
     ]}.

tdt_investigate_store_yjr6yg2s() ->
    ND   = node(),
    TD   = {triple_distributor, ND},
    GP   = get_property,
    DA   = distribution_algorithm,
    PB   = predicate_based,
    GC   = '$gen_cast',
    IS   = investigate_stream,
    SS   = store_stream,

    FR01 = file_reader_01,
    FN01 = "ygtsv-full/yagoStatistics.tsv",
    FS01 = 107,
    FR02 = file_reader_02,
    FN02 = "ygtsv-full/yagoSchema.tsv",
    FS02 = 350,
    FR03 = file_reader_03,
    FN03 = "ygtsv-full/yagoGeonamesGlosses.tsv",
    FS03 = 612,
    FR04 = file_reader_04,
    FN04 = "ygtsv-full/yagoGeonamesClassIds.tsv",
    FS04 = 673,
    FR05 = file_reader_05,
    FN05 = "ygtsv-full/yagoGeonamesClasses.tsv",
    FS05 = 673,
    FR06 = file_reader_06,
    FN06 = "ygtsv-full/yagoSimpleTaxonomy.tsv",
    FS06 = 6577,
    FR07 = file_reader_07,
    FN07 = "ygtsv-full/yagoWordnetIds.tsv",
    FS07 = 68863,
    FR08 = file_reader_08,
    FN08 = "ygtsv-full/yagoWordnetDomains.tsv",
    FS08 = 87574,
    FR09 = file_reader_09,
    FN09 = "ygtsv-full/yagoGeonamesEntityIds.tsv",
    FS09 = 106134,
    FR10 = file_reader_10,
    FN10 = "ygtsv-full/yagoDBpediaClasses.tsv",
    FS10 = 450346,
    FR11 = file_reader_11,
    FN11 = "ygtsv-full/yagoTaxonomy.tsv",
    FS11 = 451709,
    FR12 = file_reader_12,
    FN12 = "ygtsv-full/yagoMultilingualClassLabels.tsv",
    FS12 = 787651,
    %% investigate: about 1 minutes

    FR13 = file_reader_13,
    FN13 = "ygtsv-full/yagoDBpediaInstances.tsv",
    FS13 = 1144825,
    FR14 = file_reader_14,
    FN14 = "ygtsv-full/yagoMetaFacts.tsv",
    FS14 = 1347955,
    FR15 = file_reader_15,
    FN15 = "ygtsv-full/yagoImportantTypes.tsv",
    FS15 = 2723629,
    FR16 = file_reader_16,
    FN16 = "ygtsv-full/yagoLiteralFacts.tsv",
    FS16 = 3353660,
    %% investigate: about 6 minutes

    FR17 = file_reader_17,
    FN17 = "ygtsv-full/yagoFacts.tsv",
    FS17 = 4484915,
    FR18 = file_reader_18,
    FN18 = "ygtsv-full/yagoSimpleTypes.tsv",
    FS18 = 5437180,
    FR19 = file_reader_19,
    FN19 = "ygtsv-full/yagoMultilingualInstanceLabels.tsv",
    FS19 = 8164318,
    %% investigate: about 14 minutes

    FR20 = file_reader_20,
    FN20 = "ygtsv-full/yagoTypes.tsv",
    FS20 = 9019770,
    FR21 = file_reader_21,
    FN21 = "ygtsv-full/yagoLabels.tsv",
    FS21 = 15372314,
    FR22 = file_reader_22,
    FN22 = "ygtsv-full/yagoGeonamesData.tsv",
    FS22 = 32216601,
    FR23 = file_reader_23,
    FN23 = "ygtsv-full/yagoWikipediaInfo.tsv",
    FS23 = 43822207,
    FR24 = file_reader_24,
    FN24 = "ygtsv-full/yagoTransitiveType.tsv",
    FS24 = 43984617,
    FR25 = file_reader_25,
    FN25 = "ygtsv-full/yagoSources.tsv",
    FS25 = 71766807,
    %% investigate: about 2 hours

    FSL =
	[{FR01, FN01}, {FR02, FN02}, {FR03, FN03}, {FR04, FN04}, {FR05, FN05},
	 {FR06, FN06}, {FR07, FN07}, {FR08, FN08}, {FR09, FN09}, {FR10, FN10},
	 {FR11, FN11}, {FR12, FN12}, {FR13, FN13}, {FR14, FN14}, {FR15, FN15},
	 {FR16, FN16}, {FR17, FN17}, {FR18, FN18}, {FR19, FN19}, {FR20, FN20},
	 {FR21, FN21}, {FR22, FN22}, {FR23, FN23}, {FR24, FN24}, {FR25, FN25}],
    FCL =
	[{FR01, FS01}, {FR02, FS02}, {FR03, FS03}, {FR04, FS04}, {FR05, FS05},
	 {FR06, FS06}, {FR07, FS07}, {FR08, FS08}, {FR09, FS09}, {FR10, FS10},
	 {FR11, FS11}, {FR12, FS12}, {FR13, FS13}, {FR14, FS14}, {FR15, FS15},
	 {FR16, FS16}, {FR17, FS17}, {FR18, FS18}, {FR19, FS19}, {FR20, FS20},
	 {FR21, FS21}, {FR22, FS22}, {FR23, FS23}, {FR24, FS24}, {FR25, FS25}],

    R01 = {GC, {triple_distributor_finished, TD}},
    F01 = fun () -> receive X -> X end end,
    FIS = fun ({R, N}) -> tim_start(IS, R, N) end,
    FC  = fun ({R, C}) -> tim_confirm(R, C) end,
    FSS = fun ({R, N}) -> tim_start(SS, R, N) end,

    {inorder,
     [
      ?_assertMatch(PB, gen_server:call(TD, {GP, DA})),
      lists:map(FIS, FSL),
      {timeout, 86400, [?_assertMatch(R01, F01())]},
      lists:map(FC,  FCL),
      lists:map(FSS, FSL),
      {timeout, 864000, [?_assertMatch(R01, F01())]},
      lists:map(FC,  FCL)
     ]}.

tim_start(Mode, Id, File) ->
    TUT  = td_unit_test,
    ND   = node(),
    TD   = {triple_distributor, ND},
    SN   = {TUT, node()},

    M    = {start, SN, Mode, File, Id, ND},
    ?_assertMatch(ok, gen_server:cast(TD, M)).

tim_confirm(Id, Count) ->
    GP   = get_property,
    TC   = triple_count,

    ?_assertMatch(Count, gen_server:call(Id, {GP, TC})).

tdt_store() ->
    TUT  = td_unit_test,
    ND   = node(),
    TD   = {triple_distributor, ND},
    DW   = {db_writer, ND},
    SN   = {TUT, node()},
    SS   = store_stream,
    GP   = get_property,
    IP   = investigate_processes,
    TP   = store_processes,
    SP   = sender_processes,
    DA   = distribution_algorithm,
    PB   = predicate_based,
    GC   = '$gen_cast',
    DI   = db_init,
    DC   = db_close,

    FR01 = file_reader_01,
    FN01 = "ygtsv/yagoGeonamesClasses-h1k.tsv",
    FR02 = file_reader_02,
    FN02 = "ygtsv/yagoLiteralFacts-h1k-a.tsv",
    M03  = {start, SN, SS, FN01, FR01, ND},
    M04  = {start, SN, SS, FN02, FR02, ND},

    R03 = [{FR01, ND}, {FR02, ND}],
    R05 = {GC, {triple_distributor_finished, TD}},

    F01 = fun () -> receive X -> X end end,

    {timeout, 100000,
     {inorder,
      [
       ?_assertMatch(ok,      gen_server:call(DW,   DI)),
       ?_assertMatch(ok,      gen_server:call(DW,   DC)),

       ?_assertMatch([],      gen_server:call(TD,   {GP, SP})),
       ?_assertMatch(PB,      gen_server:call(TD,   {GP, DA})),

       ?_assertMatch(ok,      gen_server:cast(TD,   M03)),
       ?_assertMatch(ok,      gen_server:cast(TD,   M04)),
       ?_assertMatch(R03,     gen_server:call(TD,   {GP, TP})),
       ?_assertMatch([SN],    gen_server:call(TD,   {GP, SP})),
       {timeout, 100000,
	[?_assertMatch(R05,     F01())]},
       ?_assertMatch([],      gen_server:call(TD,   {GP, TP})),
       ?_assertMatch([],      gen_server:call(TD,   {GP, IP}))
      ]}}.

tdt_store_pbl() ->
    tdt_store_pbl(predicate_based).

tdt_store_pbl_pd() ->
    tdt_store_pbl(predicate_based).

tdt_store_pbl_rnd() ->
    tdt_store_pbl(random).

tdt_store_pbl(DstAlg) ->
    TUT  = td_unit_test,
    ND   = node(),
    TD   = {triple_distributor, ND},
    SN   = {TUT, node()},
    SS   = store_stream,
    PP   = put_property,
    WM   = write_mode,
    EM   = encode_mode,
    DA   = distribution_algorithm,

    FR01 = file_reader_01,
    FN01 = "ygtsv/yagoGeonamesClasses-h1k.tsv",
    M01  = {start, SN, SS, FN01, FR01, ND},
    FR02 = file_reader_02,
    FN02 = "ygtsv/yagoLiteralFacts-h1k-a.tsv",
    M02  = {start, SN, SS, FN02, FR02, ND},
    %% FR03 = file_reader_03,
    %% FN03 = "ygtsv/yagoGeonamesClasses-h1k.tsv",
    %% M03  = {start, SN, SS, FN03, FR03, ND},

    {timeout, 10000,
     {inorder,
      [
       ?_assertMatch(ok,  gen_server:call(TD, {PP, WM, postgres_bulk_load})),
       ?_assertMatch(ok,  gen_server:call(TD, {PP, EM, false})),
       ?_assertMatch(ok,  gen_server:call(TD, {PP, DA, DstAlg})),
       ?_assertMatch(ok,  gen_server:cast(TD, M01)),
       ?_assertMatch(ok,  gen_server:cast(TD, M02))
       %% ?_assertMatch(ok,  gen_server:cast(TD, M03))
      ]}}.

% rpc:call('b3ss02@MBA-11Z-460.local', mnesia, info, []).
% R01 = rpc:call('b3ss02@MBA-11Z-460.local', db_interfac,e db_open_tp, [{"?id", "<Chinese>", "<eat>", "<vegetables>"}]).
% {ok, C01} = R01.
% R02 = rpc:call('b3ss02@MBA-11Z-460.local', db_interface, db_next, [C01]).

%% rpc:call('b3ss02@MBA-11Z-460.local', gen_server, call, [query_node_01, {get_property, wait}]).
%% rpc:call('b3ss02@MBA-11Z-460.local', supervisor, terminate_child, [b3s, query_node_01]).
%% rpc:call('b3ss02@MBA-11Z-460.local', supervisor, delete_child, [b3s, query_node_01]).

tdt_check_data_yjr6() ->
    NW     = 'b3ss02@wild.rlab.miniy.yahoo.co.jp',
    NC     = 'b3ss02@circus.rlab.miniy.yahoo.co.jp',
    NI     = 'b3ss02@innocents.rlab.miniy.yahoo.co.jp',
    NE     = 'b3ss02@erasure.rlab.miniy.yahoo.co.jp',
    NL     = 'b3ss02@loveboat.rlab.miniy.yahoo.co.jp',
    NN     = 'b3ss02@nightbird.rlab.miniy.yahoo.co.jp',

    MN     = mnesia,
    TI     = table_info,
    SZ     = size,
    EOS    = end_of_stream,

    NTT = name_of_triple_tables,
    AGE = fun(X) -> application:get_env(b3s, X) end,
    {ok, TabLst} = AGE(NTT),
    TabW = element(2, lists:keyfind(NW, 1, TabLst)),
    TabC = element(2, lists:keyfind(NC, 1, TabLst)),
    TabI = element(2, lists:keyfind(NI, 1, TabLst)),
    TabE = element(2, lists:keyfind(NE, 1, TabLst)),
    TabL = element(2, lists:keyfind(NL, 1, TabLst)),
    TabN = element(2, lists:keyfind(NN, 1, TabLst)),

    {Dt, _}   = calendar:local_time(),
    DtArray   = [element(1, Dt), element(2, Dt), element(3, Dt)],
    DtString  = io_lib:format("~4..0B~2..0B~2..0B", DtArray),
    {ok, TIS} = application:get_env(b3s, triple_id_skel),
    TISkel    = lists:flatten(TIS ++ "_" ++ DtString ++ "_"),

    I01  = "<id_1es4j1i_1m6_zq43us>",
    S01  = "<geoclass_jetty>",
    P01  = "rdfs:subClassOf",
    O01  = "<yagoGeoEntity>",
    T01  = {triple_store, I01, S01, P01, O01},

    I02  = "<id_1f8ovls_1m6_zq43us>",
    S02  = "<geoclass_former_inlet>",
    T02  = {triple_store, I02, S02, P01, O01},

    I03  = "<id_1ywnkcj_1m6_zq43us>",
    S03  = "<geoclass_language_school>",
    T03  = {triple_store, I03, S03, P01, O01},

    I04  = "<id_10p142u_f9j_zil1pw>",
    S04  = "<Japan>",
    P04  = "<hasTLD>",
    O04  = "\".jp\"",
    T04  = {triple_store, I04, S04, P04, O04},

    I05  = "<id_inji5d_f9j_zil1xg>",
    S05  = "<Slovenia>",
    O05  = "\".si\"",
    T05  = {triple_store, I05, S05, P04, O05},

    I06  = "<id_g0bna8_1b1_1d2wwjh>",
    S06  = "<Grandpa_Never_Lies>",
    P06  = "<hasISBN>",
    O06  = "\"978-0395797709\"",
    T06  = {triple_store, I06, S06, P06, O06},

    I07  = "<id_16uh7ob_1b1_f887mo>",
    S07  = "<The_Cat_Who_Came_for_Christmas>",
    O07  = "\"0316037370\"",
    T07  = {triple_store, I07, S07, P06, O07},

    I08  = "<id_116rcaz_1b1_istdf9>",
    S08  = "<Been_Down_So_Long_It_Looks_Like_Up_to_Me>",
    O08  = "\"0-140-18930-0\"",
    T08  = {triple_store, I08, S08, P06, O08},

    I09  = "<" ++ TISkel ++ "000000000002>",
    S09  = "<Huntington_Traction_Company>",
    P09  = "<wasCreatedOnDate>",
    O09  = "\"1920-##-##\"",
    T09  = {triple_store, I09, S09, P09, O09},

    I10  = "<id_n1108r_siu_972940>",
    S10  = "<The_Gamers_(film)>",
    O10  = "\"2002-##-##\"",
    T10  = {triple_store, I10, S10, P09, O10},

    I11  = "<id_1ly8kp7_siu_1mq02we>",
    S11  = "<Michael_F._Price_College_of_Business>",
    O11  = "\"1917-##-##\"",
    T11  = {triple_store, I11, S11, P09, O11},

    I12  = "<id_12c36e6_siu_1hg6j9m>",
    S12  = "<Mind_How_You_Go_(The_Advisory_Circle_album)>",
    O12  = "\"2005-10-17\"",
    T12  = {triple_store, I12, S12, P09, O12},

    I13  = "<id_w0fpv2_siu_1evdqn2>",
    S13  = "<The_Sea_Devil's_Eye_(novel)>",
    O13  = "\"2000-##-##\"",
    T13  = {triple_store, I13, S13, P09, O13},

    I14  = "<id_enrhqx_siu_1hfqklf>",
    S14  = "<In_Love_with_You>",
    O14  = "\"2005-06-17\"",
    T14  = {triple_store, I14, S14, P09, O14},

    I15  = "<id_1xz7qj7_siu_1kwg63q>",
    S15  = "<Karolinska_Institutet>",
    O15  = "\"1810-##-##\"",
    T15  = {triple_store, I15, S15, P09, O15},

    I16  = "<id_10p142u_1b1_1qpe69z>",
    P16  = "<hasArea>",
    O16  = "\"3.77944E11\"",
    T16  = {triple_store, I16, S04, P16, O16},

    I17  = "<id_10p142u_1f1_kxscgn>",
    P17  = "<hasExpenses>",
    O17  = "\"2160000000000\"",
    T17  = {triple_store, I17, S04, P17, O17},

    I18  = "<id_10p142u_q65_zzw1r9>",
    P18  = "<hasImport>",
    O18  = "\"794700000000\"",
    T18  = {triple_store, I18, S04, P18, O18},

    I19  = "<id_10p142u_jto_1c8t5js>",
    P19  = "<hasLongitude>",
    O19  = "\"139.76666666666668\"",
    T19  = {triple_store, I19, S04, P19, O19},

    I20  = "<id_10p142u_siu_1owv735>",
    O20  = "\"-660-02-11\"",
    T20  = {triple_store, I20, S04, P09, O20},

    I21  = "<id_10p142u_l7i_1stqkfj>",
    P21  = "<hasNumberOfPeople>",
    O21  = "\"127799000\"",
    T21  = {triple_store, I21, S04, P21, O21},

    I22  = "<id_10p142u_og0_4kwhz4>",
    P22  = "<hasExport>",
    O22  = "\"800800000000\"",
    T22  = {triple_store, I22, S04, P22, O22},

    I23  = "<id_10p142u_gaa_sl7dba>",
    P23  = "<hasRevenue>",
    O23  = "\"1638000000000\"",
    T23  = {triple_store, I23, S04, P23, O23},

    I24  = "<id_10p142u_ibw_10bj9i2>",
    P24  = "<hasPopulationDensity>",
    O24  = "\"337.1\"",
    T24  = {triple_store, I24, S04, P24, O24},

    I25  = "<id_10p142u_f9j_1r3tlqi>",
    P25  = "<hasGDP>",
    O25  = "\"5869000000000\"",
    T25  = {triple_store, I25, S04, P25, O25},

    I26  = "<id_10p142u_1b1_zjhrzc>",
    P26  = "<hasGini>",
    O26  = "\"38.1\"",
    T26  = {triple_store, I26, S04, P26, O26},

    I27  = "<id_10p142u_1n8_fh6j2m>",
    P27  = "<hasLatitude>",
    O27  = "\"35.68333333333333\"",
    T27  = {triple_store, I27, S04, P27, O27},

    I28  = "<id_10p142u_1qp_zjgfsd>",
    P28  = "<hasPoverty>",
    O28  = "\"15.7\"",
    T28  = {triple_store, I28, S04, P28, O28},

    I29  = "<id_10p142u_1b1_zjhr8s>",
    P29  = "<hasGini>",
    O29  = "\"37.6\"",
    T29  = {triple_store, I29, S04, P29, O29},

    I30  = "<id_10p142u_siu_3c47xl>",
    O30  = "\"1947-05-03\"",
    T30  = {triple_store, I30, S04, P09, O30},

    I31  = "<id_10p142u_siu_3kxt6t>",
    O31  = "\"1890-11-29\"",
    T31  = {triple_store, I31, S04, P09, O31},

    I32  = "<id_10p142u_siu_1t3lkq6>",
    O32  = "\"660-02-11\"",
    T32  = {triple_store, I32, S04, P09, O32},

    I33  = "<id_10p142u_siu_tsuox3>",
    O33  = "\"1952-04-28\"",
    T33  = {triple_store, I33, S04, P09, O33},

    I34  = "<id_10p142u_lga_zil4ot>",
    P34  = "<hasUnemployment>",
    O34  = "\"4.7\"",
    T34  = {triple_store, I34, S04, P34, O34},

    I35  = "<id_10p142u_10j_zil1px>",
    P35  = "<hasInflation>",
    O35  = "\"0.3\"",
    T35  = {triple_store, I35, S04, P35, O35},

    I36  = "<id_1iignow_siu_1orwc95>",
    S36  = "<Antigua_Workers'_Union>",
    O36  = "\"1967-##-##\"",
    T36  = {triple_store, I36, S36, P09, O36},

    I37  = "<id_bvfmxj_siu_2toach>", 
    S37  = "<Never_Say_Never_(Alias_album)>",
    O37  = "\"1992-##-##\"",
    T37  = {triple_store, I37, S37, P09, O37},

    I38  = "<id_x6ig6p_siu_1trqqcd>", 
    S38  = "<How_High>",
    O38  = "\"2001-12-21\"",
    T38  = {triple_store, I38, S38, P09, O38},

    I39  = "<" ++ TISkel ++ "000000000003>", 
    S39  = "<Northwood_University-West_Baden>",
    O39  = "\"1966-##-##\"",
    T39  = {triple_store, I39, S39, P09, O39},

    TP01 = {"?id", "?sbj", "<eat>", "?obj"},
    QN01 = query_node_01,
    VP01 = #{"?id" => 1, "?sbj" => 2, "?obj" => 4},
    RL01 = lists:duplicate(6, {1, [EOS]}),
    QF01 = fun() -> tcdy_test_query(TP01, QN01, VP01, RL01) end,

    TP02 = {"?id", "?sbj", P01, "?obj"},
    QN02 = query_node_02,
    VP02 = #{"?id" => 1, "?sbj" => 2, "?obj" => 4},
    RR02 = {2, [maps:put(QN02, T02, #{}),
		maps:put(QN02, T03, #{})]},
    RL02 = [RR02, RR02, {1, [EOS]}, {1, [EOS]}, {1, [EOS]}, {1, [EOS]}],
    QF02 = fun() -> tcdy_test_query(TP02, QN02, VP02, RL02) end,

    TP03 = {"?id", S01, "?prd", "?obj"},
    QN03 = query_node_03,
    VP03 = #{"?id" => 1, "?prd" => 3, "?obj" => 4},
    RR03 = {2, [maps:put(QN03, T01, #{}), EOS]},
    RL03 = [RR03, RR03, {1, [EOS]}, {1, [EOS]}, {1, [EOS]}, {1, [EOS]}],
    QF03 = fun() -> tcdy_test_query(TP03, QN03, VP03, RL03) end,

    TP04 = {"?id", "?sbj", P04, "?obj"},
    QN04 = query_node_04,
    VP04 = #{"?id" => 1, "?sbj" => 2, "?obj" => 4},
    RR04 = {3, [maps:put(QN04, T04, #{}),
		maps:put(QN04, T05, #{}), EOS]},
    RL04 = [RR04, RR04, {1, [EOS]}, {1, [EOS]}, {1, [EOS]}, {1, [EOS]}],
    QF04 = fun() -> tcdy_test_query(TP04, QN04, VP04, RL04) end,

    TP05 = {"?id", "?sbj", P06, "?obj"},
    QN05 = query_node_05,
    VP05 = #{"?id" => 1, "?sbj" => 2, "?obj" => 4},
    RR05 = {3, [maps:put(QN05, T06, #{}),
		maps:put(QN05, T07, #{}),
		maps:put(QN05, T08, #{})]},
    RL05 = [{1, [EOS]}, {1, [EOS]}, RR05, RR05, {1, [EOS]}, {1, [EOS]}],
    QF05 = fun() -> tcdy_test_query(TP05, QN05, VP05, RL05) end,

    TP06 = {"?id", "?sbj", P09, "?obj"},
    QN06 = query_node_06,
    VP06 = #{"?id" => 1, "?sbj" => 2, "?obj" => 4},
    RR06 = {5, [maps:put(QN06, T09, #{}),
		maps:put(QN06, T10, #{}),
		maps:put(QN06, T11, #{}),
		maps:put(QN06, T12, #{}),
		maps:put(QN06, T13, #{}),
		maps:put(QN06, T14, #{}),
		maps:put(QN06, T15, #{}),
		maps:put(QN06, T36, #{}),
		maps:put(QN06, T37, #{}),
		maps:put(QN06, T38, #{}),
		maps:put(QN06, T39, #{})]},
    RL06 = [{1, [EOS]}, {1, [EOS]}, {1, [EOS]}, {1, [EOS]}, RR06, RR06],
    QF06 = fun() -> tcdy_test_query(TP06, QN06, VP06, RL06) end,

    TP07 = {"?id", S04, "?prd", "?obj"},
    QN07 = query_node_07,
    VP07 = #{"?id" => 1, "?prd" => 3, "?obj" => 4},
    R07A = {6, [maps:put(QN07, T04, #{}),
		maps:put(QN07, T18, #{}),
		maps:put(QN07, T21, #{}),
		maps:put(QN07, T24, #{}),
		maps:put(QN07, T27, #{}),
		EOS]},
    R07B = {6, [maps:put(QN07, T16, #{}),
		maps:put(QN07, T19, #{}),
		maps:put(QN07, T22, #{}),
		maps:put(QN07, T25, #{}),
		maps:put(QN07, T28, #{}),
		EOS]},
    R07C = {12, [maps:put(QN07, T17, #{}),
		 maps:put(QN07, T20, #{}),
		 maps:put(QN07, T23, #{}),
		 maps:put(QN07, T26, #{}),
		 maps:put(QN07, T29, #{}),
		 maps:put(QN07, T30, #{}),
		 maps:put(QN07, T31, #{}),
		 maps:put(QN07, T32, #{}),
		 maps:put(QN07, T33, #{}),
		 maps:put(QN07, T34, #{}),
		 maps:put(QN07, T35, #{}),
		 EOS]},
    RL07 = [R07A, R07A, R07B, R07B, R07C, R07C],
    QF07 = fun() -> tcdy_test_query(TP07, QN07, VP07, RL07) end,

    {inorder,
     [
      ?_assertMatch(874,      rpc:call(NW, MN, TI, [TabW, SZ])),
      ?_assertMatch(874,      rpc:call(NC, MN, TI, [TabC, SZ])),
      ?_assertMatch(406,      rpc:call(NI, MN, TI, [TabI, SZ])),
      ?_assertMatch(406,      rpc:call(NE, MN, TI, [TabE, SZ])),
      ?_assertMatch(362,      rpc:call(NL, MN, TI, [TabL, SZ])),
      ?_assertMatch(362,      rpc:call(NN, MN, TI, [TabN, SZ])),
      {generator, QF01},
      {generator, QF02},
      {generator, QF03},
      {generator, QF04},
      {generator, QF05},
      {generator, QF06},
      {generator, QF07}
     ]}.

tdt_check_data_yjr6_small() ->
    NW     = 'b3ss02@wild.rlab.miniy.yahoo.co.jp',
    NC     = 'b3ss02@circus.rlab.miniy.yahoo.co.jp',
    NI     = 'b3ss02@innocents.rlab.miniy.yahoo.co.jp',
    NE     = 'b3ss02@erasure.rlab.miniy.yahoo.co.jp',
    NL     = 'b3ss02@loveboat.rlab.miniy.yahoo.co.jp',
    NN     = 'b3ss02@nightbird.rlab.miniy.yahoo.co.jp',

    MN     = mnesia,
    TI     = table_info,
    SZ     = size,
    EOS    = end_of_stream,

    NTT = name_of_triple_tables,
    AGE = fun(X) -> application:get_env(b3s, X) end,
    {ok, TabLst} = AGE(NTT),
    TabW = element(2, lists:keyfind(NW, 1, TabLst)),
    TabC = element(2, lists:keyfind(NC, 1, TabLst)),
    TabI = element(2, lists:keyfind(NI, 1, TabLst)),
    TabE = element(2, lists:keyfind(NE, 1, TabLst)),
    TabL = element(2, lists:keyfind(NL, 1, TabLst)),
    TabN = element(2, lists:keyfind(NN, 1, TabLst)),

    {Dt, _}   = calendar:local_time(),
    DtArray   = [element(1, Dt), element(2, Dt), element(3, Dt)],
    DtString  = io_lib:format("~4..0B~2..0B~2..0B", DtArray),
    {ok, TIS} = application:get_env(b3s, triple_id_skel),
    TISkel    = lists:flatten(TIS ++ "_" ++ DtString ++ "_"),

    I01  = "<" ++ TISkel ++ "000000000412>",
    S01  = "<YAGO_(database)>",
    P01  = "<wasCreatedOnDate>",
    O01  = "\"2012-11-15\"",
    T01  = {triple_store, I01, S01, P01, O01},

    I02  = "<" ++ TISkel ++ "000000000912>",
    S02  = "<geoclass_cairn>",
    P02  = "<hasGeonamesClassId>",
    O02  = "\"S.CARN\"",
    T02  = {triple_store, I02, S02, P02, O02},

    I03  = "<" ++ TISkel ++ "000000000913>",
    T03  = {triple_store, I03, S02, P02, O02},

    I04  = "<" ++ TISkel ++ "000000000411>",
    T04  = {triple_store, I04, S01, P01, O01},

    I05  = "<" ++ TISkel ++ "000000000410>",
    T05  = {triple_store, I05, S01, P01, O01},

    I06  = "<" ++ TISkel ++ "000000000911>",
    T06  = {triple_store, I06, S02, P02, O02},

    TP01 = {"?id", "?sbj", P01, "?obj"},
    QN01 = query_node_01,
    VP01 = #{"?id" => 1, "?sbj" => 2, "?obj" => 4},
    R01A = {1, [EOS]},
    R01B = {1, [EOS]},
    R01C = {1, [maps:put(QN01, T01, #{}),
		maps:put(QN01, T04, #{}),
		maps:put(QN01, T05, #{})]},
    RL01 = [R01A, R01A, R01B, R01B, R01C, R01C],
    QF01 = fun() -> tcdy_test_query(TP01, QN01, VP01, RL01) end,

    TP02 = {"?id", "?sbj", "?prd", O02},
    QN02 = query_node_02,
    VP02 = #{"?id" => 1, "?sbj" => 2, "?prd" => 3},
    R02A = {1, [EOS]},
    R02B = {1, [maps:put(QN02, T02, #{}),
		maps:put(QN02, T03, #{}),
		maps:put(QN02, T06, #{})]},
    R02C = {1, [EOS]},
    RL02 = [R02A, R02A, R02B, R02B, R02C, R02C],
    QF02 = fun() -> tcdy_test_query(TP02, QN02, VP02, RL02) end,

    {inorder,
     [
      ?_assertMatch(848, rpc:call(NW, MN, TI, [TabW, SZ])),
      ?_assertMatch(848, rpc:call(NC, MN, TI, [TabC, SZ])),
      ?_assertMatch(816, rpc:call(NI, MN, TI, [TabI, SZ])),
      ?_assertMatch(816, rpc:call(NE, MN, TI, [TabE, SZ])),
      ?_assertMatch(716, rpc:call(NL, MN, TI, [TabL, SZ])),
      ?_assertMatch(716, rpc:call(NN, MN, TI, [TabN, SZ])),
      {generator, QF01},
      {generator, QF02}
     ]}.

tdt_check_data_yjr2_small() ->
    NW     = 'b3ss02@wild.rlab.miniy.yahoo.co.jp',
    %% NC     = 'b3ss02@circus.rlab.miniy.yahoo.co.jp',
    %% NI     = 'b3ss02@innocents.rlab.miniy.yahoo.co.jp',
    %% NE     = 'b3ss02@erasure.rlab.miniy.yahoo.co.jp',
    NL     = 'b3ss02@loveboat.rlab.miniy.yahoo.co.jp',
    %% NN     = 'b3ss02@nightbird.rlab.miniy.yahoo.co.jp',

    MN     = mnesia,
    TI     = table_info,
    SZ     = size,
    DI     = db_interface,
    DC     = db_close,

    NTT = name_of_triple_tables,
    AGE = fun(X) -> application:get_env(b3s, X) end,
    {ok, TabLst} = AGE(NTT),
    TabW = element(2, lists:keyfind(NW, 1, TabLst)),
    TabL = element(2, lists:keyfind(NL, 1, TabLst)),

    Fbk = fun (X) -> rpc:call(X, DI, DC, []) end,
    NodLst = [NW, NL],
    R01    = [ok, ok],

    {inorder,
     [
      ?_assertMatch(2380, rpc:call(NW, MN, TI, [TabW, SZ])),
      ?_assertMatch(2380, rpc:call(NL, MN, TI, [TabL, SZ])),
      ?_assertMatch(R01,  lists:map(Fbk, NodLst))
     ]}.

tdt_check_data_yjr6_middle() ->
    NW     = 'b3ss02@wild.rlab.miniy.yahoo.co.jp',
    NC     = 'b3ss02@circus.rlab.miniy.yahoo.co.jp',
    NI     = 'b3ss02@innocents.rlab.miniy.yahoo.co.jp',
    NE     = 'b3ss02@erasure.rlab.miniy.yahoo.co.jp',
    NL     = 'b3ss02@loveboat.rlab.miniy.yahoo.co.jp',
    NN     = 'b3ss02@nightbird.rlab.miniy.yahoo.co.jp',

    MN     = mnesia,
    TI     = table_info,
    SZ     = size,

    NTT = name_of_triple_tables,
    AGE = fun(X) -> application:get_env(b3s, X) end,
    {ok, TabLst} = AGE(NTT),
    TabW = element(2, lists:keyfind(NW, 1, TabLst)),
    TabC = element(2, lists:keyfind(NC, 1, TabLst)),
    TabI = element(2, lists:keyfind(NI, 1, TabLst)),
    TabE = element(2, lists:keyfind(NE, 1, TabLst)),
    TabL = element(2, lists:keyfind(NL, 1, TabLst)),
    TabN = element(2, lists:keyfind(NN, 1, TabLst)),

    {inorder,
     [
      %% reading file: FS01-FS12
      ?_assertMatch(894828, rpc:call(NW, MN, TI, [TabW, SZ])),
      ?_assertMatch(894828, rpc:call(NC, MN, TI, [TabC, SZ])),
      ?_assertMatch(546766, rpc:call(NI, MN, TI, [TabI, SZ])),
      ?_assertMatch(546766, rpc:call(NE, MN, TI, [TabE, SZ])),
      ?_assertMatch(519497, rpc:call(NL, MN, TI, [TabL, SZ])),
      ?_assertMatch(519497, rpc:call(NN, MN, TI, [TabN, SZ]))
     ]}.

tdt_check_data_yjr6_yjr6yg2s() ->
    NW     = 'b3ss02@wild.rlab.miniy.yahoo.co.jp',
    NC     = 'b3ss02@circus.rlab.miniy.yahoo.co.jp',
    NI     = 'b3ss02@innocents.rlab.miniy.yahoo.co.jp',
    NE     = 'b3ss02@erasure.rlab.miniy.yahoo.co.jp',
    NL     = 'b3ss02@loveboat.rlab.miniy.yahoo.co.jp',
    NN     = 'b3ss02@nightbird.rlab.miniy.yahoo.co.jp',

    MN     = mnesia,
    TI     = table_info,
    SZ     = size,
    DI     = db_interface,
    DC     = db_close,

    NTT = name_of_triple_tables,
    AGE = fun(X) -> application:get_env(b3s, X) end,
    {ok, TabLst} = AGE(NTT),
    TabW = element(2, lists:keyfind(NW, 1, TabLst)),
    TabC = element(2, lists:keyfind(NC, 1, TabLst)),
    TabI = element(2, lists:keyfind(NI, 1, TabLst)),
    TabE = element(2, lists:keyfind(NE, 1, TabLst)),
    TabL = element(2, lists:keyfind(NL, 1, TabLst)),
    TabN = element(2, lists:keyfind(NN, 1, TabLst)),

    Fbk = fun (X) -> rpc:cast(X, DI, DC, []) end,
    NodLst = [NW, NI, NL],			% for mnesia_qlc
    R01    = [true, true, true],		% for mnesia_qlc

    {inorder,
     [
      %% reading file: all yago2s files
      ?_assertMatch(110678962, rpc:call(NW, MN, TI, [TabW, SZ])),
      ?_assertMatch(110678962, rpc:call(NC, MN, TI, [TabC, SZ])),
      ?_assertMatch( 72985783, rpc:call(NI, MN, TI, [TabI, SZ])),
      ?_assertMatch( 72985783, rpc:call(NE, MN, TI, [TabE, SZ])),
      ?_assertMatch( 57595842, rpc:call(NL, MN, TI, [TabL, SZ])),
      ?_assertMatch( 57595842, rpc:call(NN, MN, TI, [TabN, SZ])),
      ?_assertMatch(R01,       lists:map(Fbk, NodLst))
     ]}.

tcdy_test_query(TP, QI, VP, RL) ->
    NW = {{1, 1}, lists:nth(1, RL), 'b3ss02@wild.rlab.miniy.yahoo.co.jp'},
    NC = {{1, 2}, lists:nth(2, RL), 'b3ss02@circus.rlab.miniy.yahoo.co.jp'},
    NI = {{2, 1}, lists:nth(3, RL), 'b3ss02@innocents.rlab.miniy.yahoo.co.jp'},
    NE = {{2, 2}, lists:nth(4, RL), 'b3ss02@erasure.rlab.miniy.yahoo.co.jp'},
    NL = {{3, 1}, lists:nth(5, RL), 'b3ss02@loveboat.rlab.miniy.yahoo.co.jp'},
    NN = {{3, 2}, lists:nth(6, RL), 'b3ss02@nightbird.rlab.miniy.yahoo.co.jp'},

    F = fun ({X, Y, Z}) ->
		FQ = fun () ->
			     tcdy_test_query_server(Z, TP, QI, X, VP, Y)
		     end,
		{generator, FQ}
	end,

    lists:map(F, [NW, NC, NI, NE, NL, NN]).

tcdy_test_query_server(Node, TriplePat, QNId, ColRow, VarPos, ResultList) ->
    SN = {td_unit_test, node()},
    MS = {start, QNId, TriplePat, ColRow, SN, VarPos},
    ME = {eval, []},
    CS = tp_query_node:child_spec(QNId),
    {NT, RL} = ResultList,

    SC = supervisor:which_children({b3s, Node}),
    A = [Node, TriplePat, QNId, ColRow, VarPos, ResultList],
    M = [MS, ME, CS, SC],
    info_msg(tcdy_test_query_server, A, M, 100),

    supervisor:start_child({b3s, Node}, CS),

    RF = fun (_) ->
		 FE = fun() ->
			      ttqs_empty(QNId, Node, RL)
		      end,
		 {generator, FE}
	 end,

    LS = [
	  ?_assertMatch(ok, gen_server:call({QNId, Node}, MS)),
	  ?_assertMatch(ok, gen_server:call({QNId, Node}, ME))
	 ],
    LF = [
	  ?_assertMatch(ok, supervisor:terminate_child({b3s, Node}, QNId)),
	  ?_assertMatch(ok, supervisor:delete_child({b3s, Node}, QNId))
	 ],
    LI = lists:duplicate(NT, abc),
    {inorder, lists:append([LS, lists:map(RF, LI), LF])}.

ttqs_empty(QNId, Node, ResultList) ->
    R = tdt_send_empty({QNId, Node}),
    A = [QNId, Node, ResultList],
    info_msg(ttqs_empty, A, R, 80),
    ttqs_empty_assert(R, ResultList, Node).

ttqs_empty_assert({_, {_, _, end_of_stream}}, ResultList, _) ->
    [
     ?_assertMatch(true, lists:member(end_of_stream, ResultList))
    ];
ttqs_empty_assert({GC, {DO, PD, Rmap}}, ResultList, Node)->
    A = [{GC, {DO, PD, Rmap}}, ResultList, Node],
    M = confirming,
    info_msg(ttqs_empty_assert, A, M, 50),

    [
     ?_assertMatch(true, lists:member(Rmap, ResultList))
    ].

tdt_check_data_local() ->
    TUT  = td_unit_test,
    ND   = node(),
    SN   = {TUT, ND},

    NodStr = atom_to_list(node()),
    NDS    = list_to_atom("b3ss02" ++ string:sub_string(NodStr, 7)),
    %% MN     = mnesia,
    %% TI     = table_info,
    %% SZ     = size,
    GP     = get_property,
    ACT    = active,
    %% INA    = inactive,
    DBA    = db_access,
    UND    = undefined,
    GC     = '$gen_cast',
    DO     = data_outer,
    EOS    = end_of_stream,
    SP     = supervisor,
    TC     = terminate_child,
    DC     = delete_child,

    I01  = "<id_1es4j1i_1m6_zq43us>",
    S01  = "<geoclass_jetty>",
    P01  = "rdfs:subClassOf",
    O01  = "<yagoGeoEntity>",
    T01  = {triple_store, I01, S01, P01, O01},

    %% I02  = "<id_1f8ovls_1m6_zq43us>",
    %% S02  = "<geoclass_former_inlet>",
    %% T02  = {triple_store, I02, S02, P01, O01},

    %% I02  = "<id_o4ua5h_1m6_zq43us>",
    %% S02  = "<geoclass_not_available>",
    %% T02  = {triple_store, I02, S02, P01, O01},

    I02  = "<id_z2h3a0_1m6_zq43us>",
    S02  = "<geoclass_first-order_administrative_division>",
    T02  = {triple_store, I02, S02, P01, O01},

    %% I03  = "<id_1ywnkcj_1m6_zq43us>",
    %% S03  = "<geoclass_language_school>",
    %% T03  = {triple_store, I03, S03, P01, O01},

    %% I03  = "<id_8bpg7_1m6_zq43us>",
    %% S03  = "<geoclass_vineyards>",
    %% T03  = {triple_store, I03, S03, P01, O01},

    I03  = "<id_s7i3yk_1m6_zq43us>",
    S03  = "<geoclass_second-order_administrative_division>",
    T03  = {triple_store, I03, S03, P01, O01},

    I04  = "<id_10p142u_f9j_zil1pw>",
    S04  = "<Japan>",
    P04  = "<hasTLD>",
    %% O04  = "\".jp\"",
    O04  = ".jp",
    T04  = {triple_store, I04, S04, P04, O04},

    I05  = "<id_inji5d_f9j_zil1xg>",
    S05  = "<Slovenia>",
    %% O05  = "\".si\"",
    O05  = ".si",
    T05  = {triple_store, I05, S05, P04, O05},

    TP01 = {"?id", "?sbj", "<eat>", "?obj"},
    SP01 = none,
    PL01 = none,
    SI01 = "1",
    QI01 = "1",
    QN01 = "1",
    QA01 = list_to_atom(QN01++"-"++QI01++"-"++SI01),
    VP01 = #{"?id" => 1, "?sbj" => 2, "?obj" => 4},
    ME01 = {eval, []},
    TQ01 = tp_query_node:spawn_process(QA01, NDS),
    MS01 = {start, QN01, QI01, SI01, TQ01, TP01, SP01, PL01, SN, VP01},
    R01A = {GC, {DO, TQ01, EOS}},

    TP02 = {"?id", "?sbj", P01, "?obj"},
    QN02 = "2",
    QA02 = list_to_atom(QN02++"-"++QI01++"-"++SI01),
    VP02 = #{"?id" => 1, "?sbj" => 2, "?obj" => 4},
    TQ02 = tp_query_node:spawn_process(QA02, NDS),
    MS02 = {start, QN02, QI01, SI01, TQ02, TP02, SP01, PL01, SN, VP02},
    R02A = {GC, {DO, TQ02, maps:put(QN02, T02, #{})}},
    R02B = {GC, {DO, TQ02, maps:put(QN02, T03, #{})}},

    TP03 = {"?id", S01, "?prd", "?obj"},
    QN03 = "3",
    QA03 = list_to_atom(QN03++"-"++QI01++"-"++SI01),
    VP03 = #{"?id" => 1, "?prd" => 3, "?obj" => 4},
    TQ03 = tp_query_node:spawn_process(QA03, NDS),
    MS03 = {start, QN03, QI01, SI01, TQ03, TP03, SP01, PL01, SN, VP03},
    R03A = {GC, {DO, TQ03, maps:put(QN03, T01, #{})}},
    R03B = {GC, {DO, TQ03, EOS}},

    TP04 = {"?id", "?sbj", P04, "?obj"},
    QN04 = "4",
    QA04 = list_to_atom(QN04++"-"++QI01++"-"++SI01),
    VP04 = #{"?id" => 1, "?sbj" => 2, "?obj" => 4},
    TQ04 = tp_query_node:spawn_process(QA04, NDS),
    MS04 = {start, QN04, QI01, SI01, TQ04, TP04, SP01, PL01, SN, VP04},
    R04A = {GC, {DO, TQ04, maps:put(QN04, T04, #{})}},
    R04B = {GC, {DO, TQ04, maps:put(QN04, T05, #{})}},
    R04C = {GC, {DO, TQ04, EOS}},
    {inorder,
     [
      %% ?_assertMatch(1642,     rpc:call(NDS, MN, TI, [Tab, SZ])),
      ?_assertMatch(true,     gen_server:call(TQ01, {GP, wait})),
      ?_assertMatch(UND,      gen_server:call(TQ01, {GP, state})),
      ?_assertMatch(ok,       gen_server:call(TQ01, MS01)),
      ?_assertMatch(false,    gen_server:call(TQ01, {GP, wait})),
      ?_assertMatch(ACT,      gen_server:call(TQ01, {GP, state})),
      ?_assertMatch(ok,       gen_server:call(TQ01, ME01)),
      ?_assertMatch(false,    gen_server:call(TQ01, {GP, wait})),
      ?_assertMatch(DBA,      gen_server:call(TQ01, {GP, state})),
      ?_assertMatch(R01A,     tdt_send_empty(TQ01)),
      ?_assertMatch(ok,       rpc:call(NDS, SP, TC, [b3s, QA01])),
      ?_assertMatch(ok,       rpc:call(NDS, SP, DC, [b3s, QA01])),

      ?_assertMatch(ok,       gen_server:call(TQ02, MS02)),
      ?_assertMatch(ok,       gen_server:call(TQ02, ME01)),
      ?_assertMatch(R02A,     tdt_send_empty(TQ02)),
      ?_assertMatch(R02B,     tdt_send_empty(TQ02)),
      ?_assertMatch(ok,       rpc:call(NDS, SP, TC, [b3s, QA02])),
      ?_assertMatch(ok,       rpc:call(NDS, SP, DC, [b3s, QA02])),

      ?_assertMatch(ok,       gen_server:call(TQ03, MS03)),
      ?_assertMatch(ok,       gen_server:call(TQ03, ME01)),
      ?_assertMatch(R03A,     tdt_send_empty(TQ03)),
      ?_assertMatch(R03B,     tdt_send_empty(TQ03)),
      ?_assertMatch(ok,       rpc:call(NDS, SP, TC, [b3s, QA03])),
      ?_assertMatch(ok,       rpc:call(NDS, SP, DC, [b3s, QA03])),

      ?_assertMatch(ok,       gen_server:call(TQ04, MS04)),
      ?_assertMatch(ok,       gen_server:call(TQ04, ME01)),
      ?_assertMatch(R04A,     tdt_send_empty(TQ04)),
      ?_assertMatch(R04B,     tdt_send_empty(TQ04)),
      ?_assertMatch(R04C,     tdt_send_empty(TQ04)),
      ?_assertMatch(ok,       rpc:call(NDS, SP, TC, [b3s, QA04])),
      ?_assertMatch(ok,       rpc:call(NDS, SP, DC, [b3s, QA04]))
     ]}.

tdt_send_empty(TPQN) ->
    TUT  = td_unit_test,
    ND   = node(),
    SN   = {TUT, ND},
    EM   = {empty, SN},
    gen_server:cast(TPQN, EM),
    info_msg(tdt_send_empty, [TPQN], {sending, EM}, 80),
    receive
	RM -> RM
    end,
    info_msg(tdt_send_empty, [TPQN], {received, RM}, 50),
    RM.

tdt_put_get_save_load() ->
    UD  = undefined,
    GP  = get_property,
    PP  = put_property,
    SP  = save_property,
    LP  = load_property,
    E1  = {error, no_clm_row_conf},
    E2  = {error, {predicate_based, {not_defined, pred_freq}}},
    E3  = {error, {table_name_not_accociated, clm_row_conf}},

    BSP = gen_server:call(node_state, {get, b3s_state_pid}),
    TDP = gen_server:call(BSP, {get, triple_distributor_pid}),
    PC  = gen_server:call(TDP, {GP, pred_clm}),
    {inorder,
     [
      ?_assertMatch(ok,  gen_server:call(TDP, {SP, pred_clm})),
      ?_assertMatch(ok,  gen_server:call(TDP, {SP, pred_freq})),
      ?_assertMatch(UD,  gen_server:call(TDP, {GP, qwe})),
      ?_assertMatch(ok,  gen_server:call(TDP, {PP, qwe, asd})),
      ?_assertMatch(asd, gen_server:call(TDP, {GP, qwe})),
      ?_assertMatch(ok,  gen_server:call(TDP, {PP, pred_clm, #{}})),
      ?_assertMatch(0,   maps:size(gen_server:call(TDP, {GP, pred_clm}))),
      ?_assertMatch(ok,  gen_server:call(TDP, {LP, pred_clm})),
      ?_assertMatch(PC,  gen_server:call(TDP, {GP, pred_clm})),
      ?_assertMatch(ok,  gen_server:call(TDP, {PP, pred_clm, #{}})),
      ?_assertMatch(0,   maps:size(gen_server:call(TDP, {GP, pred_clm}))),
      ?_assertMatch(ok,  gen_server:call(TDP, build_distribution_function)),
      ?_assertMatch(PC,  gen_server:call(TDP, {GP, pred_clm})),
      ?_assertMatch(ok,  gen_server:call(TDP, {PP, pred_freq, UD})),
      ?_assertMatch(E2,  gen_server:call(TDP, build_distribution_function)),
      ?_assertMatch(ok,  gen_server:call(TDP, {LP, pred_freq})),
      ?_assertMatch(ok,  gen_server:call(TDP, build_distribution_function)),
      ?_assertMatch(ok,  gen_server:call(TDP, {PP, clm_row_conf, UD})),
      ?_assertMatch(E1,  gen_server:call(TDP, build_distribution_function)),
      ?_assertMatch(E3,  gen_server:call(TDP, {SP, clm_row_conf})),
      ?_assertMatch(E3,  gen_server:call(TDP, {LP, clm_row_conf}))
     ]}.

tdt_reset_string_id() ->
    BS  = gen_server:call(node_state, {get, b3s_state_pid}),
    Tab = gen_server:call(BS, {get, name_of_string_id_table}),
    TDN = gen_server:call(BS, {get, triple_distributor_nodes}),
    SI  = {string_id, lists:nth(1, TDN)},
    R11 = {deleted, Tab},
    {inorder,
     [
      ?_assertMatch(ok,  gen_server:call(SI, {put, sid_table_name, Tab})),
      ?_assertMatch(ok,  gen_server:call(SI, {put, di_cursor__, undefined})),
      ?_assertMatch(R11, gen_server:call(SI, delete_table))
     ]}.

%% ====> END OF LINE <====

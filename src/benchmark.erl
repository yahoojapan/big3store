%%
%% Perform benchmark tasks.
%%
%% @copyright 2014-2019 UP FAMNIT and Yahoo Japan Corporation
%% @version 0.3
%% @since September, 2014
%% @author Kiyoshi Nitta <knitta@yahoo-corp.jp>
%% 
%% @see b3s
%% 
%% @doc Perform benchmark tasks. This is a gen_server process that
%% runs on distributed b3s systems. Typically, this process will be
%% invoked on a front server node of the distributed big3store
%% system.
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
%% <tr> <td>pid</td> <td>pid()</td> <td>local id of this process.</td>
%% </tr>
%% 
%% <tr> <td>start_date_time</td> <td>calendar:datetime()</td>
%% <td>started date and time of the process.</td> </tr>
%% 
%% <tr> <td>update_date_time</td> <td>calendar:datetime()</td>
%% <td>updated date and time of process properties.</td> </tr>
%% 
%% <tr> <td>result_freq</td> <td>maps:map()</td> <td>mapping from
%% pid() to integer().</td> </tr>
%% 
%% <tr> <td>pid_start</td> <td>maps:map()</td> <td>mapping from term()
%% to erlang:timestamp().</td> </tr>
%% 
%% <tr> <td>pid_elapse</td> <td>maps:map()</td> <td>mapping from
%% term() to integer() (in microseconds).</td> </tr>
%% 
%% <tr> <td>pid_results</td> <td>maps:map()</td> <td>mapping from
%% pid() to [{@link join_query_node:qn_graph()}].</td> </tr>
%% 
%% <tr> <td>last_task</td> <td>atom()</td> <td>identifier of the last
%% benchmark task.</td> </tr>
%% 
%% <tr> <td>process_id</td> <td>atom()</td> <td>identifier of the
%% benchmark task of this process.</td> </tr>
%% 
%% <tr> <td>tmp_report</td> <td>queue:queue()</td> <td>temporal work
%% area for report generation.</td> </tr>
%% 
%% <tr> <td>width_report</td> <td>integer()</td> <td>number of max
%% characters per reporting line.</td> </tr>
%% 
%% <tr> <td>dump_result</td> <td>boolean()</td> <td>control result
%% dump in the report.</td> </tr>
%% 
%% <tr> <td>num_calls</td> <td>maps:map()</td> <td>mapping from term()
%% to integer().</td> </tr>
%% 
%% <tr> <td>result_record_max</td> <td>integer()</td> <td>Max number
%% of records to be reported.</td> </tr>
%%
%% <tr> <td>query_tree_pid</td> <td>pid() | undefined</td> <td>process
%% id of {@link query_tree} process.</td> </tr>
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
%% <tr> <td>{@section @{update_modules, ModuleList@}}</td>
%% <td>[atom()]</td> <td>ok | {error, term()}</td> <td>update
%% modules</td> </tr>
%% 
%% <tr> <td>{@section report}</td> <td></td> <td>string()</td>
%% <td>report last benchmark task</td> </tr>
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
%% === {update_modules, ModuleList} ===
%% 
%% This message update and compile modules listed in
%% ModuleList::[atom()] on all big3store participating nodes. It uses
%% 'clm_row_conf' property of {@link node_state} for retrieving the
%% nodes. If it successfully updates all modules on all nodes, it
%% returns ok. Otherwise, it returns {error, Reason::term()}. Usually,
%% it takes several seconds. It is recommended to extend timeout
%% parameters of gen_server:call/3 and unit tests. (LINK: {@section
%% @{update_modules, ModuleList@}})
%% 
%% === report ===
%% 
%% This message returns a string() result that reports the last
%% executed benchmark task. (LINK: {@section report})
%% 
%% == handle_cast (asynchronous) message API ==
%% 
%% (LINK: {@section handle_cast (asynchronous) message API})
%% 
%% <table border="3">
%% 
%% <tr> <th>Message</th> <th>Args</th> <th>Description</th> </tr>
%% 
%% <tr> <td>{@section task_local2_0001}</td> <td></td> <td>start a
%% benchmark task for unit test</td> </tr>
%% 
%% <tr> <td>{@section task_local2_0002}</td> <td></td> <td>start a
%% benchmark task for unit test</td> </tr>
%% 
%% <tr> <td>{@section task_local2_0003}</td> <td></td> <td>start a
%% benchmark task for unit test</td> </tr>
%% 
%% <tr> <td>{@section task_local2_0004}</td> <td></td> <td>start a
%% benchmark task for unit test</td> </tr>
%% 
%% <tr> <td>{@section task_local2_0005}</td> <td></td> <td>start a
%% benchmark task for unit test</td> </tr>
%% 
%% <tr> <td>{@section task_yg_0001}</td> <td></td> <td>start a
%% benchmark task</td> </tr>
%% 
%% <tr> <td>{@section task_yg_0002}</td> <td></td> <td>start a
%% benchmark task</td> </tr>
%% 
%% <tr> <td>{@section task_yg_0003}</td> <td></td> <td>start a
%% benchmark task</td> </tr>
%% 
%% <tr> <td>{@section task_yg_0003h}</td> <td></td> <td>start a
%% benchmark task</td> </tr>
%% 
%% <tr> <td>{@section task_yg_0004}</td> <td></td> <td>start a
%% benchmark task</td> </tr>
%% 
%% <tr> <td>{@section task_yg_0004m}</td> <td></td> <td>start a
%% benchmark task</td> </tr>
%% 
%% <tr> <td>{@section task_yg_0005}</td> <td></td> <td>start a
%% benchmark task [NW08] YAGO B1</td> </tr>
%% 
%% <tr> <td>{@section task_yg_0005m}</td> <td></td> <td>start a
%% benchmark task [NW08] YAGO B1</td> </tr>
%% 
%% <tr> <td>{@section task_yg_0006}</td> <td></td> <td>start a
%% benchmark task [NW08] YAGO C2</td> </tr>
%% 
%% <tr> <td>{@section task_yg_0007}</td> <td></td> <td>start a
%% benchmark task [NW08] YAGO C1</td> </tr>
%% 
%% <tr> <td>{@section task_yg_0008}</td> <td></td> <td>start a
%% benchmark task [NW08] YAGO A1</td> </tr>
%% 
%% <tr> <td>{@section task_yg_0008m}</td> <td></td> <td>start a
%% benchmark task [NW08] YAGO A1</td> </tr>
%% 
%% <tr> <td>{@section task_yg_0009}</td> <td></td> <td>start a
%% benchmark task [NW08] YAGO B3</td> </tr>
%% 
%% <tr> <td>{@section task_yg_0010}</td> <td></td> <td>start a
%% benchmark task [NW08] YAGO B2</td> </tr>
%% 
%% <tr> <td>{@section task_yg_0010h}</td> <td></td> <td>start a
%% benchmark task [NW08] YAGO B2</td> </tr>
%% 
%% <tr> <td>{@section task_yg_0010m}</td> <td></td> <td>start a
%% benchmark task [NW08] YAGO B2</td> </tr>
%% 
%% <tr> <td>{@section task_yg_0011}</td> <td></td> <td>start a
%% benchmark task</td> </tr>
%% 
%% <tr> <td>{@section task_yg_0011m}</td> <td></td> <td>start a
%% benchmark task</td> </tr>
%% 
%% <tr> <td>{@section task_yg_0012}</td> <td></td> <td>start a
%% benchmark task</td> </tr>
%% 
%% <tr> <td>{@section task_yg_0013}</td> <td></td> <td>start a
%% benchmark task</td> </tr>
%% 
%% <tr> <td>{@section task_yg_0013m}</td> <td></td> <td>start a
%% benchmark task</td> </tr>
%% 
%% <tr> <td>{@section task_yg_0014}</td> <td></td> <td>start a
%% benchmark task</td> </tr>
%% 
%% <tr> <td>{@section task_yg_0014m}</td> <td></td> <td>start a
%% benchmark task</td> </tr>
%% 
%% <tr> <td>{@section task_yg_0015}</td> <td></td> <td>start a
%% benchmark task</td> </tr>
%% 
%% <tr> <td>{@section task_yg_0015m}</td> <td></td> <td>start a
%% benchmark task</td> </tr>
%% 
%% <tr> <td>{@section task_yg_0016}</td> <td></td> <td>start a
%% benchmark task</td> </tr>
%% 
%% <tr> <td>{@section task_yg_0017}</td> <td></td> <td>start a
%% benchmark task</td> </tr>
%% 
%% <tr> <td>{@section @{data_outer, Pid, Graph@}}</td> <td></td>
%% <td>iterate data stream</td> </tr>
%% 
%% <tr> <td>{@section @{restart_record, Pid, TaskId@}}</td> <td></td>
%% <td>(re)start recording elapsed time</td> </tr>
%% 
%% <tr> <td>{@section @{stop_record, Pid, TaskId@}}</td> <td></td>
%% <td>stop recording elapsed time</td> </tr>
%% 
%% </table>
%% 
%% === task_local2_0001 ===
%% 
%% This message starts a benchmark task for 2 servers environment of
%% unit tests. Important events are recorded in the {@link stop_watch}
%% process on the same node. (LINK: {@section task_local2_0001})
%% 
%% === task_local2_0002 ===
%% 
%% This message starts a benchmark task for 2 servers environment of
%% unit tests. Important events are recorded in the {@link stop_watch}
%% process on the same node. It uses {@link query_tree}. (LINK:
%% {@section task_local2_0002})
%% 
%% === task_local2_0003 ===
%% 
%% This message starts a benchmark task for 2 servers environment of
%% unit tests. Important events are recorded in the {@link stop_watch}
%% process on the same node. It uses {@link query_tree}. (LINK:
%% {@section task_local2_0003})
%% 
%% === task_local2_0004 ===
%% 
%% This message starts a benchmark task for 2 servers environment of
%% unit tests. Important events are recorded in the {@link stop_watch}
%% process on the same node. It uses {@link query_tree}. (LINK:
%% {@section task_local2_0004})
%% 
%% === task_local2_0005 ===
%% 
%% This message starts a benchmark task for 2 servers environment of
%% unit tests. Important events are recorded in the {@link stop_watch}
%% process on the same node. It uses {@link query_tree}. (LINK:
%% {@section task_local2_0005})
%% 
%% === task_yg_0001 ===
%% 
%% This message starts a benchmark task for YAGO2s environment. It
%% only uses triple pattern query nodes. Important events are recorded
%% in the {@link stop_watch} process on the same node. It is
%% implemented in function {@link hc_task_yg_0001/0}. (LINK: {@section
%% task_yg_0001})
%% 
%% === task_yg_0002 ===
%% 
%% This message starts a benchmark task for YAGO2s environment. It
%% uses a join query node. Important events are recorded in the {@link
%% stop_watch} process on the same node. It is implemented in function
%% {@link hc_task_yg_0002/0}. (LINK: {@section task_yg_0002})
%% 
%% === task_yg_0003 ===
%% 
%% This message starts a benchmark task for YAGO2s environment. It
%% uses a join query node. Important events are recorded in the {@link
%% stop_watch} process on the same node. It is implemented in function
%% {@link hc_task_yg_0003/0}. (LINK: {@section task_yg_0003})
%% 
%% === task_yg_0003h ===
%% 
%% This message starts a benchmark task for YAGO2s environment. It
%% uses a hash join query node. Important events are recorded in the
%% {@link stop_watch} process on the same node. It is implemented in
%% function {@link hc_task_yg_0003/0}. (LINK: {@section task_yg_0003})
%% 
%% === task_yg_0004 ===
%% 
%% This message starts a benchmark task for YAGO2s environment. It
%% uses a join query node. Important events are recorded in the {@link
%% stop_watch} process on the same node. It uses {@link
%% query_tree}. It is implemented in function {@link
%% hc_task_yg_0004/0}. (LINK: {@section task_yg_0004})
%% 
%% === task_yg_0004m ===
%% 
%% This message starts a benchmark task for YAGO2s environment. It
%% uses a memory join query node. Important events are recorded in the
%% {@link stop_watch} process on the same node. It uses {@link
%% query_tree}. It is implemented in function {@link
%% hc_task_yg_0004/0}. (LINK: {@section task_yg_0004})
%% 
%% === task_yg_0005 ===
%% 
%% This message starts a benchmark task for YAGO2s environment. It
%% uses join query nodes. Important events are recorded in the {@link
%% stop_watch} process on the same node. It uses {@link
%% query_tree}. This is the equivalent query of [NW08] YAGO B1. It is
%% implemented in function {@link hc_task_yg_0005/0}. (LINK: {@section
%% task_yg_0005})
%% 
%% === task_yg_0005m ===
%% 
%% This message starts a benchmark task for YAGO2s environment. It
%% uses memory join query nodes. Important events are recorded in the
%% {@link stop_watch} process on the same node. It uses {@link
%% query_tree}. This is the equivalent query of [NW08] YAGO B1. It is
%% implemented in function {@link hc_task_yg_0005/0}. (LINK: {@section
%% task_yg_0005})
%% 
%% === task_yg_0006 ===
%% 
%% This message starts a benchmark task for YAGO2s environment. It
%% uses a join query node. Important events are recorded in the {@link
%% stop_watch} process on the same node. It uses {@link
%% query_tree}. This is the equivalent query of [NW08] YAGO C2. It is
%% implemented in function {@link hc_task_yg_0006/0}. (LINK: {@section
%% task_yg_0006})
%% 
%% === task_yg_0007 ===
%% 
%% This message starts a benchmark task for YAGO2s environment. It
%% uses a join query node. Important events are recorded in the {@link
%% stop_watch} process on the same node. It uses {@link
%% query_tree}. This is the equivalent query of [NW08] YAGO C1. It is
%% implemented in function {@link hc_task_yg_0007/0}. (LINK: {@section
%% task_yg_0007})
%% 
%% === task_yg_0008 ===
%% 
%% This message starts a benchmark task for YAGO2s environment. It
%% uses join query nodes. Important events are recorded in the {@link
%% stop_watch} process on the same node. It uses {@link
%% query_tree}. This is the equivalent query of [NW08] YAGO A1. It is
%% implemented in function {@link hc_task_yg_0008/0}. (LINK: {@section
%% task_yg_0008})
%% 
%% === task_yg_0008m ===
%% 
%% This message starts a benchmark task for YAGO2s environment. It
%% uses memory join query nodes. Important events are recorded in the
%% {@link stop_watch} process on the same node. It uses {@link
%% query_tree}. This is the equivalent query of [NW08] YAGO A1. It is
%% implemented in function {@link hc_task_yg_0008/0}. (LINK: {@section
%% task_yg_0008})
%% 
%% === task_yg_0009 ===
%% 
%% This message starts a benchmark task for YAGO2s environment. It
%% uses a join query node. Important events are recorded in the {@link
%% stop_watch} process on the same node. It uses {@link
%% query_tree}. This is the equivalent query of [NW08] YAGO B3. It is
%% implemented in function {@link hc_task_yg_0009/0}. (LINK: {@section
%% task_yg_0009})
%% 
%% === task_yg_0010 ===
%% 
%% This message starts a benchmark task for YAGO2s environment. It
%% uses join query nodes. Important events are recorded in the {@link
%% stop_watch} process on the same node. It uses {@link
%% query_tree}. This is the equivalent query of [NW08] YAGO B2. It is
%% implemented in function {@link hc_task_yg_0010/0}. (LINK: {@section
%% task_yg_0010})
%% 
%% === task_yg_0010h ===
%% 
%% This message starts a benchmark task for YAGO2s environment. It
%% uses hash join query nodes. Important events are recorded in the
%% {@link stop_watch} process on the same node. It uses {@link
%% query_tree}. This is the equivalent query of [NW08] YAGO B2. It is
%% implemented in function {@link hc_task_yg_0010h/0}. (LINK:
%% {@section task_yg_0010h})
%% 
%% === task_yg_0010m ===
%% 
%% This message starts a benchmark task for YAGO2s environment. It
%% uses memory join query nodes. Important events are recorded in the
%% {@link stop_watch} process on the same node. It uses {@link
%% query_tree}. This is the equivalent query of [NW08] YAGO B2. It is
%% implemented in function {@link hc_task_yg_0010m/0}. (LINK:
%% {@section task_yg_0010m})
%% 
%% === task_yg_0011 ===
%% 
%% This message starts a benchmark task for YAGO2s environment. It
%% uses join query nodes. Important events are recorded in the {@link
%% stop_watch} process on the same node. It uses {@link
%% query_tree}. It is implemented in function {@link
%% hc_task_yg_0011/0}. (LINK: {@section task_yg_0011})
%% 
%% === task_yg_0011m ===
%% 
%% This message starts a benchmark task for YAGO2s environment. It
%% uses memory join query nodes. Important events are recorded in the
%% {@link stop_watch} process on the same node. It uses {@link
%% query_tree}. It is implemented in function {@link
%% hc_task_yg_0011/0}. (LINK: {@section task_yg_0011})
%% 
%% === task_yg_0012 ===
%% 
%% This message starts a benchmark task for YAGO2s environment. It
%% uses a join query node. Important events are recorded in the {@link
%% stop_watch} process on the same node. It uses {@link
%% query_tree}. It is implemented in function {@link
%% hc_task_yg_0012/0}. (LINK: {@section task_yg_0012})
%% 
%% === task_yg_0013 ===
%% 
%% This message starts a benchmark task for YAGO2s environment. It
%% uses join query nodes. Important events are recorded in the {@link
%% stop_watch} process on the same node. It uses {@link
%% query_tree}. It is implemented in function {@link
%% hc_task_yg_0013/0}. (LINK: {@section task_yg_0013})
%% 
%% === task_yg_0013m ===
%% 
%% This message starts a benchmark task for YAGO2s environment. It
%% uses a memory join query node. Important events are recorded in the
%% {@link stop_watch} process on the same node. It uses {@link
%% query_tree}. It is implemented in function {@link
%% hc_task_yg_0013/0}. (LINK: {@section task_yg_0013})
%% 
%% === task_yg_0014 ===
%% 
%% This message starts a benchmark task for YAGO2s environment. It
%% uses join query nodes. Important events are recorded in the {@link
%% stop_watch} process on the same node. It uses {@link
%% query_tree}. It is implemented in function {@link
%% hc_task_yg_0014/0}. (LINK: {@section task_yg_0014})
%% 
%% === task_yg_0014m ===
%% 
%% This message starts a benchmark task for YAGO2s environment. It
%% uses a memory join query node. Important events are recorded in the
%% {@link stop_watch} process on the same node. It uses {@link
%% query_tree}. It is implemented in function {@link
%% hc_task_yg_0014/0}. (LINK: {@section task_yg_0014})
%% 
%% === task_yg_0015 ===
%% 
%% This message starts a benchmark task for YAGO2s environment. It
%% uses join query nodes. Important events are recorded in the {@link
%% stop_watch} process on the same node. It uses {@link
%% query_tree}. It is implemented in function {@link
%% hc_task_yg_0015/0}. (LINK: {@section task_yg_0015})
%% 
%% === task_yg_0015m ===
%% 
%% This message starts a benchmark task for YAGO2s environment. It
%% uses a memory join query node. Important events are recorded in the
%% {@link stop_watch} process on the same node. It uses {@link
%% query_tree}. It is implemented in function {@link
%% hc_task_yg_0015/0}. (LINK: {@section task_yg_0015})
%% 
%% === task_yg_0016 ===
%% 
%% This message starts a benchmark task for YAGO2s environment. It is
%% a part of task_yg_0010. It uses a join query node. Important events
%% are recorded in the {@link stop_watch} process on the same node. It
%% uses {@link query_tree}. It is implemented in function {@link
%% hc_task_yg_0016/0}. (LINK: {@section task_yg_0016})
%% 
%% === task_yg_0017 ===
%% 
%% This message starts a benchmark task for YAGO2s environment. It is
%% a part of task_yg_0010. It uses a join query node. Important events
%% are recorded in the {@link stop_watch} process on the same node. It
%% uses {@link query_tree}. It is implemented in function {@link
%% hc_task_yg_0017/0}. (LINK: {@section task_yg_0017})
%% 
%% === task_yg_0018 ===
%% 
%% This message starts a benchmark task for YAGO2s environment. It
%% uses hash join query nodes. Important events are recorded in the
%% {@link stop_watch} process on the same node. It uses {@link
%% query_tree}. This is the equivalent query of [NW08] YAGO B2 and
%% task_yg_0010. It is implemented in function {@link
%% hc_task_yg_0018/0}. (LINK: {@section task_yg_0018})
%% 
%% === task_yg_0019 ===
%% 
%% This message starts a benchmark task for YAGO2s environment. It
%% uses hash join query nodes. Important events are recorded in the
%% {@link stop_watch} process on the same node. It uses {@link
%% query_tree}. This is the equivalent query of [NW08] YAGO B2 and
%% task_yg_0010. It is implemented in function {@link
%% hc_task_yg_0019/0}. (LINK: {@section task_yg_0019})
%% 
%% === {data_outer, Pid, Graph} ===
%% 
%% This message performs iteration of data stream. Parameter
%% Pid::{@type node_state:ns_pid()} is a sender process id of this
%% asynchronous message. Parameter Graph::{@type
%% join_query_node:qn_graph()} is a content of the message. The
%% parameter Graph can have atom() value 'end_of_stream'. In that
%% case, this message recognizes the stream termination of the process
%% and reports to the {@link stop_watch} process. Otherwise, it sends
%% back an {empty, {@type node_state:ns_pid()}} message to the sender
%% query node process. It is implemented in function {@link
%% hc_data_outer/2}. (LINK: {@section @{data_outer, Pid, Graph@}})
%% 
%% === {restart_record, Pid, TaskId} ===
%% 
%% This message starts or restarts recording elapsed time for
%% specified task TaskId::atom() of process Pid::{@type
%% node_state:ns_pid()}. It uses maps of pid_start and pid_elapse
%% process dictionary properties with key {Pid, TaskId}. It is
%% implemented in function {@link hc_restart_record/2}. (LINK:
%% {@section @{restart_record, Pid, TaskId@}})
%% 
%% === {stop_record, Pid, TaskId} ===
%% 
%% This message stops recording elapsed time for specified task
%% TaskId::atom() of process Pid::{@type node_state:ns_pid()}. It uses
%% maps of pid_start and pid_elapse process dictionary properties with
%% key {Pid, TaskId}. It is implemented in function {@link
%% hc_stop_record/2}. (LINK: {@section @{stop_record, Pid, TaskId@}})
%% 
%% @type bm_state() = maps:map().
%% 
-module(benchmark).
-behavior(gen_server).
-include_lib("eunit/include/eunit.hrl").
-include("record.hrl").
-export(
   [
    start/1, start/2, stop/1, stop/2,

    hr_property/1, hr_property/2, hr_b3s_property/1,
    hr_b3s_property_list/1, hr_property_list/2, hr_app_property/1,
    hc_report_generate/2,

    child_spec/1, hty_stop_qn/2,
    init/1, handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3
   ]).

%% ======================================================================
%% 
%% api
%% 

%% 
%% @doc This function starts a process of this module on specified
%% node. It returns global process id Pid. The target node must run
%% {@link b3s} application.
%% 
%% @spec start(Node::node()) -> {ok, Pid::node_state:ns_pid()} |
%% {error, Reason::term()}
%% 
start(Node) ->
    start_cc(rpc:call(Node, c, c, [benchmark]), Node, benchmark).

start_cc({ok , benchmark}, Node, ProcId) ->
    B3S = {b3s, Node},
    CS  = benchmark:child_spec(ProcId),
    start_sc(supervisor:start_child(B3S, CS), Node, ProcId);
start_cc(error, Node, _) ->
    {error, {compile_failed, benchmark, Node}}.

start_sc({ok, _}, Node, ProcId) ->
    {ok, {ProcId, Node}};
start_sc(E, Node, ProcId) ->
    {error, {E, Node, ProcId}}.

start(Node, ProcId) ->
    start_cc(rpc:call(Node, c, c, [benchmark]), Node, ProcId).

%% 
%% @doc This function stops the process of this module on specified
%% node.
%% 
%% @spec stop(Node::node()) -> ok | {error, Reason::term()}
%% 
stop(Node) ->
    B3S = {b3s, Node},
    stop_tc(supervisor:terminate_child(B3S, benchmark), Node, benchmark).

stop_tc(ok, Node, ProcId) ->
    supervisor:delete_child({b3s, Node}, ProcId);
stop_tc(E, Node, ProcId) ->
    {error, {E, Node, ProcId}}.

stop(Node, ProcId) ->
    B3S = {b3s, Node},
    stop_tc(supervisor:terminate_child(B3S, ProcId), Node, ProcId).

%% 
%% @doc Return child spec for this process. It can be used in
%% supervisor:init/0 callback implementation.
%% 
%% @spec child_spec(atom()) -> supervisor:child_spec()
%% 
child_spec(ProcId) ->
    GSOpt     = [{local, ProcId}, benchmark, [], []],
    StartFunc = {gen_server, start_link, GSOpt},
    Restart   = permanent,
    Shutdwon  = 1000,
    Type      = worker,
    Modules   = [benchmark],

    {ProcId, StartFunc, Restart, Shutdwon, Type, Modules}.

%% ======================================================================
%% 
%% gen_server behavior
%% 

%% 
%% init/1
%% 
%% @doc Initialize a benchmark process.
%% 
%% @spec init([]) -> {ok, bm_state()}
%% 
init([]) ->
    process_flag(trap_exit, true),

    {ok, RRM} = application:get_env(b3s, result_record_max),
    {ok, BSN} = application:get_env(b3s, b3s_state_nodes),
    BS = {b3s_state, lists:nth(1, BSN)},
    put(created,         true),
    put(pid,             self()),
    put(node,            node()),
    put(start_date_time, calendar:local_time()),
    put(width_report,    80),
    put(dump_result,     true),
    put(result_record_max, RRM),
    put(query_tree_pid,  undefined),
    put(mq_debug, gen_server:call(BS, {get, mq_debug})),

    State = hc_save_pd(),
    info_msg(init, [], State, -1),
    {ok, State}.

%% 
%% handle_call/3
%% 
%% @doc Handle synchronous query requests.
%% 
%% @spec handle_call(term(), {pid(), term()}, bm_state()) -> {reply,
%% term(), bm_state()}
%% 
handle_call({get, all}, _, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    {reply, erlang:get(), State};

handle_call({get, PropertyName}, _, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    {reply, erlang:get(PropertyName), State};

handle_call({put, PropertyName, Value}, _, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    erlang:put(PropertyName, Value),
    erlang:put(update_date_time, calendar:local_time()),
    {reply, ok, hc_save_pd()};

handle_call({update_modules, ModuleList}, _, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    R = hc_update_modules(ModuleList),
    {reply, R, hc_save_pd()};

handle_call(report, _, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    {reply, hc_report(), hc_save_pd()};

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
%% @spec handle_cast(term(), bm_state()) -> {noreply, bm_state()}
%% 
handle_cast({data_outer, Pid, Graph}, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_data_outer(Pid, Graph),
    {noreply, hc_save_pd()};

handle_cast({restart_record, Pid, TaskId}, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_restart_record(Pid, TaskId),
    {noreply, hc_save_pd()};

handle_cast({stop_record, Pid, TaskId}, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_stop_record(Pid, TaskId),
    {noreply, hc_save_pd()};

handle_cast(task_local2_0005, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_task_local2_0005(),
    {noreply, hc_save_pd()};

handle_cast(task_local2_0004, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_task_local2_0004(),
    {noreply, hc_save_pd()};

handle_cast(task_local2_0003, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_task_local2_0003(),
    {noreply, hc_save_pd()};

handle_cast(task_local2_0002, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_task_local2_0002(),
    {noreply, hc_save_pd()};

handle_cast(task_local2_0001, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_task_local2_0001(),
    {noreply, hc_save_pd()};

handle_cast(task_yg_0017, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_task_yg_0017(),
    {noreply, hc_save_pd()};

handle_cast(task_yg_0016, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_task_yg_0016(),
    {noreply, hc_save_pd()};

handle_cast(task_yg_0015m, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_task_yg_0015m(),
    {noreply, hc_save_pd()};

handle_cast(task_yg_0015, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_task_yg_0015(),
    {noreply, hc_save_pd()};

handle_cast(task_yg_0014m, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_task_yg_0014m(),
    {noreply, hc_save_pd()};

handle_cast(task_yg_0014, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_task_yg_0014(),
    {noreply, hc_save_pd()};

handle_cast(task_yg_0013m, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_task_yg_0013m(),
    {noreply, hc_save_pd()};

handle_cast(task_yg_0013, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_task_yg_0013(),
    {noreply, hc_save_pd()};

handle_cast(task_yg_0012, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_task_yg_0012(),
    {noreply, hc_save_pd()};

handle_cast(task_yg_0011m, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_task_yg_0011m(),
    {noreply, hc_save_pd()};

handle_cast(task_yg_0011, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_task_yg_0011(),
    {noreply, hc_save_pd()};

handle_cast(task_yg_0010m, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_task_yg_0010m(),
    {noreply, hc_save_pd()};

handle_cast(task_yg_0010h, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_task_yg_0010h(),
    {noreply, hc_save_pd()};

handle_cast(task_yg_0010, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_task_yg_0010(),
    {noreply, hc_save_pd()};

handle_cast(task_yg_0009, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_task_yg_0009(),
    {noreply, hc_save_pd()};

handle_cast(task_yg_0008m, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_task_yg_0008m(),
    {noreply, hc_save_pd()};

handle_cast(task_yg_0008, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_task_yg_0008(),
    {noreply, hc_save_pd()};

handle_cast(task_yg_0007, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_task_yg_0007(),
    {noreply, hc_save_pd()};

handle_cast(task_yg_0006, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_task_yg_0006(),
    {noreply, hc_save_pd()};

handle_cast(task_yg_0005m, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_task_yg_0005m(),
    {noreply, hc_save_pd()};

handle_cast(task_yg_0005, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_task_yg_0005(),
    {noreply, hc_save_pd()};

handle_cast(task_yg_0004m, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_task_yg_0004m(),
    {noreply, hc_save_pd()};

handle_cast(task_yg_0004, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_task_yg_0004(),
    {noreply, hc_save_pd()};

handle_cast(task_yg_0003h, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_task_yg_0003h(),
    {noreply, hc_save_pd()};

handle_cast(task_yg_0003, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_task_yg_0003(),
    {noreply, hc_save_pd()};

handle_cast(task_yg_0002, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_task_yg_0002(),
    {noreply, hc_save_pd()};

handle_cast(task_yg_0001, State) ->
    b3s_state:hc_monitor_mq(erlang:get(mq_debug)),
    hc_restore_pd(erlang:get(created), State),
    hc_task_yg_0001(),
    {noreply, hc_save_pd()};

%% default
handle_cast(Request, State) ->
    R = {unknown_request, Request},
    error_msg(handle_cast, [Request, State], R),
    {noreply, State}.

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

%% 
%% handle_info/2
%% 
%% @doc Handle exceptional query requests. 
%% 
%% @spec handle_info(term(), bm_state()) -> {noreply, bm_state()}
%% 
handle_info(_Info, State) ->
    {noreply, State}.

%% 
%% terminate/2
%% 
%% @doc Process termination. 
%% 
%% @spec terminate(term(), bm_state()) -> none()
%% 
terminate(Reason, State) ->
    info_msg(terminate, [Reason, State], terminate_normal, 0),
    ok.

%% 
%% code_change/3
%% 
%% @doc Process code change action. 
%% 
%% @spec code_change(term(), bm_state(), term()) -> {ok, bm_state()}
%% 
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ======================================================================
%% 
%% handle call/cast implementation
%% 

%% 
%% @doc This function returns string() formatted report of the last
%% executed benchmark task.
%% 
%% @spec hc_report() -> string()
%% 
hc_report() ->
    hc_report_switch(get(query_tree_pid)).

hc_report_switch(undefined) ->
    hc_report_legacy();
hc_report_switch(QTP) ->
    hc_report_query_tree(QTP).

hc_report_query_tree(QTP) ->
    TR = tmp_report,
    HL = "====> ",
    HC = "Last Benchmark Result",
    HR = " <====",
    TH = string:centre(HL ++ HC ++ HR, get(width_report)),
    put(TR, queue:in(TH, queue:new())),
    put(TR, queue:in([], get(TR))),

    put(current_date_time, calendar:local_time()),
    hr_property(last_task),
    hr_property(start_date_time),
    hr_property(update_date_time),
    hr_property(current_date_time),
    erase(current_date_time),
    hr_b3s_property(block_size),

    hr_separator($-),
    PE = gen_server:call(QTP, {get, pid_elapse}),
    hr_map(pid_elapse, PE, 0.4, microseconds),
    put(TR, queue:in([], get(TR))),
    hr_map(pid_elapse, 0.4, microseconds),
    put(TR, queue:in([], get(TR))),
    hr_map(num_calls, 0.4, 'time(s)'),
    hr_separator($.),

    hr_section_header('query'),
    Q = gen_server:call(QTP, {get, query}),
    put(TR, queue:in(io_lib:format("~p", [Q]), get(TR))),
    put(TR, queue:in([], get(TR))),

    RF = gen_server:call(QTP, {get, result_freq}),
    hr_map(result_freq, RF, 0.4, "graph(s)"),
    RFM = case maps:values(get(result_freq)) of
	      [] -> 0;
	      L  -> lists:max(L)
	  end,
    RRM = get(result_record_max),
    hr_result(hr_cons_result(QTP), get(dump_result), RFM, RRM),
    hr_separator($-),

    hr_property(pid),
    hr_property(width_report),
    hr_property(result_record_max),
    hr_b3s_property_list(data_server_nodes),
    hr_b3s_property_list(name_of_triple_tables),
    hr_b3s_property(name_of_pred_clm_table),
    hr_b3s_property(name_of_pred_freq_table),
    hr_b3s_property(name_of_string_id_table),
    hr_b3s_property(num_of_empty_msgs),
    hr_app_property(debug_level),

    put(TR, queue:in([], get(TR))),
    hc_report_generate(queue:out(get(tmp_report)), []).

hr_cons_result(QTP) ->
    QRL = queue:to_list(gen_server:call(QTP, {get, queue_result})),
    hcr_perform(lists:flatten(QRL), QTP, []).

hcr_perform([], QTP, List) ->
    maps:put(QTP, List, #{});
hcr_perform([end_of_stream], QTP, List) ->
    maps:put(QTP, List, #{});
hcr_perform([M | RestM], QTP, List) ->
    hcr_perform(RestM, QTP, [M | List]).

hc_report_legacy() ->
    TR = tmp_report,
    HL = "====> ",
    HC = "Last Benchmark Result",
    HR = " <====",
    TH = string:centre(HL ++ HC ++ HR, get(width_report)),
    put(TR, queue:in(TH, queue:new())),
    put(TR, queue:in([], get(TR))),

    put(current_date_time, calendar:local_time()),
    hr_property(last_task),
    hr_property(start_date_time),
    hr_property(update_date_time),
    hr_property(current_date_time),
    erase(current_date_time),
    hr_b3s_property(block_size),
    hr_property_list(query_string, get(query_string)),

    hr_separator($-),
    hr_map(pid_elapse, 0.4, microseconds),
    put(TR, queue:in([], get(TR))),
    hr_map(num_calls, 0.4, times),
    hr_separator($.),
    hr_map(result_freq, 0.4, "graph(s)"),
    RFM = lists:max(maps:values(get(result_freq))),
    RRM = get(result_record_max),
    hr_result(get(pid_results), get(dump_result), RFM, RRM),
    hr_separator($-),

    hr_property(pid),
    hr_property(width_report),
    hr_property(result_record_max),
    hr_b3s_property_list(name_of_triple_tables),
    hr_b3s_property(name_of_pred_clm_table),
    hr_b3s_property(name_of_pred_freq_table),
    hr_b3s_property(name_of_string_id_table),
    hr_b3s_property(num_of_empty_msgs),
    hr_app_property(debug_level),

    put(TR, queue:in([], get(TR))),
    hc_report_generate(queue:out(get(tmp_report)), []).

hc_report_generate({empty, _}, R) ->
    HL = "====> ",
    HC = "END OF LINE",
    HR = " <====",
    TH = string:centre(HL ++ HC ++ HR, get(width_report)),
    R ++ "~n" ++ TH ++ "~n";
hc_report_generate({{value, L}, Q}, R) ->
    hc_report_generate(queue:out(Q), R ++ "~n" ++ L).

hr_property(PropertyName) ->
    hr_property(PropertyName, get(PropertyName)).

hr_property(_, undefined) ->
    ok;
hr_property(PropertyName, PropertyValue) ->
    hr_pv_line(PropertyName, PropertyValue).

hr_b3s_property(PropertyName) ->
    PropertyValue = gen_server:call(b3s_state, {get, PropertyName}),
    hr_property(PropertyName, PropertyValue).

hr_b3s_property_list(PropertyName) ->
    PropertyList = gen_server:call(b3s_state, {get, PropertyName}),
    hr_property_list(PropertyName, PropertyList).

hr_property_list(_, undefined) ->
    ok;
hr_property_list(_, []) ->
    ok;
hr_property_list([], [E | Rest]) ->
    hr_v_line(E),
    hr_property_list([], Rest);
hr_property_list(PropertyName, [E | Rest]) ->
    hr_pv_line(PropertyName, E),
    hr_property_list([], Rest).

hr_app_property(PropertyName) ->
    {ok, PropertyValue} = application:get_env(b3s, PropertyName),
    hr_property(PropertyName, PropertyValue).

hr_pv_line(PropertyName, PropertyValue) ->
    TR = tmp_report,
    W  = round(get(width_report) / 2 - 2),
    F  = lists:concat(["~", W, "s : ~p"]),
    FF = lists:flatten(F),
    L  = io_lib:format(FF, [PropertyName, PropertyValue]),
    LF = lists:flatten(L),
    put(TR, queue:in(LF, get(TR))).

hr_v_line(PropertyValue) ->
    TR = tmp_report,
    W  = round(get(width_report) / 2 - 2),
    F  = lists:concat(["~", W, "s   ~p"]),
    FF = lists:flatten(F),
    L  = io_lib:format(FF, ['', PropertyValue]),
    LF = lists:flatten(L),
    put(TR, queue:in(LF, get(TR))).

hr_section_header(Title) ->
    TR = tmp_report,
    TL = lists:concat(["<<< ", Title, " >>>"]),
    TH = string:centre(TL, get(width_report)),
    put(TR, queue:in(TH, get(TR))),
    put(TR, queue:in([], get(TR))).

hr_map(PropertyName, Ratio, Comment) ->
    hr_map(PropertyName, get(PropertyName), Ratio, Comment).

hr_map(_, undefined, _, _) ->
    ok;
hr_map(PropertyName, Map, R, C) ->
    hr_section_header(PropertyName),
    F = fun({K, V}) -> hr_pv_map(K, V, R, C) end,
    lists:map(F, maps:to_list(Map)).

hr_pv_map({{QN, ND}, TN}, Val, Ratio, Comment) ->
    TR = tmp_report,
    W  = round(get(width_report) * Ratio),
    F  = lists:concat(["  ~w ~-", W, "s ~s : ~p ~s"]),
    FF = lists:flatten(F),
    L  = io_lib:format(FF, [QN, ND, TN, Val, Comment]),
    LF = lists:flatten(L),
    put(TR, queue:in(LF, get(TR)));

hr_pv_map({QN, ND}, Val, Ratio, Comment) ->
    TR = tmp_report,
    W  = round(get(width_report) * Ratio),
    F  = lists:concat(["  ~w ~-", W, "s : ~p ~s"]),
    FF = lists:flatten(F),
    L  = io_lib:format(FF, [QN, ND, Val, Comment]),
    LF = lists:flatten(L),
    put(TR, queue:in(LF, get(TR))).

hr_separator(SeedChar) ->
    TR = tmp_report,
    W  = get(width_report),
    SP = string:centre(lists:duplicate(W - 2, SeedChar), W),
    put(TR, queue:in([], get(TR))),
    put(TR, queue:in(SP, get(TR))),
    put(TR, queue:in([], get(TR))).

hr_result(undefined, _, _, _) ->
    ok;
hr_result(_, false, _, _) ->
    ok;
hr_result(_, _, RF, RRM) when RF > RRM ->
    ok;
hr_result(Results, true, _, _) ->
    hr_separator($.),
    hr_section_header('RESULTS'),
    F = fun({K, V}) -> hrr_root_node(K, V) end,
    lists:map(F, maps:to_list(Results)).

hrr_root_node(K, V) ->
    TR = tmp_report,
    put(TR, queue:in([], get(TR))),
    hr_pv_map(K, length(V), 0.4, "graph(s)"),
    put(TR, queue:in([], get(TR))),
    lists:map(fun hrr_graph/1, V).

hrr_graph(end_of_stream) ->
    ok;
hrr_graph(G) ->
    TR = tmp_report,
    lists:map(fun hrr_tuple/1, maps:to_list(G)),
    put(TR, queue:in([], get(TR))).

hrr_tuple({Node, {Tab, Tid, Sbj, Prd, {ObjV, ObjT}}}) ->
    OrigTrip = {Tid, Sbj, Prd, {ObjV, ObjT}},
    {I, S, P, O} = string_id:decode_triple(OrigTrip),

    TR = tmp_report,
    F  = "   [~4s] (~w) ~s~n          s:~s p:~s o:~s",
    L  = io_lib:format(F, [Node, Tab, I, S, P, O]),
    put(TR, queue:in(L, get(TR)));
hrr_tuple({Node, {Tab, Tid, Sbj, Prd, Obj}}) ->
    TR = tmp_report,
    F  = "   [~4s] (~w) ~s~n          s:~s p:~s o:~s",
    L  = io_lib:format(F, [Node, Tab, Tid, Sbj, Prd, Obj]),
    put(TR, queue:in(L, get(TR))).

%% 
%% @doc This function updates and compile modules on nodes.
%% 
%% @spec hc_update_modules(ModuleList::[atom()]) -> ok | {error,
%% term()}
%% 
hc_update_modules(ModuleList) ->
    A   = [ModuleList],
    HUM = hc_update_modules,
    CRC = gen_server:call(node_state, {get, clm_row_conf}),
    NL  = [node() | b3s_state:hpp_get_nodes(CRC)],

    R   = hum_confirm(NL, hum_proc_node(NL, ModuleList, [])),
    info_msg(HUM, A, {R, NL}, 10),
    R.

hum_confirm(ListA, ListB) ->
    SetA = sets:from_list(ListA),
    SetB = sets:from_list(ListB),
    SetC = sets:subtract(SetA, SetB),
    humc_return(sets:to_list(SetC)).

humc_return([]) ->
    ok;
humc_return(L) ->
    {error, {failed, L}}.

hum_proc_node([], _, NodeList) ->
    NodeList;
hum_proc_node([Node | Rest], ModuleList, NodeList) ->
    R = hum_confirm(ModuleList, hum_proc_module(ModuleList, Node, [])),
    case R of
	ok ->
	    hum_proc_node(Rest, ModuleList, [Node | NodeList]);
	_ ->
	    %% A = [[Node | Rest], ModuleList, NodeList],
	    %% error_msg(hum_proc_node, A, {failed-modules, E}),
	    hum_proc_node(Rest, ModuleList, NodeList)
    end.

hum_proc_module([], _, ModuleList) ->
    ModuleList;
hum_proc_module([Mod | Rest], Node, ModuleList) ->
    R = rpc:call(Node, c, c, [Mod]),
    A = [[Mod | Rest], Node, ModuleList],
    info_msg(hum_proc_module, A, entered, 80),
    case R of
	{ok, Mod} ->
	    hum_proc_module(Rest, Node, [Mod | ModuleList]);
	_ ->
	    hum_proc_module(Rest, Node, ModuleList)
    end.

%% 
%% @doc This function performs a benchmark task for unit test
%% environment.
%%
hc_task_local2_0005() ->
    BS            = gen_server:call(node_state, {get, b3s_state_pid}),
    [{DS, _} | _] = gen_server:call(BS, {get, data_server_nodes}),

    mnesia:stop(),
    mnesia:start(),

    SW = stop_watch,
    TN = "task_local2_0005",
    RS = gen_server:call(SW, {start, hty_prep(TN)}),
    put(process_id, benchmark),
    info_msg(hc_task_local2_0005, [], RS, 50),

    SessionId = "0005",
    QueryId   = "1",
    ProcId    = hty_construct_qtid(SessionId, QueryId),
    N         = none,
    TP1S      = {"?id1", "<Japan>",    "?prd1", "?obj1"},
    TP2S      = {"?id2", "<Slovenia>", "?prd1", "?obj2"},
    TP1       = {tp, TP1S, N, N},
    TP2       = {tp, TP2S, N, N},
    Q         = {join, TP1, TP2, N, N},

    QT        = query_tree:spawn_process(ProcId, DS),
    StartMes  = {start, Q, QT, self(), QueryId, SessionId},
    R01       = gen_server:call({QT, DS}, StartMes),
    R02       = gen_server:call({QT, DS}, {eval}),

    RF = gen_server:call(SW, {record, {TN, {task_started, R01, R02}}}),
    info_msg(hc_task_local2_0005, [], RF, 50).

%% 
%% @doc This function performs a benchmark task for unit test
%% environment.
%%
hc_task_local2_0004() ->
    BS            = gen_server:call(node_state, {get, b3s_state_pid}),
    [{DS, _} | _] = gen_server:call(BS, {get, data_server_nodes}),

    mnesia:stop(),
    mnesia:start(),

    SW = stop_watch,
    TN = "task_local2_0004",
    RS = gen_server:call(SW, {start, hty_prep(TN)}),
    put(process_id, benchmark),
    info_msg(hc_task_local2_0004, [], RS, 50),

    SessionId = "0004",
    QueryId   = "1",
    ProcId    = hty_construct_qtid(SessionId, QueryId),
    TPS       = {"?id1", "<Japan>", "?prd1", "?obj1"},
    N         = none,
    Q         = {tp, TPS, N, N},

    QT        = query_tree:spawn_process(ProcId, DS),
    StartMes  = {start, Q, QT, self(), QueryId, SessionId},
    R01       = gen_server:call({QT, DS}, StartMes),
    R02       = gen_server:call({QT, DS}, {eval}),

    RF = gen_server:call(SW, {record, {TN, {task_started, R01, R02}}}),
    info_msg(hc_task_local2_0004, [], RF, 50).

%% 
%% @doc This function performs a benchmark task for unit test
%% environment.
%%
hc_task_local2_0003() ->
    BS            = gen_server:call(node_state, {get, b3s_state_pid}),
    [{DS, _} | _] = gen_server:call(BS, {get, data_server_nodes}),

    mnesia:stop(),
    mnesia:start(),

    SW = stop_watch,
    TN = "task_local2_0003",
    RS = gen_server:call(SW, {start, hty_prep(TN)}),
    put(process_id, benchmark),
    info_msg(hc_task_local2_0003, [], RS, 50),

    SessionId = "0003",
    QueryId   = "1",
    ProcId    = hty_construct_qtid(SessionId, QueryId),
    TPS       = {"?id1", "?sbj1", "rdfs:subClassOf", "?obj1"},
    N         = none,
    Q         = {tp, TPS, N, N},

    QT        = query_tree:spawn_process(ProcId, DS),
    StartMes  = {start, Q, QT, self(), QueryId, SessionId},
    R01       = gen_server:call({QT, DS}, StartMes),
    R02       = gen_server:call({QT, DS}, {eval}),

    RF = gen_server:call(SW, {record, {TN, {task_started, R01, R02}}}),
    info_msg(hc_task_local2_0003, [], RF, 50).

%% 
%% @doc This function performs a benchmark task for unit test
%% environment.
%%
hc_task_local2_0002() ->
    BS            = gen_server:call(node_state, {get, b3s_state_pid}),
    [{DS, _} | _] = gen_server:call(BS, {get, data_server_nodes}),

    mnesia:stop(),
    mnesia:start(),

    SW = stop_watch,
    TN = "task_local2_0002",
    RS = gen_server:call(SW, {start, hty_prep(TN)}),
    put(process_id, benchmark),
    info_msg(hc_task_local2_0002, [], RS, 50),

    SessionId = "0002",
    QueryId   = "1",
    ProcId    = hty_construct_qtid(SessionId, QueryId),
    N         = none,
    TP1S      = {"?id1", "?sbj1", "<hasArea>",   "?obj1"},
    TP2S      = {"?id2", "?sbj1", "<hasImport>", "?obj2"},
    TP1       = {tp, TP1S, N, N},
    TP2       = {tp, TP2S, N, N},
    Q         = {join, TP1, TP2, N, N},

    QT        = query_tree:spawn_process(ProcId, DS),
    StartMes  = {start, Q, QT, self(), QueryId, SessionId},
    R01       = gen_server:call({QT, DS}, StartMes),
    R02       = gen_server:call({QT, DS}, {eval}),

    RF = gen_server:call(SW, {record, {TN, {task_started, R01, R02}}}),
    info_msg(hc_task_local2_0002, [], RF, 50).

%% 
%% @doc This function performs a benchmark task for unit test
%% environment.
%%
hc_task_local2_0001() ->
    BS   = gen_server:call(node_state, {get, b3s_state_pid}),
    [{DS, _} | _] = gen_server:call(BS, {get, data_server_nodes}),
    [FS | _] = gen_server:call(BS, {get, front_server_nodes}),
    BM   = {benchmark, node()},
    MEV  = {eval, []},
    MEM  = {empty, BM},
    Tab = gen_server:call(BS, {get, name_of_string_id_table}),

    mnesia:stop(),
    mnesia:start(),

    SW = stop_watch,
    TN = "task_local2_0001",
    RS = {gen_server:call(SW, {start, hty_prep(TN)}),
	  {sid_table_name, get(sid_table_name)},
	  {sid_max_id, get(sid_max_id)},
	  {b3s_state, BS},
	  {table_name, Tab},
	  {table_size, mnesia:table_info(Tab, size)},
	  {front_server, FS},
	  {node, node()}
	 },
    put(process_id, benchmark),
    info_msg(hc_task_local2_0001, [], RS, 50),

    %% P01  = "rdfs:subClassOf",
    P01  = "<hasArea>",
    P02  = "<hasImport>",

    SI01 = "qn-0001",
    QI01 = "1",
    QN01 = "1",
    QA01 = hty_construct_pid(SI01, QI01, QN01),
    TP01 = string_id:encode_triple_pattern({"?id1", "?sbj1", P01, "?obj1"}),
    VP01 = #{"?id1" => 1, "?sbj1" => 2, "?obj1" => 4},

    QN02 = "2",
    QA02 = hty_construct_pid(SI01, QI01, QN02),
    TP02 = string_id:encode_triple_pattern({"?id2", "?sbj1", P02, "?obj2"}),
    VP02 = #{"?id2" => 1, "?sbj1" => 2, "?obj2" => 4},

    QN03 = "3",
    QA03 = hty_construct_pid(SI01, QI01, QN03),
    GP03 = maps:from_list([{QN01, TP01}, {QN02, TP02}]),
    VP03 = #{"?Id1"  => [{QN01, 1}],
    	     "?sbj1" => [{QN01, 2}, {QN02, 2}],
    	     "?obj1" => [{QN01, 4}],
    	     "?id2"  => [{QN02, 1}],
    	     "?obj2" => [{QN02, 4}]},
    JV03 = ["?sbj1"],
    OL03 = [{QA01, DS}],
    IL03 = [{QA02, DS}],

    hty_restart_tpqn(QN01, QI01, SI01, TP01, VP01, DS, {QA03, DS}, outer),
    hty_restart_tpqn(QN02, QI01, SI01, TP02, VP02, DS, {QA03, DS}, inner),
    hty_restart_jqn (QN03, QI01, SI01, GP03, VP03, JV03, DS, BM, OL03, IL03),
    gen_server:call({QA03, DS}, MEV),
    gen_server:cast({QA03, DS}, MEM),
    gen_server:cast({QA03, DS}, MEM),
    gen_server:cast({QA03, DS}, MEM),
    gen_server:cast({QA03, DS}, MEM),
    gen_server:cast({QA03, DS}, MEM),

    RF = gen_server:call(SW, {record, {TN, {task_started, ok}}}),
    info_msg(hc_task_local2_0001, [], RF, 50).

%% 
%% @doc This function performs a benchmark task for the YAGO2s
%% environment. It is a part of task_yg_0010.
%% 
%% <pre>
%% ?p1    &lt;wasBornIn&gt;          ?city
%% </pre>
%% 
hc_task_yg_0017() ->
    hc_task_yg_0017(use_query_tree).

hc_task_yg_0017(direct) ->
    ok;

hc_task_yg_0017(use_query_tree) ->
    BS       = gen_server:call(node_state, {get, b3s_state_pid}),
    [FS | _] = gen_server:call(BS, {get, front_server_nodes}),
    RRM      = gen_server:call(BS, {get, result_record_max}),
    put(result_record_max, RRM),
    SW = stop_watch,
    TN = "task_yg_0017",
    RS = gen_server:call(SW, {start, hty_prep(TN)}),
    info_msg(hc_task_yg_0017, [use_query_tree], RS, 50),

    SessionId = "0017",
    QueryId   = "1",
    ProcId    = hty_construct_qtid(SessionId, QueryId),
    %% TP01A     = {"[]", "?p1", "rdfs:label", "?n1"},
    %% TP02A     = {"[]", "?p2", "rdfs:label", "?n2"},
    %% TP03A     = {"[]", "?p2", "<wasBornIn>", "?city"},
    TP04A     = {"[]", "?p1", "<wasBornIn>", "?city"},
    %% TP05A     = {"[]", "?p1", "<isMarriedTo>", "?p2"},
    N         = none,
    %% TP01      = {tp, TP01A, N, N},
    %% TP02      = {tp, TP02A, N, N},
    %% TP03      = {tp, TP03A, N, N},
    TP04      = {tp, TP04A, N, N},
    %% TP05      = {tp, TP05A, N, N},
    Q         = TP04,
    %% J01       = {join, TP05, TP04, N, N},
    %% Q         = {join, J01, TP03, N, N},
    %% J02       = {join, J01, TP03, N, N},
    %% J03       = {join, J02, TP02, N, N},
    %% Q         = {join, J03, TP01, N, N},
    QT        = query_tree:spawn_process(ProcId, FS),
    BM        = {get(process_id), node()},
    StartMes  = {start, Q, QT, BM, QueryId, SessionId},
    R01       = gen_server:call({QT, FS}, StartMes),
    R02       = gen_server:call({QT, FS}, {eval}),
    DateTime  = calendar:local_time(),

    put(query_tree_pid, QT),
    put(pid_start, maps:put(QT, os:timestamp(), get(pid_start))),
    RM = {task_started, DateTime, R01, R02},
    RF = gen_server:call(SW, {record, {TN, RM}}),
    info_msg(hc_task_yg_0017, [use_query_tree], RF, 50).

%% 
%% @doc This function performs a benchmark task for the YAGO2s
%% environment. It is a part of task_yg_0010.
%% 
%% <pre>
%% ?p1    &lt;isMarriedTo&gt;        ?p2
%% </pre>
%% 
hc_task_yg_0016() ->
    hc_task_yg_0016(use_query_tree).

hc_task_yg_0016(direct) ->
    ok;

hc_task_yg_0016(use_query_tree) ->
    BS       = gen_server:call(node_state, {get, b3s_state_pid}),
    [FS | _] = gen_server:call(BS, {get, front_server_nodes}),
    RRM      = gen_server:call(BS, {get, result_record_max}),
    put(result_record_max, RRM),
    SW = stop_watch,
    TN = "task_yg_0016",
    RS = gen_server:call(SW, {start, hty_prep(TN)}),
    info_msg(hc_task_yg_0016, [use_query_tree], RS, 50),

    SessionId = "0016",
    QueryId   = "1",
    ProcId    = hty_construct_qtid(SessionId, QueryId),
    %% TP01A     = {"[]", "?p1", "rdfs:label", "?n1"},
    %% TP02A     = {"[]", "?p2", "rdfs:label", "?n2"},
    %% TP03A     = {"[]", "?p2", "<wasBornIn>", "?city"},
    %% TP04A     = {"[]", "?p1", "<wasBornIn>", "?city"},
    TP05A     = {"[]", "?p1", "<isMarriedTo>", "?p2"},
    N         = none,
    %% TP01      = {tp, TP01A, N, N},
    %% TP02      = {tp, TP02A, N, N},
    %% TP03      = {tp, TP03A, N, N},
    %% TP04      = {tp, TP04A, N, N},
    TP05      = {tp, TP05A, N, N},
    Q         = TP05,
    %% J01       = {join, TP05, TP04, N, N},
    %% Q         = {join, J01, TP03, N, N},
    %% J02       = {join, J01, TP03, N, N},
    %% J03       = {join, J02, TP02, N, N},
    %% Q         = {join, J03, TP01, N, N},
    QT        = query_tree:spawn_process(ProcId, FS),
    BM        = {get(process_id), node()},
    StartMes  = {start, Q, QT, BM, QueryId, SessionId},
    R01       = gen_server:call({QT, FS}, StartMes),
    R02       = gen_server:call({QT, FS}, {eval}),
    DateTime  = calendar:local_time(),

    put(query_tree_pid, QT),
    put(pid_start, maps:put(QT, os:timestamp(), get(pid_start))),
    RM = {task_started, DateTime, R01, R02},
    RF = gen_server:call(SW, {record, {TN, RM}}),
    info_msg(hc_task_yg_0016, [use_query_tree], RF, 50).

%% 
%% @doc This function performs a benchmark task for the YAGO2s
%% environment. This is another light query that produces no so big
%% traffic.
%% 
%% <table bgcolor="#E8FFFB">
%% <tr> <th>subject</th> <th>predicate</th> <th>object</th>
%% <th>num</th> </tr>
%% 
%% <tr> <td>?dev</td> <td>&lt;created&gt;</td> <td>?pl</td> <td
%% align="right">277,069</td> </tr>
%% 
%% <tr> <td>?pl</td> <td>rdf:type</td>
%% <td>&lt;wordnet_software_106566077&gt;</td> <td
%% align="right">8,548</td> </tr>
%% 
%% <tr> <td>&lt;Erlang_(programming_language)&gt;</td>
%% <td>&lt;linksTo&gt;</td> <td>?pl</td> <td align="right">41</td>
%% </tr>
%% 
%% </table>
%% 
%% <pre>
%% sparql
%% prefix yago: &lt;http://yago-knowledge.org/resource/&gt;
%% select * from &lt;http://yago-knowledge.org/resource&gt; where {
%%   ?dev yago:created ?pl .
%%   ?pl rdf:type yago:wordnet_software_106566077 .
%%   &lt;http://yago-knowledge.org/resource/Erlang_(programming_language)&gt; yago:linksTo ?pl .
%% } ;
%% </pre>
%% 
hc_task_yg_0015m() ->
    BS       = gen_server:call(node_state, {get, b3s_state_pid}),
    [FS | _] = gen_server:call(BS, {get, front_server_nodes}),
    RRM      = gen_server:call(BS, {get, result_record_max}),
    put(result_record_max, RRM),
    SW = stop_watch,
    TN = "task_yg_0015m",
    RS = gen_server:call(SW, {start, hty_prep(TN)}),
    info_msg(hc_task_yg_0015m, [use_query_tree], RS, 50),

    SessionId = "0015m",
    QueryId   = "1",
    ProcId    = hty_construct_qtid(SessionId, QueryId),
    N         = none,
    TP1       = {tp, {"[]", "?dev", "<created>", "?pl"}, N, N},
    TP2A      = {"[]", "?pl", "rdf:type", "<wordnet_software_106566077>"},
    TP2       = {tp, TP2A, N, N},
    TP3A      = {"[]", "<Erlang_(programming_language)>", "<linksTo>", "?pl"},
    TP3       = {tp, TP3A, N, N},
    J1        = {mjoin, TP2, TP3, N, N},
    Q         = {join, J1, TP1, N, N},
    QT        = query_tree:spawn_process(ProcId, FS),
    BM        = {get(process_id), node()},
    StartMes  = {start, Q, QT, BM, QueryId, SessionId},
    R01       = gen_server:call({QT, FS}, StartMes),
    R02       = gen_server:call({QT, FS}, {eval}),
    DateTime  = calendar:local_time(),

    put(query_tree_pid, QT),
    put(pid_start, maps:put(QT, os:timestamp(), get(pid_start))),
    RM = {task_started, DateTime, R01, R02},
    RF = gen_server:call(SW, {record, {TN, RM}}),
    info_msg(hc_task_yg_0015m, [], RF, 50).

%% 
%% @doc This function performs a benchmark task for the YAGO2s
%% environment. This is another light query that produces no so big
%% traffic.
%% 
%% <table bgcolor="#E8FFFB">
%% <tr> <th>subject</th> <th>predicate</th> <th>object</th>
%% <th>num</th> </tr>
%% 
%% <tr> <td>?dev</td> <td>&lt;created&gt;</td> <td>?pl</td> <td
%% align="right">277,069</td> </tr>
%% 
%% <tr> <td>?pl</td> <td>rdf:type</td>
%% <td>&lt;wordnet_software_106566077&gt;</td> <td
%% align="right">8,548</td> </tr>
%% 
%% <tr> <td>&lt;Erlang_(programming_language)&gt;</td>
%% <td>&lt;linksTo&gt;</td> <td>?pl</td> <td align="right">41</td>
%% </tr>
%% 
%% </table>
%% 
%% <pre>
%% sparql
%% prefix yago: &lt;http://yago-knowledge.org/resource/&gt;
%% select * from &lt;http://yago-knowledge.org/resource&gt; where {
%%   ?dev yago:created ?pl .
%%   ?pl rdf:type yago:wordnet_software_106566077 .
%%   &lt;http://yago-knowledge.org/resource/Erlang_(programming_language)&gt; yago:linksTo ?pl .
%% } ;
%% </pre>
%% 
hc_task_yg_0015() ->
    hc_task_yg_0015(use_query_tree).

%% 
%% <tr> <td>?pl</td> <td>&lt;linksTo&gt;</td>
%% <td>&lt;Erlang_(programming_language)&gt;</td> <td
%% align="right">106</td> </tr>
%% 
hc_task_yg_0015(direct) ->
    ok;

hc_task_yg_0015(use_query_tree) ->
    BS       = gen_server:call(node_state, {get, b3s_state_pid}),
    [FS | _] = gen_server:call(BS, {get, front_server_nodes}),
    RRM      = gen_server:call(BS, {get, result_record_max}),
    put(result_record_max, RRM),
    SW = stop_watch,
    TN = "task_yg_0015",
    RS = gen_server:call(SW, {start, hty_prep(TN)}),
    info_msg(hc_task_yg_0015, [use_query_tree], RS, 50),

    SessionId = "0015",
    QueryId   = "1",
    ProcId    = hty_construct_qtid(SessionId, QueryId),
    N         = none,
    TP1       = {tp, {"[]", "?dev", "<created>", "?pl"}, N, N},
    TP2A      = {"[]", "?pl", "rdf:type", "<wordnet_software_106566077>"},
    TP2       = {tp, TP2A, N, N},
    TP3A      = {"[]", "<Erlang_(programming_language)>", "<linksTo>", "?pl"},
    TP3       = {tp, TP3A, N, N},
    J1        = {join, TP3, TP2, N, N},
    Q         = {join, J1, TP1, N, N},
    QT        = query_tree:spawn_process(ProcId, FS),
    BM        = {get(process_id), node()},
    StartMes  = {start, Q, QT, BM, QueryId, SessionId},
    R01       = gen_server:call({QT, FS}, StartMes),
    R02       = gen_server:call({QT, FS}, {eval}),
    DateTime  = calendar:local_time(),

    put(query_tree_pid, QT),
    put(pid_start, maps:put(QT, os:timestamp(), get(pid_start))),
    RM = {task_started, DateTime, R01, R02},
    RF = gen_server:call(SW, {record, {TN, RM}}),
    info_msg(hc_task_yg_0015, [use_query_tree], RF, 50).

%% 
%% @doc This function performs a benchmark task for the YAGO2s
%% environment. This is another light query that produces no so big
%% traffic.
%% 
%% <table bgcolor="#E8FFFB">
%% <tr> <th>subject</th> <th>predicate</th> <th>object</th>
%% <th>num</th> </tr>
%% 
%% <tr> <td>&lt;Ericsson&gt;</td> <td>&lt;created&gt;</td>
%% <td>?pl</td> <td align="right">5</td> </tr>
%% 
%% <tr> <td>?pl</td> <td>rdf:type</td>
%% <td>&lt;wordnet_language_106282651&gt;</td> <td
%% align="right">10160</td> </tr>
%% 
%% <tr> <td>?pl</td> <td>&lt;wasCreatedOnDate&gt;</td> <td>?dt</td>
%% <td align="right"></td> </tr>
%% 
%% </table>
%% 
%% <pre>
%% sparql
%% prefix yago: &lt;http://yago-knowledge.org/resource/&gt;
%% select * from &lt;http://yago-knowledge.org/resource&gt; where {
%%   yago:Ericsson yago:created ?pl .
%%   ?pl rdf:type yago:wordnet_language_106282651 .
%%   ?pl yago:wasCreatedOnDate ?dt .
%% } ;
%% </pre>
%% 
hc_task_yg_0014m() ->
    BS       = gen_server:call(node_state, {get, b3s_state_pid}),
    [FS | _] = gen_server:call(BS, {get, front_server_nodes}),
    RRM      = gen_server:call(BS, {get, result_record_max}),
    put(result_record_max, RRM),
    SW = stop_watch,
    TN = "task_yg_0014m",
    RS = gen_server:call(SW, {start, hty_prep(TN)}),
    info_msg(hc_task_yg_0014m, [use_query_tree], RS, 50),

    SessionId = "0014m",
    QueryId   = "1",
    ProcId    = hty_construct_qtid(SessionId, QueryId),
    N         = none,
    TP1       = {tp, {"[]", "?pl", "<wasCreatedOnDate>", "?dt"}, N, N},
    TP2A      = {"[]", "?pl", "rdf:type", "<wordnet_language_106282651>"},
    TP2       = {tp, TP2A, N, N},
    TP3       = {tp, {"[]", "<Ericsson>", "<created>", "?pl"}, N, N},
    J1        = {mjoin, TP3, TP2, N, N},
    Q         = {join, J1, TP1, N, N},
    QT        = query_tree:spawn_process(ProcId, FS),
    BM        = {get(process_id), node()},
    StartMes  = {start, Q, QT, BM, QueryId, SessionId},
    R01       = gen_server:call({QT, FS}, StartMes),
    R02       = gen_server:call({QT, FS}, {eval}),
    DateTime  = calendar:local_time(),

    put(query_tree_pid, QT),
    put(pid_start, maps:put(QT, os:timestamp(), get(pid_start))),
    RM = {task_started, DateTime, R01, R02},
    RF = gen_server:call(SW, {record, {TN, RM}}),
    info_msg(hc_task_yg_0014m, [], RF, 50).

%% 
%% @doc This function performs a benchmark task for the YAGO2s
%% environment. This is another light query that produces no so big
%% traffic.
%% 
%% <table bgcolor="#E8FFFB">
%% <tr> <th>subject</th> <th>predicate</th> <th>object</th>
%% <th>num</th> </tr>
%% 
%% <tr> <td>&lt;Ericsson&gt;</td> <td>&lt;created&gt;</td>
%% <td>?pl</td> <td align="right">5</td> </tr>
%% 
%% <tr> <td>?pl</td> <td>rdf:type</td>
%% <td>&lt;wordnet_language_106282651&gt;</td> <td
%% align="right">10160</td> </tr>
%% 
%% <tr> <td>?pl</td> <td>&lt;wasCreatedOnDate&gt;</td> <td>?dt</td>
%% <td align="right"></td> </tr>
%% 
%% </table>
%% 
%% <pre>
%% sparql
%% prefix yago: &lt;http://yago-knowledge.org/resource/&gt;
%% select * from &lt;http://yago-knowledge.org/resource&gt; where {
%%   yago:Ericsson yago:created ?pl .
%%   ?pl rdf:type yago:wordnet_language_106282651 .
%%   ?pl yago:wasCreatedOnDate ?dt .
%% } ;
%% </pre>
%% 
hc_task_yg_0014() ->
    hc_task_yg_0014(use_query_tree).

hc_task_yg_0014(direct) ->
    ok;

hc_task_yg_0014(use_query_tree) ->
    BS       = gen_server:call(node_state, {get, b3s_state_pid}),
    [FS | _] = gen_server:call(BS, {get, front_server_nodes}),
    RRM      = gen_server:call(BS, {get, result_record_max}),
    put(result_record_max, RRM),
    SW = stop_watch,
    TN = "task_yg_0014",
    RS = gen_server:call(SW, {start, hty_prep(TN)}),
    info_msg(hc_task_yg_0014, [use_query_tree], RS, 50),

    SessionId = "0014",
    QueryId   = "1",
    ProcId    = hty_construct_qtid(SessionId, QueryId),
    N         = none,
    TP1       = {tp, {"[]", "?pl", "<wasCreatedOnDate>", "?dt"}, N, N},
    TP2A      = {"[]", "?pl", "rdf:type", "<wordnet_language_106282651>"},
    TP2       = {tp, TP2A, N, N},
    TP3       = {tp, {"[]", "<Ericsson>", "<created>", "?pl"}, N, N},
    J1        = {join, TP3, TP2, N, N},
    Q         = {join, J1, TP1, N, N},
    QT        = query_tree:spawn_process(ProcId, FS),
    BM        = {get(process_id), node()},
    StartMes  = {start, Q, QT, BM, QueryId, SessionId},
    R01       = gen_server:call({QT, FS}, StartMes),
    R02       = gen_server:call({QT, FS}, {eval}),
    DateTime  = calendar:local_time(),

    put(query_tree_pid, QT),
    put(pid_start, maps:put(QT, os:timestamp(), get(pid_start))),
    RM = {task_started, DateTime, R01, R02},
    RF = gen_server:call(SW, {record, {TN, RM}}),
    info_msg(hc_task_yg_0014, [use_query_tree], RF, 50).

%% 
%% @doc This function performs a benchmark task for the YAGO2s
%% environment. This is another light query that produces no so big
%% traffic.
%% 
%% <table bgcolor="#E8FFFB">
%% <tr> <th>subject</th> <th>predicate</th> <th>object</th>
%% <th>num</th> </tr>
%% 
%% <tr> <td>?p</td> <td>rdf:type</td>
%% <td>&lt;wikicategory_Japanese_computer_scientists&gt;</td> <td
%% align="right">21</td> </tr>
%% 
%% <tr> <td>?p</td> <td>&lt;created&gt;</td> <td>?o</td> <td
%% align="right">277,069</td> </tr>
%% 
%% <tr> <td>?o</td> <td>rdf:type</td>
%% <td>&lt;wordnet_programming_language_106898352&gt;</td> <td
%% align="right">574</td> </tr>
%% 
%% </table>
%% 
%% <pre>
%% sparql
%% prefix yago: &lt;http://yago-knowledge.org/resource/&gt;
%% select * from &lt;http://yago-knowledge.org/resource&gt; where {
%%   ?p rdf:type yago:wikicategory_Japanese_computer_scientists .
%%   ?p yago:created ?o .
%%   ?o rdf:type yago:wordnet_programming_language_106898352 .
%% } ;
%% </pre>
%% 
hc_task_yg_0013m() ->
    BS       = gen_server:call(node_state, {get, b3s_state_pid}),
    [FS | _] = gen_server:call(BS, {get, front_server_nodes}),
    RRM      = gen_server:call(BS, {get, result_record_max}),
    put(result_record_max, RRM),
    SW = stop_watch,
    TN = "task_yg_0013m",
    RS = gen_server:call(SW, {start, hty_prep(TN)}),
    info_msg(hc_task_yg_0013m, [], RS, 50),

    SessionId = "0013m",
    QueryId   = "1",
    ProcId    = hty_construct_qtid(SessionId, QueryId),
    N         = none,
    TP1A      = {"[]", "?o", "rdf:type",
	         "<wordnet_programming_language_106898352>"},
    TP1       = {tp, TP1A, N, N},
    TP2       = {tp, {"[]", "?p", "<created>", "?o"}, N, N},
    TP3A      = {"[]", "?p", "rdf:type",
		 "<wikicategory_Japanese_computer_scientists>"},
    TP3       = {tp, TP3A, N, N},
    J1        = {join, TP3, TP2, N, N},
    Q         = {mjoin, J1, TP1, N, N},
    QT        = query_tree:spawn_process(ProcId, FS),
    BM        = {get(process_id), node()},
    StartMes  = {start, Q, QT, BM, QueryId, SessionId},
    R01       = gen_server:call({QT, FS}, StartMes),
    R02       = gen_server:call({QT, FS}, {eval}),
    DateTime  = calendar:local_time(),

    put(query_tree_pid, QT),
    put(pid_start, maps:put(QT, os:timestamp(), get(pid_start))),
    RM = {task_started, DateTime, R01, R02},
    RF = gen_server:call(SW, {record, {TN, RM}}),
    info_msg(hc_task_yg_0013m, [], RF, 50).

%% 
%% @doc This function performs a benchmark task for the YAGO2s
%% environment. This is another light query that produces no so big
%% traffic.
%% 
%% <table bgcolor="#E8FFFB">
%% <tr> <th>subject</th> <th>predicate</th> <th>object</th>
%% <th>num</th> </tr>
%% 
%% <tr> <td>?p</td> <td>rdf:type</td>
%% <td>&lt;wikicategory_Japanese_computer_scientists&gt;</td> <td
%% align="right">21</td> </tr>
%% 
%% <tr> <td>?p</td> <td>&lt;created&gt;</td> <td>?o</td> <td
%% align="right">277,069</td> </tr>
%% 
%% <tr> <td>?o</td> <td>rdf:type</td>
%% <td>&lt;wordnet_programming_language_106898352&gt;</td> <td
%% align="right">574</td> </tr>
%% 
%% </table>
%% 
%% <pre>
%% sparql
%% prefix yago: &lt;http://yago-knowledge.org/resource/&gt;
%% select * from &lt;http://yago-knowledge.org/resource&gt; where {
%%   ?p rdf:type yago:wikicategory_Japanese_computer_scientists .
%%   ?p yago:created ?o .
%%   ?o rdf:type yago:wordnet_programming_language_106898352 .
%% } ;
%% </pre>
%% 
hc_task_yg_0013() ->
    hc_task_yg_0013(use_query_tree).

hc_task_yg_0013(direct) ->
    ok;

hc_task_yg_0013(use_query_tree) ->
    BS       = gen_server:call(node_state, {get, b3s_state_pid}),
    [FS | _] = gen_server:call(BS, {get, front_server_nodes}),
    RRM      = gen_server:call(BS, {get, result_record_max}),
    put(result_record_max, RRM),
    SW = stop_watch,
    TN = "task_yg_0013",
    RS = gen_server:call(SW, {start, hty_prep(TN)}),
    info_msg(hc_task_yg_0013, [use_query_tree], RS, 50),

    SessionId = "0013",
    QueryId   = "1",
    ProcId    = hty_construct_qtid(SessionId, QueryId),
    N         = none,
    TP1A      = {"[]", "?o", "rdf:type",
	         "<wordnet_programming_language_106898352>"},
    TP1       = {tp, TP1A, N, N},
    TP2       = {tp, {"[]", "?p", "<created>", "?o"}, N, N},
    TP3A      = {"[]", "?p", "rdf:type",
		 "<wikicategory_Japanese_computer_scientists>"},
    TP3       = {tp, TP3A, N, N},
    J1        = {join, TP3, TP2, N, N},
    Q         = {join, J1, TP1, N, N},
    QT        = query_tree:spawn_process(ProcId, FS),
    BM        = {get(process_id), node()},
    StartMes  = {start, Q, QT, BM, QueryId, SessionId},
    R01       = gen_server:call({QT, FS}, StartMes),
    R02       = gen_server:call({QT, FS}, {eval}),
    DateTime  = calendar:local_time(),

    put(query_tree_pid, QT),
    put(pid_start, maps:put(QT, os:timestamp(), get(pid_start))),
    RM = {task_started, DateTime, R01, R02},
    RF = gen_server:call(SW, {record, {TN, RM}}),
    info_msg(hc_task_yg_0013, [use_query_tree], RF, 50).

%% 
%% @doc This function performs a benchmark task for the YAGO2s
%% environment. This is another light query that produces no so big
%% traffic.
%% 
%% <table bgcolor="#E8FFFB">
%% <tr> <th>subject</th> <th>predicate</th> <th>object</th> </tr>
%% 
%% <tr> <td>&lt;Eiiti_Wada&gt;</td> <td>rdf:type</td> <td>?c</td>
%% </tr>
%% 
%% <tr> <td>&lt;Akinori_Yonezawa&gt;</td> <td>rdf:type</td>
%% <td>?c</td> </tr>
%% 
%% <tr> <td>&lt;Tadao_Kasami&gt;</td> <td>rdf:type</td> <td>?c</td>
%% </tr>
%% 
%% <tr> <td>&lt;Ken_Sakamura&gt;</td> <td>rdf:type</td> <td>?c</td>
%% </tr>
%% 
%% <tr> <td>&lt;Nobuo_Mii&gt;</td> <td>rdf:type</td> <td>?c</td> </tr>
%% 
%% <tr> <td>&lt;Makoto_Nagao&gt;</td> <td>rdf:type</td> <td>?c</td>
%% </tr>
%% 
%% <tr> <td>&lt;Eiichi_Goto&gt;</td> <td>rdf:type</td> <td>?c</td>
%% </tr>
%% 
%% <tr> <td>&lt;Kozo_Sugiyama&gt;</td> <td>rdf:type</td> <td>?c</td>
%% </tr>
%% 
%% <tr> <td>&lt;Tatsuo_Kobayashi&gt;</td> <td>rdf:type</td>
%% <td>?c</td> </tr>
%% 
%% <tr> <td>&lt;Masaru_Kitsuregawa&gt;</td> <td>rdf:type</td>
%% <td>?c</td> </tr>
%% 
%% <tr> <td>&lt;Tomoyuki_Nishita&gt;</td> <td>rdf:type</td>
%% <td>?c</td> </tr>
%% 
%% <tr> <td>&lt;Makoto_Murata&gt;</td> <td>rdf:type</td> <td>?c</td>
%% </tr>
%% 
%% <tr> <td>&lt;Hisao_Yamada&gt;</td> <td>rdf:type</td> <td>?c</td>
%% </tr>
%% 
%% <tr> <td>&lt;Masaru_Tomita&gt;</td> <td>rdf:type</td> <td>?c</td>
%% </tr>
%% 
%% <tr> <td>&lt;Takeo_Kanade&gt;</td> <td>rdf:type</td> <td>?c</td>
%% </tr>
%% 
%% <tr> <td>&lt;Yukihiro_Matsumoto&gt;</td> <td>rdf:type</td>
%% <td>?c</td> </tr>
%% 
%% <tr> <td>&lt;Jun_Murai&gt;</td> <td>rdf:type</td> <td>?c</td> </tr>
%% 
%% <tr> <td>&lt;Nobuo_Yoneda&gt;</td> <td>rdf:type</td> <td>?c</td>
%% </tr>
%% 
%% <tr> <td>&lt;Mitsunori_Miki&gt;</td> <td>rdf:type</td> <td>?c</td>
%% </tr>
%% 
%% <tr> <td>&lt;Hiroshi_Ishii&gt;</td> <td>rdf:type</td> <td>?c</td>
%% </tr>
%% 
%% </table>
%% 
hc_task_yg_0012() ->
    hc_task_yg_0012(use_query_tree).

hc_task_yg_0012(direct) ->
    ok;

hc_task_yg_0012(use_query_tree) ->
    BS       = gen_server:call(node_state, {get, b3s_state_pid}),
    [FS | _] = gen_server:call(BS, {get, front_server_nodes}),
    RRM      = gen_server:call(BS, {get, result_record_max}),
    put(result_record_max, RRM),
    SW = stop_watch,
    TN = "task_yg_0012",
    RS = gen_server:call(SW, {start, hty_prep(TN)}),
    info_msg(hc_task_yg_0012, [use_query_tree], RS, 50),

    SessionId = "0012",
    QueryId   = "1",
    ProcId    = hty_construct_qtid(SessionId, QueryId),
    TP01A     = {"[]", "<Eiiti_Wada>", "rdf:type", "?c"},
    TP02A     = {"[]", "<Akinori_Yonezawa>", "rdf:type", "?c"},
    TP03A     = {"[]", "<Tadao_Kasami>", "rdf:type", "?c"},
    TP04A     = {"[]", "<Ken_Sakamura>", "rdf:type", "?c"},
    TP05A     = {"[]", "<Nobuo_Mii)>", "rdf:type", "?c"},
    TP06A     = {"[]", "<Makoto_Nagao>", "rdf:type", "?c"},
    TP07A     = {"[]", "<Eiichi_Goto>", "rdf:type", "?c"},
    TP08A     = {"[]", "<Kozo_Sugiyama>", "rdf:type", "?c"},
    TP09A     = {"[]", "<Tatsuo_Kobayashi>", "rdf:type", "?c"},
    TP10A     = {"[]", "<Masaru_Kitsuregawa>", "rdf:type", "?c"},
    TP11A     = {"[]", "<Tomoyuki_Nishita>", "rdf:type", "?c"},	
    TP12A     = {"[]", "<Makoto_Murata>", "rdf:type", "?c"},
    TP13A     = {"[]", "<Hisao_Yamada>", "rdf:type", "?c"},
    TP14A     = {"[]", "<Masaru_Tomita>", "rdf:type", "?c"},
    TP15A     = {"[]", "<Takeo_Kanade>", "rdf:type", "?c"},
    TP16A     = {"[]", "<Yukihiro_Matsumoto>", "rdf:type", "?c"},
    TP17A     = {"[]", "<Jun_Murai>", "rdf:type", "?c"},
    TP18A     = {"[]", "<Nobuo_Yoneda>", "rdf:type", "?c"},
    TP19A     = {"[]", "<Mitsunori_Miki>", "rdf:type", "?c"},
    TP20A     = {"[]", "<Hiroshi_Ishii>", "rdf:type", "?c"},
    N         = none,
    TP01      = {tp, TP01A, N, N},
    TP02      = {tp, TP02A, N, N},
    TP03      = {tp, TP03A, N, N},
    TP04      = {tp, TP04A, N, N},
    TP05      = {tp, TP05A, N, N},
    TP06      = {tp, TP06A, N, N},
    TP07      = {tp, TP07A, N, N},
    TP08      = {tp, TP08A, N, N},
    TP09      = {tp, TP09A, N, N},
    TP10      = {tp, TP10A, N, N},
    TP11      = {tp, TP11A, N, N},
    TP12      = {tp, TP12A, N, N},
    TP13      = {tp, TP13A, N, N},
    TP14      = {tp, TP14A, N, N},
    TP15      = {tp, TP15A, N, N},
    TP16      = {tp, TP16A, N, N},
    TP17      = {tp, TP17A, N, N},
    TP18      = {tp, TP18A, N, N},
    TP19      = {tp, TP19A, N, N},
    TP20      = {tp, TP20A, N, N},
    J01       = {join, TP20, TP19, N, N},
    J02       = {join, J01, TP18, N, N},
    J03       = {join, J02, TP17, N, N},
    J04       = {join, J03, TP16, N, N},
    J05       = {join, J04, TP15, N, N},
    J06       = {join, J05, TP14, N, N},
    J07       = {join, J06, TP13, N, N},
    J08       = {join, J07, TP12, N, N},
    J09       = {join, J08, TP11, N, N},
    J10       = {join, J09, TP10, N, N},
    J11       = {join, J10, TP09, N, N},
    J12       = {join, J11, TP08, N, N},
    J13       = {join, J12, TP07, N, N},
    J14       = {join, J13, TP06, N, N},
    J15       = {join, J14, TP05, N, N},
    J16       = {join, J15, TP04, N, N},
    J17       = {join, J16, TP03, N, N},
    J18       = {join, J17, TP02, N, N},
    Q         = {join, J18, TP01, N, N},
    QT        = query_tree:spawn_process(ProcId, FS),
    BM        = {get(process_id), node()},
    StartMes  = {start, Q, QT, BM, QueryId, SessionId},
    R01       = gen_server:call({QT, FS}, StartMes),
    R02       = gen_server:call({QT, FS}, {eval}),
    DateTime  = calendar:local_time(),

    put(query_tree_pid, QT),
    put(pid_start, maps:put(QT, os:timestamp(), get(pid_start))),
    RM = {task_started, DateTime, R01, R02},
    RF = gen_server:call(SW, {record, {TN, RM}}),
    info_msg(hc_task_yg_0012, [use_query_tree], RF, 50).

%% 
%% @doc This function performs a benchmark task for the YAGO2s
%% environment. It tries to test a circular query.
%% 
%% <table bgcolor="#E8FFFB">
%% <tr> <th>subject</th> <th>predicate</th> <th>object</th> </tr>
%% 
%% <tr> <td>?p1</td> <td>[]</td> <td>?p4</td> </tr>
%% 
%% <tr> <td>?p4</td> <td>[]</td> <td>?p3</td> </tr>
%% 
%% <tr> <td>?p4</td> <td>&lt;actedIn&gt;</td> <td>?movie2</td> </tr>
%% 
%% <tr> <td>?p3</td> <td>&lt;actedIn&gt;</td> <td>?movie1</td> </tr>
%% 
%% <tr> <td>?p2</td> <td>&lt;directed&gt;</td> <td>?movie2</td> </tr>
%% 
%% <tr> <td>?p2</td> <td>&lt;influences&gt;</td> <td>?p1</td> </tr>
%% 
%% <tr> <td>?p1</td> <td>&lt;directed&gt;</td> <td>?movie1</td> </tr>
%% 
%% <tr> <td>&lt;Tim_Burton&gt;</td> <td>&lt;directed&gt;</td>
%% <td>?movie1</td> </tr>
%% 
%% <tr> <td>&lt;Johnny_Depp&gt;</td> <td>&lt;actedIn&gt;</td>
%% <td>?movie1</td> </tr>
%% 
%% </table>
%% 
hc_task_yg_0011m() ->
    BS       = gen_server:call(node_state, {get, b3s_state_pid}),
    [FS | _] = gen_server:call(BS, {get, front_server_nodes}),
    RRM      = gen_server:call(BS, {get, result_record_max}),
    put(result_record_max, RRM),
    SW = stop_watch,
    TN = "task_yg_0011m",
    RS = gen_server:call(SW, {start, hty_prep(TN)}),
    info_msg(hc_task_yg_0011m, [use_query_tree], RS, 50),

    SessionId = "0011m",
    QueryId   = "1",
    ProcId    = hty_construct_qtid(SessionId, QueryId),
    TP01A     = {"[]", "?p1", "?prd1", "?p4"},
    TP02A     = {"[]", "?p4", "?prd1", "?p3"},
    TP03A     = {"[]", "?p4", "<actedIn>", "?movie2"},
    TP04A     = {"[]", "?p3", "<actedIn>", "?movie1"},
    TP05A     = {"[]", "?p2", "<directed>", "?movie2"},
    TP06A     = {"[]", "?p2", "<influences>", "?p1"},
    TP07A     = {"[]", "?p1", "<directed>", "?movie1"},
    TP08A     = {"[]", "<Tim_Burton>", "<directed>", "?movie1"},
    TP09A     = {"[]", "<Johnny_Depp>", "<actedIn>", "?movie1"},
    N         = none,
    TP01      = {tp, TP01A, N, N},
    TP02      = {tp, TP02A, N, N},
    TP03      = {tp, TP03A, N, N},
    TP04      = {tp, TP04A, N, N},
    TP05      = {tp, TP05A, N, N},
    TP06      = {tp, TP06A, N, N},
    TP07      = {tp, TP07A, N, N},
    TP08      = {tp, TP08A, N, N},
    TP09      = {tp, TP09A, N, N},
    J01       = {mjoin, TP09, TP08, N, N},
    J02       = {join, J01, TP07, N, N},
    J03       = {mjoin, J02, TP06, N, N},
    J04       = {join, J03, TP05, N, N},
    J05       = {join, J04, TP04, N, N},
    J06       = {join, J05, TP03, N, N},
    J07       = {join, J06, TP02, N, N},
    Q         = {join, J07, TP01, N, N},
    QT        = query_tree:spawn_process(ProcId, FS),
    BM        = {get(process_id), node()},
    StartMes  = {start, Q, QT, BM, QueryId, SessionId},
    R01       = gen_server:call({QT, FS}, StartMes),
    R02       = gen_server:call({QT, FS}, {eval}),
    DateTime  = calendar:local_time(),

    put(query_tree_pid, QT),
    put(pid_start, maps:put(QT, os:timestamp(), get(pid_start))),
    RM = {task_started, DateTime, R01, R02},
    RF = gen_server:call(SW, {record, {TN, RM}}),
    info_msg(hc_task_yg_0011m, [], RF, 50).

%% 
%% @doc This function performs a benchmark task for the YAGO2s
%% environment. It tries to test a circular query.
%% 
%% <table bgcolor="#E8FFFB">
%% <tr> <th>subject</th> <th>predicate</th> <th>object</th> </tr>
%% 
%% <tr> <td>?p1</td> <td>[]</td> <td>?p4</td> </tr>
%% 
%% <tr> <td>?p4</td> <td>[]</td> <td>?p3</td> </tr>
%% 
%% <tr> <td>?p4</td> <td>&lt;actedIn&gt;</td> <td>?movie2</td> </tr>
%% 
%% <tr> <td>?p3</td> <td>&lt;actedIn&gt;</td> <td>?movie1</td> </tr>
%% 
%% <tr> <td>?p2</td> <td>&lt;directed&gt;</td> <td>?movie2</td> </tr>
%% 
%% <tr> <td>?p2</td> <td>&lt;influences&gt;</td> <td>?p1</td> </tr>
%% 
%% <tr> <td>?p1</td> <td>&lt;directed&gt;</td> <td>?movie1</td> </tr>
%% 
%% <tr> <td>&lt;Tim_Burton&gt;</td> <td>&lt;directed&gt;</td>
%% <td>?movie1</td> </tr>
%% 
%% <tr> <td>&lt;Johnny_Depp&gt;</td> <td>&lt;actedIn&gt;</td>
%% <td>?movie1</td> </tr>
%% 
%% </table>
%% 
hc_task_yg_0011() ->
    hc_task_yg_0011(use_query_tree).

hc_task_yg_0011(direct) ->
    ok;

hc_task_yg_0011(use_query_tree) ->
    BS       = gen_server:call(node_state, {get, b3s_state_pid}),
    [FS | _] = gen_server:call(BS, {get, front_server_nodes}),
    RRM      = gen_server:call(BS, {get, result_record_max}),
    put(result_record_max, RRM),
    SW = stop_watch,
    TN = "task_yg_0011",
    RS = gen_server:call(SW, {start, hty_prep(TN)}),
    info_msg(hc_task_yg_0011, [use_query_tree], RS, 50),

    SessionId = "0011",
    QueryId   = "1",
    ProcId    = hty_construct_qtid(SessionId, QueryId),
    TP01A     = {"[]", "?p1", "?prd1", "?p4"},
    TP02A     = {"[]", "?p4", "?prd1", "?p3"},
    TP03A     = {"[]", "?p4", "<actedIn>", "?movie2"},
    TP04A     = {"[]", "?p3", "<actedIn>", "?movie1"},
    TP05A     = {"[]", "?p2", "<directed>", "?movie2"},
    TP06A     = {"[]", "?p2", "<influences>", "?p1"},
    TP07A     = {"[]", "?p1", "<directed>", "?movie1"},
    TP08A     = {"[]", "<Tim_Burton>", "<directed>", "?movie1"},
    TP09A     = {"[]", "<Johnny_Depp>", "<actedIn>", "?movie1"},
    N         = none,
    TP01      = {tp, TP01A, N, N},
    TP02      = {tp, TP02A, N, N},
    TP03      = {tp, TP03A, N, N},
    TP04      = {tp, TP04A, N, N},
    TP05      = {tp, TP05A, N, N},
    TP06      = {tp, TP06A, N, N},
    TP07      = {tp, TP07A, N, N},
    TP08      = {tp, TP08A, N, N},
    TP09      = {tp, TP09A, N, N},
    J01       = {join, TP09, TP08, N, N},
    J02       = {join, J01, TP07, N, N},
    J03       = {join, J02, TP06, N, N},
    J04       = {join, J03, TP05, N, N},
    J05       = {join, J04, TP04, N, N},
    J06       = {join, J05, TP03, N, N},
    J07       = {join, J06, TP02, N, N},
    Q         = {join, J07, TP01, N, N},
    QT        = query_tree:spawn_process(ProcId, FS),
    BM        = {get(process_id), node()},
    StartMes  = {start, Q, QT, BM, QueryId, SessionId},
    R01       = gen_server:call({QT, FS}, StartMes),
    R02       = gen_server:call({QT, FS}, {eval}),
    DateTime  = calendar:local_time(),

    put(query_tree_pid, QT),
    put(pid_start, maps:put(QT, os:timestamp(), get(pid_start))),
    RM = {task_started, DateTime, R01, R02},
    RF = gen_server:call(SW, {record, {TN, RM}}),
    info_msg(hc_task_yg_0011, [use_query_tree], RF, 50).

%% 
%% @doc This function performs a benchmark task for the YAGO2s
%% environment. This is the equivalent query of [NW08] YAGO B2 and
%% task_yg_0010:
%% 
%% <pre>
%% ?p1    rdfs:label                 ?n1
%% ?p2    rdfs:label                 ?n2
%% ?p1    &lt;wasBornIn&gt;          ?city
%% ?p1    &lt;isMarriedTo&gt;        ?p2
%% ?p2    &lt;wasBornIn&gt;          ?city
%% </pre>
%% 
hc_task_yg_0010m() ->
    hc_task_yg_0010m(use_query_tree).

hc_task_yg_0010m(direct) ->
    ok;

hc_task_yg_0010m(use_query_tree) ->
    BS       = gen_server:call(node_state, {get, b3s_state_pid}),
    [FS | _] = gen_server:call(BS, {get, front_server_nodes}),
    RRM      = gen_server:call(BS, {get, result_record_max}),
    put(result_record_max, RRM),
    SW = stop_watch,
    TN = "task_yg_0010m",
    RS = gen_server:call(SW, {start, hty_prep(TN)}),
    info_msg(hc_task_yg_0010m, [use_query_tree], RS, 50),

    SessionId = "0010m",
    QueryId   = "1",
    ProcId    = hty_construct_qtid(SessionId, QueryId),
    %% TP01A     = {"[]", "?p1", "rdfs:label", "?n1"},
    %% TP02A     = {"[]", "?p2", "rdfs:label", "?n2"},
    TP03A     = {"[]", "?p2", "<wasBornIn>", "?city"},
    TP04A     = {"[]", "?p1", "<wasBornIn>", "?city"},
    TP05A     = {"[]", "?p1", "<isMarriedTo>", "?p2"},
    N         = none,
    %% TP01      = {tp, TP01A, N, N},
    %% TP02      = {tp, TP02A, N, N},
    TP03      = {tp, TP03A, N, N},
    TP04      = {tp, TP04A, N, N},
    TP05      = {tp, TP05A, N, N},
    J01       = {mjoin, TP04, TP05, N, N},
    Q         = {join, J01, TP03, N, N},
    %% J02       = {join, J01, TP03, N, N},
    %% J03       = {join, J02, TP02, N, N},
    %% Q         = {join, J03, TP01, N, N},
    QT        = query_tree:spawn_process(ProcId, FS),
    BM        = {get(process_id), node()},
    StartMes  = {start, Q, QT, BM, QueryId, SessionId},
    R01       = gen_server:call({QT, FS}, StartMes),
    R02       = gen_server:call({QT, FS}, {eval}),
    DateTime  = calendar:local_time(),

    put(query_tree_pid, QT),
    put(pid_start, maps:put(QT, os:timestamp(), get(pid_start))),
    RM = {task_started, DateTime, R01, R02},
    RF = gen_server:call(SW, {record, {TN, RM}}),
    info_msg(hc_task_yg_0010m, [], RF, 50).

%% 
%% @doc This function performs a benchmark task for the YAGO2s
%% environment. This is the equivalent query of [NW08] YAGO B2 and
%% task_yg_0010:
%% 
%% <pre>
%% ?p1    rdfs:label                 ?n1
%% ?p2    rdfs:label                 ?n2
%% ?p1    &lt;wasBornIn&gt;          ?city
%% ?p1    &lt;isMarriedTo&gt;        ?p2
%% ?p2    &lt;wasBornIn&gt;          ?city
%% </pre>
%% 
hc_task_yg_0010h() ->
    hc_task_yg_0010h(use_query_tree).

hc_task_yg_0010h(direct) ->
    ok;

hc_task_yg_0010h(use_query_tree) ->
    BS       = gen_server:call(node_state, {get, b3s_state_pid}),
    [FS | _] = gen_server:call(BS, {get, front_server_nodes}),
    RRM      = gen_server:call(BS, {get, result_record_max}),
    put(result_record_max, RRM),
    SW = stop_watch,
    TN = "task_yg_0010h",
    RS = gen_server:call(SW, {start, hty_prep(TN)}),
    info_msg(hc_task_yg_0010h, [use_query_tree], RS, 50),

    SessionId = "0010h",
    QueryId   = "1",
    ProcId    = hty_construct_qtid(SessionId, QueryId),
    %% TP01A     = {"[]", "?p1", "rdfs:label", "?n1"},
    %% TP02A     = {"[]", "?p2", "rdfs:label", "?n2"},
    TP03A     = {"[]", "?p2", "<wasBornIn>", "?city"},
    TP04A     = {"[]", "?p1", "<wasBornIn>", "?city"},
    TP05A     = {"[]", "?p1", "<isMarriedTo>", "?p2"},
    N         = none,
    %% TP01      = {tp, TP01A, N, N},
    %% TP02      = {tp, TP02A, N, N},
    TP03      = {tp, TP03A, N, N},
    TP04      = {tp, TP04A, N, N},
    TP05      = {tp, TP05A, N, N},
    J01       = {hjoin, TP05, TP04, N, N},
    Q         = {join, J01, TP03, N, N},
    %% J02       = {join, J01, TP03, N, N},
    %% J03       = {join, J02, TP02, N, N},
    %% Q         = {join, J03, TP01, N, N},
    QT        = query_tree:spawn_process(ProcId, FS),
    BM        = {get(process_id), node()},
    StartMes  = {start, Q, QT, BM, QueryId, SessionId},
    R01       = gen_server:call({QT, FS}, StartMes),
    R02       = gen_server:call({QT, FS}, {eval}),
    DateTime  = calendar:local_time(),

    put(query_tree_pid, QT),
    put(pid_start, maps:put(QT, os:timestamp(), get(pid_start))),
    RM = {task_started, DateTime, R01, R02},
    RF = gen_server:call(SW, {record, {TN, RM}}),
    info_msg(hc_task_yg_0010h, [], RF, 50).

%% 
%% @doc This function performs a benchmark task for the YAGO2s
%% environment. This is the equivalent query of [NW08] YAGO B2:
%% 
%% <pre>
%% ?p1    rdfs:label                 ?n1
%% ?p2    rdfs:label                 ?n2
%% ?p1    &lt;wasBornIn&gt;          ?city
%% ?p1    &lt;isMarriedTo&gt;        ?p2
%% ?p2    &lt;wasBornIn&gt;          ?city
%% </pre>
%% 
hc_task_yg_0010() ->
    hc_task_yg_0010(use_query_tree).

hc_task_yg_0010(direct) ->
    ok;

hc_task_yg_0010(use_query_tree) ->
    BS       = gen_server:call(node_state, {get, b3s_state_pid}),
    [FS | _] = gen_server:call(BS, {get, front_server_nodes}),
    RRM      = gen_server:call(BS, {get, result_record_max}),
    put(result_record_max, RRM),
    SW = stop_watch,
    TN = "task_yg_0010",
    RS = gen_server:call(SW, {start, hty_prep(TN)}),
    info_msg(hc_task_yg_0010, [use_query_tree], RS, 50),

    SessionId = "0010",
    QueryId   = "1",
    ProcId    = hty_construct_qtid(SessionId, QueryId),
    %% TP01A     = {"[]", "?p1", "rdfs:label", "?n1"},
    %% TP02A     = {"[]", "?p2", "rdfs:label", "?n2"},
    TP03A     = {"[]", "?p2", "<wasBornIn>", "?city"},
    TP04A     = {"[]", "?p1", "<wasBornIn>", "?city"},
    TP05A     = {"[]", "?p1", "<isMarriedTo>", "?p2"},
    N         = none,
    %% TP01      = {tp, TP01A, N, N},
    %% TP02      = {tp, TP02A, N, N},
    TP03      = {tp, TP03A, N, N},
    TP04      = {tp, TP04A, N, N},
    TP05      = {tp, TP05A, N, N},
    J01       = {join, TP05, TP04, N, N},
    Q         = {join, J01, TP03, N, N},
    %% J02       = {join, J01, TP03, N, N},
    %% J03       = {join, J02, TP02, N, N},
    %% Q         = {join, J03, TP01, N, N},
    QT        = query_tree:spawn_process(ProcId, FS),
    BM        = {get(process_id), node()},
    StartMes  = {start, Q, QT, BM, QueryId, SessionId},
    R01       = gen_server:call({QT, FS}, StartMes),
    R02       = gen_server:call({QT, FS}, {eval}),
    DateTime  = calendar:local_time(),

    put(query_tree_pid, QT),
    put(pid_start, maps:put(QT, os:timestamp(), get(pid_start))),
    RM = {task_started, DateTime, R01, R02},
    RF = gen_server:call(SW, {record, {TN, RM}}),
    info_msg(hc_task_yg_0010, [use_query_tree], RF, 50).

%% 
%% @doc This function performs a benchmark task for the YAGO2s
%% environment. This is the equivalent query of [NW08] YAGO B3:
%% 
%% <pre>
%% ?p1    &lt;hasFamilyName&gt;      ?fn
%% ?p2    &lt;hasFamilyName&gt;      ?fn
%% ?p1    rdf:type           &lt;wordnet_scientist_110560637&gt;
%% ?p1    &lt;hasWonPrize&gt;        ?award
%% ?p1    &lt;wasBornIn&gt;          ?city
%% ?p2    rdf:type           &lt;wordnet_scientist_110560637&gt;
%% ?p2    &lt;hasWonPrize&gt;        ?award
%% ?p2    &lt;wasBornIn&gt;          ?city
%% </pre>
%% 
hc_task_yg_0009() ->
    hc_task_yg_0009(use_query_tree).

hc_task_yg_0009(direct) ->
    ok;

hc_task_yg_0009(use_query_tree) ->
    BS       = gen_server:call(node_state, {get, b3s_state_pid}),
    [FS | _] = gen_server:call(BS, {get, front_server_nodes}),
    RRM      = gen_server:call(BS, {get, result_record_max}),
    put(result_record_max, RRM),
    SW = stop_watch,
    TN = "task_yg_0009",
    RS = gen_server:call(SW, {start, hty_prep(TN)}),
    info_msg(hc_task_yg_0009, [use_query_tree], RS, 50),

    SessionId = "0009",
    QueryId   = "1",
    ProcId    = hty_construct_qtid(SessionId, QueryId),
    TP01A     = {"[]", "?p1", "<hasFamilyName>", "?fn1"},
    TP02A     = {"[]", "?p2", "<hasFamilyName>", "?fn2"},
    TP03A     = {"[]", "?p2", "<wasBornIn>", "?city"},
    TP04A     = {"[]", "?p2", "rdf:type", "<wordnet_scientist_110560637>"},
    TP05A     = {"[]", "?p2", "<hasWonPrize>", "?award"},
    TP06A     = {"[]", "?p1", "<wasBornIn>", "?city"},
    TP07A     = {"[]", "?p1", "rdf:type", "<wordnet_scientist_110560637>"},
    TP08A     = {"[]", "?p1", "<hasWonPrize>", "?award"},
    N         = none,
    TP01      = {tp, TP01A, N, N},
    TP02      = {tp, TP02A, N, N},
    TP03      = {tp, TP03A, N, N},
    TP04      = {tp, TP04A, N, N},
    TP05      = {tp, TP05A, N, N},
    TP06      = {tp, TP06A, N, N},
    TP07      = {tp, TP07A, N, N},
    TP08      = {tp, TP08A, N, N},
    J01       = {join, TP08, TP07, N, N},
    J02       = {join, J01, TP06, N, N},
    J03       = {join, J02, TP05, N, N},
    J04       = {join, J03, TP04, N, N},
    J05       = {join, J04, TP03, N, N},
    J06       = {join, J05, TP02, N, N},
    Q         = {join, J06, TP01, N, N},
    QT        = query_tree:spawn_process(ProcId, FS),
    BM        = {get(process_id), node()},
    StartMes  = {start, Q, QT, BM, QueryId, SessionId},
    R01       = gen_server:call({QT, FS}, StartMes),
    R02       = gen_server:call({QT, FS}, {eval}),
    DateTime  = calendar:local_time(),

    put(query_tree_pid, QT),
    put(pid_start, maps:put(QT, os:timestamp(), get(pid_start))),
    RM = {task_started, DateTime, R01, R02},
    RF = gen_server:call(SW, {record, {TN, RM}}),
    info_msg(hc_task_yg_0009, [use_query_tree], RF, 50).

%% 
%% @doc This function performs a benchmark task for the YAGO2s
%% environment. This is the equivalent query of [NW08] YAGO A1:
%% 
%% <pre>
%% ?p     &lt;hasGivenName&gt;       ?gn
%% ?p     &lt;hasFamilyName&gt;      ?fn
%% ?p     rdf:type           &lt;wordnet_scientist_110560637&gt;
%% ?p     &lt;wasBornIn&gt;          ?city
%% ?p     &lt;hasAcademicAdvisor&gt; ?a
%% ?a     &lt;wasBornIn&gt;          ?city2
%% ?city  &lt;isLocatedIn&gt;        &lt;Switzerland&gt;
%% ?city2 &lt;isLocatedIn&gt;        &lt;Germany&gt;
%% </pre>
%% 
hc_task_yg_0008m() ->
    BS       = gen_server:call(node_state, {get, b3s_state_pid}),
    [FS | _] = gen_server:call(BS, {get, front_server_nodes}),
    RRM      = gen_server:call(BS, {get, result_record_max}),
    put(result_record_max, RRM),
    SW = stop_watch,
    TN = "task_yg_0008m",
    RS = gen_server:call(SW, {start, hty_prep(TN)}),
    info_msg(hc_task_yg_0008m, [use_query_tree], RS, 50),

    SessionId = "0008m",
    QueryId   = "1",
    ProcId    = hty_construct_qtid(SessionId, QueryId),
    TP01A     = {"[]", "?p", "<hasGivenName>", "?gn"},
    TP02A     = {"[]", "?p", "<hasFamilyName>", "?fn"},
    TP03A     = {"[]", "?city2", "<isLocatedIn>", "<Germany>"},
    TP04A     = {"[]", "?a", "<wasBornIn>", "?city2"},
    TP05A     = {"[]", "?city", "<isLocatedIn>", "<Switzerland>"},
    TP06A     = {"[]", "?p", "<wasBornIn>", "?city"},
    TP07A     = {"[]", "?p", "rdf:type", "<wordnet_scientist_110560637>"},
    TP08A     = {"[]", "?p", "<hasAcademicAdvisor>", "?a"},
    N         = none,
    TP01      = {tp, TP01A, N, N},
    TP02      = {tp, TP02A, N, N},
    TP03      = {tp, TP03A, N, N},
    TP04      = {tp, TP04A, N, N},
    TP05      = {tp, TP05A, N, N},
    TP06      = {tp, TP06A, N, N},
    TP07      = {tp, TP07A, N, N},
    TP08      = {tp, TP08A, N, N},
    J01       = {mjoin, TP07, TP08, N, N},
    J02       = {join, J01, TP06, N, N},
    J03       = {join, J02, TP05, N, N},
    J04       = {mjoin, J03, TP04, N, N},
    J05       = {mjoin, J04, TP03, N, N},
    J06       = {join, J05, TP02, N, N},
    Q         = {join, J06, TP01, N, N},
    QT        = query_tree:spawn_process(ProcId, FS),
    BM        = {get(process_id), node()},
    StartMes  = {start, Q, QT, BM, QueryId, SessionId},
    R01       = gen_server:call({QT, FS}, StartMes),
    R02       = gen_server:call({QT, FS}, {eval}),
    DateTime  = calendar:local_time(),

    put(query_tree_pid, QT),
    put(pid_start, maps:put(QT, os:timestamp(), get(pid_start))),
    RM = {task_started, DateTime, R01, R02},
    RF = gen_server:call(SW, {record, {TN, RM}}),
    info_msg(hc_task_yg_0008m, [], RF, 50).

%% 
%% @doc This function performs a benchmark task for the YAGO2s
%% environment. This is the equivalent query of [NW08] YAGO A1:
%% 
%% <pre>
%% ?p     &lt;hasGivenName&gt;       ?gn
%% ?p     &lt;hasFamilyName&gt;      ?fn
%% ?p     rdf:type           &lt;wordnet_scientist_110560637&gt;
%% ?p     &lt;wasBornIn&gt;          ?city
%% ?p     &lt;hasAcademicAdvisor&gt; ?a
%% ?a     &lt;wasBornIn&gt;          ?city2
%% ?city  &lt;isLocatedIn&gt;        &lt;Switzerland&gt;
%% ?city2 &lt;isLocatedIn&gt;        &lt;Germany&gt;
%% </pre>
%% 
hc_task_yg_0008() ->
    hc_task_yg_0008(use_query_tree).

hc_task_yg_0008(direct) ->
    ok;

hc_task_yg_0008(use_query_tree) ->
    BS       = gen_server:call(node_state, {get, b3s_state_pid}),
    [FS | _] = gen_server:call(BS, {get, front_server_nodes}),
    RRM      = gen_server:call(BS, {get, result_record_max}),
    put(result_record_max, RRM),
    SW = stop_watch,
    TN = "task_yg_0008",
    RS = gen_server:call(SW, {start, hty_prep(TN)}),
    info_msg(hc_task_yg_0008, [use_query_tree], RS, 50),

    SessionId = "0008",
    QueryId   = "1",
    ProcId    = hty_construct_qtid(SessionId, QueryId),
    TP01A     = {"[]", "?p", "<hasGivenName>", "?gn"},
    TP02A     = {"[]", "?p", "<hasFamilyName>", "?fn"},
    TP03A     = {"[]", "?city2", "<isLocatedIn>", "<Germany>"},
    TP04A     = {"[]", "?a", "<wasBornIn>", "?city2"},
    TP05A     = {"[]", "?city", "<isLocatedIn>", "<Switzerland>"},
    TP06A     = {"[]", "?p", "<wasBornIn>", "?city"},
    TP07A     = {"[]", "?p", "rdf:type", "<wordnet_scientist_110560637>"},
    TP08A     = {"[]", "?p", "<hasAcademicAdvisor>", "?a"},
    N         = none,
    TP01      = {tp, TP01A, N, N},
    TP02      = {tp, TP02A, N, N},
    TP03      = {tp, TP03A, N, N},
    TP04      = {tp, TP04A, N, N},
    TP05      = {tp, TP05A, N, N},
    TP06      = {tp, TP06A, N, N},
    TP07      = {tp, TP07A, N, N},
    TP08      = {tp, TP08A, N, N},
    J01       = {join, TP08, TP07, N, N},
    J02       = {join, J01, TP06, N, N},
    J03       = {join, J02, TP05, N, N},
    J04       = {join, J03, TP04, N, N},
    J05       = {join, J04, TP03, N, N},
    J06       = {join, J05, TP02, N, N},
    Q         = {join, J06, TP01, N, N},
    QT        = query_tree:spawn_process(ProcId, FS),
    BM        = {get(process_id), node()},
    StartMes  = {start, Q, QT, BM, QueryId, SessionId},
    R01       = gen_server:call({QT, FS}, StartMes),
    R02       = gen_server:call({QT, FS}, {eval}),
    DateTime  = calendar:local_time(),

    put(query_tree_pid, QT),
    put(pid_start, maps:put(QT, os:timestamp(), get(pid_start))),
    RM = {task_started, DateTime, R01, R02},
    RF = gen_server:call(SW, {record, {TN, RM}}),
    info_msg(hc_task_yg_0008, [use_query_tree], RF, 50).

%% 
%% @doc This function performs a benchmark task for the YAGO2s
%% environment. This is the equivalent query of [NW08] YAGO C1.
%% 
hc_task_yg_0007() ->
    hc_task_yg_0007(use_query_tree).

hc_task_yg_0007(direct) ->
    ok;

hc_task_yg_0007(use_query_tree) ->
    BS       = gen_server:call(node_state, {get, b3s_state_pid}),
    [FS | _] = gen_server:call(BS, {get, front_server_nodes}),
    RRM      = gen_server:call(BS, {get, result_record_max}),
    put(result_record_max, RRM),
    SW = stop_watch,
    TN = "task_yg_0007",
    RS = gen_server:call(SW, {start, hty_prep(TN)}),
    info_msg(hc_task_yg_0007, [use_query_tree], RS, 50),

    SessionId = "0007",
    QueryId   = "1",
    ProcId    = hty_construct_qtid(SessionId, QueryId),
    TP01A     = {"[]", "?p2", "rdf:type", "<wordnet_scientist_110560637>"},
    TP02A     = {"[]", "?p2", "?prd2", "?city"},
    TP03A     = {"[]", "?city", "rdf:type", "<wordnet_site_108651247>"}, 
    TP04A     = {"[]", "?p1", "?prd1", "?city"},
    TP05A     = {"[]", "?p1", "rdf:type", "<wordnet_scientist_110560637>"},
    N         = none,
    TP01      = {tp, TP01A, N, N},
    TP02      = {tp, TP02A, N, N},
    TP03      = {tp, TP03A, N, N},
    TP04      = {tp, TP04A, N, N},
    TP05      = {tp, TP05A, N, N},
    J01       = {join, TP05, TP04, N, N},
    J02       = {join, J01, TP03, N, N},
    J03       = {join, J02, TP02, N, N},
    Q         = {join, J03, TP01, N, N},
    QT        = query_tree:spawn_process(ProcId, FS),
    BM        = {get(process_id), node()},
    StartMes  = {start, Q, QT, BM, QueryId, SessionId},
    R01       = gen_server:call({QT, FS}, StartMes),
    R02       = gen_server:call({QT, FS}, {eval}),
    DateTime  = calendar:local_time(),

    put(query_tree_pid, QT),
    put(pid_start, maps:put(QT, os:timestamp(), get(pid_start))),
    RM = {task_started, DateTime, R01, R02},
    RF = gen_server:call(SW, {record, {TN, RM}}),
    info_msg(hc_task_yg_0007, [use_query_tree], RF, 50).

%% 
%% @doc This function performs a benchmark task for the YAGO2s
%% environment. This is the equivalent query of [NW08] YAGO C2.
%% 
%% <pre>
%% sparql
%% prefix yago: &lt;http://yago-knowledge.org/resource/&gt;
%% select * from &lt;http://yago-knowledge.org/resource&gt; where {
%%   ?c1 rdfs:label "London" .
%%   ?c1 rdf:type yago:wordnet_site_108651247 .
%%   ?p  ?prd1 ?c1 .
%%   ?p  ?prd2 ?c2 .
%%   ?c2 rdfs:label "Paris" .
%%   ?c2 rdf:type yago:wordnet_site_108651247 .
%% } ;
%% 
%% a) ?c1 rdfs:label "London" .
%% b) ?c1 rdf:type yago:wordnet_site_108651247 .
%% c) ?p  ?prd1 ?c1 .
%% d) ?p  ?prd2 ?c2 .
%% e) ?c2 rdfs:label "Paris" .
%% f) ?c2 rdf:type yago:wordnet_site_108651247 .
%% g) filter(?prd1 != rdfs:label) .
%% h) filter(?prd2 != rdfs:label) .
%% i) filter(?prd1 != yago:linksTo) .
%% j) filter(?prd2 != yago:linksTo) .
%% 
%% a:5 ab:2 abc:73,235 abcd:4,573,471 abcde:5,837 abcdef:5,826
%% e:8 f:190,641 ef:2 def:37,674 cdef:2,444,134 acdef:5,900
%% abcdefghij:10
%% 
%% </pre>
%% 
hc_task_yg_0006() ->
    hc_task_yg_0006(use_query_tree).

hc_task_yg_0006(direct) ->
    ok;

%%   ?c1 rdf:type yago:wordnet_village_108672738 .
hc_task_yg_0006(use_query_tree) ->
    BS       = gen_server:call(node_state, {get, b3s_state_pid}),
    [FS | _] = gen_server:call(BS, {get, front_server_nodes}),
    RRM      = gen_server:call(BS, {get, result_record_max}),
    put(result_record_max, RRM),
    SW = stop_watch,
    TN = "task_yg_0006",
    RS = gen_server:call(SW, {start, hty_prep(TN)}),
    info_msg(hc_task_yg_0006, [use_query_tree], RS, 50),

    SessionId = "0006",
    QueryId   = "1",
    ProcId    = hty_construct_qtid(SessionId, QueryId),
    TP01A     = {"[]", "?c1", "rdf:type", "<wordnet_site_108651247>"},
    TP02A     = {"[]", "?c1", "rdfs:label", "\"London\""},
    TP03A     = {"[]", "?p", "?prd2", "?c2"},
    TP04A     = {"[]", "?p", "?prd1", "?c1"},
    TP05A     = {"[]", "?c2", "rdf:type", "<wordnet_site_108651247>"},
    TP06A     = {"[]", "?c2", "rdfs:label", "\"Paris\""},
    N         = none,
    TP01      = {tp, TP01A, N, N},
    TP02      = {tp, TP02A, N, N},
    TP03      = {tp, TP03A, N, N},
    TP04      = {tp, TP04A, N, N},
    TP05      = {tp, TP05A, N, N},
    TP06      = {tp, TP06A, N, N},
    J01       = {join, TP06, TP05, N, N},
    J02       = {join, J01, TP04, N, N},
    J03       = {join, J02, TP03, N, N},
    J04       = {join, J03, TP02, N, N},
    Q         = {join, J04, TP01, N, N},
    QT        = query_tree:spawn_process(ProcId, FS),
    BM        = {get(process_id), node()},
    StartMes  = {start, Q, QT, BM, QueryId, SessionId},
    R01       = gen_server:call({QT, FS}, StartMes),
    R02       = gen_server:call({QT, FS}, {eval}),
    DateTime  = calendar:local_time(),

    put(query_tree_pid, QT),
    put(pid_start, maps:put(QT, os:timestamp(), get(pid_start))),
    RM = {task_started, DateTime, R01, R02},
    RF = gen_server:call(SW, {record, {TN, RM}}),
    info_msg(hc_task_yg_0006, [use_query_tree], RF, 50).

%% 
%% @doc This function performs a benchmark task for the YAGO2s
%% environment. This is the equivalent query of [NW08] YAGO B1.
%% 
%% <table border="3">
%% <tr><th>Pattern</th><th>Num</th></tr>
%% 
%% <tr><td>?s yago:isLocatedIn yago:England</td><td
%% align="right">4,183</td></tr>
%% 
%% <tr><td>?s yago:livesIn ?o</td><td align="right">33,610</td></tr>
%% 
%% <tr><td>?c yago:isLocatedIn yago:England . ?s yago:livesIn
%% ?c</td><td align="right">228</td></tr>
%% 
%% <tr><td>?s yago:actedIn ?o</td><td align="right">127,502</td></tr>
%% 
%% <tr><td>?c yago:isLocatedIn yago:England . ?s yago:livesIn
%% ?c . ?s yago:actedIn ?o</td><td align="right">1</td></tr>
%% 
%% </table>
%% 
hc_task_yg_0005m() ->
    BS       = gen_server:call(node_state, {get, b3s_state_pid}),
    [FS | _] = gen_server:call(BS, {get, front_server_nodes}),
    RRM      = gen_server:call(BS, {get, result_record_max}),
    put(result_record_max, RRM),
    SW = stop_watch,
    TN = "task_yg_0005m",
    RS = gen_server:call(SW, {start, hty_prep(TN)}),
    info_msg(hc_task_yg_0005m, [use_query_tree], RS, 50),

    SessionId = "0005m",
    QueryId   = "1",
    ProcId    = hty_construct_qtid(SessionId, QueryId),
    TP01A     = {"?i1", "?c1", "<isLocatedIn>", "<England>"},
    TP02A     = {"?i2", "?a1", "<livesIn>", "?c1"},
    TP03A     = {"?i3", "?a1", "<actedIn>", "?movie"},
    TP04A     = {"?i4", "?a2", "<actedIn>", "?movie"},
    TP05A     = {"?i5", "?a2", "<livesIn>", "?c2"},
    TP06A     = {"?i6", "?c2", "<isLocatedIn>", "<England>"},
    N         = none,
    TP01      = {tp, TP01A, N, N},
    TP02      = {tp, TP02A, N, N},
    TP03      = {tp, TP03A, N, N},
    TP04      = {tp, TP04A, N, N},
    TP05      = {tp, TP05A, N, N},
    TP06      = {tp, TP06A, N, N},
    J01       = {mjoin, TP05, TP06, N, N},
    J02       = {join, J01, TP04, N, N},
    J03       = {mjoin, J02, TP03, N, N},
    J04       = {join, J03, TP02, N, N},
    Q         = {join, J04, TP01, N, N},
    QT        = query_tree:spawn_process(ProcId, FS),
    BM        = {get(process_id), node()},
    StartMes  = {start, Q, QT, BM, QueryId, SessionId},
    R01       = gen_server:call({QT, FS}, StartMes),
    R02       = gen_server:call({QT, FS}, {eval}),
    DateTime  = calendar:local_time(),

    put(query_tree_pid, QT),
    put(pid_start, maps:put(QT, os:timestamp(), get(pid_start))),
    RM = {task_started, DateTime, R01, R02},
    RF = gen_server:call(SW, {record, {TN, RM}}),
    info_msg(hc_task_yg_0005m, [], RF, 50).

%% 
%% @doc This function performs a benchmark task for the YAGO2s
%% environment. This is the equivalent query of [NW08] YAGO B1.
%% 
%% <table border="3">
%% <tr><th>Pattern</th><th>Num</th></tr>
%% 
%% <tr><td>?s yago:isLocatedIn yago:England</td><td
%% align="right">4,183</td></tr>
%% 
%% <tr><td>?s yago:livesIn ?o</td><td align="right">33,610</td></tr>
%% 
%% <tr><td>?c yago:isLocatedIn yago:England . ?s yago:livesIn
%% ?c</td><td align="right">228</td></tr>
%% 
%% <tr><td>?s yago:actedIn ?o</td><td align="right">127,502</td></tr>
%% 
%% <tr><td>?c yago:isLocatedIn yago:England . ?s yago:livesIn
%% ?c . ?s yago:actedIn ?o</td><td align="right">1</td></tr>
%% 
%% </table>
%% 
hc_task_yg_0005() ->
    hc_task_yg_0005(use_query_tree).

hc_task_yg_0005(use_query_tree) ->
    BS       = gen_server:call(node_state, {get, b3s_state_pid}),
    [FS | _] = gen_server:call(BS, {get, front_server_nodes}),
    RRM      = gen_server:call(BS, {get, result_record_max}),
    put(result_record_max, RRM),
    SW = stop_watch,
    TN = "task_yg_0005",
    RS = gen_server:call(SW, {start, hty_prep(TN)}),
    info_msg(hc_task_yg_0005, [use_query_tree], RS, 50),

    SessionId = "0005",
    QueryId   = "1",
    ProcId    = hty_construct_qtid(SessionId, QueryId),
    TP01A     = {"?i1", "?c1", "<isLocatedIn>", "<England>"},
    TP02A     = {"?i2", "?a1", "<livesIn>", "?c1"},
    TP03A     = {"?i3", "?a1", "<actedIn>", "?movie"},
    TP04A     = {"?i4", "?a2", "<actedIn>", "?movie"},
    TP05A     = {"?i5", "?a2", "<livesIn>", "?c2"},
    TP06A     = {"?i6", "?c2", "<isLocatedIn>", "<England>"},
    N         = none,
    TP01      = {tp, TP01A, N, N},
    TP02      = {tp, TP02A, N, N},
    TP03      = {tp, TP03A, N, N},
    TP04      = {tp, TP04A, N, N},
    TP05      = {tp, TP05A, N, N},
    TP06      = {tp, TP06A, N, N},
    J01       = {join, TP06, TP05, N, N},
    J02       = {join, J01, TP04, N, N},
    J03       = {join, J02, TP03, N, N},
    J04       = {join, J03, TP02, N, N},
    Q         = {join, J04, TP01, N, N},
    QT        = query_tree:spawn_process(ProcId, FS),
    BM        = {get(process_id), node()},
    StartMes  = {start, Q, QT, BM, QueryId, SessionId},
    R01       = gen_server:call({QT, FS}, StartMes),
    R02       = gen_server:call({QT, FS}, {eval}),
    DateTime  = calendar:local_time(),

    put(query_tree_pid, QT),
    put(pid_start, maps:put(QT, os:timestamp(), get(pid_start))),
    RM = {task_started, DateTime, R01, R02},
    RF = gen_server:call(SW, {record, {TN, RM}}),
    info_msg(hc_task_yg_0005, [use_query_tree], RF, 50).

%% 
%% @doc This function performs a benchmark task for the YAGO2s
%% environment.
%% 
%% <pre>
%% sparql
%% prefix yago: &lt;http://yago-knowledge.org/resource/&gt;
%% select * from &lt;http://yago-knowledge.org/resource&gt; where {
%%   ?sbj yago:startedOnDate ?obj1 .
%%   ?sbj yago:endedOnDate ?obj2 .
%% } ;
%% </pre>
%% 
hc_task_yg_0004m() ->
    BS       = gen_server:call(node_state, {get, b3s_state_pid}),
    [FS | _] = gen_server:call(BS, {get, front_server_nodes}),
    RRM      = gen_server:call(BS, {get, result_record_max}),
    put(result_record_max, RRM),
    SW = stop_watch,
    TN = "task_yg_0004m",
    RS = gen_server:call(SW, {start, hty_prep(TN)}),
    info_msg(hc_task_yg_0004m, [], RS, 50),

    SessionId = "0004m",
    QueryId   = "1",
    ProcId    = hty_construct_qtid(SessionId, QueryId),
    TP01A     = {"?id1", "?sbj", "<startedOnDate>", "?obj1"},
    TP02A     = {"?id2", "?sbj", "<endedOnDate>",   "?obj2"},
    N         = none,
    TP01      = {tp, TP01A, N, N},
    TP02      = {tp, TP02A, N, N},
    Q         = {mjoin, TP02, TP01, N, N},
    QT        = query_tree:spawn_process(ProcId, FS),
    BM        = {get(process_id), node()},
    StartMes  = {start, Q, QT, BM, QueryId, SessionId},
    R01       = gen_server:call({QT, FS}, StartMes),
    R02       = gen_server:call({QT, FS}, {eval}),
    DateTime  = calendar:local_time(),

    put(query_tree_pid, QT),
    put(pid_start, maps:put(QT, os:timestamp(), get(pid_start))),
    RM = {task_started, DateTime, R01, R02},
    RF = gen_server:call(SW, {record, {TN, RM}}),
    info_msg(hc_task_yg_0004m, [], RF, 50).

%% 
%% @doc This function performs a benchmark task for the YAGO2s
%% environment.
%% 
%% <pre>
%% sparql
%% prefix yago: &lt;http://yago-knowledge.org/resource/&gt;
%% select * from &lt;http://yago-knowledge.org/resource&gt; where {
%%   ?sbj yago:startedOnDate ?obj1 .
%%   ?sbj yago:endedOnDate ?obj2 .
%% } ;
%% </pre>
%% 
hc_task_yg_0004() ->
    hc_task_yg_0004(use_query_tree).

hc_task_yg_0004(direct) ->
    ok;

hc_task_yg_0004(use_query_tree) ->
    BS       = gen_server:call(node_state, {get, b3s_state_pid}),
    [FS | _] = gen_server:call(BS, {get, front_server_nodes}),
    RRM      = gen_server:call(BS, {get, result_record_max}),
    put(result_record_max, RRM),
    SW = stop_watch,
    TN = "task_yg_0004",
    RS = gen_server:call(SW, {start, hty_prep(TN)}),
    info_msg(hc_task_yg_0004, [use_query_tree], RS, 50),

    SessionId = "0004",
    QueryId   = "1",
    ProcId    = hty_construct_qtid(SessionId, QueryId),
    TP01A     = {"?id1", "?sbj", "<startedOnDate>", "?obj1"},
    TP02A     = {"?id2", "?sbj", "<endedOnDate>",   "?obj2"},
    N         = none,
    TP01      = {tp, TP01A, N, N},
    TP02      = {tp, TP02A, N, N},
    Q         = {join, TP02, TP01, N, N},
    QT        = query_tree:spawn_process(ProcId, FS),
    BM        = {get(process_id), node()},
    StartMes  = {start, Q, QT, BM, QueryId, SessionId},
    R01       = gen_server:call({QT, FS}, StartMes),
    R02       = gen_server:call({QT, FS}, {eval}),
    DateTime  = calendar:local_time(),

    put(query_tree_pid, QT),
    put(pid_start, maps:put(QT, os:timestamp(), get(pid_start))),
    RM = {task_started, DateTime, R01, R02},
    RF = gen_server:call(SW, {record, {TN, RM}}),
    info_msg(hc_task_yg_0004, [use_query_tree], RF, 50).

%% 
%% @doc This function performs a benchmark task for the YAGO2s
%% environment.
%% 
%% <table border="3">
%% <tr><th>Pattern</th><th>Num</th></tr>
%% 
%% <tr><td>yago:Japan ?p ?o</td><td align="right">918</td></tr>
%% 
%% <tr><td>yago:Slovenia ?p ?o</td><td align="right">688</td></tr>
%% 
%% </table>
%% 
hc_task_yg_0003h() ->
    BS       = gen_server:call(node_state, {get, b3s_state_pid}),
    [FS | _] = gen_server:call(BS, {get, front_server_nodes}),
    RRM      = gen_server:call(BS, {get, result_record_max}),
    put(result_record_max, RRM),
    SW = stop_watch,
    TN = "task_yg_0003h",
    RS = gen_server:call(SW, {start, hty_prep(TN)}),
    info_msg(hc_task_yg_0003h, [], RS, 50),

    SessionId = "0003h",
    QueryId   = "1",
    ProcId    = hty_construct_qtid(SessionId, QueryId),
    TP01A     = {"?id1", "<Japan>", "?prd", "?obj1"},
    TP02A     = {"?id2", "<Slovenia>", "?prd",   "?obj2"},
    N         = none,
    TP01      = {tp, TP01A, N, N},
    TP02      = {tp, TP02A, N, N},
    Q         = {hjoin, TP02, TP01, N, N},
    QT        = query_tree:spawn_process(ProcId, FS),
    BM        = {get(process_id), node()},
    StartMes  = {start, Q, QT, BM, QueryId, SessionId},
    R01       = gen_server:call({QT, FS}, StartMes),
    R02       = gen_server:call({QT, FS}, {eval}),
    DateTime  = calendar:local_time(),

    put(query_tree_pid, QT),
    put(pid_start, maps:put(QT, os:timestamp(), get(pid_start))),
    RM = {task_started, DateTime, R01, R02},
    RF = gen_server:call(SW, {record, {TN, RM}}),
    info_msg(hc_task_yg_0003h, [], RF, 50).

%% 
%% @doc This function performs a benchmark task for the YAGO2s
%% environment.
%% 
%% <table border="3">
%% <tr><th>Pattern</th><th>Num</th></tr>
%% 
%% <tr><td>yago:Japan ?p ?o</td><td align="right">918</td></tr>
%% 
%% <tr><td>yago:Slovenia ?p ?o</td><td align="right">688</td></tr>
%% 
%% </table>
%% 
hc_task_yg_0003() ->
    hc_task_yg_0003(use_query_tree).

hc_task_yg_0003(use_query_tree) ->
    BS       = gen_server:call(node_state, {get, b3s_state_pid}),
    [FS | _] = gen_server:call(BS, {get, front_server_nodes}),
    RRM      = gen_server:call(BS, {get, result_record_max}),
    put(result_record_max, RRM),
    SW = stop_watch,
    TN = "task_yg_0003",
    RS = gen_server:call(SW, {start, hty_prep(TN)}),
    info_msg(hc_task_yg_0003, [use_query_tree], RS, 50),

    SessionId = "0003",
    QueryId   = "1",
    ProcId    = hty_construct_qtid(SessionId, QueryId),
    TP01A     = {"?id1", "<Japan>", "?prd", "?obj1"},
    TP02A     = {"?id2", "<Slovenia>", "?prd",   "?obj2"},
    N         = none,
    TP01      = {tp, TP01A, N, N},
    TP02      = {tp, TP02A, N, N},
    Q         = {join, TP02, TP01, N, N},
    QT        = query_tree:spawn_process(ProcId, FS),
    BM        = {get(process_id), node()},
    StartMes  = {start, Q, QT, BM, QueryId, SessionId},
    R01       = gen_server:call({QT, FS}, StartMes),
    R02       = gen_server:call({QT, FS}, {eval}),
    DateTime  = calendar:local_time(),

    put(query_tree_pid, QT),
    put(pid_start, maps:put(QT, os:timestamp(), get(pid_start))),
    RM = {task_started, DateTime, R01, R02},
    RF = gen_server:call(SW, {record, {TN, RM}}),
    info_msg(hc_task_yg_0003, [], RF, 50).

%% 
%% @doc This function performs a benchmark task for the YAGO2s
%% environment.
%% 
hc_task_yg_0002() ->
    hc_task_yg_0002(direct_variable_columns).

hc_task_yg_0002(use_query_tree) ->
    ok;

hc_task_yg_0002(direct_variable_columns) ->
    SW = stop_watch,
    TN = "task_yg_0002",
    BM   = {list_to_atom(TN), node()},
    MEV  = {eval, []},
    MEM  = {empty, BM},
    RS = gen_server:call(SW, {start, hty_prep(TN)}),
    info_msg(hc_task_yg_0002, [direct_variable_columns], RS, 50),

    P01 = "<startedOnDate>",
    P02 = "<endedOnDate>",

    SI01 = "qn-0002",
    QI01 = "1",
    QN01 = "1",
    QA01 = hty_construct_pid(SI01, QI01, QN01),
    TP01o = {"?id1", "?sbj", P01, "?obj1"},
    case ?STRING_ID_CODING_METHOD of
	string_integer ->
	    TP01 = string_id:encode_triple_pattern(TP01o)
    end,
    VP01 = #{"?id1" => 1, "?sbj" => 2, "?obj1" => 4},

    QN02 = "2",
    QA02 = hty_construct_pid(SI01, QI01, QN02),
    TP02o = {"?id2", "?sbj", P02, "?obj2"},
    case ?STRING_ID_CODING_METHOD of
	string_integer ->
	    TP02 = string_id:encode_triple_pattern(TP02o),
	    db_interface:db_disconnect()
    end,
    VP02 = #{"?id2" => 1, "?sbj" => 2, "?obj2" => 4},

    QN03 = "3",
    QA03 = hty_construct_pid(SI01, QI01, QN03),
    GP03 = maps:from_list([{QN01, TP01}, {QN02, TP02}]),
    VP03 = #{"?Id1"  => [{QN01, 1}],
    	     "?sbj"  => [{QN01, 2}, {QN02, 2}],
    	     "?obj1" => [{QN01, 4}],
    	     "?id2"  => [{QN02, 1}],
    	     "?obj2" => [{QN02, 4}]},
    JV03 = ["?sbj"],

    QS1 = lists:flatten(io_lib:format("(~s) ~s ~s ~s", tuple_to_list(TP01o))),
    QS2 = lists:flatten(io_lib:format("(~s) ~s ~s ~s", tuple_to_list(TP02o))),
    QSL = [lists:flatten(QS1), lists:flatten(QS2)],
    put(query_string, QSL),

    DSL = hty_get_one_nodes_for_each_column(),
    DS1 = lists:nth(1, DSL),
    FO = fun (N) -> {QA01, N} end,
    FI = fun (N) -> {QA02, N} end,
    FE = fun (_) -> gen_server:cast({QA03, DS1}, MEM) end,
    OL03 = lists:map(FO, DSL),
    IL03 = lists:map(FI, DSL),
    FR = fun (N) ->
		 hty_restart_tpqn(QN01, QI01, SI01, TP01, VP01, N, {QA03, DS1}, outer),
		 hty_restart_tpqn(QN02, QI01, SI01, TP02, VP02, N, {QA03, DS1}, inner)
	 end,

    lists:map(FR, DSL),
    hty_restart_jqn(QN03, QI01, SI01, GP03, VP03, JV03, DS1, BM, OL03, IL03),
    gen_server:call({QA03, DS1}, MEV),
    lists:map(FE, lists:duplicate(10, a)),

    RF = gen_server:call(SW, {record, {TN, {task_started, ok}}}),
    info_msg(hc_task_yg_0002, [direct_variable_columns], RF, 50).

%% 
%% @doc This function performs a benchmark task for the YAGO2s
%% environment.
%% 
hc_task_yg_0001() ->
    hc_task_yg_0001(direct_variable_columns).

hc_task_yg_0001(use_query_tree) ->
    ok;

hc_task_yg_0001(direct) ->
    TN = "task_yg_0001",
    BM   = {list_to_atom(TN), node()},
    MEV  = {eval, []},
    MEM  = {empty, BM},
    F01  = fun (X) -> 
		   gen_server:call(X, MEV),
		   gen_server:cast(X, MEM)
	   end,

    SW = stop_watch,
    RS = gen_server:call(SW, {start, hty_prep(TN)}),
    info_msg(hc_task_yg_0001, [direct], RS, 50),

    NC = lists:nth(1, hty_get_nodes_from_column_id(1)),
    NI = lists:nth(1, hty_get_nodes_from_column_id(2)),
    NE = lists:nth(1, hty_get_nodes_from_column_id(3)),
    NL = lists:nth(1, hty_get_nodes_from_column_id(4)),
    NN = lists:nth(1, hty_get_nodes_from_column_id(5)),

    SI01 = "qn-0001",
    QI01 = "1",
    QN01 = "1",
    QA01 = hty_construct_pid(SI01, QI01, QN01),
    TP01o = {"?id", "?sbj", "<startedOnDate>", "?obj"},
    case ?STRING_ID_CODING_METHOD of
	string_integer ->
	    TP01 = string_id:encode_triple_pattern(TP01o),
	    db_interface:db_disconnect()
    end,
    VP01 = #{"?id" => 1, "?sbj" => 2, "?obj" => 4},
    QL01 = [{QA01, NC}, {QA01, NI}, {QA01, NE}, {QA01, NL}, {QA01, NN}],

    hty_restart_tpqn(QN01, QI01, SI01, TP01, VP01, NC, BM, outer),
    hty_restart_tpqn(QN01, QI01, SI01, TP01, VP01, NI, BM, outer),
    hty_restart_tpqn(QN01, QI01, SI01, TP01, VP01, NE, BM, outer),
    hty_restart_tpqn(QN01, QI01, SI01, TP01, VP01, NL, BM, outer),
    hty_restart_tpqn(QN01, QI01, SI01, TP01, VP01, NN, BM, outer),
    lists:map(F01, QL01),

    RF = gen_server:call(SW, {record, {TN, {task_started, ok}}}),
    info_msg(hc_task_yg_0001, [], RF, 50);

hc_task_yg_0001(direct_variable_columns) ->
    TN  = "task_yg_0001",
    BM  = {list_to_atom(TN), node()},
    MEV = {eval, []},
    MEM = {empty, BM},

    SW = stop_watch,
    RS = gen_server:call(SW, {start, hty_prep(TN)}),
    info_msg(hc_task_yg_0001, [direct_variable_columns], RS, 50),

    SI01 = "qn-0001",
    QI01 = "1",
    QN01 = "1",
    QA01 = hty_construct_pid(SI01, QI01, QN01),
    TP01o = {"?id", "?sbj", "<startedOnDate>", "?obj"},
    case ?STRING_ID_CODING_METHOD of
	string_integer ->
	    TP01 = string_id:encode_triple_pattern(TP01o),
	    db_interface:db_disconnect()
    end,
    VP01 = #{"?id" => 1, "?sbj" => 2, "?obj" => 4},

    QS = io_lib:format("(~s) ~s ~s ~s", tuple_to_list(TP01o)),
    put(query_string, [lists:flatten(QS)]),

    F = fun (N) -> 
		hty_restart_tpqn(QN01, QI01, SI01, TP01, VP01, N, BM, outer),
		gen_server:call({QA01, N}, MEV),
		gen_server:cast({QA01, N}, MEM)
	end,
    lists:map(F, hty_get_one_nodes_for_each_column()),

    RF = gen_server:call(SW, {record, {TN, {task_started, ok}}}),
    info_msg(hc_task_yg_0001, [direct_variable_columns], RF, 50).

hty_get_one_nodes_for_each_column() ->
    BS = b3s_state,
    CR = gen_server:call(BS, {get, clm_row_conf}),
    KL = maps:keys(CR),
    F = fun (C) -> lists:nth(1, hty_get_nodes_from_column_id(C)) end,
    lists:map(F, KL).

hty_get_nodes_from_column_id(ColumnId) ->
    BS = b3s_state,
    DS = gen_server:call(BS, {get, data_server_nodes}),
    F = fun ({N, C}) when C == ColumnId -> {true, N}; (_) -> false end,
    lists:filtermap(F, DS).

hty_prep(TN) ->
    put(process_id,  list_to_atom(TN)),
    put(result_freq, #{}),
    put(pid_start,   #{}),
    put(pid_elapse,  #{}),
    put(pid_results, #{}),
    put(num_calls,   #{}),

    {Dt, Tm} = calendar:local_time(),
    DtArray  = [element(1, Dt), element(2, Dt), element(3, Dt)],
    TmArray  = [element(1, Tm), element(2, Tm), element(3, Tm)],
    DtStr    = io_lib:format("~4..0B~2..0B~2..0B", DtArray),
    TmStr    = io_lib:format("~2..0B~2..0B~2..0B", TmArray),
    TaskName = lists:flatten(TN ++ "_" ++ DtStr ++ "_" ++ TmStr),

    TNAtom   = list_to_atom(TaskName),
    put(last_task, TNAtom),
    TNAtom.

hty_restart_tpqn(Nid, Qid, Sid, TrPat, VarPos, Node, ParPid, Side) ->
    hty_stop_qn(hty_construct_pid(Sid, Qid, Nid), Node),
    hty_start_tpqn(Nid, Qid, Sid, TrPat, VarPos, Node, ParPid, Side).

hty_start_tpqn(Nid, Qid, Sid, TrPat, VarPos, Node, ParPid, Side) ->
    A    = [Nid, Qid, Sid, TrPat, VarPos, Node, ParPid, Side],
    SW   = stop_watch,
    QA01 = hty_construct_pid(Sid, Qid, Nid),
    QN01 = {QA01, Node},
    R01A = tp_query_node:spawn_process(QA01, Node),
    N    = none,
    MS01 = {start, Nid, Qid, Sid, R01A, TrPat, N, N, ParPid, VarPos, Side},
    R01B = gen_server:call(QN01, MS01),

    PS   = pid_start,
    put(PS,  maps:put(QN01, os:timestamp(), get(PS))),

    R  = {hty_start_tpqn, QN01, {R01A, R01B}},
    RS = gen_server:call(SW, {record, R}),
    info_msg(hty_start_tpqn, A, RS, 80).

hty_restart_jqn(Nid, Qid, Sid, GrPat, VarPos, JoiVar,
		Node, ParPid, OutPid, InnPid) ->
    hty_stop_qn(hty_construct_pid(Sid, Qid, Nid), Node),
    hty_start_jqn(Nid, Qid, Sid, GrPat, VarPos, JoiVar,
		  Node, ParPid, OutPid, InnPid).

hty_start_jqn(Nid, Qid, Sid, GrPat, VarPos, JoiVar,
	      Node, ParPid, OutPid, InnPid) ->
    A    = [Nid, Qid, Sid, GrPat, VarPos, JoiVar,
	    Node, ParPid, OutPid, InnPid],
    SW   = stop_watch,
    QA01 = hty_construct_pid(Sid, Qid, Nid),
    QN01 = {QA01, Node},
    R01A = join_query_node:spawn_process(QA01, Node),
    N    = none,
    MS01 = {start, Nid, Qid, Sid, R01A,
	    GrPat, N, N,
	    ParPid, OutPid, InnPid, VarPos, JoiVar},
    R01B = gen_server:call(QN01, MS01),

    PS   = pid_start,
    put(PS,  maps:put(QN01, os:timestamp(),      get(PS))),

    R  = {hty_start_jqn, QN01, {R01A, R01B}},
    RS = gen_server:call(SW, {record, R}),
    info_msg(hty_start_jqn, A, RS, 80).

hty_stop_qn(Id, Node) ->
    A    = [Id, Node],
    B3SN = {b3s, Node},
    R01A = supervisor:terminate_child(B3SN, Id),
    R01B = supervisor:delete_child(B3SN, Id),
    info_msg(hty_stop_qn, A, {R01A, R01B}, 80).

hty_construct_pid(SessionId, QueryId, NodeId) ->
    Things = [SessionId, '-', QueryId, '-', NodeId],
    list_to_atom(lists:concat(Things)).

hty_construct_qtid(SessionId, QueryId) ->
    Things = ['qt-', SessionId, '-', QueryId],
    list_to_atom(lists:concat(Things)).

%% 
%% @doc This function performs iteration of data stream.
%% 
%% @spec hc_data_outer(Pid::node_state:ns_pid(),
%% Graph::join_query_node:qn_graph() | end_of_stream) -> ok
%% 
hc_data_outer(Pid, end_of_stream) ->
    A  = [Pid, end_of_stream],

    PS   = pid_start,
    PE   = pid_elapse,
    S    = maps:get(Pid, get(PS)),
    E    = timer:now_diff(os:timestamp(), S),
    put(PE, maps:put(Pid, E, get(PE))),

    R  = {outer_data_stream_terminated, E, Pid},
    SW = stop_watch,
    RS = gen_server:call(SW, {record, R}),
    info_msg(hc_data_outer, A, RS, 10);

hc_data_outer(Pid, GraphList) when is_list(GraphList) ->
    F = fun(X) -> hc_data_outer(Pid, X) end,
    R = lists:foreach(F, GraphList),
    A = [Pid, GraphList],
    info_msg(hc_data_outer, A, {ok, R}, 80);

hc_data_outer(Pid, Graph) ->
    A = [Pid, Graph],
    gen_server:cast(Pid, {empty, {get(process_id), node()}}),

    MapRF = get(result_freq),
    case maps:is_key(Pid, MapRF) of
	true  -> Freq = maps:get(Pid, MapRF);
	false -> Freq = 0
    end,
    NewRF = maps:put(Pid, Freq + 1, MapRF),
    put(result_freq, NewRF),

    MapR = get(pid_results),
    case maps:is_key(Pid, MapR) of
	true  -> List = maps:get(Pid, MapR);
	false -> List = []
    end,
    RRM = get(result_record_max),
    case length(List) of
    	L when L < RRM ->
	    NewR = maps:put(Pid, [Graph | List], MapR);
	_ ->
	    NewR = MapR
    end,
    put(pid_results, NewR),

    info_msg(hc_data_outer, A, {ok, NewRF, NewR}, 80).

%% 
%% @doc This message starts or restarts recording elapsed time for
%% specified task of specified process.
%% 
%% @spec hc_restart_record(Pid::node_state:ns_pid(), TaskId::atom())
%% -> ok
%% 
hc_restart_record(Pid, TaskId) ->
    put(pid_start, maps:put({Pid, TaskId}, os:timestamp(), get(pid_start))),
    case maps:is_key({Pid, TaskId}, get(num_calls)) of
	true ->
	    NC = maps:get({Pid, TaskId}, get(num_calls)) + 1;
	false ->
	    NC = 1
    end,
    put(num_calls, maps:put({Pid, TaskId}, NC, get(num_calls))),
    ok.

%% 
%% @doc This message stops recording elapsed time for specified task
%% of specified process.
%% 
%% @spec hc_stop_record(Pid::node_state:ns_pid(), TaskId::atom()) ->
%% ok | stop_called_before_start
%% 
hc_stop_record(Pid, TaskId) ->
    hsr_perform(Pid, TaskId, maps:is_key({Pid, TaskId}, get(pid_start))).

hsr_perform(Pid, TaskId, false) ->
    A = [Pid, TaskId, false],
    E = stop_called_before_start,
    error_msg(hsr_perform, A, E),
    E;
hsr_perform(Pid, TaskId, _) ->
    S = maps:get({Pid, TaskId}, get(pid_start)),
    E = timer:now_diff(os:timestamp(), S),
    case maps:is_key({Pid, TaskId}, get(pid_elapse)) of
	true ->
	    ES = maps:get({Pid, TaskId}, get(pid_elapse)) + E;
	false ->
	    ES = E
    end,
    put(pid_elapse, maps:put({Pid, TaskId}, ES, get(pid_elapse))),
    ok.

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
bm_test_() ->
    application:load(b3s),
    bt_site(b3s_state:get(test_mode)).

bt_site(local2) ->
    TUT  = td_unit_test,
    register(TUT, self()),

    {timeout, 100000,
     {inorder,
      [
       ?_assertMatch(ok, b3s:start()),
       ?_assertMatch(ok, b3s:bootstrap()),
       {generator, fun triple_distributor:tdt_register_local/0},
       {generator, fun triple_distributor:tdt_investigate/0},
       {generator, fun triple_distributor:tdt_flush_local/0},
       {generator, fun triple_distributor:tdt_store/0},
       {generator, fun bsl2/0},
       ?_assertMatch(ok, b3s:stop())
      ]}};

bt_site(_) ->
    [].

bsl2() ->
    case ?STRING_ID_CODING_METHOD of
	string_integer ->
	    bsl2_sid()
    end.

bsl2_sid() ->
    BSF = gen_server:call(node_state, {get, b3s_state_pid}),
    [{DSN, _}|_] = gen_server:call(BSF, {get, data_server_nodes}),
    {_, FSN} = BSF,
    BSD = {b3s_state, DSN},
    BMD = {benchmark, DSN},
    TDF = {triple_distributor, FSN},
    CRC = gen_server:call(TDF, {get_property, clm_row_conf}),
    PCM = gen_server:call(TDF, {get_property, pred_clm}),
    PFM = gen_server:call(TDF, {get_property, pred_freq}),

    NF  = not_finished,
    R01 = [FSN, DSN],
    R02 = {ok, BMD},
    R03 = maps:put({'qn-0001-1-3', DSN}, 2, #{}),
    F01 = fun (X) ->
		  GQ = {get, queue_result},
		  QR = gen_server:call({X, DSN}, GQ),
		  case QR of
		      undefined -> NF;
		      _ ->
			  QL = queue:to_list(QR),
			  FL = fun (Y) -> length(Y) end,
			  lists:sum(lists:map(FL, QL)) - 1
		  end
	  end,

    {inorder,
     [
      ?_assertMatch(ok,  gen_server:call(BSF, {put, clm_row_conf, CRC})),
      ?_assertMatch(ok,  gen_server:call(BSF, {put, pred_clm,     PCM})),
      ?_assertMatch(ok,  gen_server:call(BSF, {put, pred_freq,    PFM})),
      ?_assertMatch(ok,  gen_server:call(BSF, {clone, DSN})),
      ?_assertMatch(R01, gen_server:call(BSD, propagate)),
      {timeout, 10000,
       ?_assertMatch(R02, benchmark:start(DSN))},
      ?_assertMatch(ok,  gen_server:cast(BMD, task_local2_0001)),
      {timeout, 10000,
       ?_assertMatch(ok,  timer:sleep(10000))},
      ?_assertMatch(R03, gen_server:call(BMD, {get, result_freq})),
      ?_assertMatch(ok,  gen_server:cast(BMD, task_local2_0002)),
      {timeout, 10000,
       ?_assertMatch(ok,  timer:sleep(10000))},
      ?_assertMatch(2,   F01('qt-0002-1')),
      ?_assertMatch(ok,  gen_server:cast(BMD, task_local2_0003)),
      {timeout, 10000,
       ?_assertMatch(ok,  timer:sleep(10000))},
      ?_assertMatch(672, F01('qt-0003-1')),
      %% ?_assertMatch(642, F01('qt-0003-1')),
      ?_assertMatch(ok,  gen_server:cast(BMD, task_local2_0004)),
      {timeout, 10000,
       ?_assertMatch(ok,  timer:sleep(10000))},
      ?_assertMatch(65,  F01('qt-0004-1')),
      ?_assertMatch(ok,  gen_server:cast(BMD, task_local2_0005)),
      {timeout, 10000,
       ?_assertMatch(ok,  timer:sleep(10000))},
      ?_assertMatch(351, F01('qt-0005-1')),
      ?_assertMatch(NF,  NF)
     ]}.    

%% ====> END OF LINE <====

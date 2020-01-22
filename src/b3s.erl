%%
%% big3store main application.
%%
%% @copyright 2013-2019 UP FAMNIT and Yahoo Japan Corporation
%% @version 0.3
%% @since August, 2013
%% @author Kiyoshi Nitta <knitta@yahoo-corp.jp>
%% 
%% @see join_query_node
%% @see tp_query_node
%% @see db_interface
%% @see file_reader
%% @see triple_distributor
%% 
%% @doc Start a big3store server. This module is implemented as an
%% erlang <A
%% href="http://www.erlang.org/doc/man/application.html">application</A>. It
%% invokes a root <A
%% href="http://www.erlang.org/doc/man/supervisor.html">supervisor</A>
%% process by calling {@link invoke/0}. It also provides basic command
%% line client interfaces.
%%
%% == environment variables ==
%% 
%% This application recognises following environment variables.
%% (LINK: {@section environment variables})
%%
%% <table border="3">
%% <tr><th>Name</th><th>Type</th><th>Description</th></tr>
%%
%% <tr> <td>b3s_mode</td> <td>data_server | front_server</td>
%% <td>Behavior of this application.</td> </tr>
%%
%% <tr> <td>test_mode</td> <td>local1 | local2 | yjr6 | ...</td>
%% <td>Site type of unit tests.</td> </tr>
%%
%% <tr> <td>is_test</td> <td>boolean()</td> <td>Perform the boot
%% process in the test mode if true.</td> </tr>
%%
%% <tr> <td>benchmark_task</td> <td>atom()</td> <td>Name of benchmark
%% task to be executed.</td> </tr>
%%
%% <tr> <td>front_server_nodes</td> <td>[node()]</td> <td>List of
%% front server node names.</td> </tr>
%%
%% <tr> <td>data_server_nodes</td> <td>[{node(), integer()}]</td>
%% <td>List of node name and column of data servers.</td> </tr>
%%
%% <tr> <td>b3s_state_nodes</td> <td>[node()]</td> <td>List of node
%% names that run {@link b3s_state} processes.</td> </tr>
%% 
%% <tr> <td>triple_distributor_nodes</td> <td>[node()]</td> <td>List
%% of node names that run {@link triple_distributor} processes.</td>
%% </tr>
%% 
%% <tr> <td>column_id</td> <td>{@link
%% triple_distributor:td_column_id()}</td> <td>Column id of data
%% server. (DEPRECATED)</td> </tr>
%%
%% <tr> <td>num_of_empty_msgs</td> <td>integer()</td> <td>The number of
%% empty messages that can be sent at once.</td> </tr>
%%
%% <tr> <td>name_of_triple_tables</td> <td>[{node(), atom()}]</td>
%% <td>Names of triple tables for data server columns.</td> </tr>
%%
%% <tr> <td>name_of_pred_clm_table</td> <td>atom()</td> <td>Name of
%% pred_clm table.</td> </tr>
%%
%% <tr> <td>name_of_pred_freq_table</td> <td>atom()</td> <td>Name of
%% pred_freq table.</td> </tr>
%%
%% <tr> <td>store_report_frequency</td> <td>integer()</td>
%% <td>Report frequency of storing process.</td> </tr>
%%
%% <tr> <td>triple_id_skel</td> <td>string()</td> <td>Skeleton string
%% for generating triple id.</td> </tr>
%%
%% <tr> <td>data_files</td> <td>[string()]</td> <td>List of path
%% strings to be loaded.</td> </tr>
%%
%% <tr> <td>result_record_max</td> <td>integer()</td> <td>Max number
%% of records to be reported.</td> </tr>
%%
%% <tr> <td>{@section debug_level}</td> <td>integer()</td> <td>Debug
%% Level.</td> </tr>
%%
%% </table>
%%
%% === debug_level ===
%%
%% It must have integer() value that indicate the granularity of log
%% messages to be recorded on <A
%% href="http://www.erlang.org/doc/man/error_logger.html">error_logger</A>. The
%% greater value produce more precise messages. The 0 value restricts
%% the most of log messages. The default debug messages will be
%% produced with the value greater than 50. (LINK: {@section
%% debug_level})
%%
-module(b3s).
-behavior(application).
-behavior(supervisor).
-export([

	 bootstrap/0, bootstrap_command/0, start_stop_watch/0,
	 report_stop_watch/0, report_benchmark/0, update_modules/0,
	 start_benchmark_task/0, investigate_files/0, load_files/0,

	 invoke/0, terminate/0,
	 opt_command_line/2, is_registered_process/1,
	 start/0, stop/0,
	 init/1, start/2, stop/1
	]).
-include_lib("eunit/include/eunit.hrl").

%% 
%% @doc This function performs the bootstrapping task of destributed
%% big3store system. It is a command line interface function.
%% 
%% @spec bootstrap_command() -> none()
%% 
bootstrap_command() ->
    application:load(b3s),
    case bootstrap() of
	{error, Reason} ->
	    error_msg(bootstrap_command, [], Reason);
	R ->
	    info_msg(bootstrap_command, [], R, 80)
    end,
    gen_event:stop(error_logger),
    halt().

%% 
%% @doc This function performs the bootstrapping task of destributed
%% big3store system. All configuration parameters must be set in
%% application environment variables.
%% 
%% @spec bootstrap() -> ok | {error, Reason::term()}
%% 
bootstrap() ->
    {ok, FSNS} = application:get_env(b3s, front_server_nodes),
    {ok, DSNS} = application:get_env(b3s, data_server_nodes),
    {ok, BSNS} = application:get_env(b3s, b3s_state_nodes),
    {ok, TDNS} = application:get_env(b3s, triple_distributor_nodes),
    {ok, DALG} = application:get_env(b3s, distribution_algorithm),

    [BSN | _] = BSNS,
    [TDN | _] = TDNS,
    BS = {b3s_state, BSN},
    TD = {triple_distributor, TDN},

    R01 = supervisor:terminate_child({b3s, BSN}, b3s_state),
    R02 = supervisor:terminate_child({b3s, TDN}, triple_distributor),
    R03 = supervisor:delete_child({b3s, BSN}, b3s_state),
    R04 = supervisor:delete_child({b3s, TDN}, triple_distributor),
    R05 = supervisor:start_child({b3s, BSN}, b3s_state:child_spec()),
    R06 = supervisor:start_child({b3s, TDN}, triple_distributor:child_spec()),
    R07 = gen_server:call(BS, {put, triple_distributor_pid, TD}),
    R08 = rpc:call(TDN, triple_distributor, register_columns, [DSNS]),
    CRC = gen_server:call(TD, {get_property, clm_row_conf}),
    R09 = gen_server:call(BS, {put, clm_row_conf, CRC}),
    R10 = gen_server:call(BS, {put, front_server_nodes,       FSNS}),
    R11 = gen_server:call(BS, {put, data_server_nodes,        DSNS}),
    R12 = gen_server:call(BS, {put, b3s_state_nodes,          BSNS}),
    R13 = gen_server:call(BS, {put, triple_distributor_nodes, TDNS}),
    R14 = gen_server:call(BS, propagate),
    R15 = supervisor:start_child({b3s, TDN}, string_id:child_spec()),
    R16 = gen_server:call(TD, {put_property, distribution_algorithm, DALG}),

    case application:get_env(b3s, is_test) of
	{ok, true} ->
	    M = {omitting_for_test, bootstrap_aws},
	    info_msg(bootstrap, [], M, 10);
	_ -> bootstrap_aws()
    end,
    put(b3s_state_nodes, BSNS),
    user_interface:ui_batch_pull_queue(BSN, 'boot-fs'),

    R = [
	 {front_server_nodes,       FSNS},
	 {data_server_nodes,        DSNS},
	 {b3s_state_nodes,          BSNS},
	 {triple_distributor_nodes, TDNS},
	 R01, R02, R03, R04, R05, R06, R07, R08, R09, R10,
	 R11, R12, R13, R14, R15, R16
	],
    info_msg(bootstrap, [], R, 50).

bootstrap_aws() ->
    {ok, BSNL} = application:get_env(b3s, b3s_state_nodes),
    put(b3s_state_nodes, BSNL),
    BSN = lists:nth(1, get(b3s_state_nodes)),
    BS = {b3s_state, BSN},

    ids_fs_aws_boot(),
    AII = aws_instance_id,
    ANI = aws_node_instance_map,
    ASF = aws_spot_fleet_id,
    ANF = aws_node_fleet_map,
    case get(AII) of
	undefined ->
	    R01 = undefined;
	_ ->
	    R01 = gen_server:call(BS, {put, ANI, #{BSN => get(AII)}})
    end,
    case get(ASF) of
	undefined ->
	    R02 = undefined;
	_ ->
	    R02 = gen_server:call(BS, {put, ANF, #{BSN => get(ASF)}})
    end,

    case gen_server:call(BS, {get, distribution_algorithm}) of
	predicate_based ->
	    A = ["bucket", "load", "predicate", "dictionaries"],
	    R03 = user_interface:ui_operate_aws(A);
	_ ->
	    R03 = none
    end,

    SI = gen_server:call(BS, {get, name_of_string_id_table}),
    LA = ["bucket", "list", atom_to_list(SI)],
    R04 = user_interface:ui_operate_aws(LA),
    SF = "SELECT count(*) FROM pg_class" ++
	 " WHERE relkind = 'r' AND relname like '~s%';",
    SQ = io_lib:format(SF, [SI]),
    PC = user_interface:uops_get_connection(),
    R05 = user_interface:uops_perform_sql(PC, SQ),
    case {R04, R05} of
	{[], _} ->
	    M = io_lib:format("string id table file ~s does not found.", [SI]),
	    info_msg(bootstrap_aws, [], lists:flatten(M), 50);
	{_, {ok, _, [{<<"0">>}]}} ->
	    AA = ["bucket", "get", "string", "id", "tables"],
	    AP = ["load", "string", "id", "tables"],
	    info_msg(bootstrap_aws, [], ["aws"] ++ AA, 50),
	    user_interface:ui_operate_aws(AA),
	    info_msg(bootstrap_aws, [], ["psql"] ++ AP, 50),
	    user_interface:ui_operate_psql(AP);
	_ ->
	    M = io_lib:format("string id table ~s was already created.", [SI]),
	    info_msg(bootstrap_aws, [], lists:flatten(M), 50)
    end,

    R = [
	 R01, R02, R03, R04, R05
	],
    info_msg(bootstrap_aws, [], R, 50).

bootstrap_test_() ->
    application:load(b3s),
    {ok, FSNS} = application:get_env(b3s, front_server_nodes),
    {ok, DSNS} = application:get_env(b3s, data_server_nodes),
    {ok, BSNS} = application:get_env(b3s, b3s_state_nodes),
    {ok, TDNS} = application:get_env(b3s, triple_distributor_nodes),
    [BSN | _] = BSNS,
    [{DSN, _} | _] = DSNS,
    BS = {b3s_state, BSN},
    NS = {node_state, DSN},

    {inorder,
     [
      ?_assertMatch(ok,   application:start(b3s)),
      ?_assertMatch(ok,   bootstrap()),
      ?_assertMatch(FSNS, gen_server:call(BS, {get, front_server_nodes})),
      ?_assertMatch(DSNS, gen_server:call(BS, {get, data_server_nodes})),
      ?_assertMatch(BSNS, gen_server:call(BS, {get, b3s_state_nodes})),
      ?_assertMatch(TDNS, gen_server:call(BS, {get, triple_distributor_nodes})),
      ?_assertMatch(FSNS, gen_server:call(NS, {get, front_server_nodes})),
      ?_assertMatch(BS,   gen_server:call(NS, {get, b3s_state_pid})),
      ?_assertMatch(ok,   application:stop(b3s))
     ]}.

%% 
%% @doc Start stop watch.
%% 
%% @spec start_stop_watch() -> none()
%% 
start_stop_watch() ->
    start_stop_watch("started from b3s:start_stop_watch/0").

start_stop_watch(Label) ->
    application:load(b3s),
    NND = "ls@localhost.localdomain",
    NN = list_to_atom(opt_command_line(node_name, NND)),
    R = gen_server:call({stop_watch, NN}, {start, Label}),
    info_msg(start_stop_watch, [], R, 100),
    gen_event:stop(error_logger),
    halt().

%% 
%% @doc Report stop watch.
%% 
%% @spec report_stop_watch() -> none()
%% 
report_stop_watch() ->
    report_stop_watch("reporting from b3s:report_stop_watch/0").

report_stop_watch(Label) ->
    application:load(b3s),
    NND = "ls@localhost.localdomain",
    NN = list_to_atom(opt_command_line(node_name, NND)),
    R1 = gen_server:call({stop_watch, NN}, {record, Label}),
    R2 = gen_server:call({stop_watch, NN}, report),
    info_msg(report_stop_watch, [], {R1, R2}, 100),
    gen_event:stop(error_logger),
    halt().

%% 
%% @doc This function reports the latest benchmark result. It is
%% called from erl command.
%% 
%% @spec report_benchmark() -> none()
%% 
report_benchmark() ->
    application:load(b3s),
    invoke_update_env([{b3s_mode,             atom},
		       {test_mode,            atom},
		       {benchmark_task,       atom},
		       %% {name_of_triple_table, atom},
		       {debug_level,          integer}]),
    {ok, [FSN | _]} = application:get_env(b3s, front_server_nodes),
    {ok, Task} = application:get_env(b3s, benchmark_task),
    FSNS = atom_to_list(FSN),
    NN   = list_to_atom(opt_command_line(node_name, FSNS)),
    io:fwrite(gen_server:call({Task, NN}, report, 3600000)),
    halt().

%% 
%% @doc This function recompiles source files on alive nodes. It is
%% called from erl command.
%% 
%% @spec update_modules() -> none()
%% 
update_modules() ->
    b3s:start(),
    b3s:bootstrap(),
    BS  = b3s_state,
    R01 = gen_server:start_link({local, BS}, BS, [], []),
    {ok, [FSN | _]} = application:get_env(b3s, front_server_nodes),
    FSS = atom_to_list(FSN),
    NN  = list_to_atom(opt_command_line(node_name, FSS)),
    R02 = gen_server:call(BS, {clone, NN}),
    BSN = {BS, NN},
    R03 = gen_server:call(BSN, propagate),
    R04 = rpc:call(NN, c, c, [benchmark]),
    R05 = benchmark:stop(NN),
    R06 = benchmark:start(NN),
    R07 = calendar:local_time(),
    R   = {R01, R02, R03, R04, R05, R06, R07, NN},
    io:fwrite("update_modules(): ~p~n", [R]),

    ML =
	[
	 db_interface,
	 tp_query_node,
	 join_query_node,
	 query_tree,
	 node_state,
	 repeater,
	 stop_watch
	],
    UPDM = {update_modules, ML},
    R11 = gen_server:call({benchmark, NN}, UPDM, 300000),
    R12 = calendar:local_time(),
    io:fwrite("* recompile: ~p (~p)~n", [R11, R12]),
    halt().

%% 
%% @doc This function starts a benchmark task. It is called from erl
%% command. It recovers pred_clm from permanent storage saved by
%% {@link db_interface:db_put_map/2} using {@link
%% db_interface:db_get_map/1}.
%% 
%% @spec start_benchmark_task() -> none()
%% 
start_benchmark_task() ->
    application:load(b3s),
    invoke_update_env([{b3s_mode,             atom},
		       {test_mode,            atom},
		       {benchmark_task,       atom},
		       %% {name_of_triple_table, atom},
		       {debug_level,          integer}]),
    b3s:start(),
    b3s:bootstrap(),

    BSP = b3s_state_pid,
    TDP = triple_distributor_pid,
    DA  = distribution_algorithm,
    put(BSP, gen_server:call(node_state, {get, BSP})),
    put(TDP, gen_server:call(get(BSP), {get, TDP})),
    put(DA, gen_server:call(get(TDP), {get_property, DA})),
    case get(DA) of
	predicate_based ->
	    sbt_predicate_based();
	random -> 
	    sbt_random();
	_ ->
	    E = {unknown_distribution_algorithm, get(DA)},
	    error_msg(start_benchmark_task, [], E)
    end,
    halt().

sbt_predicate_based() ->
    BS  = b3s_state,
    R01 = gen_server:start_link({local, BS}, BS, [], []),
    {ok, [FSN | _]} = application:get_env(b3s, front_server_nodes),
    FSS = atom_to_list(FSN),
    NN  = list_to_atom(opt_command_line(node_name, FSS)),
    TPC = gen_server:call(BS, {get, name_of_pred_clm_table}),
    TPF = gen_server:call(BS, {get, name_of_pred_freq_table}),
    PC  = rpc:call(NN, db_interface, db_get_map, [TPC]),
    PF  = rpc:call(NN, db_interface, db_get_map, [TPF]),
    R02 = gen_server:call(BS, {put, pred_clm, PC}),
    R03 = gen_server:call(BS, {put, pred_freq, PF}),
    R04 = gen_server:call(BS, {clone, NN}),
    BSN = {BS, NN},
    R05 = gen_server:call(BSN, propagate),
    {ok, Task} = application:get_env(b3s, benchmark_task),
    R06 = benchmark:stop(NN, Task),
    R07 = benchmark:start(NN, Task),
    R08 = calendar:local_time(),
    F01 = "~n* preparation:~n~p~n",
    A01 = [R01, R02, R03, R04, R05, R06, R07, R08, Task],
    io:fwrite(F01, [A01]),

    BM  = {Task, NN},
    R11 = gen_server:cast(BM, Task),
    R12 = calendar:local_time(),
    F02 = "~n* ~w started '~s'.~n~p~n",
    A02 = [R11, R12],
    io:fwrite(F02, [BM, Task, A02]).

sbt_random() ->
    BS  = b3s_state,
    R01 = gen_server:start_link({local, BS}, BS, [], []),
    {ok, [FSN | _]} = application:get_env(b3s, front_server_nodes),
    FSS = atom_to_list(FSN),
    NN  = list_to_atom(opt_command_line(node_name, FSS)),
    R04 = gen_server:call(BS, {clone, NN}),
    BSN = {BS, NN},
    R05 = gen_server:call(BSN, propagate),
    {ok, Task} = application:get_env(b3s, benchmark_task),
    R06 = benchmark:stop(NN, Task),
    R07 = benchmark:start(NN, Task),
    R08 = calendar:local_time(),
    F01 = "~n* preparation:~n~p~n",
    A01 = [R01, R04, R05, R06, R07, R08, Task],
    io:fwrite(F01, [A01]),

    BM  = {Task, NN},
    R11 = gen_server:cast(BM, Task),
    R12 = calendar:local_time(),
    F02 = "~n* ~w started '~s'.~n~p~n",
    A02 = [R11, R12],
    io:fwrite(F02, [BM, Task, A02]).

%% 
%% @doc This function investigate TSV dumped triple data files. It is
%% called from erl command.
%% 
%% @spec investigate_files() -> none()
%% 
investigate_files() ->
    b3s:start(),
    b3s:bootstrap(),
    {ok, NTT} = application:get_env(b3s, name_of_triple_tables),
    {ok, [FSN|_]} = application:get_env(b3s, front_server_nodes),
    gen_server:call({b3s_state, FSN}, {put, name_of_triple_tables, NTT}),
    {ok, DFS} = application:get_env(b3s, data_files),
    put(counter, 1),
    lists:map(fun if_perform/1, DFS),
    b3s:stop(),
    halt().

if_perform(File) ->
    C  = put(counter, get(counter) + 1),
    F1 = "* investigating[~p](~s)...",
    A1 = [C, File],
    io:fwrite(F1, A1),

    {ok, [TDN|_]} = application:get_env(b3s, triple_distributor_nodes),
    RPT = {repeater, TDN},
    Pid = list_to_atom(lists:concat([file_reader_, C])),
    M   = {start, RPT, investigate_stream, File, Pid, TDN},
    R   = gen_server:cast({triple_distributor, TDN}, M),

    F2 = " done. ~p~n~p~n~p~n",
    A2 = [calendar:local_time(), M, R],
    io:fwrite(F2, A2).

%% 
%% @doc This function load TSV dumped triple data files. It is called
%% from erl command.
%% 
%% @spec load_files() -> none()
%% 
load_files() ->
    b3s:start(),
    {ok, WriteMode} = application:get_env(b3s, write_mode),
    load_files(WriteMode).

load_files(on_the_fly) ->
    {ok, [TDN|_]} = application:get_env(b3s, triple_distributor_nodes),
    PC = gen_server:call({triple_distributor, TDN}, {get_property, pred_clm}),
    {ok, [FSN|_]} = application:get_env(b3s, front_server_nodes),
    gen_server:call({b3s_state, FSN}, {put, pred_clm, PC}),
    gen_server:call({b3s_state, FSN}, propagate),
    {ok, DFS} = application:get_env(b3s, data_files),
    put(counter, 1),
    lists:map(fun lf_perform/1, DFS),
    b3s:stop(),
    halt();
load_files(postgres_bulk_load) ->
    {ok, DFS} = application:get_env(b3s, data_files),
    {ok, NSI} = application:get_env(b3s, name_of_string_id_table),
    {ok, [TDN|_]} = application:get_env(b3s, triple_distributor_nodes),
    TD = {triple_distributor, TDN},
    SI = {string_id, TDN},

    gen_server:call(TD, {put_property, write_mode, postgres_bulk_load}),
    gen_server:call(TD, {load_property, pred_clm}),
    gen_server:call(TD, {load_property, pred_freq}),
    gen_server:call(SI, {put, sid_table_name, NSI}),
    gen_server:call(SI, delete_table),
    gen_server:call(SI, {create_table, NSI}),
    gen_server:call(SI, make_index),

    put(counter, 1),
    lists:map(fun lf_perform/1, DFS),
    b3s:stop(),
    halt().

lf_perform(File) ->
    C  = put(counter, get(counter) + 1),
    F1 = "* loading[~p](~s)...",
    A1 = [C, File],
    io:fwrite(F1, A1),

    {ok, [TDN|_]} = application:get_env(b3s, triple_distributor_nodes),
    RPT = {repeater, TDN},
    Pid = list_to_atom(lists:concat([file_reader_, C])),
    M   = {start, RPT, store_stream, File, Pid, TDN},
    R   = gen_server:cast({triple_distributor, TDN}, M),

    A2 = [calendar:local_time(), M, R],
    F2 = " done. ~p~n~p~n~p~n",
    io:fwrite(F2, A2).

%% 
%% @doc Invoke command. This function should be called from erl
%% command.
%% 
%% @spec invoke() -> none()
%% 
invoke() ->
    application:load(b3s),
    invoke_update_env([{b3s_mode,             atom},
		       {test_mode,            atom},
		       {debug_level,          integer}]),
    {ok, Mode} = application:get_env(b3s, b3s_mode),
    try b3s:start() of
	ok ->
	    P = b3sroot,
	    register(P, self()),
	    M = {started, Mode, {self(), node()}},
	    info_msg(invoke, [], M, 0),
	    invoke_data_servers(Mode),
	    invoke_loop();
	R ->
	    info_msg(invoke, [], {failed, Mode, R}, 0),
	    gen_event:stop(error_logger),
	    halt()
    catch
	E ->
	    error_msg(invoke, [], {error, Mode, E}),
	    gen_event:stop(error_logger),
	    halt()
    end.

invoke_update_env([]) ->
    ok;
invoke_update_env([{Name, atom} | Rest]) ->
    {ok, E} = application:get_env(b3s, Name),
    L = atom_to_list(E),
    A = list_to_atom(opt_command_line(Name, L)),
    application:set_env(b3s, Name, A),
    invoke_update_env(Rest);
invoke_update_env([{Name, list} | Rest]) ->
    {ok, E} = application:get_env(b3s, Name),
    S = opt_command_line(Name, E),
    {ok, T, _} = erl_scan:string(S),
    {ok, L} = erl_parse:parse_term(T),
    application:set_env(b3s, Name, L),
    invoke_update_env(Rest);
invoke_update_env([{Name, string} | Rest]) ->
    {ok, E} = application:get_env(b3s, Name),
    application:set_env(b3s, Name, opt_command_line(Name, E)),
    invoke_update_env(Rest);
invoke_update_env([{Name, integer} | Rest]) ->
    {ok, E} = application:get_env(b3s, Name),
    L = integer_to_list(E),
    I = list_to_integer(opt_command_line(Name, L)),
    application:set_env(b3s, Name, I),
    invoke_update_env(Rest).

invoke_update_env_test_() ->
    application:load(b3s),
    E1 = {b3s_mode,          atom},
    %% E2 = {front_server_node, atom},
    %% E3 = {data_server_nodes, list},
    %% E4 = {column_id,         integer},
    E5 = {debug_level,       integer},
    L1 = [E1, E5],
    %% L1 = [E1, E2, E4, E5],
    %% L2 = [E1, E2, E3, E4, E5],
    {inorder,
     [
      %% ?_assertMatch(ok, invoke_update_env(L2)),
      ?_assertMatch(ok, invoke_update_env(L1))
     ]}.

invoke_data_servers(data_server_aws) ->
    info_msg(invoke_data_servers, [data_server_aws], ids_ds_aws_boot(), 50);
invoke_data_servers(data_server) ->
    info_msg(invoke_data_servers, [data_server], ok, 50);
invoke_data_servers(front_server_aws) ->
    info_msg(invoke_data_servers, [front_server_aws], ok, 50);
invoke_data_servers(front_server) ->
    info_msg(invoke_data_servers, [front_server], ok, 50).

ids_fs_aws_boot() ->
    CL1 = "curl -s http://169.254.169.254/latest/meta-data/instance-id",
    CL2 = "../aws/bin/get-spot-fleet-id.sh",
    AII = aws_instance_id,
    ASF = aws_spot_fleet_id,
    put(AII, ifab_get_id(CL1, "i-")),
    put(ASF, ifab_get_id(CL2, "sfr-")),

    R = [{AII, get(AII)}, {ASF, get(ASF)}],
    info_msg(ids_fs_aws_boot, [], R, 50),
    R.

ids_fs_aws_boot_t() ->
    CL1 = "curl -s http://169.254.169.254/latest/meta-data/instance-id",
    CL2 = "../aws/bin/get-spot-fleet-id.sh",
    AII = aws_instance_id,
    ASF = aws_spot_fleet_id,
    RS1 = ifab_get_id(CL1, "i-"),
    RS2 = ifab_get_id(CL2, "sfr-"),
    RS3 = [{AII, RS1}, {ASF, RS2}],
    RS4 = ids_fs_aws_boot(),

    [
     ?_assertMatch(RS1, get(AII)),
     ?_assertMatch(RS2, get(ASF)),
     ?_assertMatch(RS3, RS4)
    ].

ifab_get_id(Result, Prefix) ->
    R = string:trim(os:cmd(Result)),
    case string:str(R, Prefix) of
	1 -> RR = R;
	_ -> RR = "undefined"
    end,
    list_to_atom(RR).

ids_ds_aws_boot() ->
    {ok, [BSN | _]} = application:get_env(b3s, b3s_state_nodes),
    idab_wait(gen_server:call({b3s_state, BSN}, lock)).

idab_wait(ok) ->
    idab_collect_info(),
    idab_determine_info(),
    R = idab_perform_registeration(),
    {ok, [BSN | _]} = application:get_env(b3s, b3s_state_nodes),
    gen_server:call({b3s_state, BSN}, unlock),
    R;
idab_wait(_) ->
    timer:sleep(500),
    {ok, [BSN | _]} = application:get_env(b3s, b3s_state_nodes),
    idab_wait(gen_server:call({b3s_state, BSN}, lock)).

ids_ds_aws_boot_t() ->
    ANI = aws_node_instance_map,
    ANF = aws_node_fleet_map,
    DSN = data_server_nodes,
    NTT = name_of_triple_tables,
    CRC = clm_row_conf,

    [{ANI, R01}, {ANF, R02}, {DSN, R03},
     {NTT, R04}, {CRC, R05}] = ids_ds_aws_boot(),
    [
     ?_assertMatch(R01, get(ANI)),
     ?_assertMatch(R02, get(ANF)),
     ?_assertMatch(R03, get(DSN)),
     ?_assertMatch(R04, get(NTT)),
     ?_assertMatch(R05, get(CRC))
    ].

idab_collect_info() ->
    {ok, BSNS} = application:get_env(b3s, b3s_state_nodes),
    put(b3s_state_nodes, BSNS),
    [BSN | _] = BSNS,
    BS = {b3s_state, BSN},
    put(b3s_state_pid, BS),

    CL1 = "curl -s http://169.254.169.254/latest/meta-data/instance-id",
    CL2 = "../aws/bin/get-spot-fleet-id.sh",
    DCL = list_to_atom(lists:nth(1, string:split(atom_to_list(node()), "@"))),

    put(aws_instance_id, ifab_get_id(CL1, "i-")),
    put(aws_spot_fleet_id, ifab_get_id(CL2, "sfr-")),
    put(ds_column_label, DCL),
    put(ds_column_table_name,
	list_to_atom(lists:flatten(io_lib:format("ts_~s", [DCL])))),

    ANI = aws_node_instance_map,
    ANF = aws_node_fleet_map,
    DSN = data_server_nodes,
    NTT = name_of_triple_tables,
    CRC = clm_row_conf,

    put(ANI, gen_server:call(BS, {get, ANI})),
    put(ANF, gen_server:call(BS, {get, ANF})),
    put(DSN, gen_server:call(BS, {get, DSN})),
    put(NTT, gen_server:call(BS, {get, NTT})),
    put(CRC, gen_server:call(BS, {get, CRC})),

    T = gen_server:call(BS, {get, triple_distributor_nodes}),
    put(triple_distributor_nodes, T),
    put(triple_distributor_pid, {triple_distributor, lists:nth(1, T)}).

idab_determine_info() ->
    N = node(),
    DCI = ds_column_id,
    CRC = clm_row_conf,
    F1 = fun (_, V) ->
		 case V of
		     N -> true;
		     _ -> false
		 end
	 end,
    F2 = fun (C, V) ->
		 case maps:size(maps:filter(F1, V)) of
		     0 -> nop;
		     _ -> put(DCI, C)
		 end
	 end,
    maps:map(F2, get(CRC)),
    case get(DCI) of
	undefined ->
	    put(DCI, maps:size(get(CRC)) + 1);
	_ ->
	    put(data_server_rebooted, true)
    end,

    AII = aws_instance_id,
    ANI = aws_node_instance_map,
    case get(AII) of
	undefined -> nil;
	_ ->
	    case get(ANI) of
		undefined ->
		    put(ANI, #{node() => get(AII)});
		_ ->
		    put(ANI, maps:put(node(), get(AII), get(ANI)))
	    end
    end,
    ASF = aws_spot_fleet_id,
    ANF = aws_node_fleet_map,
    case get(AII) of
	undefined -> nil;
	_ ->
	    case get(ANF) of
		undefined ->
		    put(ANF, #{node() => get(ASF)});
		_ ->
		    put(ANF, maps:put(node(), get(ASF), get(ANF)))
	    end
    end,
    DSN = data_server_nodes,
    put(DSN, lists:append(get(DSN), [{node(), get(DCI)}])),
    NTT = name_of_triple_tables,
    put(NTT, lists:append(get(NTT), [{node(), get(ds_column_table_name)}])).

idab_perform_registeration() ->
    BS = get(b3s_state_pid),
    TD = get(triple_distributor_pid),

    ANI = aws_node_instance_map,
    ANF = aws_node_fleet_map,
    DSN = data_server_nodes,
    NTT = name_of_triple_tables,
    CRC = clm_row_conf,

    gen_server:call(BS, {put, ANI, get(ANI)}),
    gen_server:call(BS, {put, ANF, get(ANF)}),
    gen_server:call(BS, {put, DSN, get(DSN)}),
    gen_server:call(BS, {put, NTT, get(NTT)}),

    case get(data_server_rebooted) of
	true -> nop;
	_ ->
	    CI = get(ds_column_id),
	    RN = {register_node, CI, node()},
	    {registered, {{CI, _}, _}} = gen_server:call(TD, RN),
	    put(CRC, gen_server:call(TD, {get_property, CRC})),
	    gen_server:call(BS, {put, CRC, get(CRC)})
    end,
    gen_server:call({node_state, node()},
		    {put, column_id, get(ds_column_id)}),

    user_interface:ui_batch_pull_queue(node(), 'boot-ds'),

    R = [{ANI, get(ANI)}, {ANF, get(ANF)}, {DSN, get(DSN)},
	 {NTT, get(NTT)}, {CRC, get(CRC)}],
    info_msg(idab_perform_registeration, [], R, 50),
    R.

invoke_data_servers_test_() ->
    application:load(b3s),
    ids_test_mode(b3s_state:get(test_mode)).

ids_test_mode(local1) ->
    {inorder,
     [
      ?_assertMatch(ok, application:start(b3s)),
      {generator, fun()-> ids_fs_aws_boot_t() end},
      ?_assertMatch(ok, b3s:bootstrap()),
      {generator, fun()-> ids_ds_aws_boot_t() end},
      ?_assertMatch(ok, application:stop(b3s))
     ]};

ids_test_mode(_) ->
    [].

%% 
%% @doc Main loop for waiting `terminate' signal.
%% 
%% @spec invoke_loop() -> none()
%% 
invoke_loop() ->
    receive
	terminate ->
	    {ok, Mode} = application:get_env(b3s, b3s_mode),
	    terminate_children(),
	    b3s:stop(),
	    mnesia:stop(),

	    M = {terminate, Mode, {self(), node()}},
	    info_msg(invoke_loop, [], M, 0),
	    gen_event:stop(error_logger),
	    halt();
	M ->
	    info_msg(invoke_loop, [], M, 0),
	    invoke_loop()
    end.

%% 
%% @doc Terminates child processes.
%% 
terminate_children() ->
    tc_perform(supervisor:which_children(b3s)).

tc_perform([{Id, Child, Type, Modules} | ChiList]) ->
    C = {Id, Child, Type, Modules},
    M = {terminating_child_processes, C},
    A = [C | ChiList],

    info_msg(tc_perform, A, M, 0),
    ok = supervisor:terminate_child(b3s, Id),
    tc_perform(ChiList);

tc_perform([]) ->
    ok.

%% 
%% @doc Terminate application server. This function should be called
%% from erl command. It may take following command line options.
%%
%% <dl>
%%   <dt><tt>-node_name &lt;name@host&gt;</tt></dt>
%%   <dd>Node name specification of Erlang runtime system on which
%%   test suite processes are running.</dd>
%% </dl>
%% 
%% @spec terminate() -> none()
%% 
terminate() ->
    NND = "ls@localhost.localdomain",
    NN = list_to_atom(opt_command_line(node_name, NND)),
    B3S = {b3sroot, NN},
    B3S ! terminate,
    M = {bye, B3S},

    application:load(b3s),
    info_msg(terminate, [], M, 0),
    gen_event:stop(error_logger),
    halt().

%% 
%% @doc Start as an <A
%% href="http://www.erlang.org/doc/man/application.html">application</A>.
%% 
start() ->
    application:start(b3s).

%% 
%% @doc Stop the <A
%% href="http://www.erlang.org/doc/man/application.html">application</A>.
%% 
stop() ->
    application:stop(b3s).

%% ======================================================================
%% 
%% supervisor behavior
%% 

%% 
%% @doc <A
%% href="http://www.erlang.org/doc/man/supervisor.html">Supervisor</A>
%% callback.
%% 
init(Args) ->
    VL = [
	  b3s_mode,
	  test_mode,
	  benchmark_task,
	  front_server_nodes,
	  data_server_nodes,
	  b3s_state_nodes,
	  triple_distributor_nodes,
	  num_of_empty_msgs,
	  name_of_triple_tables,
	  name_of_pred_clm_table,
	  name_of_pred_freq_table,
	  store_report_frequency,
	  triple_id_skel,
	  result_record_max,
	  debug_level
	 ],
    {ok, Mode} = application:get_env(b3s, b3s_mode),
    mnesia:start(),
    init(Args, init_check_env(VL), Mode).

init(_, undef, _) ->
    ignone;

init(Args, ok, _) ->
    CS = [
	  node_state:child_spec(),
	  db_writer:child_spec(),
	  repeater:child_spec(),
	  stop_watch:child_spec()
	 ],
    R = {ok, {{one_for_one, 100, 10}, CS}},

    info_msg(init, [Args, ok], R, 0),
    R.

init_test_() ->
    {inorder,
     [
      ?_assertMatch(ok, application:start(b3s)),
      ?_assertMatch(ok, application:stop(b3s))
     ]}.

%% 
%% @doc Check environment variables.
%% 
init_check_env([]) ->
    F = "b3s:init_check_env: ~w = ~w~n",
    R = ok,
    error_logger:info_msg(F, [all, R]),
    R;

init_check_env([Var | EnvVarList]) ->
    AGE = application:get_env(Var),
    init_check_env(AGE, [Var | EnvVarList]).

init_check_env({ok, Val}, [Var | EnvVarList]) ->
    F = "b3s:init_check_env: ~w = ~p~n",
    error_logger:info_msg(F, [Var, Val]),
    init_check_env(EnvVarList);

init_check_env(_, [Var | _]) ->
    F = "b3s:init_check_env: ~w = ~w~n",
    R = undef,
    error_logger:error_msg(F, [Var, R]),
    R.

init_check_env_test_() ->
    [
     ?_assertMatch(undef, init_check_env([debug_level])),
     ?_assertMatch(undef, init_check_env([qwe])),
     ?_assertMatch(ok, init_check_env([]))
    ].

%% ======================================================================
%% 
%% application behavior
%% 

%% 
%% @doc Start the <A
%% href="http://www.erlang.org/doc/man/application.html">application</A>. <A
%% href="http://www.erlang.org/doc/man/application.html">application</A>
%% callback.
%% 
start(StartType, StartArgs) ->

    %% find the way to detect stopping errors. [TODO]
    %% mnesia:start(),

    R = supervisor:start_link({local, b3s}, b3s, []),
    info_msg(start, [StartType, StartArgs], R, 0),
    R.

%% 
%% @doc Start the <A
%% href="http://www.erlang.org/doc/man/application.html">application</A>.
%% <A
%% href="http://www.erlang.org/doc/man/application.html">application</A>
%% callback.
%% 
stop(State) ->

    %% find the way to detect stopping errors. [TODO]
    %% mnesia:stop(),

    R = ok,
    info_msg(stop, [State], R, 0),
    R.

%% ======================================================================
%% 
%% utilities
%% 

%% 
%% @doc Get a command line option as string.
%% 
%% @spec opt_command_line(atom(), string()) -> string()
%% 
opt_command_line(OptName, DefaultValue) ->
    IGA = init:get_argument(OptName),
    if
	IGA == error ->
	    DefaultValue;
	true ->
	    {ok, [[OptValue]]} = IGA,
	    OptValue
    end.

%% 
%% @doc Check given registered name is an active process or not.
%% 
%% @spec is_registered_process(atom()) -> true | false
%% 
is_registered_process(Name) ->
    case whereis(Name) of
	undefined ->
	    false;
	Pid ->
	    is_process_alive(Pid)
    end.

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

%% ====> END OF LINE <====

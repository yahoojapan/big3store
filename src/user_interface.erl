%%
%% User Interface
%%
%% @copyright 2015-2016 UP FAMNIT and Yahoo! Japan Corporation
%% @version 0.3
%% @since September, 2015
%% @author Iztok Savnik <iztok.savnik@famnit.upr.si>
%% 
%% @see b3s
%% 
%% @doc This module provides functions for implementing user interafce
%% of entire big3store system.
%% 
%% @type ui_state() = maps:map(). Map structure
%% that manages properties for operating the gen_server process.
%%
-module(user_interface).
-behavior(gen_server).
-export(
   [

    main/0, init/1, handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3, spawn_process/2, child_spec/1

   ]).
-include_lib("eunit/include/eunit.hrl").

%% ======================================================================
%% 
%% gen_server behavior
%% 

%% 
%% init/1
%% 
%% @doc Initialize a user_interface process.
%% 
%% @spec init([]) -> {ok, ui_state()}
%% 
init([]) ->
    process_flag(trap_exit, true),
    State = #{wait => true, pid => self()},
    info_msg(init, [], State, -1),
    {ok, State}.

%% 
%% handle_call/3
%% 
%% @doc Handle synchronous query requests.
%% 
%% @spec handle_call(term(), {pid(), term()}, ui_state()) -> {reply, term(), ui_state()}
%% 
handle_call({get, all}, _, State) ->
    hc_restore_pd(get(created), State),
    info_msg(handle_call, [get, all], get(), 10),
    {reply, get(), hc_save_pd()};

handle_call({get, Name}, _, State) ->
    hc_restore_pd(get(created), State),
    info_msg(handle_call, [get, Name], get(Name), 10),
    {reply, get(Name), hc_save_pd()};

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
%% @spec handle_cast(term(), ui_state()) -> {noreply, ui_state()}
%% 
handle_cast({data_outer, From, Block}, State) ->
    hc_restore_pd(get(created), State),
    hc_data_outer(From, Block),
    {noreply, hc_save_pd()};

%% default
handle_cast(Request, State) ->
    R = {unknown_request, Request},
    error_msg(handle_cast, [get(self), Request, State], R),
    {noreply, hc_save_pd()}.

%% 
%% @doc Restore process dictionaries from state map structure.
%% 
%% @spec hc_restore_pd([{atom(), term()}] | undefined, ui_state()) -> ok
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
%% @spec hc_save_pd() -> ui_state()
%% 
hc_save_pd() ->
    maps:from_list(get()).

%% 
%% handle_info/2
%% 
%% @doc Handle exceptional query requests. 
%% 
%% @spec handle_info(term(), ui_state()) -> {noreply, ui_state()}
%% 
handle_info(_Info, State) ->
    {noreply, State}.

%% 
%% terminate/2
%% 
%% @doc Process termination. 
%% 
%% @spec terminate(term(), ui_state()) -> none()
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
%% @spec code_change(term(), ui_state(), term()) -> {ok, ui_state()}
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

%% 
%% @doc Convert contents of file QF to erlang term. 
%% 
%% @spec file_to_term(QF::string()) -> ok|fail
%% 
file_to_term(QF) ->
    %% cat file to variable
    QS = os:cmd("cat "++QF),

    %% parse and convert QS to erlang structure
    {ok,Tokens,_} = erl_scan:string(QS),
%   io:fwrite("query-tokens=~70p~n", [Tokens]),
    {ok,Term} = erl_parse:parse_term(Tokens),
    io:fwrite("query term=~70p~n", [Term]),
    Term.

%% ======================================================================
%% 
%% handling session
%% 

%% 
%% @doc Create query tree for parameter Query. Query tree is created 
%% inside the session of user_interface process. Initiate evaluation
%% of spawned query tree, 
%% 
%% @spec spawn_query_tree(Query::query_tree:qt_query()) ->
%% node_state:ns_pid()|fail
%% 
spawn_query_tree(Query) ->
    %% determine session and tree id
    TreeId = get(tree_id),
    put(tree_id, integer_to_list(list_to_integer(TreeId)+1)),
    SessionId = get(session_id),

    %% spawn query tree process
    ProcId = list_to_atom("qt-"++SessionId++"-"++TreeId),
    Node = lists:nth(1, get(b3s_state_nodes)),
    QT = query_tree:spawn_process(ProcId, Node),
%    QT = query_tree:spawn_process(ProcId, 'b3ss01@shu.local.si'),
%   info_msg(spawn_query_tree, [get(self), {query_tree_pid, QT}, {query, Query}, get(state)], debug_check, 50),

    %% send start and eval to query tree
    S1 = {start, Query, QT, get(self), TreeId, SessionId},
    gen_server:call(QT, S1),
    gen_server:call(QT, {eval}), 
    QT.

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
    GSOpt = [{local, Id}, user_interface, [], []],
    StartFunc = {gen_server, start_link, GSOpt},
    Restart = permanent,
    Shutdwon = 1000,
    Type = worker,
    Modules = [user_interface],

    {Id, StartFunc, Restart, Shutdwon, Type, Modules}.

%% 
%% @doc Spawns process which is an instance of given module with given identifier 
%% at given node.
%% 
%% @spec spawn_process( Id::atom(), Node::node() ) -> node_state:ns_pid()
%% 
spawn_process(Id, Node) -> 
    ChildSpec = user_interface:child_spec(Id),
    supervisor:start_child({b3s, Node}, ChildSpec),
    {Id, Node}.

%% ======================================================================
%% 
%% handle call/cast implementation
%% 

%% 
%% hc_data_outer/2
%%
%% @doc Processing data message from root query node of query tree.
%% 
%% @spec hc_data_outer(pid(), query_node:qn_graph()) -> ok
%% 
hc_data_outer(From, Block) ->

    %% print block of graphs to stdout
    print_block(Block),

    info_msg(hc_data_outer, [get(self), {from,From}, {block,Block}, get(state)], block_stored_in_result, 50),
    ok.

%% 
%% print_block/2
%%
%% @doc Print block of graphs to terminal. 
%% 
%% @spec print_block(Block::[query_node:qn_graph()]) -> ok
%% 
print_block(Block) ->
    pb_perform(Block, 1).

pb_perform(no_data, _) -> 
    ok;
pb_perform([], _) -> 
    ok;
pb_perform([end_of_stream], _) -> 
    ok;
pb_perform([G|L], Count) -> 
    io:fwrite("\n### graph ~4..0w ###\n\n", [Count]),
    F = "| ~5s | ~-20s | ~-30s | ~-50s | ~-30s | ~-50s |\n",
    Hqi = 'QN Id',
    Htn = string:chars($ , 5) ++ "Table Name",
    Hti = string:chars($ , 10) ++ "Triple Id",
    Hsb = string:chars($ , 21) ++ "Subject",
    Hpd = string:chars($ , 11) ++ "Predicate",
    Hob = string:chars($ , 22) ++ "Object",
    io:fwrite(F, [Hqi, Htn, Hti, Hsb, Hpd, Hob]),
    Sqi = string:chars($-, 4) ++ ":",
    Stn = string:chars($-, 20),
    Sti = string:chars($-, 30),
    Ssb = string:chars($-, 50),
    Spd = string:chars($-, 30),
    Sob = string:chars($-, 50),
    io:fwrite(F, [Sqi, Stn, Sti, Ssb, Spd, Sob]),

    pb_graph(G),
    pb_perform(L, Count + 1).

pb_graph(end_of_stream) ->
    ok;
pb_graph(G) ->
    lists:map(fun pb_tuple/1, maps:to_list(G)).

pb_tuple({Node, {Tab, Tid, Sbj, Prd, {ObjV, ObjT}}}) ->
    F = "| ~5s | ~-20s | ~-30s | ~-50s | ~-30s | ~-50s |\n",
    N  = lists:nth(1, get(b3s_state_nodes)),
    OT = {Tid, Sbj, Prd, {ObjV, ObjT}},
    case rpc:call(N, string_id, decode_triple, [OT]) of
	{I, S, P, O} ->
	    io:fwrite(F, [Node, Tab, I, S, P, O]);
	E ->
	    A = {Tab, Tid, Sbj, Prd, {ObjV, ObjT}},
	    io:fwrite("decode error: ~s\n~p\n", [E, A])
    end;
pb_tuple({Node, {Tab, Tid, Sbj, Prd, Obj}}) ->
    F = "| ~5s | ~-20s | ~-30s | ~-50s | ~-30s | ~-50s |\n",
    io:fwrite(F, [Node, Tab, Tid, Sbj, Prd, Obj]).

%% ======================================================================
%% 
%% command line interface
%% 

%% 
%% @doc This function provedes an entry point of user interface
%% module. It is provided to Erlang node interpreter command erl as a
%% starting function.
%% 
%% @spec main() -> none()
%% 
main() ->
    application:load(b3s),

    io:fwrite("big3store v0.2~n",[]),

    {ok, RRM} = application:get_env(b3s, result_record_max),
    {ok, BSN} = application:get_env(b3s, b3s_state_nodes),
    put(result_record_max, RRM),
    put(b3s_state_nodes,   BSN),
    put(width_report,      80),
    put(node,              node()),
    put(start_date_time,   calendar:local_time()),

    %% set env; put in separate function
    put(prompt, "big3store=# "),
    put(edit_file, "query_file"),
    %% [TODO] session_id should be read from node_state module
    put(session_id, "1"),
    put(tree_id, "1"),
    put(self, {self(),node()}),

    ui_loop_commands(io:get_line(get(prompt))),

    halt().

%%
%% ui_loop_commands/1
%%
%% @doc Main UI loop.
%%
%% @spec ui_loop_commands(Cmd::(io:server_no_data()|string())) -> term()

ui_loop_commands({error, Descr}) -> 
    error_msg(ui_loop_commands, [get(self), {reason, Descr}, {all,get()}, get(state)], io_read_error),
    ui_loop_commands(io:get_line(get(prompt)));
   
ui_loop_commands(eof) -> 
    io:fwrite("~nbye~n",[]);
%    info_msg(ui_loop_commands, [get(self), {all,get()}, get(state)], end_of_io_session, 50);
   
ui_loop_commands(Cmd) -> 
    %% parse command
    Comm = string:tokens(string:strip(Cmd, right, $\n), " "),
    case Comm of 
       [] -> Cm = "", 
             Params = [];
       _  -> [Cm|Params] = Comm
    end,

    %% store params
    put(cmd_params, Params),
%    info_msg(ui_loop_commands, [get(self), {command, C}, {all,get()}, get(state)], display_command, 50),

    %% interpret command
    C = list_to_atom(Cm),
    R = ui_interpret(C),
    io:fwrite("~s",[R]),
   
    %% loop
    ui_loop_commands(io:get_line(get(prompt))).

%%
%% @doc Interpreter of user interface commands.
%%
%% @spec ui_interpret(Cmd::(io:server_no_data()|string())) -> string()

ui_interpret(C) when (C =:= edit) or (C =:= vi) -> 
    %% get file name if set othrwse use default
    CP = get(cmd_params),
    case CP of 
      [] -> File = get(edit_file);
      _  -> [File|_] = CP
    end,

    %% put cmd tgthr and exe
    Cm = lists:concat(["gnome-terminal -e 'vi ", File, "'"]),
    os:cmd(Cm);

ui_interpret(active) -> 
    CP = get(cmd_params),
    case CP of 
      [] -> get(edit_file)++"\n";
      _  -> [File|_] = CP,
            put(edit_file, File),
            File++" active\n"
    end;
    
ui_interpret(exec) -> 
    case ui_confirm_b3s_state_alive() of
	true ->

	    %% get query-file name
	    CP = get(cmd_params),
	    case CP of 
		[] -> QF = get(edit_file);
		_  -> [QF|_] = CP
	    end,

	    %% read query-file into string and convert to term
	    case (catch file_to_term(QF)) of
		{'EXIT', _} ->
		    "illegal query file " ++ QF ++ ".\n";
		Query ->

		    %% spawn query tree and exec query
		    QT = spawn_query_tree(Query),
		    %% info_msg(ui_interpret, [get(self), {command,exec}, {query,Term}, {query_tree_pid,QT}, get(state)], query_executed, 50),

		    %% read and print results from QT 
		    ui_print_results(QT),
		    "ok\n"
	    end;

	_ ->
	    "b3s_state is not alive.\n"
    end;

ui_interpret(cat) -> 
    CP = get(cmd_params),
    case CP of 
      [] -> "error: no file\n";
      _  -> [File|_] = CP,
            os:cmd("cat "++File)++"\n"
    end;
  
ui_interpret(less) -> 
    CP = get(cmd_params),
    case CP of 
      [] -> "error: no file\n";
      _  -> [File|_] = CP,
            os:cmd("less "++File)++"\n"
    end;
  
ui_interpret(cp) -> 
    SP = string:join(get(cmd_params)," "),
    os:cmd("cp "++SP);
  
ui_interpret(mv) -> 
    SP = string:join(get(cmd_params)," "),
    os:cmd("mv "++SP);
  
ui_interpret(ls) -> 
    CP = get(cmd_params),
    case CP of 
      [] -> os:cmd("ls");
      _  -> [Ptrn|_] = CP,
            os:cmd("ls "++Ptrn)
    end;

ui_interpret(ll) -> 
    CP = get(cmd_params),
    case CP of 
      [] -> os:cmd("ls -al");
      _  -> [Ptrn|_] = CP,
            os:cmd("ls -al "++Ptrn)
    end;

ui_interpret(get_self) -> 
    io:fwrite("~70p~n", [{self(), node()}]);

ui_interpret(config) ->
    ui_config_report(ui_confirm_b3s_state_alive());

ui_interpret(debug_level) -> 
    ui_set_debug_level();
  
ui_interpret(gc) -> 
    CP = get(cmd_params),
    F = "performing genserver:call(~w, ~w)...\n\n~p\n",
    case CP of
	[Node, Proc, Command] ->
	    N = ui_resolve_node_synonym(Node),
	    P = ui_resolve_process_synonym(Proc),
	    C = list_to_atom(Command),
	    R = ui_perform_gc({P, N}, C),
	    io_lib:format(F, [{P, N}, C, R]);
	[Node, Proc, Command, Argument] ->
	    N = ui_resolve_node_synonym(Node),
	    P = ui_resolve_process_synonym(Proc),
	    C = ui_resolve_command_synonym(Command),
	    A = list_to_atom(Argument),
	    R = ui_perform_gc({P, N}, {C, A}),
	    io_lib:format(F, [{P, N}, {C, A}, R]);
	[Node, Proc, Command, Arg1, Arg2] ->
	    N = ui_resolve_node_synonym(Node),
	    P = ui_resolve_process_synonym(Proc),
	    C = ui_resolve_command_synonym(Command),
	    A1 = list_to_atom(Arg1),
	    A2 = ui_retype_argument(Arg2),
	    R = ui_perform_gc({P, N}, {C, A1, A2}),
	    io_lib:format(F, [{P, N}, {C, A1, A2}, R]);
	_ -> "usage: gc <node> <process> <message>...\n"
    end;
  
ui_interpret(eps) -> 
    ui_process_report(ui_confirm_b3s_state_alive());

ui_interpret(boot) -> 
    Node = lists:nth(1, get(b3s_state_nodes)),
    RC = rpc:call(Node, b3s, bootstrap, []),
    io_lib:format("~p\n", [RC]);

ui_interpret(sb) -> 
    BA = ui_confirm_b3s_state_alive(),
    CP = get(cmd_params),
    case CP of
	[Task] ->
	    ui_start_benchmark(BA, list_to_atom(Task)),
	    "";
	_ -> "usage: sb <benchmark task name>\n"
    end;

ui_interpret(bb) -> 
    BA = ui_confirm_b3s_state_alive(),
    CP = get(cmd_params),
    case CP of
	L when length(L) > 0 -> usb_iterate(BA, L, [], []);
	_ -> "usage: bb <benchmark task name>...

example:

### suit for predicate_based distribution algorithm ###

    bb task_yjr5x1_0001 task_yjr5x1_0002 task_yjr5x1_0003 task_yjr5x1_0004 task_yjr5x1_0005 task_yjr5x1_0008 task_yjr5x1_0010 task_yjr5x1_0011 task_yjr5x1_0013 task_yjr5x1_0014 task_yjr5x1_0015 task_yjr5x1_0003h task_yjr5x1_0004m task_yjr5x1_0005m task_yjr5x1_0008m task_yjr5x1_0010h task_yjr5x1_0010m task_yjr5x1_0011m task_yjr5x1_0013m task_yjr5x1_0014m task_yjr5x1_0015m

### suit for random distribution algorithm ###

    bb task_yjr5x1_0001 task_yjr5x1_0002 task_yjr5x1_0003 task_yjr5x1_0004 task_yjr5x1_0005 task_yjr5x1_0008 task_yjr5x1_0010 task_yjr5x1_0011 task_yjr5x1_0013 task_yjr5x1_0014 task_yjr5x1_0015 task_yjr5x1_0003h task_yjr5x1_0004m task_yjr5x1_0010h task_yjr5x1_0010m task_yjr5x1_0011m task_yjr5x1_0013m task_yjr5x1_0014m task_yjr5x1_0015m

### suit for rapid tasks ###

    bb task_yjr5x1_0001 task_yjr5x1_0002 task_yjr5x1_0004 task_yjr5x1_0013 task_yjr5x1_0014 task_yjr5x1_0004m task_yjr5x1_0013m

"
    end;

ui_interpret(rb) -> 
    BA = ui_confirm_b3s_state_alive(),
    CP = get(cmd_params),
    case CP of
	[Task] ->
	    ui_report_benchmark(BA, list_to_atom(Task)),
	    "";
	_ -> "usage: rb <benchmark task name>\n"
    end;

ui_interpret(cb) -> 
    F1 = "| ~-24s | ~-10s | ~-10s |\n",
    F2 = "| ------------------------ | ----------:| ----------:|\n",
    BA = ui_confirm_b3s_state_alive(),
    CP = get(cmd_params),
    case CP of
	[] -> "usage: cb all | <benchmark task name>...\n";
	["all"] ->
	    io:fwrite(F1, ['Task Name', 'Elapsed', 'Triples']),
	    io:fwrite(F2),
	    ui_check_all_benchmark(BA),
	    "";
	L ->
	    io:fwrite(F1, ['Task Name', 'Elapsed', 'Triples']),
	    io:fwrite(F2),
	    F = fun (X) ->
			ui_check_benchmark(BA, list_to_atom(X))
		end,
	    lists:foreach(F, L),
	    ""
    end;

ui_interpret(inv) -> 
    ui_investigate(ui_confirm_b3s_state_alive(), get(cmd_params));

ui_interpret(dst) -> 
    ui_distribute(ui_confirm_b3s_state_alive());

ui_interpret(dfs) -> 
    CP = get(cmd_params),
    ui_manage_datafiles(ui_confirm_b3s_state_alive(), CP);

ui_interpret(mnesia) -> 
    CP = get(cmd_params),
    ui_operate_mnesia(CP);

ui_interpret(kill) -> 
    CP = get(cmd_params),
    ui_operate_kill(CP);

ui_interpret(help) -> 
"The upper case words in command description stand for command 
parameters. Square brackets '[]' denote optional parameters. 
Symbol '|' denotes alternatives. 

Query Commands

active [F] \t Alias 'a'.
    - \t\t Prints active query.
    F \t\t Set active query file to F. 
edit [F] \t Alias 'e'.
    - \t\t Edit active query.
    F \t\t Edit file F.
exec [Q] \t Alias '!'.
    - \t\t Execute active query file.
    Q \t\t Execute query file Q. 

DB Commands

commit \t\t Commit current changes.
close [B] 
    - \t\t Close active database.
    B \t\t Close database B.
create [D] 
    - \t\t Create database default databased.
    D \t\t Create database with name D.
open [B] 
    - \t\t Open default database.
    B \t\t Open database file B.

Unix Commands

ls [P] 
    - \t\t Run unix 'ls' on current dir.
    P \t\t Run 'ls' with pattern P. 
cp F1 F2 \t Unix command 'cp'.
less F \t\t Run unix 'more' on file F.
mv F1 F2 \t Unix command 'mv'.
vi F \t\t Unix editor 'vi'.

System Management Commands

config [C]
    - \t\t Report important parameters.
    modules N1, N2, ...
\t\t List loaded module status.
    modules short N1, N2, ...
\t\t List loaded module status shortly.

debug_level [L]
    L \t\t Set debug level of all nodes.
    N L \t Set debug level of specified node.

gc N P C [A1 [A2]]
\t\t Invoke genserver:call/2.

eps [N]
    - \t\t List all processes.
    all \t List all processes.
    N1, N2, ...
\t\t List processes on specified nodes.
    all M1, M2, ...
\t\t List processes of specified modules.

boot \t\t Perform bootstrap process.

inv
    - \t\t Start investigation process of loading triples.
    est N \t Report estimated complete time.
    stat \t Report process status.
    save \t Save investigated result.
    kill-finished
\t\t Kill finished processes.
    kill-all \t Kill all inv/dst processes.

dst \t\t Start distribution process of loading triples.

dfs C
    - \t\t List files to be loaded.
    add F1, F2, ...
\t\t Add files to be loaded.
    rem F \t Remove a file to be loaded.
    clr \t Clear all files to be loaded.

mnesia N C
    info \t Report mnesia (database) status.
    table_info 'T' 'K'
\t\t Report mnesia table information.

kill N P1, P2, ...
\t\t Kill processes on a node.

Benchmark Commands

sb T \t\t Start a benchmark task.

cb [T]
    all \t Report summary reports of all benchmark tasks.
    T1, T2, ...\t Report summary reports of specified benchmark tasks.

rb T \t\t Report full result of benchmark task.

bb T1, T2, ...\t Execute benchmark tasks sequentially. (batch)

Miscellaneous

help \t\t Print help message.
Ctrl-d \t\t Quit b3s.

(see HOWTO/user-interface.md for detail)
\n";

ui_interpret('') -> "";

ui_interpret(_) -> "error: no such command\n".

%% 
%% reporting and setting support functions
%% 

ui_b3s_property(PropertyName) ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    PropertyValue = gen_server:call(BS, {get, PropertyName}),
    benchmark:hr_property(PropertyName, PropertyValue).

ui_td_property(PropertyName) ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    TD = gen_server:call(BS, {get, triple_distributor_pid}),
    PropertyValue = gen_server:call(TD, {get_property, PropertyName}),
    benchmark:hr_property(PropertyName, PropertyValue).

ui_b3s_property_list_kv(PropertyName) ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    PropertyList = gen_server:call(BS, {get, PropertyName}),
    ui_property_list_kv(PropertyName, PropertyList, 0).

ui_property_list_kv(_, [], Count) ->
    Count;
ui_property_list_kv(PName, [{Key, Value} | Rest], 0) ->
    F = "~16s (~w)",
    KV = lists:flatten(io_lib:format(F, [Key, Value])),
    benchmark:hr_property(PName, KV),
    ui_property_list_kv(PName, Rest, 1);
ui_property_list_kv(PName, [{Key, Value} | Rest], Count) ->
    F = "~16s (~w)",
    KV = lists:flatten(io_lib:format(F, [Key, Value])),
    benchmark:hr_property('', KV),
    ui_property_list_kv(PName, Rest, Count + 1).

ui_rpc_get_property(Node, Property) ->
    R = rpc:call(Node, application, get_env, [b3s, Property]),
    case R of
	{ok, Value} ->
	    N = lists:sublist(atom_to_list(Node), 12),
	    L = N ++ ":" ++ atom_to_list(Property),
	    benchmark:hr_property(list_to_atom(L), Value);
	_ ->
	    ok
    end.

ui_rpc_get_property_all(Property) ->
    ui_rpc_get_property_fs(Property),
    ui_rpc_get_property_ds(Property).

ui_rpc_get_property_fs(Property) ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    DS = gen_server:call(BS, {get, front_server_nodes}),
    F = fun (Node) -> ui_rpc_get_property(Node, Property) end,
    lists:foreach(F, DS).

ui_rpc_get_property_ds(Property) ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    DS = gen_server:call(BS, {get, data_server_nodes}),
    F = fun ({Node, _}) -> ui_rpc_get_property(Node, Property) end,
    lists:foreach(F, DS).

ui_rpc_set_property(Node, Property, Value) ->
    rpc:call(Node, application, set_env, [b3s, Property, Value]).

ui_rpc_set_property_all(Property, Value) ->
    ui_rpc_set_property_fs(Property, Value),
    ui_rpc_set_property_ds(Property, Value).

ui_rpc_set_property_fs(Property, Value) ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    FS = gen_server:call(BS, {get, front_server_nodes}),
    F = fun (Node) -> ui_rpc_set_property(Node, Property, Value) end,
    lists:foreach(F, FS).

ui_rpc_set_property_ds(Property, Value) ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    DS = gen_server:call(BS, {get, data_server_nodes}),
    F = fun ({Node, _}) -> ui_rpc_set_property(Node, Property, Value) end,
    lists:foreach(F, DS).

ui_resolve_node_synonym("FS") ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    FS = gen_server:call(BS, {get, front_server_nodes}),
    lists:nth(1, FS);
ui_resolve_node_synonym(DS) when is_atom(DS) ->
    DS;
ui_resolve_node_synonym(DS) ->
    SS = string:str(DS, "DS"),
    SL = length(DS),
    urns_process_ds(SS, SL, DS).

urns_process_ds(1, L, DS) when L > 2 ->
    case (catch list_to_integer(string:substr(DS, 3))) of
	{'EXIT', _} ->
	    list_to_atom(DS);
	N ->
	    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
	    DSN = gen_server:call(BS, {get, data_server_nodes}),
	    case lists:keyfind(N, 2, DSN) of
		{Node, N} ->
		    Node;
		_ ->
		    list_to_atom(DS)
	    end
    end;
urns_process_ds(_, _, N) ->
    list_to_atom(N).

ui_resolve_process_synonym("BS") ->
    b3s_state;
ui_resolve_process_synonym("NS") ->
    node_state;
ui_resolve_process_synonym("TD") ->
    triple_distributor;
ui_resolve_process_synonym("SW") ->
    stop_watch;
ui_resolve_process_synonym(P) ->
    list_to_atom(P).

ui_resolve_command_synonym("G") ->
    get;
ui_resolve_command_synonym("P") ->
    put;
ui_resolve_command_synonym("GP") ->
    get_property;
ui_resolve_command_synonym("PP") ->
    put_property;
ui_resolve_command_synonym("I") ->
    info;
ui_resolve_command_synonym("TI") ->
    table_info;
ui_resolve_command_synonym(C) ->
    list_to_atom(C).

ui_retype_argument([$' | A]) ->
    case string:rchr(A, $') of
	0 -> A;
	I -> list_to_atom(string:substr(A, 1, I - 1))
    end;
ui_retype_argument(A) ->
    L = [fun ura_integer/1, fun ura_float/1],
    ura_apply(L, A).

ura_apply([], A) ->
    A;
ura_apply([F | R], A) ->
    ura_apply(R, F(A)).

ura_integer(A) ->
    case (catch list_to_integer(A)) of
	{'EXIT', _} -> A;
	I -> I
    end.

ura_float(A) ->
    case (catch list_to_float(A)) of
	{'EXIT', _} -> A;
	F -> F
    end.

ui_perform_gc(Process, Message) ->
    case (catch gen_server:call(Process, Message)) of
	{'EXIT', E} -> E;
	R -> R
    end.

ui_confirm_b3s_state_alive() ->
    BSN = lists:nth(1, get(b3s_state_nodes)),
    case rpc:call(BSN, erlang, whereis, [b3s_state]) of
	undefined -> false;
	{badrpc,nodedown} -> false;
	BSP ->
	    rpc:call(BSN, erlang, is_process_alive, [BSP])
    end.

ui_config_report(false) ->
    "b3s_state not alive.\n";
ui_config_report(_) ->
    case get(cmd_params) of
	["help"] ->
	    io:fwrite("usage: config [modules [short] <module>...]\n");
	[] ->
	    ucr_default();
	["modules"] ->
	    ucr_modules();
	["modules", "short"] ->
	    ucr_modules_short();
	["modules", "short" | L] ->
	    F = fun (Node) ->
			N = ui_resolve_node_synonym(Node),
			ucrm_node_short(N)
		end,
	    lists:foreach(F, L);
	["modules" | L] ->
	    F = fun (Node) ->
			N = ui_resolve_node_synonym(Node),
			ucrm_node(N)
		end,
	    lists:foreach(F, L)
    end,
    "\n".

ucr_default() ->
    TR = tmp_report,
    put(TR, queue:new()),
    ui_b3s_property(front_server_nodes),
    ui_b3s_property_list_kv(data_server_nodes),
    ui_b3s_property_list_kv(name_of_triple_tables),
    benchmark:hr_property(b3s_state_nodes),
    ui_b3s_property(triple_distributor_nodes),
    ui_b3s_property(num_of_empty_msgs),
    ui_b3s_property(block_size),
    ui_td_property(distribution_algorithm),
    ui_b3s_property(name_of_pred_clm_table),
    ui_b3s_property(name_of_pred_freq_table),
    ui_b3s_property(name_of_string_id_table),

    put(TR, queue:in([], get(TR))),
    ui_rpc_get_property_all(debug_level),

    put(TR, queue:in([], get(TR))),
    R = benchmark:hc_report_generate(queue:out(get(tmp_report)), []),
    io:fwrite(R),
    "".

ucr_modules() ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    FS = gen_server:call(BS, {get, front_server_nodes}),
    DS = gen_server:call(BS, {get, data_server_nodes}),
    FF = fun (X) -> ucrm_node(X) end,
    DF = fun ({X, _}) -> ucrm_node(X) end,
    lists:foreach(FF, FS),
    lists:foreach(DF, DS),
    "".

ucr_modules_short() ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    FS = gen_server:call(BS, {get, front_server_nodes}),
    DS = gen_server:call(BS, {get, data_server_nodes}),
    FF = fun (X) -> ucrm_node_short(X) end,
    DF = fun ({X, _}) -> ucrm_node_short(X) end,
    lists:foreach(FF, FS),
    lists:foreach(DF, DS),
    "".

ucrm_node(Node) ->
    NAP = net_adm:ping(Node),
    ucrmn_performe(Node, NAP).

ucrmn_performe(Node, pang) ->
    io:fwrite("\n### ~s ###\nnode down...\n\n", [Node]);
ucrmn_performe(Node, pong) ->
    FM = "| ~-18s | ~-9s | ~-80s |\n",
    io:fwrite("\n### ~s ###\n\n", [Node]),
    io:fwrite(FM, [string:chars($ , 4) ++ "module name",
		   is_native,
		   string:chars($ , 25) ++ "which (object code path)"]),
    io:fwrite(FM, [string:chars($-, 18),
		   string:chars($-, 9),
		   string:chars($-, 80)]),
    {ok, ML} = rpc:call(Node, application, get_key, [b3s, modules]),
    FN = fun (X) -> ucrm_module(Node, X) end,
    lists:foreach(FN, ML).

ucrm_node_short(Node) ->
    NAP = net_adm:ping(Node),
    ucrmns_performe(Node, NAP).

ucrmns_performe(Node, pang) ->
    io:fwrite("\n### ~s ###\nnode down...\n\n", [Node]);
ucrmns_performe(Node, pong) ->
    FM = "| ~-18s | ~-9s |\n",
    io:fwrite("\n### ~s ###\n\n", [Node]),
    io:fwrite(FM, [string:chars($ , 4) ++ "module name",
		   is_native]),
    io:fwrite(FM, [string:chars($-, 18),
		   string:chars($-, 9)]),
    {ok, ML} = rpc:call(Node, application, get_key, [b3s, modules]),
    FN = fun (X) -> ucrm_module_short(Node, X) end,
    lists:foreach(FN, ML).

ucrm_module(Node, Mod) ->
    IMN = rpc:call(Node, code, is_module_native, [Mod]),
    WCH = rpc:call(Node, code, which, [Mod]),
    io:fwrite("| ~-18w | ~-9w | ~-80s |\n", [Mod, IMN, WCH]).

ucrm_module_short(Node, Mod) ->
    IMN = rpc:call(Node, code, is_module_native, [Mod]),
    case IMN of
	true ->
	    io:fwrite("| ~-18w | ~-9w |\n", [Mod, IMN]);
	_ ->
	    ok
    end.

ui_process_report(false) ->
    "b3s_state is not alive.\n";
ui_process_report(_) ->
    case get(cmd_params) of
	[] ->
	    NL = ui_all_nodes(),
	    ML = [];
	["all"] ->
	    NL = ui_all_nodes(),
	    ML = [];
	["all" | ML] ->
	    NL = ui_all_nodes();
	NL ->
	    ML = []
    end,
    F = fun (X) -> upr_perform(X, ML) end,
    lists:foreach(F, NL),
    "\n".

ui_all_nodes() ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    DS = gen_server:call(BS, {get, data_server_nodes}),
    FS = gen_server:call(BS, {get, front_server_nodes}),
    uan_append(DS, FS, []).

uan_append([], [], R) ->
    lists:reverse(R);
uan_append([{D, _} | DR], [], R) ->
    uan_append(DR, [], [D | R]);
uan_append(DL, [F | FR], R) ->
    uan_append(DL, FR, [F | R]).

upr_perform(Node, ModuleList) ->
    N = ui_resolve_node_synonym(Node),
    io:fwrite("\n### ~s ###\n\n", [N]),
    F = "| ~-20s | ~-20s |\n",
    Hpi = string:chars($ , 5) ++ "Process Id",
    Hmn = string:chars($ , 4) ++ "Module Name",
    io:fwrite(F, [Hpi, Hmn]),
    Spi = string:chars($-, 20),
    Smn = string:chars($-, 20),
    io:fwrite(F, [Spi, Smn]),

    case (catch rpc:call(N, supervisor, which_children, [b3s])) of
	{'EXIT', E} ->
	    io:fwrite("  ~p\n", [E]);
	R ->
	    up_each_process(R, sets:from_list(ModuleList), ModuleList)
    end.

up_each_process([], _, _) ->
    "";
up_each_process([{Id, _, _, [M]} | R], MS, []) ->
    F = "| ~-20s | ~-20s |\n",
    io:fwrite(F, [Id, M]),
    up_each_process(R, MS, []);
up_each_process([{Id, _, _, [M]} | R], ModuleSet, ML) ->
    F = "| ~-20s | ~-20s |\n",
    case sets:is_element(atom_to_list(M), ModuleSet) of
	true -> io:fwrite(F, [Id, M]);
	false -> ok
    end,
    up_each_process(R, ModuleSet, ML);
up_each_process(_, _, _) ->
    "".

ui_set_debug_level() ->
    case get(cmd_params) of
	[] ->
	    "usage: debug_level <number>\n";
	[DL] ->
	    case (catch list_to_integer(DL)) of
		{'EXIT', _} ->
		    "error: not integer (" ++ DL ++ ")\n";
		NDL ->
		    ui_rpc_set_property_all(debug_level, NDL),
		    F = "setting debug_level to ~w...\n",
		    io_lib:format(F, [NDL])
	    end;
	[Node, DL] ->
	    case (catch list_to_integer(DL)) of
		{'EXIT', _} ->
		    "error: not integer (" ++ DL ++ ")\n";
		NDL ->
		    N = ui_resolve_node_synonym(Node),
		    ui_rpc_set_property(N, debug_level, NDL),
		    F = "setting debug_level of ~w to ~w...\n",
		    io_lib:format(F, [N, NDL])
	    end
    end.

%% 
%% benchmark related functions
%% 
ui_start_benchmark(false, _) ->
    "b3s_state is not alive.\n";
ui_start_benchmark(_, Task) ->
    NN  = lists:nth(1, get(b3s_state_nodes)),
    BS  = {b3s_state, NN},
    TDP = gen_server:call(BS, {get, triple_distributor_pid}),
    DA  = gen_server:call(TDP, {get_property, distribution_algorithm}),
    case DA of
	predicate_based ->
	    usb_predicate_based(Task);
	random ->
	    usb_random(Task);
	_ ->
	    F = "error: unknown distribution_algorithm(~p).\n",
	    io_lib:format(F, [DA])
    end.

usb_predicate_based(Task) ->
    NN  = lists:nth(1, get(b3s_state_nodes)),
    BS  = {b3s_state, NN},
    TPC = gen_server:call(BS, {get, name_of_pred_clm_table}),
    TPF = gen_server:call(BS, {get, name_of_pred_freq_table}),
    PC  = rpc:call(NN, db_interface, db_get_map, [TPC]),
    PF  = rpc:call(NN, db_interface, db_get_map, [TPF]),
    R02 = gen_server:call(BS, {put, pred_clm, PC}),
    R03 = gen_server:call(BS, {put, pred_freq, PF}),
    R05 = gen_server:call(BS, propagate),
    R06 = benchmark:stop(NN, Task),
    R07 = benchmark:start(NN, Task),
    F01 = "~n* preparation:~n~p~n",
    A01 = [R02, R03, R05, R06, R07],

    BM  = {Task, NN},
    R11 = gen_server:cast(BM, Task),
    R12 = calendar:local_time(),
    F02 = "~n* ~w started '~s'.~n~p~n",
    A02 = [R11, R12],
    io:fwrite(F01 ++ F02, [A01, BM, Task, A02]).

usb_random(Task) ->
    NN  = lists:nth(1, get(b3s_state_nodes)),
    R06 = benchmark:stop(NN, Task),
    R07 = benchmark:start(NN, Task),
    F01 = "~n* preparation:~n~p~n",
    A01 = [R06, R07],

    BM  = {Task, NN},
    R11 = gen_server:cast(BM, Task),
    R12 = calendar:local_time(),
    F02 = "~n* ~w started '~s'.~n~p~n",
    A02 = [R11, R12],
    io:fwrite(F01 ++ F02, [A01, BM, Task, A02]).

usb_iterate(false, _, _, _) ->
    "b3s_state is not alive.\n";
usb_iterate(_, [], ElaLst, Mes) when length(ElaLst) > 2 ->
    F1 = "\n| ~-24s | ~-10s | ~-10s |\n",
    F2 = "\n    total          ~24w minutes",
    F3 = "\n    average        ~24w microseconds",
    F4 = "\n    middle average ~24w microseconds\n\n",
    H  = io_lib:format(F1, ['Task Name', 'Elapsed', 'Triples']),
    S  = "| ------------------------ | ----------:| ----------:|\n",

    E  = lists:sum(ElaLst),
    T  = io_lib:format(F2, [E / 1000 / 1000 / 60]),
    A  = io_lib:format(F3, [E / length(ElaLst)]),

    M  = lists:delete(lists:max(ElaLst), ElaLst),
    ML = lists:delete(lists:min(M), M),
    AM = io_lib:format(F4, [lists:sum(ML) / length(ML)]),

    H ++ S ++ Mes ++ T ++ A ++ AM;
usb_iterate(_, [], ElaLst, Mes) ->
    F1 = "\n| ~-24s | ~-10s | ~-10s |\n",
    F2 = "\n    total          ~24w minutes",
    F3 = "\n    average        ~24w microseconds\n\n",
    H  = io_lib:format(F1, ['Task Name', 'Elapsed', 'Triples']),
    S  = "| ------------------------ | ----------:| ----------:|\n",

    E  = lists:sum(ElaLst),
    T  = io_lib:format(F2, [E / 1000 / 1000 / 60]),
    A  = io_lib:format(F3, [E / length(ElaLst)]),

    H ++ S ++ Mes ++ T ++ A;
usb_iterate(BA, [Task | Rest], ElaLst, Mes) ->
    TA = list_to_atom(Task),
    ui_start_benchmark(BA, TA),
    case usb_wait_termination(TA) of
	-1 ->
	    EL = [0 | ElaLst];
	Elapse ->
	    EL = [Elapse | ElaLst]
    end,

    {PE, RF} = ui_check_benchmark(BA, TA),
    F = "| ~-24s | ~10w | ~10w |\n",
    A = [Task, PE, RF],
    M = io_lib:format(F, A),
    usb_iterate(BA, Rest, EL, Mes ++ M).

usb_wait_termination(Task) ->
    NN  = lists:nth(1, get(b3s_state_nodes)),
    case (catch gen_server:call({Task, NN}, {get, query_tree_pid})) of
	{'EXIT', _} -> ok;
	{QT, _} -> usbwt_check_elapse(QT, 1000);
	_ -> usbwt_check_elapse(Task, 1000)
    end.

usbwt_check_elapse(Proc, Time) ->
    NN  = lists:nth(1, get(b3s_state_nodes)),
    case (catch gen_server:call({Proc, NN}, {get, pid_elapse})) of
	{'EXIT', E1} ->
	    io:fwrite("ERROR: ~p\n", [E1]),
	    -1;
	PEM when is_map(PEM) ->
	    case maps:values(PEM) of
		[PE] ->
		    PE;
		[]   ->
		    timer:sleep(Time),
		    usbwt_check_elapse(Proc, Time);
		[I | _] when is_integer(I) ->
		    F1 = fun ({{_, eval}, _}) -> false;
			     ({_, V}) when is_integer(V) -> true;
			     (_) -> false end,
		    F2 = fun ({_, V}) -> V end,
		    EL = lists:filter(F1, maps:to_list(PEM)),
		    VL = lists:map(F2, EL),
		    lists:sum(VL) / length(VL);
		E2    -> 
		    io:fwrite("ERROR: ~p\n", [E2]),
		    -1
	    end;
	_ ->
	    timer:sleep(Time),
	    usbwt_check_elapse(Proc, Time)
    end.

ui_report_benchmark(false, _) ->
    "b3s_state is not alive.\n";
ui_report_benchmark(_, Task) ->
    NN  = lists:nth(1, get(b3s_state_nodes)),
    io:fwrite(gen_server:call({Task, NN}, report, 3600000)).

ui_check_all_benchmark(false) ->
    "b3s_state is not alive.\n";
ui_check_all_benchmark(_) ->
    NN  = lists:nth(1, get(b3s_state_nodes)),
    case (catch rpc:call(NN, supervisor, which_children, [b3s])) of
	{'EXIT', E} ->
	    io:fwrite("  ~p\n", [E]);
	R ->
	    ucab_each_process(R, [])
    end.

ucab_each_process([], L) ->
    LS = lists:sort(L),
    F = fun (X) -> ui_check_benchmark(true, X) end,
    lists:foreach(F, LS),
    "";
ucab_each_process([{Id, _, _, [benchmark]} | R], L) ->
    ucab_each_process(R,  [Id | L]);
ucab_each_process([_ | R], L) ->
    ucab_each_process(R, L).

ui_check_benchmark(false, _) ->
    "b3s_state is not alive.\n";
ui_check_benchmark(_, Task) ->
    NN  = lists:nth(1, get(b3s_state_nodes)),
    case (catch gen_server:call({Task, NN}, {get, query_tree_pid})) of
	{'EXIT', _} -> ok;
	{QT, _} -> ucb_write_task(Task, QT);
	_ -> ucb_write_task(Task, Task)
    end.

ucb_write_task(Task, Proc) ->
    NN  = lists:nth(1, get(b3s_state_nodes)),
    case (catch gen_server:call({Proc, NN}, {get, pid_elapse})) of
	{'EXIT', _} -> PE = '*';
	PEM when is_map(PEM) ->
	    case maps:values(PEM) of
		[PE] -> ok;
		[]   -> PE = undefined;
		[I | _] when is_integer(I) ->
		    F1 = fun ({{_, eval}, _}) -> false;
			     ({_, V}) when is_integer(V) -> true;
			     (_) -> false end,
		    F2 = fun ({_, V}) -> V end,
		    EL = lists:filter(F1, maps:to_list(PEM)),
		    VL = lists:map(F2, EL),
		    PE = lists:sum(VL) / length(VL);
		_    -> PE = illegal
	    end;
	PE -> ok
    end,
    case (catch gen_server:call({Proc, NN}, {get, result_freq})) of
	{'EXIT', _} -> RF = '*';
	RFM when is_map(RFM) ->
	    case maps:values(RFM) of
		[RF] -> ok;
		[II | _] when is_integer(II) ->
		    RF = lists:sum(maps:values(RFM));
		[]   -> RF = undefined;
		_    -> RF = illegal
	    end;
	RF -> ok
    end,
    F   = "| ~-24w | ~10w | ~10w |\n",
    A   = [Task, PE, RF],
    io:fwrite(F, A),
    {PE, RF}.

%% 
%% distribution related functions
%% 
ui_manage_datafiles(false, _) ->
    "b3s_state is not alive.\n";

ui_manage_datafiles(_, ["add"]) ->
    io:fwrite("usage: dfs add <data file>...\n");

ui_manage_datafiles(_, ["add" | NewFiles]) ->
    BSN = lists:nth(1, get(b3s_state_nodes)),
    DFS = gen_server:call({b3s_state, BSN}, {get, data_files}),
    NewDFS = lists:append(DFS, NewFiles),
    gen_server:call({b3s_state, BSN}, {put, data_files, NewDFS}),
    F = "adding data file(s) from data_files list...\n~p\n",
    io:fwrite(F, [NewFiles]);

ui_manage_datafiles(_, ["rem"]) ->
    io:fwrite("usage: dfs rem <data file> | <number>\n"),
    umd_list();

ui_manage_datafiles(_, ["rem" | [Rest]]) ->
    case ura_integer(Rest) of
	N when is_integer(N) ->
	    DF = umd_resolve_data_file(N);
	L ->
	    DF = L
    end,
    case DF of
	undefined ->
	    io:fwrite("corresponding data file was not found.\n"),
	    umd_list();
	_ ->
	    BSN = lists:nth(1, get(b3s_state_nodes)),
	    DFS = gen_server:call({b3s_state, BSN}, {get, data_files}),
	    NewDFS = lists:delete(DF, DFS),
	    gen_server:call({b3s_state, BSN}, {put, data_files, NewDFS}),
	    F = "removing data file ~s from data_files list...\n",
	    io:fwrite(F, [DF])
    end;

ui_manage_datafiles(_, ["clr"]) ->
    io:fwrite("clearing data_files list...\n"),
    BSN = lists:nth(1, get(b3s_state_nodes)),
    gen_server:call({b3s_state, BSN}, {put, data_files, []}),
    umd_list();

ui_manage_datafiles(_, _) ->
    io:fwrite("usage: dfs [add | rem | clr]\n"),
    umd_list().

umd_list() ->
    BSN = lists:nth(1, get(b3s_state_nodes)),
    DFS = gen_server:call({b3s_state, BSN}, {get, data_files}),
    TR = tmp_report,
    put(TR, queue:new()),
    umd_list_perform(DFS, 0),
    put(TR, queue:in([], get(TR))),
    R = benchmark:hc_report_generate(queue:out(get(tmp_report)), []),
    io:fwrite(R),
    "".

umd_list_perform([], _) ->
    ok;
umd_list_perform([DF | R], N) ->
    NN = N + 1,
    benchmark:hr_property(integer_to_list(NN), DF),
    umd_list_perform(R, NN).

umd_resolve_data_file(N) ->
    BSN = lists:nth(1, get(b3s_state_nodes)),
    DFS = gen_server:call({b3s_state, BSN}, {get, data_files}),
    case (catch lists:nth(N, DFS)) of
	{'EXIT', _} ->
	    undefined;
	DF ->
	    DF
    end.

ui_investigate(false, _) ->
    "b3s_state is not alive.\n";
ui_investigate(_, ["est" | _]) ->
    BSN = lists:nth(1, get(b3s_state_nodes)),
    case (catch rpc:call(BSN, supervisor, which_children, [b3s])) of
	{'EXIT', E} ->
	    io:fwrite("  ~p\n", [E]);
	DFS ->
	    put(uii_counter, 1),
	    uii_estimate(DFS, 0, 0, get(cmd_params))
    end;
ui_investigate(_, ["stat"]) ->
    BSN = lists:nth(1, get(b3s_state_nodes)),
    case (catch rpc:call(BSN, supervisor, which_children, [b3s])) of
	{'EXIT', E} ->
	    io:fwrite("  ~p\n", [E]);
	DFS ->
	    put(uii_counter, 1),
	    lists:foreach(fun uii_status/1, lists:reverse(DFS)),
	    ""
    end;
ui_investigate(_, ["kill-finished"]) ->
    BSN = lists:nth(1, get(b3s_state_nodes)),
    case (catch rpc:call(BSN, supervisor, which_children, [b3s])) of
	{'EXIT', E} ->
	    io:fwrite("  ~p\n", [E]);
	DFS ->
	    put(uii_counter, 1),
	    lists:foreach(fun uii_kill_finished/1, lists:reverse(DFS)),
	    ""
    end;
ui_investigate(_, ["kill-all"]) ->
    BSN = lists:nth(1, get(b3s_state_nodes)),
    case (catch rpc:call(BSN, supervisor, which_children, [b3s])) of
	{'EXIT', E} ->
	    io:fwrite("  ~p\n", [E]);
	DFS ->
	    put(uii_counter, 1),
	    lists:foreach(fun uii_kill_all/1, lists:reverse(DFS)),
	    ""
    end;
ui_investigate(_, ["save"]) ->
    BSN = lists:nth(1, get(b3s_state_nodes)),
    BS  = {b3s_state, BSN},
    TD  = gen_server:call(BS, {get, triple_distributor_pid}),
    R1 = gen_server:call(TD, {save_property, pred_clm}),
    R2 = gen_server:call(TD, {save_property, pred_freq}),
    io:fwrite("pred_clm: ~p\npred_freq: ~p\n", [R1, R2]),
    "";
ui_investigate(_, []) ->
    BSN = lists:nth(1, get(b3s_state_nodes)),
    BS  = {b3s_state, BSN},
    TD  = gen_server:call(BS, {get, triple_distributor_pid}),
    RP  = {repeater, BSN},
    put(triple_distributor_pid, TD),
    put(repeater_pid, RP),
    put(uii_counter, 1),
    DFS = gen_server:call(BS, {get, data_files}),
    lists:foreach(fun uii_perform/1, DFS),
    "";
ui_investigate(_, _) ->
    "usage: inv [ est <max> | stat | save | kill-finished | kill-all]\n".

uii_perform(File) ->
    C  = put(uii_counter, get(uii_counter) + 1),
    F1 = "* investigating[~p](~s)...",
    A1 = [C, File],
    io:fwrite(F1, A1),

    RPT = get(repeater_pid),
    BSN = lists:nth(1, get(b3s_state_nodes)),
    Pid = list_to_atom(lists:concat([file_reader_, C])),
    M   = {start, RPT, investigate_stream, File, Pid, BSN},
    R   = gen_server:cast(get(triple_distributor_pid), M),

    F2 = " done. ~p~n~p~n~p~n",
    A2 = [calendar:local_time(), M, R],
    io:fwrite(F2, A2).

uii_status({FR, _, _, [file_reader]}) ->
    BSN = lists:nth(1, get(b3s_state_nodes)),
    EOF = gen_server:call({FR, BSN}, {get_property, file_reader_eof}),
    NOT = gen_server:call({FR, BSN}, {get_property, number_of_triple_latest}),
    ELA = gen_server:call({FR, BSN}, {get_property, process_sec_latest}),
    FN  = gen_server:call({FR, BSN}, {get_property, file_name}),
    case NOT of
	0 ->
	    TC  = gen_server:call({FR, BSN}, {get_property, triple_count});
	TC ->
	    ok
    end,
    F   = "~-16s  ~8w triples, ~5w seconds, eof(~s), <~s>.\n",
    io:fwrite(F, [FR, TC, ELA, EOF, FN]);
uii_status(_) ->
    "".

uii_kill_finished({FR, _, _, [file_reader]}) ->
    BSN = lists:nth(1, get(b3s_state_nodes)),
    EOF = gen_server:call({FR, BSN}, {get_property, file_reader_eof}),
    TC  = gen_server:call({FR, BSN}, {get_property, triple_count}),
    FN  = gen_server:call({FR, BSN}, {get_property, file_name}),
    F1   = "* ~-16s is killed... <~s>.\n",
    F2   = "* ~-16s is running... ~w triples. <~s>.\n",
    case EOF of
	true ->
	    uok_one_process(BSN, atom_to_list(FR)),
	    io:fwrite(F1, [FR, FN]);
	_ ->
	    io:fwrite(F2, [FR, TC, FR])
    end;
uii_kill_finished(_) ->
    "".

uii_kill_all({FR, _, _, [file_reader]}) ->
    BSN = lists:nth(1, get(b3s_state_nodes)),
    FN  = gen_server:call({FR, BSN}, {get_property, file_name}),
    F1   = "* ~-16s is killed... <~s>.\n",
    uok_one_process(BSN, atom_to_list(FR)),
    io:fwrite(F1, [FR, FN]);
uii_kill_all(_) ->
    "".

uii_estimate(_, _, _, ["est"]) ->
    "usage: inv est <total triples>\n";
uii_estimate([], Triples, {{YY, MM, DD}, {H, M, S}}, ["est", T]) ->
    Total   = list_to_integer(T),
    Percent = trunc(Triples / Total * 10000) / 100,
    Start   = 
	calendar:datetime_to_gregorian_seconds({{YY, MM, DD}, {H, M, S}}),
    {{YYc, MMc, DDc}, {Hc, Mc, Sc}} = calendar:local_time(),
    Current = 
	calendar:datetime_to_gregorian_seconds({{YYc, MMc, DDc}, {Hc, Mc, Sc}}),
    Est     = Start + (Current - Start) / Triples * Total,
    {{YYe, MMe, DDe}, {He, Me, Se}} =
	calendar:gregorian_seconds_to_datetime(trunc(Est)),

    F1 = "[~4..0w/~2..0w/~2..0w ~2..0w:~2..0w:~2..0w] started.\n",
    F2 = "[~4..0w/~2..0w/~2..0w ~2..0w:~2..0w:~2..0w] ~9w/~9w (~6.2f%)\n",
    F3 = "[~4..0w/~2..0w/~2..0w ~2..0w:~2..0w:~2..0w] estimated.\n",
    A1 = [YY, MM, DD, H, M, S],
    A2 = [YYc, MMc, DDc, Hc, Mc, Sc, Triples, Total, Percent],
    A3 = [YYe, MMe, DDe, He, Me, Se],
    io_lib:format(F1 ++ F2 ++ F3, lists:concat([A1, A2, A3]));
uii_estimate([], _, _, _) ->
    "no start time\n";
uii_estimate([{FR, _, _, [file_reader]} | Rest], Triples, 0, Total) ->
    BSN = lists:nth(1, get(b3s_state_nodes)),
    TC  = gen_server:call({FR, BSN}, {get_property, triple_count}),
    RST = gen_server:call({FR, BSN}, {get_property, request_start_time}),
    uii_estimate(Rest, Triples + TC, RST, Total);
uii_estimate([{FR, _, _, [file_reader]} | Rest], Triples, StartTime, Total) ->
    BSN = lists:nth(1, get(b3s_state_nodes)),
    TC  = gen_server:call({FR, BSN}, {get_property, triple_count}),
    uii_estimate(Rest, Triples + TC, StartTime, Total);
uii_estimate([_ | Rest], Triples, StartTime, Total) ->
    uii_estimate(Rest, Triples, StartTime, Total).

ui_distribute(false) ->
    "b3s_state is not alive.\n";
ui_distribute(_) ->
    BSN = lists:nth(1, get(b3s_state_nodes)),
    BS  = {b3s_state, BSN},
    TD  = gen_server:call(BS, {get, triple_distributor_pid}),
    RP  = {repeater, BSN},
    put(triple_distributor_pid, TD),
    put(repeater_pid, RP),
    put(uii_counter, 1),

    gen_server:call(TD, {put_property, write_mode, postgres_bulk_load}),
    gen_server:call(TD, {put_property, encode_mode, false}),
    gen_server:call(TD, {load_property, pred_clm}),
    gen_server:call(TD, {load_property, pred_freq}),

    DFS = gen_server:call(BS, {get, data_files}),
    lists:foreach(fun uid_perform/1, DFS),
    "".

uid_perform(File) ->
    C  = put(uii_counter, get(uii_counter) + 1),
    F1 = "* distributing[~p](~s)...",
    A1 = [C, File],
    io:fwrite(F1, A1),

    RPT = get(repeater_pid),
    BSN = lists:nth(1, get(b3s_state_nodes)),
    Pid = list_to_atom(lists:concat([file_reader_, C])),
    M   = {start, RPT, store_stream, File, Pid, BSN},
    R   = gen_server:cast(get(triple_distributor_pid), M),

    F2 = " done. ~p~n~p~n~p~n",
    A2 = [calendar:local_time(), M, R],
    io:fwrite(F2, A2).

%% 
%% mnesia related functions
%% 
ui_operate_mnesia([Node, Command]) ->
    N = ui_resolve_node_synonym(Node),
    C = ui_resolve_command_synonym(Command),
    case (catch rpc:call(N, mnesia, C, [])) of
	{'EXIT', R} -> ok;
	R -> ok
    end,
    io:fwrite("~p\n", [R]),
    "";
ui_operate_mnesia([Node, Command, Arg]) ->
    N = ui_resolve_node_synonym(Node),
    C = ui_resolve_command_synonym(Command),
    A = ui_retype_argument(Arg),
    case (catch rpc:call(N, mnesia, C, [A])) of
	{'EXIT', R} -> ok;
	R -> ok
    end,
    io:fwrite("~p\n", [R]),
    "";
ui_operate_mnesia([Node, Command, Arg1, Arg2]) ->
    N = ui_resolve_node_synonym(Node),
    C = ui_resolve_command_synonym(Command),
    A1 = ui_retype_argument(Arg1),
    A2 = ui_retype_argument(Arg2),
    case (catch rpc:call(N, mnesia, C, [A1, A2])) of
	{'EXIT', R} -> ok;
	R -> ok
    end,
    io:fwrite("~p\n", [R]),
    "";
ui_operate_mnesia(_) ->
    "usage: mnesia <node> {info (I) | table_info (TI)} [<args> ...]\n".

%% 
%% kill related functions
%% 
ui_operate_kill([]) ->
    "usage: kill <node> <process>\n";
ui_operate_kill([_]) ->
    "usage: kill <node> <process>\n";
ui_operate_kill([Node | ProcessList]) ->
    F = fun (X) -> uok_one_process(Node, X) end,
    lists:foreach(F, ProcessList),
    "".

uok_one_process(Node, Process) ->
    N = ui_resolve_node_synonym(Node),
    P = list_to_atom(Process),
    case (catch rpc:call(N, supervisor, terminate_child, [b3s, P])) of
	{'EXIT', Rt} -> ok;
	Rt -> ok
    end,
    case (catch rpc:call(N, supervisor, delete_child, [b3s, P])) of
	{'EXIT', Rd} -> ok;
	Rd -> ok
    end,
    io:fwrite("[~w]\n", [N]),
    io:fwrite("supervisor:terminate_child(b3s, ~w).\n~p\n", [P, Rt]),
    io:fwrite("supervisor:   delete_child(b3s, ~w).\n~p\n", [P, Rd]),
    "".

%%
%% ui_print_results/1
%%
%% @doc Read blocks from query tree QT and print graphs to terminal.
%%
%% @spec ui_print_results(QT::node_state:ns_pid()) -> term()
%%
ui_print_results(QT) -> 
    case new_query_tree_protocol of
	new_query_tree_protocol ->
	    {Id,_} = QT,
	    io:fwrite("\n"),
	    uprs_perform(Id, -1);
	_ ->
	    ui_print_results_prev(QT)
    end.

uprs_perform(QT, -1) ->
    uprs_perform(QT, usbwt_check_elapse(QT, 1000));
uprs_perform(QT, Elapse) ->
    BSN = lists:nth(1, get(b3s_state_nodes)),
    {data_block, Block} = gen_server:call({QT, BSN}, {read}),
    print_block(Block),
    case Block of
	no_data ->
	    io:fwrite("\n~w microseconds\n", [Elapse]);
	_ ->
	    case lists:member(end_of_stream, Block) of 
		true  ->
		    io:fwrite("\n~w microseconds\n", [Elapse]);
		false ->
		    uprs_perform(QT, Elapse)
	    end
    end.

ui_print_results_prev(QT) -> 

    {data_block, Block} = gen_server:call(QT, {read}),
%   info_msg(ui_print_results, [get(self), {command,exec}, {query_tree_pid,QT}, {block,Block}, get(state)], block_read, 50),
    timer:sleep(100),
    
    case Block of
    no_data -> 
         ui_print_results_prev(QT);

    _ -> print_block(Block),
         case lists:member(end_of_stream, Block) of 
         true  -> ok;
         false -> ui_print_results_prev(QT)
         end
    end.

%% ===================================================================
%% 
%% @doc Unit tests.
%% 
ui_test_() ->
    application:load(b3s),
    ut_site(b3s_state:get(test_mode)).

ut_site(local1) ->
    {inorder,
     [
      ?_assertMatch(ok, b3s:start()),
      ?_assertMatch(ok, b3s:bootstrap()),
      ?_assertMatch(ok, b3s:stop())
     ]};

ut_site(_) ->
    [].

%% ====> END OF LINE <====

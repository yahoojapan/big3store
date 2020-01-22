%%
%% User Interface
%%
%% @copyright 2015-2019 UP FAMNIT and Yahoo Japan Corporation
%% @version 0.3
%% @since September, 2015
%% @author Iztok Savnik <iztok.savnik@famnit.upr.si>
%% @author Kiyoshi Nitta <knitta@yahoo-corp.jp>
%% 
%% @see b3s
%% @see b3s_state
%% @see node_state
%% 
%% @doc This module provides functions for implementing user interafce
%% of entire big3store system.
%% 
%% @type ui_state() = maps:map(). Map structure
%% that manages properties for operating the gen_server process.
%%
%% @type ui_statement() = string(). A Command string of the user
%% interface. See <a
%% href="https://github.com/big3store/big3store/blob/master/src/HOWTO/user-interface.md">user
%% interface reference</a> for the detailed description.
%%
%% @type ui_job_tuple() = {node(), 'completed' | 'boot-fs' | 'boot-ds'
%% | 'load-ds'}. A semaphore that represents an execution of the
%% specified job on the specified node. Atom 'completed' has a special
%% meaning. It indicates that all jobs have been successfully executed
%% and finished. Job 'boot-fs' represents that a booting procedure is
%% executed on the front server node. Job 'boot-fs' represents that a
%% booting procedure is executed on the data server node. Job
%% 'load-ds' represents that a loading procedure is executed on the
%% data server node.
%%
-module(user_interface).
-behavior(gen_server).
-export(
   [

    main/0, init/1, handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3, spawn_process/2, child_spec/1,
    ui_interpret/1, ui_operate_aws/1, ui_operate_psql/1,
    uops_get_connection/0, uops_perform_sql/2,
    uoa_sns_publish/1, uoadi_get_node_instance_map/0
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
    put(prompt_count,      0),
    put(b3s_state_pid,     {b3s_state,  lists:nth(1, get(b3s_state_nodes))}),
    put(node_state_pid,    {node_state, lists:nth(1, get(b3s_state_nodes))}),

    %% set env; put in separate function
    put(edit_file, "query_file"),
    %% [TODO] session_id should be read from node_state module
    put(session_id, "1"),
    put(tree_id, "1"),
    put(self, {self(),node()}),

    ui_loop_commands(io:get_line(generate_prompt())),

    halt().

generate_prompt() ->
    PC = prompt_count,
    put(PC, get(PC) + 1),
    {H, M, _} = time(),
    L = lists:nth(1, string:split(atom_to_list(node()), "@")),
    F = "~s ~2..0w:~2..0w [~w] big3store=# ",
    P = io_lib:format(F, [L, H, M, get(PC)]),
    put(prompt, P),
    P.

%%
%% ui_loop_commands/1
%%
%% @doc Main UI loop.
%%
%% @spec ui_loop_commands(Cmd::(io:server_no_data()|string())) -> term()

ui_loop_commands({error, Descr}) -> 
    error_msg(ui_loop_commands, [get(self), {reason, Descr}, {all,get()}, get(state)], io_read_error),
    ui_loop_commands(io:get_line(generate_prompt()));
   
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
    ui_loop_commands(io:get_line(generate_prompt())).

%%
%% @doc Interpreter of user interface commands.
%%
%% @spec ui_interpret(Cmd::(io:server_no_data()|string())) -> string()

ui_interpret(C) when (C =:= edit) or (C =:= e) or (C =:= vi) -> 
    %% get file name if set othrwse use default
    CP = get(cmd_params),
    case CP of 
      [] -> File = get(edit_file);
      _  -> [File|_] = CP
    end,

    %% put cmd tgthr and exe
    Cm = lists:concat(["gnome-terminal -e 'vi ", File, "'"]),
    os:cmd(Cm);

ui_interpret(a) -> 
    ui_interpret(active);
ui_interpret(active) -> 
    CP = get(cmd_params),
    case CP of 
      [] -> get(edit_file)++"\n";
      _  -> [File|_] = CP,
            put(edit_file, File),
            File++" active\n"
    end;
    
ui_interpret('!') -> 
    ui_interpret(exec);
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
      _  -> os:cmd("ls " ++ string:join(CP, " "))
    end;

ui_interpret(ll) -> 
    CP = get(cmd_params),
    case CP of 
      [] -> os:cmd("ls -al");
      _  -> [Ptrn|_] = CP,
            os:cmd("ls -al "++Ptrn)
    end;

ui_interpret(df) -> 
    CP = get(cmd_params),
    case CP of 
	[] -> os:cmd("df .");
	_  -> os:cmd("df " ++ string:join(CP, " "))
    end;

ui_interpret(ps) -> 
    CP = get(cmd_params),
    case CP of 
	[] -> os:cmd("ps auxw");
	GS ->
	    UIP = fun F([], Command) ->
			  os:cmd(Command ++ "|grep -v grep");
		      F([GrepString | Rest], C) ->
			  F(Rest, C ++ "|grep " ++ GrepString)
		  end,
	    UIP(GS, "ps auxw")
    end;

ui_interpret(top) -> 
    HD = "top -b ",
    SO = "-o %CPU",
    FT = " | head -10",
    CP = get(cmd_params),
    case CP of
	[] -> os:cmd(HD ++ SO ++ FT);
	[N] ->
	    case (catch list_to_integer(N)) of
		{'EXIT', _} ->
		    os:cmd(HD ++ string:join(CP, " ") ++ FT);
		NI ->
		    FT2 = io_lib:format(" | head -~w", [NI + 7]),
		    os:cmd(HD ++ SO ++ FT2)
	    end;
	[N | RO] ->
	    case (catch list_to_integer(N)) of
		{'EXIT', _} ->
		    os:cmd(HD ++ string:join(CP, " ") ++ FT);
		NI ->
		    FT2 = io_lib:format(" | head -~w", [NI + 7]),
		    OPT = string:join(RO, " "),
		    os:cmd(HD ++ OPT ++ FT2)
	    end;
	_ ->
	    OPT = string:join(CP, " "),
	    os:cmd(HD ++ OPT ++ FT)
    end;

ui_interpret(ev) -> 
    ui_interpret(eval);

ui_interpret(eval) -> 
    case string:join(get(cmd_params), " ") of
	[] ->
	    "usage: eval <erlang expression>\n";
	A ->
	    lists:flatten(io_lib:format("~p\n", [ura_expr(A)]))
    end;

ui_interpret(get_self) -> 
    io_lib:format("~70p~n", [{self(), node()}]);

ui_interpret(c) ->
    ui_interpret(config);

ui_interpret(config) ->
    ui_config_report(ui_confirm_b3s_state_alive());

ui_interpret(debug_level) -> 
    ui_set_debug_level();
  
ui_interpret(gcg) -> 
    ui_interpret(gen_server_call_get);

ui_interpret(gen_server_call_get) -> 
    CP = get(cmd_params),
    F = "store gen_server:call(~w, ~w) to ~s\n~p\n",
    case CP of
	[Prop, Node, Proc, Command] ->
	    N = ui_resolve_node_synonym(Node),
	    P = ui_resolve_process_synonym(Proc),
	    C = ui_resolve_command_synonym(Command),
	    R = ui_perform_gc({P, N}, C),
	    put(list_to_atom(Prop), R),
	    io_lib:format(F, [{P, N}, C, Prop, R]);
	[Prop, Node, Proc, Command, Argument] ->
	    N = ui_resolve_node_synonym(Node),
	    P = ui_resolve_process_synonym(Proc),
	    C = ui_resolve_command_synonym(Command),
	    A = list_to_atom(Argument),
	    R = ui_perform_gc({P, N}, {C, A}),
	    put(list_to_atom(Prop), R),
	    io_lib:format(F, [{P, N}, {C, A}, Prop, R]);
	[Prop, Node, Proc, Command, Arg1 | Args] ->
	    N = ui_resolve_node_synonym(Node),
	    P = ui_resolve_process_synonym(Proc),
	    C = ui_resolve_command_synonym(Command),
	    A1 = list_to_atom(Arg1),
	    A2 = ui_retype_argument(string:join(Args, " ")),
	    R = ui_perform_gc({P, N}, {C, A1, A2}),
	    put(list_to_atom(Prop), R),
	    io_lib:format(F, [{P, N}, {C, A1, A2}, Prop, R]);
	_ -> "usage: gen_server_call_get <proc. dict. porp.> <node> <process> <message>...\n"
    end;

ui_interpret(gc) -> 
    ui_interpret(gen_server_call);

ui_interpret(gen_server_call) -> 
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
	[Node, Proc, Command, Arg1 | Args] ->
	    N = ui_resolve_node_synonym(Node),
	    P = ui_resolve_process_synonym(Proc),
	    C = ui_resolve_command_synonym(Command),
	    A1 = list_to_atom(Arg1),
	    A2 = ui_retype_argument(string:join(Args, " ")),
	    R = ui_perform_gc({P, N}, {C, A1, A2}),
	    io_lib:format(F, [{P, N}, {C, A1, A2}, R]);
	_ -> "usage: gen_server_call <node> <process> <message>...\n"
    end;
  
ui_interpret(eps) -> 
    ui_interpret(erlang_ps);

ui_interpret(erlang_ps) -> 
    ui_process_report(ui_confirm_b3s_state_alive());

ui_interpret(boot) -> 
    Node = lists:nth(1, get(b3s_state_nodes)),
    RC = rpc:call(Node, b3s, bootstrap, []),
    io_lib:format("~p\n", [RC]);

ui_interpret(sb) -> 
    ui_interpret(start_benchmark);

ui_interpret(start_benchmark) -> 
    BA = ui_confirm_b3s_state_alive(),
    CP = get(cmd_params),
    case CP of
	[Task] ->
	    ui_start_benchmark(BA, list_to_atom(Task)),
	    "";
	_ -> "usage: start_benchmark <benchmark task name>\n"
    end;

ui_interpret(bb) -> 
    ui_interpret(batch_benchmark);

ui_interpret(batch_benchmark) -> 
    BA = ui_confirm_b3s_state_alive(),
    CP = get(cmd_params),
    case CP of
	L when length(L) > 0 -> usb_iterate(BA, L, [], []);
	_ -> "usage: batch_benchmark <benchmark task name>...

example:

### suit for predicate_based distribution algorithm ###

    bb task_yg_0001 task_yg_0002 task_yg_0003 task_yg_0004 task_yg_0005 task_yg_0008 task_yg_0010 task_yg_0011 task_yg_0013 task_yg_0014 task_yg_0015 task_yg_0003h task_yg_0004m task_yg_0005m task_yg_0008m task_yg_0010h task_yg_0010m task_yg_0011m task_yg_0013m task_yg_0014m task_yg_0015m

### suit for random distribution algorithm ###

    bb task_yg_0001 task_yg_0002 task_yg_0003 task_yg_0004 task_yg_0005 task_yg_0008 task_yg_0010 task_yg_0011 task_yg_0013 task_yg_0014 task_yg_0015 task_yg_0003h task_yg_0004m task_yg_0010h task_yg_0010m task_yg_0011m task_yg_0013m task_yg_0014m task_yg_0015m

### suit for middle tasks ###

    bb task_yg_0001 task_yg_0002 task_yg_0004 task_yg_0005 task_yg_0013 task_yg_0014 task_yg_0015 task_yg_0004m task_yg_0013m task_yg_0014m task_yg_0015m

### suit for rapid tasks ###

    bb task_yg_0001 task_yg_0002 task_yg_0004 task_yg_0013 task_yg_0014 task_yg_0004m task_yg_0013m

"
    end;

ui_interpret(rb) -> 
    ui_interpret(report_benchmark);

ui_interpret(report_benchmark) -> 
    BA = ui_confirm_b3s_state_alive(),
    CP = get(cmd_params),
    case CP of
	[Task] ->
	    ui_report_benchmark(BA, list_to_atom(Task)),
	    "";
	_ -> "usage: report_benchmark <benchmark task name>\n"
    end;

ui_interpret(cb) -> 
    ui_interpret(check_benchmark);

ui_interpret(check_benchmark) -> 
    F1 = "| ~-24s | ~-10s | ~-10s |\n",
    F2 = "| ------------------------ | ----------:| ----------:|\n",
    BA = ui_confirm_b3s_state_alive(),
    CP = get(cmd_params),
    case CP of
	[] -> "usage: check_benchmark all | <benchmark task name>...\n";
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
    ui_interpret(investigate);

ui_interpret(investigate) -> 
    ui_investigate(ui_confirm_b3s_state_alive(), get(cmd_params));

ui_interpret(dst) -> 
    ui_interpret(distribute);

ui_interpret(distribute) -> 
    ui_distribute(ui_confirm_b3s_state_alive(), get(cmd_params));

ui_interpret(dfs) -> 
    ui_interpret(data_files);

ui_interpret(data_files) -> 
    CP = get(cmd_params),
    ui_manage_datafiles(ui_confirm_b3s_state_alive(), CP);

ui_interpret(mnesia) -> 
    CP = get(cmd_params),
    ui_operate_mnesia(CP);

ui_interpret(kill) -> 
    CP = get(cmd_params),
    ui_operate_kill(CP);

ui_interpret(aws) -> 
    CP = get(cmd_params),
    ui_operate_aws(CP);

ui_interpret(local) -> 
    CP = get(cmd_params),
    ui_operate_local(CP);

ui_interpret(pr) -> 
    ui_interpret(property);

ui_interpret(property) -> 
    CP = get(cmd_params),
    ui_operate_property(CP);

ui_interpret(psql) -> 
    CP = get(cmd_params),
    ui_operate_psql(CP);

ui_interpret(remote) -> 
    CP = get(cmd_params),
    ui_operate_remote(CP);

ui_interpret(m) -> 
    ui_interpret(memory);

ui_interpret(memory) -> 
    CP = get(cmd_params),
    ui_operate_memory(CP);

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
df [P]
    - \t\t Run unix 'df' on current dir.
    P \t\t Run 'df' with pattern P. 
ps [P..]
    - \t\t Run unix 'ps'.
    P \t\t Run 'ps' and filter with pattern P.
top [N [Opt]]
    - \t\t Run unix 'top'.
    N \t\t Run 'top' with process limit N. 
    Opt \t\t Run 'top' with process limit N and options Opt. 

System Management Commands

config [C] \t Alias 'c'.
    - \t\t Report important parameters.
    modules N1, N2, ...
\t\t List loaded module status.
    modules short N1, N2, ...
\t\t List loaded module status shortly.

debug_level [L]
    L \t\t Set debug level of all nodes.
    N L \t Set debug level of specified node.

gen_server_call N P C [A1 [A2]]
\t\t Alias 'gc'.
\t\t Invoke genserver:call/2.

gen_server_call_get V N P C [A1 [A2]]
\t\t Alias 'gcg'.
\t\t Invoke genserver:call/2 and 
\t\t store its result to Erlang variable V.

eval E \t\t Alias 'ev'.
\t\t evaluate an erlang expression E.

erlang_ps [N] \t Alias 'eps'.
    - \t\t List all processes.
    all \t List all processes.
    N1, N2, ...
\t\t List processes on specified nodes.
    all M1, M2, ...
\t\t List processes of specified modules.

boot \t\t Perform bootstrap process.

investigate \t Alias 'inv'.
    - \t\t Start the investigation process of loading triples.
    est N \t Report estimated complete time.
    stat \t Report process status.
    save \t Save investigated result.
    kill-finished
\t\t Kill finished processes.
    kill-all \t Kill all inv/dst processes.
    reuse red_freq
\t\t Start the investigated process using existing pred_freq value.
    is finished  Wait until all investigation processes terminate.

distribute \t Alias 'dst '.
\t\t Start the distribution process of loading triples.
    reuse red_freq
\t\t Start the distribution process using existing pred_freq value.
    is finished  Wait until all distribution processes terminate.

data_files C \t Alias 'dfs'.
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

aws \t\t Perform aws operations.
    run \t Run data server instances.
    describe \t List alive instances.
    terminate \t Terminate instances.
    bucket \t Save / load data between S3 bucket.
    metadata \t Display major instance metadata.
    sns \t Send a notification message.
    servers.cf \t Print shell cf file of b3s servers.
    create \t Create image or template.
    unregister \t Unregister data servers.
    reboot \t Reboot instances.

local \t\t Perform local server operations.
    run \t Run data server erlang nodes on the front server instance.
    terminate \t Kill data server nodes.
    restart-ds \t Restart data sever nodes.

property \t Perform property management operations. Alias 'pr'.
    cp \t\t Copy a property between erlang processes.
    write \t Write a property contents to a file.
    find \t Find properties and display their values. Alias 'f'.
    backup \t Backup major b3s_state properties.
    restore \t Restore major b3s_state properties.
    construct \t Construct specific predicate.

psql \t\t Perform postgresql operations.
    load \t Load data from a file to a table.
    encode \t Perform string-id encoding process.
    save \t Save data from a table to a file.
    describe \t List psql tables.

remote \t\t Perform UI commands on remote data severs.
    <node> C \t Perform a user interface command C on a remote node.
    all C \t Perform a user interface command C on all data server nodes.
    load all encoded column tables
\t\t Let all data servers load corresponding encoded column table.
    restart \t Restart data server erlang nodes.

memory \t\t Peform memory investigations. Alias 'm'.
    summary \t Report a short summary. Alias 's'.

Benchmark Commands

start_benchmark T
\t\t Alias 'sb'.
\t\t Start a benchmark task.

check_benchmark [T]
\t\t Alias 'cb'.
    all \t Report summary reports of all benchmark tasks.
    T1, T2, ...\t Report summary reports of specified benchmark tasks.

report_benchmark T
\t\t Alias 'rb'.
\t\t Report full result of benchmark task.

batch_benchmark T1, T2, ...
\t\t Alias 'bb'.
\t\t Execute benchmark tasks sequentially. (batch)

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
ui_property_list_kv(_, undefined, Count) ->
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

ui_resolve_node_synonym("S") ->
    node();
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
    NS = string:substr(DS, 3),
    case (catch list_to_integer(NS)) of
	{'EXIT', _} ->
	    upd_with_row(DS);
	C ->
	    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
	    CR = gen_server:call(BS, {get, clm_row_conf}),
	    case maps:get(C, CR, not_found) of
		not_found ->
		    list_to_atom(DS);
		M ->
		    case maps:get(1, M, not_found) of
			not_found ->
			    list_to_atom(DS);
			Node ->
			    Node
		    end
	    end
    end;
urns_process_ds(_, _, N) ->
    list_to_atom(N).

upd_with_row(String) ->
    NS = string:substr(String, 3),
    SL = string:split(NS, "-"),
    F = fun (S) ->
		case (catch list_to_integer(S)) of
		    {'EXIT', _} ->
			false;
		    N -> N
		end
	end,
    case lists:map(F, SL) of
	[false, _] ->
	    list_to_atom(String);
	[_, false] ->
	    list_to_atom(String);
	[C, R] ->
	    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
	    CR = gen_server:call(BS, {get, clm_row_conf}),
	    case maps:get(C, CR, not_found) of
		not_found ->
		    list_to_atom(String);
		M ->
		    case maps:get(R, M, not_found) of
			not_found ->
			    list_to_atom(String);
			Node ->
			    info_msg(upd_with_row, [String], Node, 50),
			    Node
		    end
	    end;
	_ ->
	    list_to_atom(String)
    end.

ui_resolve_process_synonym("BS") ->
    b3s_state;
ui_resolve_process_synonym("NS") ->
    node_state;
ui_resolve_process_synonym("TD") ->
    triple_distributor;
ui_resolve_process_synonym("SW") ->
    stop_watch;
ui_resolve_process_synonym("SI") ->
    string_id;
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
    L = [fun ura_expr/1, fun ura_term/1],
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

ura_expr(A) ->
    case (catch erl_scan:string(A ++ ".")) of
	{'EXIT', E} ->
	    error_msg(ura_expr, [A], {scan_error, E}),
	    A;
	{ok, Tokens, _} ->
	    case (catch erl_parse:parse_exprs(Tokens)) of
		{'EXIT', E} ->
		    error_msg(ura_expr, [A], {parse_error, E}),
		    A;
		{ok, [Expr]} ->
		    BS = erl_eval:bindings(erl_eval:new_bindings()),
		    case (catch erl_eval:expr(Expr, BS)) of
			{'EXIT', E} ->
			    error_msg(ura_expr, [A], {expr_error, E}),
			    A;
			{value, V, _} ->
			    V
		    end;
		_ -> A
	    end;
	_ -> A
    end.

ura_term(A) ->
    case (catch erl_scan:string(A ++ ".")) of
	{'EXIT', _} ->
	    %% error_msg(ura_term, [A], {scan_error, E}),
	    A;
	{ok, Tokens, _} ->
	    case (catch erl_parse:parse_term(Tokens)) of
		{'EXIT', E} ->
		    error_msg(ura_term, [A], {parse_error, E}),
		    A;
		{ok, Term} ->
		    Term;
		_ -> A
	    end;
	_ -> A
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
	    io:fwrite("usage: config [help | down | debug | modules [short|NodeList]]\n");
	[] ->
	    ucr_default();
	["down"] ->
	    ucr_list_down_data_servers();
	["debug"] ->
	    ucr_list_debug_level();
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

ucr_ldds() ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    DS = gen_server:call(BS, {get, data_server_nodes}),
    CR = maps:to_list(gen_server:call(BS, {get, clm_row_conf})),
    FC = fun ({C, R}) ->
		 FR = fun ({_, N}) ->
			      FD = fun ({Nd, _})
					 when Nd == N
					      -> {true, N};
				       (_)    -> false
				   end,
			      case lists:filtermap(FD, DS) of
				  [] -> {true, {C, N}};
				  _ ->  false
			      end
		      end,
		 L = maps:to_list(R),
		 case lists:filtermap(FR, L) of
		     [] -> false;
		     D  -> {true, D}
		 end
	 end,
    put(down_data_servers, lists:filtermap(FC, CR)).

ucr_list_down_data_servers() ->
    ucr_ldds(),
    io:fwrite("~p\n", [get(down_data_servers)]),
    "".

ucr_list_debug_level() ->
    TR = tmp_report,
    put(TR, queue:new()),
    ui_rpc_get_property_all(debug_level),
    put(TR, queue:in([], get(TR))),
    R = benchmark:hc_report_generate(queue:out(get(tmp_report)), []),
    io:fwrite(R),
    "".

ucr_default() ->
    TR = tmp_report,
    put(TR, queue:new()),
    ui_b3s_property(front_server_nodes),
    ui_b3s_property_list_kv(data_server_nodes),
    %% ui_b3s_property_list_kv(name_of_triple_tables),
    %% benchmark:hr_property(b3s_state_nodes),
    %% ui_b3s_property(triple_distributor_nodes),
    ui_b3s_property(num_of_empty_msgs),
    ui_b3s_property(block_size),
    ui_td_property(distribution_algorithm),
    ui_b3s_property(name_of_pred_clm_table),
    ui_b3s_property(name_of_pred_freq_table),
    ui_b3s_property(name_of_string_id_table),
    ui_b3s_property(name_of_pred_string_id),
    ucr_ldds(),
    benchmark:hr_property(down_data_servers),

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
	[Node, "all"] ->
	    NL = [Node],
	    ML = [all];
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
    F = "| ~-20s | ~-20s | ~8s | ~8s | ~8s |\n",
    Hpi = string:chars($ , 5) ++ "Process Id",
    Hmn = string:chars($ , 4) ++ "Module Name",
    Hth = "To. Heap",
    Hmc = "Mes. Cnt",
    Hmm = "Mes. Max",
    io:fwrite(F, [Hpi, Hmn, Hth, Hmc, Hmm]),
    Spi = string:chars($-, 20),
    Smn = string:chars($-, 20),
    S08 = string:chars($-, 8),
    io:fwrite(F, [Spi, Smn, S08, S08, S08]),

    case ModuleList of
	[all] ->
	    FA = fun (P) ->
			 case rpc:call(N, erlang, process_info, [P, current_function]) of
			     undefined ->
				 false;
			     CF ->
				 MD = element(1, element(2, CF)),
				 ID = element(2, element(2, CF)),
				 up_wriite_process(N, ID, MD, P),
				 true
			 end
		 end,
	    R = lists:filtermap(FA, rpc:call(N, erlang, processes, [])),
	    up_each_process(R, sets:from_list(ModuleList), ModuleList, N);

	_ ->
	    case (catch rpc:call(N, supervisor, which_children, [b3s])) of
		{'EXIT', E} ->
		    io:fwrite("  ~p\n", [E]);
		R ->
		    up_each_process(R, sets:from_list(ModuleList), ModuleList, N)
	    end
    end.

up_each_process([], _, _, _) ->
    "";
up_each_process([{Id, _, _, [M]} | R], MS, [], N) ->
    WI = rpc:call(N, erlang, whereis, [Id]),
    up_wriite_process(N, Id, M, WI),
    up_each_process(R, MS, [], N);
up_each_process([{Id, _, _, [M]} | R], ModuleSet, ML, N) ->
    case sets:is_element(atom_to_list(M), ModuleSet) of
	true -> 
	    WI = rpc:call(N, erlang, whereis, [Id]),
	    up_wriite_process(N, Id, M, WI);
	false -> ok
    end,
    up_each_process(R, ModuleSet, ML, N);
up_each_process(_, _, _, _) ->
    "".

up_wriite_process(Node, Pname, Module, Pid) ->
    PI = rpc:call(Node, erlang, process_info, [Pid]),
    TH = element(2, lists:keyfind(total_heap_size, 1, PI)),
    P  = {Pname, Node},
    U  = '------',
    MC = case gen_server:call(P, {get, mq_count}) of
	     undefined -> U;
	     {unknown_request, _} -> U;
	     {recorded_handle_call, _} -> U;
	     C -> C
	 end,
    MM = case gen_server:call(P, {get, mq_maxlen}) of
	     undefined -> U;
	     {unknown_request, _} -> U;
	     {recorded_handle_call, _} -> U;
	     M -> M
	 end,
    F = "| ~-20s | ~-20s | ~8w | ~8w | ~8w |\n",
    io:fwrite(F, [Pname, Module, TH, MC, MM]).

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
    TM = E / 1000 / 1000 / 60,
    T  = io_lib:format(F2, [TM]),
    A  = io_lib:format(F3, [E / length(ElaLst)]),

    M  = lists:delete(lists:max(ElaLst), ElaLst),
    ML = lists:delete(lists:min(M), M),
    AM = io_lib:format(F4, [lists:sum(ML) / length(ML)]),

    usb_notify(TM),
    H ++ S ++ Mes ++ T ++ A ++ AM;
usb_iterate(_, [], ElaLst, Mes) ->
    F1 = "\n| ~-24s | ~-10s | ~-10s |\n",
    F2 = "\n    total          ~24w minutes",
    F3 = "\n    average        ~24w microseconds\n\n",
    H  = io_lib:format(F1, ['Task Name', 'Elapsed', 'Triples']),
    S  = "| ------------------------ | ----------:| ----------:|\n",

    E  = lists:sum(ElaLst),
    TM = E / 1000 / 1000 / 60,
    T  = io_lib:format(F2, [TM]),
    A  = io_lib:format(F3, [E / length(ElaLst)]),

    usb_notify(TM),
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

    case ui_check_benchmark(BA, TA) of
	{PE, RF} ->
	    F = "| ~-24s | ~10w | ~10w |\n",
	    A = [Task, PE, RF],
	    M = io_lib:format(F, A),
	    usb_iterate(BA, Rest, EL, Mes ++ M);
	E ->
	    io_lib:format("error: ~p\n", [E])
    end.

usb_notify(TM) ->
    NN = lists:nth(1, get(b3s_state_nodes)),
    BS = {b3s_state, NN},
    NS = {node_state, NN},
    F1 = " (total ~w minutes)",
    A1 = [round(TM * 1000) / 1000],
    SM = lists:flatten(io_lib:format(F1, A1)),
    gen_server:call(BS, {put, benchmark_last_batch_elapsed, SM}),
    GC = {get, ui_run_command_finish_benchmark},
    SL = gen_server:call(erlang:get(b3s_state_pid), GC),
    UI = user_interface,
    UN = usb_notify_f2,
    FU = fun (Statement) ->
		 S = string:split(Statement, " ", all),
		 case (catch gen_server:call(NS, {perform_ui, S})) of
		     {'EXIT', E} ->
			 node_state:error_msg(UI, UN, [Statement], E),
			 R = io_lib:format("process ~p is busy.\n", [NS]);
		     R -> R
		 end,
		 node_state:info_msg(UI, UN, [Statement], R, 50)
	 end,
    lists:map(FU, SL).

usb_wait_termination(Task) ->
    NN  = lists:nth(1, get(b3s_state_nodes)),
    case (catch gen_server:call({Task, NN}, {get, query_tree_pid}, 50000)) of
	{'EXIT', E} -> 
	    A = {{get, query_tree_pid}, E},
	    error_msg(usb_wait_termination, [Task], A),
	    -1;
	{QT, _} -> usbwt_check_elapse(QT, 1000);
	_ -> usbwt_check_elapse(Task, 1000)
    end.

usbwt_check_elapse(Proc, Time) ->
    NN  = lists:nth(1, get(b3s_state_nodes)),
    case (catch gen_server:call({Proc, NN}, {get, pid_elapse}, 50000)) of
	{'EXIT', E1} ->
	    A = {{get, pid_elapse}, E1},
	    error_msg(usbwt_check_elapse, [Proc, Time], A),
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
ui_check_all_benchmark(TF) ->
    NN  = lists:nth(1, get(b3s_state_nodes)),
    case (catch rpc:call(NN, supervisor, which_children, [b3s])) of
	{'EXIT', E} ->
	    A = {which_children, E},
	    error_msg(ui_check_all_benchmark, [TF], A);
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
ui_check_benchmark(TF, Task) ->
    NN  = lists:nth(1, get(b3s_state_nodes)),
    case (catch gen_server:call({Task, NN}, {get, query_tree_pid}, 50000)) of
	{'EXIT', E} ->
	    A = {{get, query_tree_pid}, E},
	    error_msg(ui_check_benchmark, [TF, Task], A);
	{QT, _} ->
	    ucb_write_task(Task, QT);
	_ ->
	    ucb_write_task(Task, Task)
    end.

ucb_write_task(Task, Proc) ->
    NN  = lists:nth(1, get(b3s_state_nodes)),
    case (catch gen_server:call({Proc, NN}, {get, pid_elapse}, 50000)) of
	{'EXIT', EE} ->
	    AE = {{get, pid_elapse}, EE},
	    error_msg(ucb_write_task, [Task, Proc], AE),
	    PE = '*';
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
    case (catch gen_server:call({Proc, NN}, {get, result_freq}, 50000)) of
	{'EXIT', EF} ->
	    AF = {{get, result_freq}, EF},
	    error_msg(ucb_write_task, [Task, Proc], AF),
	    RF = '*';
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
    io:fwrite("usage: data_files add <data file>...\n");

ui_manage_datafiles(_, ["add" | NewFiles]) ->
    BSN = lists:nth(1, get(b3s_state_nodes)),
    DFS = gen_server:call({b3s_state, BSN}, {get, data_files}),
    NewDFS = lists:append(DFS, NewFiles),
    gen_server:call({b3s_state, BSN}, {put, data_files, NewDFS}),
    F = "adding data file(s) from data_files list...\n~p\n",
    io:fwrite(F, [NewFiles]);

ui_manage_datafiles(_, ["rem"]) ->
    io:fwrite("usage: data_files rem <data file> | <number>\n"),
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
    io:fwrite("usage: data_files [add | rem | clr]\n"),
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
ui_investigate(_, ["reuse", "pred_freq"]) ->
    uii_reuse_pred_freq();
ui_investigate(_, ["is", "finished"]) ->
    uii_is_finished();
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
    "usage: investigate [est <max>|stat|save|kill-finished|kill-all|reuse pred_freq|is finished]\n".

uii_is_finished() ->
    SLP = 1000,
    MF  = "finished.                \n",
    MA  = "~w processes alive.\r",
    GIP = {get_property, investigate_processes},
    BSN = lists:nth(1, get(b3s_state_nodes)),
    BS  = {b3s_state, BSN},
    TD  = gen_server:call(BS, {get, triple_distributor_pid}),
    FR = fun (_, []) -> MF;
	     (F, L)  ->
		 io:fwrite(MA, [length(L)]),
		 timer:sleep(SLP),
		 F(F, gen_server:call(TD, GIP))
	 end,
    FR(FR, gen_server:call(TD, GIP)).

uii_reuse_pred_freq() ->
    BSN = lists:nth(1, get(b3s_state_nodes)),
    BS  = {b3s_state, BSN},
    TD  = gen_server:call(BS, {get, triple_distributor_pid}),
    put(clm_row_conf, gen_server:call(BS, {get, clm_row_conf})),
    put(pred_freq, gen_server:call(BS, {get, pred_freq})),
    put(pred_clm, #{}),
    put(sender_processes, []),
    DA = gen_server:call(BS, {get, distribution_algorithm}),
    triple_distributor:hcis_assign_column(DA, 0),
    gen_server:call(BS, {put, pred_clm, get(pred_clm)}),
    gen_server:call(TD, {put_property, pred_clm, get(pred_clm)}),
    "".

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

ui_distribute(false, _) ->
    "b3s_state is not alive.\n";
ui_distribute(_, ["rebuild", "encoded", "column", "dump", "files"]) ->
    uid_recdf(),
    "";
ui_distribute(_, ["is", "finished"]) ->
    uid_is_finished();
ui_distribute(_, []) ->
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
    "";
ui_distribute(_, _) ->
    "usage: distribute [rebuild encoded column dump files|is finished]\n".

uid_is_finished() ->
    SLP = 1000,
    MF  = "finished.                \n",
    MA  = "~w processes alive.\r",
    GSP = {get_property, store_processes},
    BSN = lists:nth(1, get(b3s_state_nodes)),
    BS  = {b3s_state, BSN},
    TD  = gen_server:call(BS, {get, triple_distributor_pid}),
    FR = fun (_, []) -> MF;
	     (F, L)  ->
		 io:fwrite(MA, [length(L)]),
		 timer:sleep(SLP),
		 F(F, gen_server:call(TD, GSP))
	 end,
    FR(FR, gen_server:call(TD, GSP)).

uid_recdf() ->
    L1 = os:cmd("ls -1 bak/bak"),
    FL = string:split(L1, "\n", all),
    F1 = fun (X) ->
		 case string:str(X, "tse_") of
		     0 -> false;
		     _ ->
			 {true, uid_recdf_proc_file(X)}
		 end
	 end,

    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    PC = gen_server:call(BS, {get, pred_clm}),
    PI = gen_server:call(BS, {get, pred_string_id}),
    IC = id_clm,
    put(IC, #{}),
    CS = clm_serial,
    put(CS, #{}),
    F2 = fun (P, C) ->
		 put(IC, maps:put(maps:get(P, PI), C, get(IC))),
		 put(CS, maps:put(C, 0, get(CS)))
	 end,
    maps:map(F2, PC),

    DA = distribution_algorithm,
    put(DA, gen_server:call(BS, {get, DA})),

    uid_recdf_open_output_file(),
    io:fwrite("~s\n", [string:join(lists:filtermap(F1, FL), "\n")]),
    uid_recdf_close_output_file(),
    io:fwrite("~s", [os:cmd("ls -alR bak/")]).

uid_recdf_proc_file(FileName) ->
    DA = get(distribution_algorithm),
    FN = "bak/bak/" ++ FileName,
    case file:open(FN, read) of
	{ok, FR} ->
	    uid_recdf_proc_line(FN, FR, file:read_line(FR), 0, DA);
	_ ->
	    "open_failed: " ++ FN
    end.

uid_recdf_proc_line(FileName, IoDevice, {ok, Data}, Count, predicate_based) ->
    TE  = string:trim(Data),
    TS  = string:split(TE, "\t", all),
    PE  = list_to_integer(lists:nth(4, TS)),
    CL  = maps:get(PE, get(id_clm)),
    OFM = get(uid_recdf_output_file_map),
    CS  = clm_serial,
    SN  = maps:get(CL, get(CS)) + 1,
    put(CS, maps:put(CL, SN, get(CS))),
    C1  = [integer_to_list(SN)],
    T26 = lists:sublist(TS, 2, 6),
    NT  = string:join(lists:append(C1, T26), "\t"),
    file:write(maps:get(CL, OFM), io_lib:format("~s\n", [NT])),

    BS  = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    SRF = gen_server:call(BS, {get, store_report_frequency}),
    case Count rem SRF of
	0 ->
	    io:fwrite("~s (~8w)\r", [FileName, Count]);
	_ -> ok
    end,
    uid_recdf_proc_line(FileName, IoDevice, file:read_line(IoDevice), Count + 1, predicate_based);
uid_recdf_proc_line(FileName, IoDevice, {ok, Data}, Count, random) ->
    TE  = string:trim(Data),
    TS  = string:split(TE, "\t", all),
    CS  = clm_serial,
    CL  = rand:uniform(maps:size(get(CS))),
    OFM = get(uid_recdf_output_file_map),
    SN  = maps:get(CL, get(CS)) + 1,
    put(CS, maps:put(CL, SN, get(CS))),
    C1  = [integer_to_list(SN)],
    T26 = lists:sublist(TS, 2, 6),
    NT  = string:join(lists:append(C1, T26), "\t"),
    file:write(maps:get(CL, OFM), io_lib:format("~s\n", [NT])),

    BS  = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    SRF = gen_server:call(BS, {get, store_report_frequency}),
    case Count rem SRF of
	0 ->
	    io:fwrite("~s (~8w)\r", [FileName, Count]);
	_ -> ok
    end,
    uid_recdf_proc_line(FileName, IoDevice, file:read_line(IoDevice), Count + 1, random);
uid_recdf_proc_line(FileName, IoDevice, eof, Count, _) ->
    file:close(IoDevice),
    io_lib:format("processed ~s (~8w)", [FileName, Count]);
uid_recdf_proc_line(FileName, IoDevice, {error, Reason}, Count, _) ->
    file:close(IoDevice),
    io_lib:format("error ~s (~8w): ~p", [FileName, Count, Reason]).

uid_recdf_open_output_file() ->
    BS  = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    CRC = gen_server:call(BS, {get, clm_row_conf}),
    OFM = uid_recdf_output_file_map,
    FS  = "bak/tse_col~2..0w.tsv",
    F = fun (X) ->
		FN = io_lib:format(FS, [X]),
		{ok, FO} = file:open(FN, write),
		put(OFM, maps:put(X, FO, get(OFM)))
	end,
    put(OFM, #{}),
    lists:map(F, maps:keys(CRC)),
    "".

uid_recdf_close_output_file() ->
    OFM = uid_recdf_output_file_map,
    F = fun (X) -> file:close(maps:get(X, get(OFM))) end,
    lists:map(F, maps:keys(get(OFM))),
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
ui_operate_kill(["all", "query", "nodes"]) ->
    FB = "kill DS~2..0w-~2..0w: ~s (~s)",
    ML = [tp_query_node, join_query_node, mj_query_node, hj_query_node],
    FC = fun ({C, RM}) ->
		 FR = fun ({R, N}) ->
			      FK = fun ({Id, _, _, [M]}) ->
					   case lists:member(M, ML) of
					       true ->
						   P = atom_to_list(Id),
						   uok_one_process(N, P),
						   {true, io_lib:format(FB, [C, R, P, M])};
					       false ->
						   false
					   end
				   end,
			      case (catch rpc:call(N, supervisor, which_children, [b3s])) of
				  {'EXIT', E} ->
				      io:fwrite("  ~p\n", [E]),
				      [];
				  CL ->
				      string:join(lists:filtermap(FK, CL), "\n")
			      end
		      end,
		 string:join(lists:map(FR, maps:to_list(RM)), "\n")
	 end,

    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    CR = maps:to_list(gen_server:call(BS, {get, clm_row_conf})),
    string:join(lists:map(FC, CR), "\n") ++ "\n";
ui_operate_kill([Node | ProcessList]) ->
    F = fun (X) -> uok_one_process(Node, X) end,
    lists:foreach(F, ProcessList),
    "";
ui_operate_kill(_) ->
    "usage: kill {<node> <processes>|all query nodes}\n".

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
%% aws operation functions
%% 
ui_operate_aws(["run" | Node]) ->
    uoa_run_instances(Node);
ui_operate_aws(["describe" | Args]) ->
    uoa_describe(Args);
ui_operate_aws(["terminate" | Node]) ->
    uoa_terminate_instances(Node);
ui_operate_aws(["bucket" | Args]) ->
    uoa_bucket(Args, uoa_is_aws_mode());
ui_operate_aws(["metadata" | Args]) ->
    uoa_metadata(Args);
ui_operate_aws(["sns" | Args]) ->
    uoa_sns(Args);
ui_operate_aws(["servers.cf" | Args]) ->
    uoa_servers_cf(Args);
ui_operate_aws(["create" | Args]) ->
    uoa_create(Args);
ui_operate_aws(["delete" | Args]) ->
    uoa_delete(Args);
ui_operate_aws(["unregister" | Args]) ->
    uoa_unregister(Args);
ui_operate_aws(["reboot" | Args]) ->
    uoa_reboot(Args);
ui_operate_aws(_) ->
    "usage: aws [run|describe|terminate|bucket|metadata|sns|servers.cf|create|delete|unregister|reboot] [<args>...]\n".

%% confirm aws mode
uoa_is_aws_mode() ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    case gen_server:call(BS, {get, aws_node_instance_map}) of
	undefined -> false;
	_ ->         true
    end.

%% publish a sns message
uoa_sns_publish(Message) ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    case gen_server:call(BS, {get, aws_node_instance_map}) of
	undefined ->
	    io:fwrite("~s\n", [Message]);
	_ ->
	    {H, M, S} = time(),
	    F = "../aws/bin/sns-publish.sh [~2..0w:~2..0w:~2..0w] '~s'",
	    C = io_lib:format(F, [H, M, S, Message]),
	    I = string:trim(os:cmd(C ++ " | grep MessageId")),
	    io:fwrite("<<<~s>>>\n~s\n", [Message, I])
    end.

%% create and obtain aws_node_instance_map property on front server's
%% b3s_state process
uoadi_get_node_instance_map() ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    ANIM = gen_server:call(BS, {get, aws_node_instance_map}),
    uoadi_gnim_confirm_map(ANIM).

uoadi_gnim_confirm_map(ANIM) when is_map(ANIM) ->
    ANIM;
uoadi_gnim_confirm_map(_) ->
    HN = string:trim(os:cmd("curl -s http://169.254.169.254/latest/meta-data/hostname")),
    II = string:trim(os:cmd("curl -s http://169.254.169.254/latest/meta-data/instance-id")),
    ND = lists:flatten(io_lib:format("~s@~s", [fs, HN])),
    M  = #{list_to_atom(ND) => list_to_atom(II)},
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    gen_server:call(BS, {put, aws_node_instance_map, M}),
    M.

%% run data server instances
uoa_run_instances([Node, NOR]) ->
    ui_batch_push_queue(Node, 'boot-ds'),
    C = io_lib:format("../aws/bin/run-data-server.sh ~s ~s", [Node, NOR]),
    io:fwrite("~s\n", [C]),
    timer:sleep(1000),
    io:fwrite("~s\n", [os:cmd(C)]);
uoa_run_instances([Node, NOR | Rest]) ->
    uoa_run_instances([Node, NOR]),
    uoa_run_instances(Rest);
uoa_run_instances(_) ->
    "usage: aws run <node name> <num of rows> [<column number> <num of rows>...]\n".

%% describe
uoa_describe(["instance"]) ->
    uoa_describe_instances([]);
uoa_describe(["instance" | Node]) ->
    uoa_describe_instances(Node);
uoa_describe(["image"]) ->
    uoa_describe_image();
uoa_describe(_) ->
    "usage: aws describe {instance [<nodes>] | image}\n".

%% describe instances
uoa_describe_instances([]) ->
    RFS = os:cmd("../aws/bin/describe-front-server-instances.sh"),
    RDS = os:cmd("../aws/bin/describe-data-server-instances.sh"),

    F = "[instance id]\t\t[type]\t\t[public address]\n~s~s",
    io:fwrite(F, [RFS, RDS]),
    "";
uoa_describe_instances([Node]) ->
    N = ui_resolve_node_synonym(Node),
    udi_grep(N, list_to_atom(Node));
uoa_describe_instances([Node | Rest]) ->
    uoa_describe_instances([Node]),
    uoa_describe_instances(Rest).

udi_grep(N, N) ->
    uoa_describe_instances([]);
udi_grep(N, _) ->
    M = uoadi_get_node_instance_map(),
    FS = "../aws/bin/describe-front-server-instances.sh | grep ~s",
    RS = os:cmd(io_lib:format(FS, [maps:get(N, M)])),
    FD = "../aws/bin/describe-data-server-instances.sh | grep ~s",
    RD = os:cmd(io_lib:format(FD, [maps:get(N, M)])),

    H = "[instance id]\t\t[type]\t\t[public address]\n~s~s",
    io:fwrite(H, [RS, RD]),
    "".

%% terminate instances
uoa_terminate_instances([]) ->
    "aws terminate <node>|<instance id>|all|data servers|front server|quick and no save\n";
uoa_terminate_instances(["quick", "and", "no", "save"]) ->
    uoa_terminate_instances_quick_and_no_save();
uoa_terminate_instances(["front", "server"]) ->
    uoa_bucket_save_elog_terminate();
uoa_terminate_instances(["FS"]) ->
    uoa_terminate_instances(["data", "servers"]);
uoa_terminate_instances(["all"]) ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    DS = gen_server:call(BS, {get, data_server_nodes}),
    F = fun ({N, _}) ->
		ui_batch_push_queue(N, 'terminate-ds')
	end,
    lists:map(F, DS),
    uoa_terminate_instances(["data", "servers"]);
uoa_terminate_instances(["data", "servers"]) ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    DS = gen_server:call(BS, {get, data_server_nodes}),
    F = fun ({N, C}) ->
		DL = list_to_atom(lists:flatten(io_lib:format("DS~w", [C]))),
		uti_check(N, DL)
	end,
    lists:flatten(io_lib:format("~p\n", [lists:map(F, DS)]));
uoa_terminate_instances([Node]) ->
    N = ui_resolve_node_synonym(Node),
    io:fwrite("terminate instance ~w.\n", [N]),
    uti_check(N, list_to_atom(Node));
uoa_terminate_instances([Node | Rest]) ->
    uoa_terminate_instances([Node]),
    uoa_terminate_instances(Rest).

uoa_terminate_instances_quick_and_no_save() ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    ANF = gen_server:call(BS, {get, aws_node_fleet_map}),
    ANI = gen_server:call(BS, {get, aws_node_instance_map}),
    FF = fun (_, ID) ->
		 A = atom_to_list(ID),
		 C = "../aws/bin/cancel-spot.sh " ++ A,
		 uops_perform_cmd(C)
	 end,
    maps:map(FF, ANF),
    FI = fun (_, ID) ->
		 A = atom_to_list(ID),
		 C = "../aws/bin/terminate-instances.sh " ++ A,
		 uops_perform_cmd(C)
	 end,
    maps:map(FI, ANI),
    "bye\n".

uti_check(Id, Id) ->
    C = "../aws/bin/terminate-instances.sh " ++ Id,
    uops_perform_cmd(C);
uti_check(Node, DL) ->
    S = "aws bucket save elog and terminate",
    C = string:split(S, " ", all),
    ui_operate_remote([atom_to_list(DL) | C]),
    uti_unregister(Node, true, true).

uti_unregister(Node, Terminate, RemoveFromCRC) ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    DS = gen_server:call(BS, {get, data_server_nodes}),
    NT = gen_server:call(BS, {get, name_of_triple_tables}),
    NI = gen_server:call(BS, {get, aws_node_instance_map}),
    NF = gen_server:call(BS, {get, aws_node_fleet_map}),
    CR = gen_server:call(BS, {get, clm_row_conf}),
    TD = gen_server:call(BS, {get, triple_distributor_pid}),
    F1 = fun ({N, _}) ->
		 case N of
		     Node -> false;
		     _ -> true
		 end
	 end,
    F2 = fun (K, _) ->
		 case K of
		     Node -> false;
		     _ -> true
		 end
	 end,
    F3 = fun (_, V) ->
		 case V of
		     Node -> false;
		     _ -> true
		 end
	 end,
    F4 = fun (_, V) ->
		 case {maps:size(maps:filter(F3, V)), Terminate} of
		     {0, true} ->
			 SFR = maps:get(Node, NF),
			 F = "../aws/bin/cancel-spot.sh ~s",
			 C = io_lib:format(F, [SFR]),
			 io:fwrite("~s\n", [C]),
			 os:cmd(C),
			 false;
		     _ -> true
		 end
	 end,
    gen_server:call(BS, {put, data_server_nodes, lists:filter(F1, DS)}),
    gen_server:call(BS, {put, name_of_triple_tables, lists:filter(F1, NT)}),
    gen_server:call(BS, {put, aws_node_instance_map, maps:filter(F2, NI)}),
    gen_server:call(BS, {put, aws_node_fleet_map, maps:filter(F2, NF)}),
    case RemoveFromCRC of
	true ->
	    CRC = maps:filter(F4, CR),
	    gen_server:call(BS, {put, clm_row_conf, CRC}),
	    gen_server:call(TD, {put_property, clm_row_conf, CRC});
	_ -> crc_not_removed
    end.

%% bucket operations
uoa_bucket(_, false) ->
    "no aws instance is running.\n";
uoa_bucket(["list"], _) ->
    uoa_bucket_list([], []);
uoa_bucket(["list" | TermList], _) ->
    uoa_bucket_list(TermList, []);
uoa_bucket(["save"], _) ->
    "usage: aws bucket save {predicate dictionaries|column tables|encoded column tables|string id tables|benchmark <tasks>|elog}\n";
uoa_bucket(["save", "predicate", "dictionaries"], _) ->
    uoa_bucket_save_predicate_dictionaries();
uoa_bucket(["save", "column", "tables"], _) ->
    uoa_bucket_save_column_tables();
uoa_bucket(["save", "encoded", "column", "tables"], _) ->
    uoa_bucket_save_encoded_column_tables();
uoa_bucket(["save", "string", "id", "tables"], _) ->
    uoa_bucket_save_string_id_tables();
uoa_bucket(["save", "benchmark" | Tasks], _) ->
    uoa_bucket_save_benchmark(Tasks);
uoa_bucket(["save", "elog"], _) ->
    uoa_bucket_save_elog();
uoa_bucket(["save", "elog", "and", "terminate"], _) ->
    uoa_bucket_save_elog_terminate();
uoa_bucket(["get"], _) ->
    "usage: aws bucket get {column tables|encoded column table <node>|string id tables}\n";
uoa_bucket(["get", "column", "tables"], _) ->
    uoa_bucket_get_column_tables();
uoa_bucket(["get", "encoded", "column", "table", Node], _) ->
    uoa_bucket_get_encoded_column_table(Node);
uoa_bucket(["get", "string", "id", "tables"], _) ->
    uoa_bucket_get_string_id_tables();
uoa_bucket(["load"], _) ->
    "usage: aws bucket load predicate dictionaries\n";
uoa_bucket(["load", "predicate", "dictionaries"], _) ->
    uoa_bucket_load_predicate_dictionaries();
uoa_bucket(_, _) ->
    "usage: aws bucket list|save|get|load [<args>...]\n".

uoa_bucket_list([], []) ->
    C = "../aws/bin/s3-ls.sh",
    os:cmd(C);
uoa_bucket_list([Term | Rest], GrepCommand) ->
    GT = io_lib:format(" | grep ~s", [Term]),
    uoa_bucket_list(Rest, GrepCommand ++ GT);
uoa_bucket_list([], GrepCommand) ->
    C = "../aws/bin/s3-ls.sh",
    os:cmd(C ++ GrepCommand).

uoa_bucket_save_predicate_dictionaries() ->
    %% put(debug, true),
    L = [{pred_clm,       name_of_pred_clm_table},
	 {pred_freq,      name_of_pred_freq_table},
	 {pred_string_id, name_of_pred_string_id}],
    lists:map(fun ubsp_process_pred/1, L),
    uoa_sns_publish("finish aws bucket save predicate dictionaries"),
    "".

ubsp_process_pred({Pred, TableName}) ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    TD = gen_server:call(BS, {get, triple_distributor_pid}),
    TN = gen_server:call(BS, {get, TableName}),

    V = gen_server:call(TD, {get_property, Pred}),
    PF = io_lib:format("~s.txt", [TN]),
    {ok, FO} = file:open(PF, write),
    file:write(FO, io_lib:format("~p", [V])),
    file:close(FO),

    CM = io_lib:format("../aws/bin/s3-mv.sh ~s dat", [PF]),
    io:fwrite("~s\n", [uops_perform_cmd(CM)]).

uoa_bucket_save_column_tables() ->
    %% put(debug, true),
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    DS = gen_server:call(BS, {get, data_server_nodes}),
    lists:map(fun ubsct_process_ds/1, DS),
    uoa_sns_publish("finish aws bucket save column tables"),
    "".

ubsct_process_ds({_, ColumnId}) ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    TD = gen_server:call(BS, {get, triple_distributor_pid}),
    FP = gen_server:call(TD, {get_property, column_file_path}),
    FN = io_lib:format(FP, [ColumnId]),
    DA = gen_server:call(BS, {get, distribution_algorithm}),
    TC = maps:size(gen_server:call(BS, {get, clm_row_conf})),
    AN = io_lib:format("ts_~s_~2..0w_of_~2..0w.7z", [DA, ColumnId, TC]),
    CC = io_lib:format("7za a ~s ~s", [AN, FN]),
    CM = io_lib:format("../aws/bin/s3-mv.sh ~s dat", [AN]),
    CR = io_lib:format("rm ~s", [FN]),
    io:fwrite("~s\n", [uops_perform_cmd(CC)]),
    io:fwrite("~s\n", [uops_perform_cmd(CM)]),
    io:fwrite("~s\n", [uops_perform_cmd(CR)]).

uoa_bucket_save_encoded_column_tables() ->
    %% put(debug, true),
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    DS = gen_server:call(BS, {get, data_server_nodes}),
    lists:map(fun ubsect_process_ds/1, DS),
    uoa_sns_publish("finish aws bucket save encoded column tables"),
    "".

ubsect_process_ds({_, ColumnId}) ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    DA = gen_server:call(BS, {get, distribution_algorithm}),
    TC = maps:size(gen_server:call(BS, {get, clm_row_conf})),
    AN = io_lib:format("tse_~s_~2..0w_of_~2..0w.7z", [DA, ColumnId, TC]),
    FN = io_lib:format("bak/tse_col~2..0w.tsv", [ColumnId]),
    CC = io_lib:format("7za a ~s ~s", [AN, FN]),
    CM = io_lib:format("../aws/bin/s3-mv.sh ~s dat", [AN]),
    CR = io_lib:format("rm ~s", [FN]),
    io:fwrite("~s\n", [uops_perform_cmd(CC)]),
    io:fwrite("~s\n", [uops_perform_cmd(CM)]),
    io:fwrite("~s\n", [uops_perform_cmd(CR)]).

uoa_bucket_save_string_id_tables() ->
    %% put(debug, true),
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    DS = gen_server:call(BS, {get, data_server_nodes}),
    SI = gen_server:call(BS, {get, name_of_string_id_table}),
    AN = io_lib:format("~s.7z", [SI]),
    CM = io_lib:format("../aws/bin/s3-mv.sh ~s dat", [AN]),
    lists:map(fun ubssit_process_ds/1, DS),
    io:fwrite("~s\n", [uops_perform_cmd(CM)]),
    uoa_sns_publish("finish aws bucket save string id tables"),
    "".

ubssit_process_ds({_, ColumnId}) ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    SI = gen_server:call(BS, {get, name_of_string_id_table}),
    AN = io_lib:format("~s.7z", [SI]),
    FN = io_lib:format("bak/si_col~2..0w.tsv", [ColumnId]),
    CC = io_lib:format("7za a ~s ~s", [AN, FN]),
    CR = io_lib:format("rm ~s", [FN]),
    io:fwrite("~s\n", [uops_perform_cmd(CC)]),
    io:fwrite("~s\n", [uops_perform_cmd(CR)]).

uoa_bucket_save_benchmark([]) ->
    "usage: aws bucket save benchmark <tasks>\n";
uoa_bucket_save_benchmark(Tasks) ->
    %% put(debug, true),
    put(cmd_params, Tasks),
    NN = lists:nth(1, get(b3s_state_nodes)),
    BA = ui_confirm_b3s_state_alive(),
    RI = usb_iterate(BA, Tasks, [], []),
    RS = ubsb_write_file("benchmark-summary", RI),
    FB = fun ({Id, _, _, [benchmark]}) -> 
		 B = gen_server:call({Id, NN}, report, 3600000),
		 F = io_lib:format("benchmark-~s", [Id]),
		 {true, ubsb_write_file(F, B)};
	     (_) -> false
	 end,
    RB = case (catch rpc:call(NN, supervisor, which_children, [b3s])) of
	     {'EXIT', E} ->
		 lists:flatten(io_lib:format("~p", [E]));
	     R ->
		 lists:flatten(string:join(lists:filtermap(FB, R), "\n"))
	 end,

    FN = ubsb_file_name("benchmark-result", "7z"),
    C1 = io_lib:format("7za a ~s benchmark-*.txt", [FN]),
    C2 = io_lib:format("../aws/bin/s3-mv.sh ~s spool", [FN]),
    C3 = "rm benchmark-*.txt",
    R1 = io_lib:format("~s\n", [uops_perform_cmd(C1)]),
    R2 = io_lib:format("~s\n", [uops_perform_cmd(C2)]),
    R3 = io_lib:format("~s\n", [uops_perform_cmd(C3)]),
    RL = [R1, R2, R3],
    RC = lists:flatten(string:join(RL, "")),

    HD = "\n====> Result Summary <====\n",
    FT = "====> END OF LINE <====\n",
    BD = "\n~s\n~s\n~s\n",
    AG = [RI, RS, RB, RC],
    io_lib:format(HD ++ "~s" ++ FT ++ BD, AG).

ubsb_write_file(FileName, Result) ->
    FN = ubsb_file_name(FileName, "txt"),
    {ok, FO} = file:open(FN, write),
    file:write(FO, io_lib:format(Result, [])),
    file:close(FO),
    lists:flatten("wrote file: " ++ FN).

ubsb_file_name(FileName, Suffix) ->
    {YY, MM, DD} = date(),
    {H, M, S} = time(),
    DT = [FileName, YY, MM, DD, H, M, S, Suffix],
    SF = "~s-~4..0w~2..0w~2..0w-~2..0w~2..0w~2..0w.~s",
    io_lib:format(SF, DT).

uoa_bucket_save_elog() ->
    %% put(debug, true),
    FN = ubsb_file_name(io_lib:format("elog-~s", [node()]), "7z"),
    CL = ["make lsal",
	  io_lib:format("7za a ~s sample_log/*.log", [FN]),
	  io_lib:format("../aws/bin/s3-mv.sh ~s spool", [FN]),
	  "rm sample_log/*.log"],
    F = fun (C) ->
		io_lib:format("~s\n", [uops_perform_cmd(C)])
	end,
    lists:flatten(string:join(lists:map(F, CL), "")).

uoa_bucket_save_elog_terminate() ->
    uoa_bucket_save_elog(),
    ui_batch_pull_queue(node(), 'terminate-ds'),
    ID = string:trim(os:cmd("curl -s http://169.254.169.254/latest/meta-data/instance-id")),
    C = "../aws/bin/terminate-instances.sh " ++ ID,
    uops_perform_cmd(C).

uoa_bucket_get_column_tables() ->
    %% put(debug, true),
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    DS = gen_server:call(BS, {get, data_server_nodes}),
    lists:map(fun ubgct_process_ds/1, DS),
    uoa_sns_publish("finish aws bucket get column tables"),
    "".

ubgct_process_ds({_, ColumnId}) ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    DA = gen_server:call(BS, {get, distribution_algorithm}),
    TC = maps:size(gen_server:call(BS, {get, clm_row_conf})),
    AN = io_lib:format("ts_~s_~2..0w_of_~2..0w.7z", [DA, ColumnId, TC]),
    CC = io_lib:format("../aws/bin/s3-cp.sh dat/~s", [AN]),
    CU = io_lib:format("7za x ~s", [AN]),
    CR = io_lib:format("rm ~s", [AN]),
    io:fwrite("~s\n", [uops_perform_cmd(CC)]),
    io:fwrite("~s\n", [uops_perform_cmd(CU)]),
    io:fwrite("~s\n", [uops_perform_cmd(CR)]).

uoa_bucket_get_encoded_column_table(Node) ->
    %% put(debug, true),
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    DA = gen_server:call(BS, {get, distribution_algorithm}),
    DL = gen_server:call(BS, {get, data_server_nodes}),
    TL = gen_server:call(BS, {get, name_of_triple_tables}),
    DS = ui_resolve_node_synonym(Node),
    F = fun ({N, C}) when N =:= DS -> {true, C}; (_) -> false end,
    case lists:filtermap(F, DL) of
	[] -> CIL = [];
	CIL -> ok
    end,
    case lists:filtermap(F, TL) of
	[] -> TEL = [];
	TEL -> ok
    end,
    case CIL ++ TEL of
	[] -> io_lib:format("illegal data server: ~s\n", [Node]);
	[CI, TE] ->
	    SF = "SELECT count(*) FROM pg_class" ++
		 " WHERE relkind = 'r' AND relname like '~s';",
	    SQ = io_lib:format(SF, [TE]),
	    case uops_perform_sql(uops_get_connection(), SQ) of
		{ok, _, [{<<"0">>}]} ->
		    TC = maps:size(gen_server:call(BS, {get, clm_row_conf})),
		    AN = io_lib:format("tse_~s_~2..0w_of_~2..0w.7z", [DA, CI, TC]),
		    CC = io_lib:format("../aws/bin/s3-cp.sh dat/~s", [AN]),
		    CU = io_lib:format("7za x ~s", [AN]),
		    CR = io_lib:format("rm ~s", [AN]),
		    io:fwrite("~s\n", [uops_perform_cmd(CC)]),
		    io:fwrite("~s\n", [uops_perform_cmd(CU)]),
		    io:fwrite("~s\n", [uops_perform_cmd(CR)]),
		    %% FM = "(~s) finish aws bucket get encoded column table",
		    %% uoa_sns_publish(io_lib:format(FM, [Node])),
		    io_lib:format("file ~s was downloaded on ~s.\n", [AN, Node]);
		_ ->
		    io_lib:format("table ~s was already created on ~s.\n", [TE, Node])
	    end
    end.

uoa_bucket_get_encoded_column_table_force(Node) ->
    %% put(debug, true),
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    DA = gen_server:call(BS, {get, distribution_algorithm}),
    DL = gen_server:call(BS, {get, data_server_nodes}),
    TL = gen_server:call(BS, {get, name_of_triple_tables}),
    DS = ui_resolve_node_synonym(Node),
    F = fun ({N, C}) when N =:= DS -> {true, C}; (_) -> false end,
    case lists:filtermap(F, DL) of
	[] -> CIL = [];
	CIL -> ok
    end,
    case lists:filtermap(F, TL) of
	[] -> TEL = [];
	TEL -> ok
    end,
    case CIL ++ TEL of
	[] -> io_lib:format("illegal data server: ~s\n", [Node]);
	[CI, _] ->
	    TC = maps:size(gen_server:call(BS, {get, clm_row_conf})),
	    AN = io_lib:format("tse_~s_~2..0w_of_~2..0w.7z", [DA, CI, TC]),
	    CC = io_lib:format("../aws/bin/s3-cp.sh dat/~s", [AN]),
	    CU = io_lib:format("7za x ~s", [AN]),
	    CR = io_lib:format("rm ~s", [AN]),
	    io:fwrite("~s\n", [uops_perform_cmd(CC)]),
	    io:fwrite("~s\n", [uops_perform_cmd(CU)]),
	    io:fwrite("~s\n", [uops_perform_cmd(CR)]),
	    io_lib:format("file ~s was downloaded on ~s.\n", [AN, Node])
    end.

uoa_bucket_get_string_id_tables() ->
    %% put(debug, true),
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    SI = gen_server:call(BS, {get, name_of_string_id_table}),
    AN = io_lib:format("~s.7z", [SI]),
    CC = io_lib:format("../aws/bin/s3-cp.sh dat/~s", [AN]),
    CU = io_lib:format("7za x ~s", [AN]),
    CR = io_lib:format("rm ~s", [AN]),
    io:fwrite("~s\n", [uops_perform_cmd(CC)]),
    io:fwrite("~s\n", [uops_perform_cmd(CU)]),
    io:fwrite("~s\n", [uops_perform_cmd(CR)]),
    uoa_sns_publish("finish aws bucket get string id tables"),
    "".

uoa_bucket_load_predicate_dictionaries() ->
    %% put(debug, true),
    L = [{pred_clm,       name_of_pred_clm_table},
	 {pred_freq,      name_of_pred_freq_table},
	 {pred_string_id, name_of_pred_string_id}],
    lists:map(fun ublp_process_pred/1, L),
    "".

ublp_process_pred({Pred, TableName}) ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    TN = gen_server:call(BS, {get, TableName}),

    CM = io_lib:format("../aws/bin/s3-cp.sh dat/~s.txt", [TN]),
    io:fwrite("~s\n", [uops_perform_cmd(CM)]),
    PF = io_lib:format("~s.txt", [TN]),
    PS = os:cmd("cat " ++ PF),
    case upp_scan_parse(PS) of
	fail ->
	    io:fwrite("~s({~s, ~s}): ~s\n",
		      [ublp_process_pred, Pred, TableName, failed]);
	Term ->
	    TD = gen_server:call(BS, {get, triple_distributor_pid}),
	    RB = gen_server:call(BS, {put, Pred, Term}),
	    RT = gen_server:call(TD, {put_property, Pred, Term}),
	    io:fwrite("~p ~p\n", [RB, RT])
    end.

upp_scan_parse(String) ->
    case erl_scan:string(String ++ ".") of
	{ok, Tokens, _} ->
	    case erl_parse:parse_term(Tokens) of
		{ok, Term} ->
		    Term;
		_ -> fail
	    end;
	_ -> fail
    end.

%% metadata
uoa_metadata(_) ->
    AA = string:trim(os:cmd("curl -s http://169.254.169.254/latest/meta-data/ami-id")),
    HN = string:trim(os:cmd("curl -s http://169.254.169.254/latest/meta-data/hostname")),
    ID = string:trim(os:cmd("curl -s http://169.254.169.254/latest/meta-data/instance-id")),
    IT = string:trim(os:cmd("curl -s http://169.254.169.254/latest/meta-data/instance-type")),
    MA = string:trim(os:cmd("curl -s http://169.254.169.254/latest/meta-data/mac")),
    AZ = string:trim(os:cmd("curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone")),
    PH = string:trim(os:cmd("curl -s http://169.254.169.254/latest/meta-data/public-hostname")),
    SG = string:trim(os:cmd("curl -s http://169.254.169.254/latest/meta-data/security-groups")),

    FPH = io_lib:format(("public-hostname:   ~s\n"), [PH]),
    FHN = io_lib:format(("hostname:          ~s\n"), [HN]),
    FID = io_lib:format(("instance-id:       ~s\n"), [ID]),
    FIT = io_lib:format(("instance-type:     ~s\n"), [IT]),
    FAA = io_lib:format(("ami-id:            ~s\n"), [AA]),
    FMA = io_lib:format(("mac:               ~s\n"), [MA]),
    FAZ = io_lib:format(("availability-zone: ~s\n"), [AZ]),
    FSG = io_lib:format(("security-groups:   ~s\n"), [SG]),

    %% IA = string:trim(os:cmd("curl -s http://169.254.169.254/latest/meta-data/iam/info")),
    %% SD = string:trim(os:cmd("curl -s http://169.254.169.254/latest/meta-data/services/domain")),
    %% FIA = io_lib:format(("iam/info:          ~s\n"), [IA]),
    %% FSD = io_lib:format(("services/domain:   ~s\n"), [SD]),

    RL = [FPH, FHN, FID, FIT, FAA, FMA, FAZ, FSG],
    string:join(RL, "").

%% sns
uoa_sns(["publish" | Args]) ->
    uoa_sns_publish(string:join(Args, " ")),
    "";
uoa_sns(_) ->
    "usage: aws sns publish <message strings>\n".

%% print and write servers.cf shell script style configuration
uoa_servers_cf(_) ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    CR = maps:to_list(gen_server:call(BS, {get, clm_row_conf})),
    AM = ["aws", "metadata"],
    FF = "ds_c~2..0wr~2..0w=~s",
    FH = fun (L) ->
		 case string:str(L, "public-hostname") of
		     0 -> false;
		     _ -> true
		 end
	 end,
    FC = fun ({C, RM}) ->
		FR = fun ({R, N}) ->
			     M = ui_operate_remote([atom_to_list(N) | AM]),
			     LH = lists:filtermap(FH, string:split(M, "\n", all)),
			     info_msg(uoa_servers_cf, [], {C, R, N, LH}, 50),
			     HH = string:strip(lists:nth(2, string:split(LH, ":"))),
			     io_lib:format(FF ++ "\nlocal_" ++ FF, [C, R, HH, C, R, N])
		     end,
		string:join(lists:map(FR, maps:to_list(RM)), "\n")
	end,
    PH = string:trim(os:cmd("curl -s http://169.254.169.254/latest/meta-data/public-hostname")),
    FS = io_lib:format("\nfs=~s\n", [PH]),
    R = FS ++ string:join(lists:map(FC, CR), "\n") ++ "\n\n",
    {ok, FO} = file:open("../aws/cf/servers.cf", write),
    file:write(FO, R),
    file:close(FO),
    R.

% create
uoa_create(["image", Name]) ->
    %% put(debug, true),
    C = "../aws/bin/create-ami.sh " ++ Name,
    lists:flatten(io_lib:format("~p\n", [uops_perform_cmd(C)]));
uoa_create(["template", "front", "server"]) ->
    %% put(debug, true),
    CLT = "../aws/bin/create-launch-template-front-server.sh",
    CTA = "../aws/bin/create-tags-ami.sh",
    A = [uops_perform_cmd(CLT), uops_perform_cmd(CTA)],
    lists:flatten(io_lib:format("~s~s", A));
uoa_create(["data", "server", "archive"]) ->
    CL = "../aws/bin/setup-bucket.sh",
    A = [uops_perform_cmd(CL)],
    lists:flatten(io_lib:format("~s", A));
uoa_create(_) ->
    "usage: aws create {image <name>|template front server|data server archive}\n".

% describe images
uoa_describe_image() ->
    DLT = "../aws/bin/describe-launch-templates.sh",
    DIG = "../aws/bin/describe-images.sh",
    DSS = "../aws/bin/describe-snapshots.sh",

    FIG = fun (L) ->
		  case string:split(L, "\t", all) of
		      [_, _] ->
			  F = "\t\t\t\t~s",
			  lists:flatten(io_lib:format(F, [L]));
		      _ ->
			  L
		  end
	  end,
    CIG = uops_perform_cmd(DIG),
    LIG = string:split(CIG, "\n", all),
    RIG = string:join(lists:map(FIG, LIG), "\n"),

    FSS = fun ([]) -> "";
	      (L)  ->
		  [S, V, T] = string:split(L, "\t", all),
		  F = "~s\t~s\t~s",
		  lists:flatten(io_lib:format(F, [T, S, V]))
	  end,
    CSS = uops_perform_cmd(DSS),
    LSS = string:split(CSS, "\n", all),
    RSS = string:join(lists:map(FSS, LSS), "\n"),

    R = [uops_perform_cmd(DLT), RIG, RSS],
    lists:flatten(io_lib:format("\n~s~s~s\n", R)).

% delete resources
uoa_delete(["image", Name]) ->
    C = "../aws/bin/deregister-image.sh " ++ Name,
    lists:flatten(io_lib:format("~s", [uops_perform_cmd(C)]));
uoa_delete(["snapshot", Name]) ->
    C = "../aws/bin/delete-snapshot.sh " ++ Name,
    lists:flatten(io_lib:format("~s", [uops_perform_cmd(C)]));
uoa_delete(["bucket", Name]) ->
    C = "../aws/bin/s3-rm.sh " ++ Name,
    lists:flatten(io_lib:format("~s", [uops_perform_cmd(C)]));
uoa_delete(_) ->
    "usage: aws delete {image <image id> | snapshot <snapshot id> | bucket <file path>}\n".

% unregister
uoa_unregister(["all"]) ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    DS = gen_server:call(BS, {get, data_server_nodes}),
    F = fun ({N, _}) ->
		NL = atom_to_list(N),
		uoa_unregister([NL])
	end,
    string:join(lists:map(F, DS), "");
uoa_unregister([Node]) ->
    N = ui_resolve_node_synonym(Node),
    uti_unregister(N, false, false),
    lists:flatten("unregistered " ++ Node ++ ".\n");
uoa_unregister([Node | Rest]) ->
    uoa_unregister([Node]) ++ uoa_unregister(Rest);
uoa_unregister(_) ->
    "usage: aws unregister {all|<node>...}\n".

% reboot
uoa_reboot(["all"]) ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    DS = gen_server:call(BS, {get, data_server_nodes}),
    F = fun ({N, _}) ->
		NL = atom_to_list(N),
		uoa_reboot([NL])
	end,
    string:join(lists:map(F, DS), "");
uoa_reboot([Node]) ->
    N = ui_resolve_node_synonym(Node),
    uti_unregister(N, false, false),
    ui_batch_push_queue(N, 'boot-ds'),
    rpc:call(N, os, cmd, ["reboot"]),
    lists:flatten("rebooting " ++ Node ++ ".\n");
uoa_reboot([Node | Rest]) ->
    uoa_reboot([Node]) ++ uoa_reboot(Rest);
uoa_reboot(_) ->
    "usage: aws reboot {all|<node>...}\n".

%% 
%% local operation functions
%% 
ui_operate_local(["run" | Args]) ->
    uol_run_nodes(Args);
ui_operate_local(["terminate" | Args]) ->
    uol_terminate_nodes(Args);
ui_operate_local(["restart-ds" | Args]) ->
    uol_restart_ds(Args);
ui_operate_local(_) ->
    "usage: local run|terminate|restart-ds <args>...\n".

uol_run_nodes(["all"]) ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    urn_process_node(gen_server:call(BS, {get, data_server_nodes})),
    timer:sleep(3000),
    gen_server:call(BS, propagate),
    "";
uol_run_nodes([Node, ColumnId]) ->
    %% put(debug, true),
    H = list_to_atom(lists:nth(2, string:split(atom_to_list(node()), "@"))),
    N = list_to_atom(lists:flatten(io_lib:format("~s@~s", [Node, H]))),
    case (catch list_to_integer(ColumnId)) of
    	{'EXIT', _} ->
    	    io_lib:format("~s must be an integer.", [ColumnId]);
    	CI ->
    	    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    	    TD = gen_server:call(BS, {get, triple_distributor_pid}),
    	    RN = {register_node, CI, N},
    	    gen_server:call(TD, RN),
    	    CRC = gen_server:call(TD, {get_property, clm_row_conf}),
    	    gen_server:call(BS, {put, clm_row_conf, CRC}),

    	    F = "make start-ds-aws DS=~s FSHN=~s",
    	    C = io_lib:format(F, [Node, H]),
    	    uops_perform_cmd(C)
    end;
uol_run_nodes(_) ->
    "usage: local run {all|<node name> <column id>}\n".

urn_process_node([{DSN, CID}]) ->
    DCL = list_to_atom(lists:nth(1, string:split(atom_to_list(DSN), "@"))),
    HST = list_to_atom(lists:nth(2, string:split(atom_to_list(node()), "@"))),
    F = "make start-ds DS=~s FSHN=~s",
    C = io_lib:format(F, [DCL, HST]),
    io:fwrite("~s\n", [uops_perform_cmd(C)]),

    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    TD = gen_server:call(BS, {get, triple_distributor_pid}),
    gen_server:call(TD, {register_node, CID, DSN}),
    CRC = clm_row_conf,
    gen_server:call(BS, {put, CRC, gen_server:call(TD, {get_property, CRC})}),

    "";
urn_process_node([N | Rest]) ->
    urn_process_node([N]),
    urn_process_node(Rest);
urn_process_node([]) ->
    "".

uol_terminate_nodes(["all"]) ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    utn_process_node(gen_server:call(BS, {get, data_server_nodes})),
    C = "make terminate-fs FS=fs",
    io:fwrite("~s\n", [uops_perform_cmd(C)]),
    "";
uol_terminate_nodes([Node]) ->
    DH = lists:nth(2, string:split(atom_to_list(node()), "@")),
    DS = list_to_atom(lists:flatten(io_lib:format("~s@~s", [Node, DH]))),
    uti_unregister(DS, false, true),
    F = "make term DS=~s",
    R = lists:flatten(uops_perform_cmd(io_lib:format(F, [Node]))),
    io_lib:format("~s\n", [R]);

uol_terminate_nodes(_) ->
    "usage: local terminate all|<short node name>\n".

utn_process_node([{DSN, _}]) ->
    DCL = list_to_atom(lists:nth(1, string:split(atom_to_list(DSN), "@"))),
    F = "make terminate-ds DS=~s",
    C = io_lib:format(F, [DCL]),
    io:fwrite("~s\n", [uops_perform_cmd(C)]),
    "";
utn_process_node([N | Rest]) ->
    utn_process_node([N]),
    utn_process_node(Rest);
utn_process_node([]) ->
    "".

uol_restart_ds([N]) ->
    %% put(debug, true),
    FS = lists:nth(1, get(b3s_state_nodes)),
    FH = list_to_atom(lists:nth(2, string:split(atom_to_list(FS), "@"))),
    F  = "echo make restart-ds-aws DS=~s FSHN=~s > a.sh; at -f a.sh now + 1 minutes",
    NA = list_to_atom(N),
    case ui_resolve_node_synonym(N) of
	NA -> "no node " ++ N ++ "\n";
	DS ->
	    uti_unregister(DS, false, false),
	    timer:sleep(1000),
	    DL = list_to_atom(lists:nth(1, string:split(atom_to_list(DS), "@"))),
	    R  = lists:flatten(uops_perform_cmd(io_lib:format(F, [DL, FH]))),
	    info_msg(uol_restart_ds, [N], {command, R}, 50),
	    io_lib:format("~s\n", [R])
    end;

uol_restart_ds(_) ->
    "usage: local restart-ds <node>\n".

%% 
%% property operation functions
%% 
ui_operate_property(["cp" | Args]) ->
    uop_cp_property(Args);
ui_operate_property(["write" | Args]) ->
    uop_write_property(Args);
ui_operate_property(["find" | Args]) ->
    io_lib:format("~p\n", [uop_find_property(Args, [])]);
ui_operate_property(["f" | Args]) ->
    ui_operate_property(["find" | Args]);
ui_operate_property(["F" | Args]) ->
    ui_operate_property(["find" | Args]);
ui_operate_property(["backup", "b3s", "state", "to" | Args]) ->
    uop_backup_b3s_state(Args);
ui_operate_property(["restore", "b3s", "state", "from" | Args]) ->
    uop_restore_b3s_state(Args);
ui_operate_property(["construct", "clm_row_conf" | Args]) ->
    uop_cons_crc(Args);
ui_operate_property(["append", "data", "server" | Args]) ->
    uop_append_ds(Args);
ui_operate_property(_) ->
    "usage: property {cp|write|find|backup b3s state to|restore b3s state from|construct clm_row_conf|append data server} <args>...\n".

uop_cp_property([SrcNode, SrcProc, Property, DstNode, DstProc]) ->
    SN = ui_resolve_node_synonym(SrcNode),
    SP = ui_resolve_process_synonym(SrcProc),
    P  = list_to_atom(Property),
    DN = ui_resolve_node_synonym(DstNode),
    DP = ui_resolve_process_synonym(DstProc),
    F = "copy {~s, ~s}: ~s to {~s, ~s}.\n",
    io:fwrite(F, [SP, SN, P, DP, DN]),
    ucp_perform(SrcProc, DstProc, [], [], SP, SN, P, DP, DN);
uop_cp_property(_) ->
    "usage: property cp <source node> <source process>" ++
    " <property name> <destination node> <destination process>\n".

ucp_perform(    "BS",    DstNode, [],           PutCommand,   SP, SN, P, DP, DN) ->
    ucp_perform([],      DstNode, get,          PutCommand,   SP, SN, P, DP, DN);
ucp_perform(    "NS",    DstNode, [],           PutCommand,   SP, SN, P, DP, DN) ->
    ucp_perform([],      DstNode, get,          PutCommand,   SP, SN, P, DP, DN);
ucp_perform(    _,       DstNode, [],           PutCommand,   SP, SN, P, DP, DN) ->
    ucp_perform([],      DstNode, get_property, PutCommand,   SP, SN, P, DP, DN);
ucp_perform(    SrcNode, "BS",    GetCommand,   [],           SP, SN, P, DP, DN) ->
    ucp_perform(SrcNode, [],      GetCommand,   put,          SP, SN, P, DP, DN);
ucp_perform(    SrcNode, "NS",    GetCommand,   [],           SP, SN, P, DP, DN) ->
    ucp_perform(SrcNode, [],      GetCommand,   put,          SP, SN, P, DP, DN);
ucp_perform(    SrcNode, _,       GetCommand,   [],           SP, SN, P, DP, DN) ->
    ucp_perform(SrcNode, [],      GetCommand,   put_property, SP, SN, P, DP, DN);

ucp_perform(_, _, GC, PC, SP, SN, P, DP, DN) ->
    V = gen_server:call({SP, SN}, {GC, P}),
    R = gen_server:call({DP, DN}, {PC, P, V}),
    io:fwrite("~s\n", [R]),
    "".

uop_write_property([SrcNode, SrcProc, Property, Path]) ->
    SN = ui_resolve_node_synonym(SrcNode),
    SP = ui_resolve_process_synonym(SrcProc),
    PR  = list_to_atom(Property),
    PT  = list_to_atom(Path),
    F = "write {~s, ~s}: ~s to ~s.\n",
    io:fwrite(F, [SP, SN, PR, PT]),
    uwp_perform(SrcProc, [], SP, SN, PR, PT);
uop_write_property(_) ->
    "usage: property write <source node> <source process>" ++
    " <property name> <destination file path>\n".

uwp_perform(    "BS", [],             SP, SN, PR, PT) ->
    uwp_perform([],   get,            SP, SN, PR, PT);
uwp_perform(    "NS", [],             SP, SN, PR, PT) ->
    uwp_perform([],   get,            SP, SN, PR, PT);
uwp_perform(    _,    [],             SP, SN, PR, PT) ->
    uwp_perform([],   get_property,   SP, SN, PR, PT);

uwp_perform(_, GC, SP, SN, PR, PT) ->
    V = gen_server:call({SP, SN}, {GC, PR}),
    {ok, FO} = file:open(PT, write),
    VL = io_lib:format("~p", [V]),
    file:write(FO, VL),
    file:close(FO),
    "".

uop_find_property([Node, "BS" | Args], []) ->
    uop_find_property([Node, "BS" | Args], get);
uop_find_property([Node, "NS" | Args], []) ->
    uop_find_property([Node, "NS" | Args], get);
uop_find_property([Node, "SI" | Args], []) ->
    uop_find_property([Node, "NS" | Args], get);
uop_find_property([Node, Proc | Args], []) ->
    P1 = string:prefix(Proc, "task"),
    P2 = string:prefix(Proc, "qt"),
    case {P1, P2} of
	{nomatch, nomatch} ->
	    uop_find_property([Node, Proc | Args], get_property);
	_ ->
	    uop_find_property([Node, Proc | Args], get)
    end;
uop_find_property([Node, Proc, GrepString], GetCommand) ->
    N = ui_resolve_node_synonym(Node),
    P = ui_resolve_process_synonym(Proc),
    F = fun ({PN, _}) ->
		case string:str(atom_to_list(PN), GrepString) of
		    0 -> false;
		    _ -> true
		end
	end,
    case (catch gen_server:call({P, N}, {GetCommand, all})) of
	{'EXIT', _} -> [];
	PL -> lists:filter(F, PL)
    end;
uop_find_property([Node, Proc, GrepString | Rest], GetCommand) ->
    F = fun ({PN, _}) ->
		case string:str(atom_to_list(PN), GrepString) of
		    0 -> false;
		    _ -> true
		end
	end,
    PL = uop_find_property([Node, Proc | Rest], GetCommand),
    lists:filter(F, PL);
uop_find_property(_, _) ->
    "usage: property find <source node> <source process> <string>...\n".

uop_backup_b3s_state(["all"]) ->
    F  = "backup b3s_state to DS~w-~w (~s)",
    FC = fun ({C, RM}) ->
		 FR = fun ({R, N}) ->
			      uop_backup_b3s_state([N]),
			      io_lib:format(F, [C, R, N])
		      end,
		 lists:map(FR, maps:to_list(RM))
	 end,

    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    CR = maps:to_list(gen_server:call(BS, {get, clm_row_conf})),
    R  = lists:map(FC, CR),
    string:join(R, "\n") ++ "\n";
uop_backup_b3s_state([Node]) ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    DL = gen_server:call(BS, {get, data_server_nodes}),
    DS = ui_resolve_node_synonym(Node),
    FD = fun ({N, _}) when N == DS -> true; (_) -> false end,
    case length(lists:filtermap(FD, DL)) of
	0 -> io_lib:format("illegal data server: ~s\n", [Node]);
	_ ->
	    uop_cp_property(["FS", "BS", "clm_row_conf", Node, "NS"]),
	    uop_cp_property(["FS", "BS", "pred_clm", Node, "NS"]),
	    uop_cp_property(["FS", "BS", "pred_freq", Node, "NS"]),
	    uop_cp_property(["FS", "BS", "data_server_nodes", Node, "NS"]),
	    uop_cp_property(["FS", "BS", "name_of_triple_tables", Node, "NS"]),
	    uop_cp_property(["FS", "BS", "aws_node_instance_map", Node, "NS"]),
	    uop_cp_property(["FS", "BS", "aws_node_fleet_map", Node, "NS"]),
	    "ok\n"
    end;
uop_backup_b3s_state(_) ->
    "usage: property backup b3s state to <node>\n".

uop_restore_b3s_state([Node]) ->
    uop_cp_property([Node, "NS", "clm_row_conf", "FS", "BS"]),
    uop_cp_property([Node, "NS", "pred_clm", "FS", "BS"]),
    uop_cp_property([Node, "NS", "pred_freq", "FS", "BS"]),
    uop_cp_property([Node, "NS", "data_server_nodes", "FS", "BS"]),
    uop_cp_property([Node, "NS", "name_of_triple_tables", "FS", "BS"]),
    uop_cp_property([Node, "NS", "aws_node_instance_map", "FS", "BS"]),
    uop_cp_property([Node, "NS", "aws_node_fleet_map", "FS", "BS"]),
    "ok\n";
uop_restore_b3s_state(_) ->
    "usage: property restore b3s state from <node>\n".

uop_cons_crc(_) ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    TD = gen_server:call(BS, {get, triple_distributor_pid}),
    NDS = "no data server defined.\n",
    F = fun ({N, C}) ->
		M = #{1 => N},
		put(clm_row_conf, maps:put(C, M, get(clm_row_conf)))
	end,
    case gen_server:call(BS, {get, data_server_nodes}) of
	undefined -> NDS;
	[] -> NDS;
	DL ->
	    put(clm_row_conf, #{}),
	    lists:map(F, DL),
	    CRC = get(clm_row_conf),
	    gen_server:call(BS, {put, clm_row_conf, CRC}),
	    gen_server:call(TD, {put_property, clm_row_conf, CRC}),
	    io_lib:format("constructed clm_row_conf: ~p\n", [CRC])
    end.

uop_append_ds([Column, Node]) ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    DS = gen_server:call(BS, {get, data_server_nodes}),
    case (catch list_to_integer(Column)) of
	{'EXIT', _} ->
	    uop_append_ds([]);
	CL ->
	    ND = list_to_atom(Node),
	    NL = lists:append(DS, [{ND, CL}]),
	    gen_server:call(BS, {put, data_server_nodes, NL}),
	    io_lib:format("~p\n", [NL])
    end;
uop_append_ds(_) ->
    "usage: property append data server <column> <node>\n".

%% 
%% postgre sql operation functions
%% 
ui_operate_psql(["load" | Args]) ->
    uops_load(uops_get_connection(), Args);
ui_operate_psql(["encode" | Args]) ->
    uops_encode(uops_get_connection(), Args);
ui_operate_psql(["save" | Args]) ->
    uops_save(uops_get_connection(), Args);
ui_operate_psql(["describe" | Args]) ->
    uops_describe(uops_get_connection(), Args);
ui_operate_psql(_) ->
    "usage: psql load|encode|save|describe\n".

uops_perform_sql(fail, _) ->
    fail;
uops_perform_sql(C, Sql) ->
    {H, M, S} = time(),
    io:fwrite("~2..0w:~2..0w:~2..0w ~p ~s\n", [H, M, S, C, Sql]),
    case get(debug) of
	undefined -> epgsql:squery(C, Sql);
	_ -> ok
    end.

uops_perform_cmd(C) ->
    {H, M, S} = time(),
    io:fwrite("~2..0w:~2..0w:~2..0w ~s\n", [H, M, S, C]),
    case get(debug) of
	undefined -> os:cmd(C);
	_ -> ok
    end.

uops_get_connection() ->
    ugc_pdic(get(psql_connection)).

ugc_pdic(undefined) ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    LHS = gen_server:call(BS, {get, epgsql_host}),
    USN = gen_server:call(BS, {get, epgsql_user}),
    PWD = gen_server:call(BS, {get, epgsql_pass}),
    OPT = [{port, gen_server:call(BS, {get, epgsql_port})}], 
    ugc_perform(epgsql:connect(LHS, USN, PWD, OPT));
ugc_pdic(C) ->
    C.

ugc_perform({ok, C}) ->
    io:fwrite("psql connected ~p\n", [C]),
    put(psql_connection, C),
    C;
ugc_perform(E) ->
    error_msg(dip_get_connection, [get(self), {error,E}], epgsql_connect_error),
    fail.

%% load
uops_load(fail, _) ->
    "failed connecting postgre sql server.";
uops_load(_, ["column", "tables"]) ->
    %% put(debug, true),
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    DS = gen_server:call(BS, {get, data_server_nodes}),
    lists:map(fun uops_load_ds/1, DS),
    epgsql:close(put(psql_connection, undefined)),
    uoa_sns_publish("finish psql load column tables"),
    "";
uops_load(_, ["encoded", "column", "table", Node]) ->
    uops_load_encoded_column_table(Node);
uops_load(_, ["encoded", "column", "table", Node, "force"]) ->
    uops_load_encoded_column_table_force(Node);
uops_load(C, ["string", "id", "tables"]) ->
    %% put(debug, true),
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    SI = gen_server:call(BS, {get, name_of_string_id_table}),
    FL = string:split(string:trim(os:cmd("ls bak/si_*.tsv")), "\n", all),
    SQ = io_lib:format("SELECT reset_si('~s');", [SI]),

    io:fwrite("~p\n", [FL]),

    io:fwrite("~p\n", [uops_perform_sql(C, SQ)]),
    put(column_id, 1),
    lists:map(fun uops_load_sit_fl/1, FL),
    epgsql:close(put(psql_connection, undefined)),
    uoa_sns_publish("finish psql load string id tables"),
    "";
uops_load(_, _) ->
    "usage: psql load {column tables|encoded column table <node> [force]|string id tables}\n".

uops_load_ds({_, ColumnId}) ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    TD = gen_server:call(BS, {get, triple_distributor_pid}),
    FP = gen_server:call(TD, {get_property, column_file_path}),
    WD = string:trim(os:cmd("pwd")),
    FN = io_lib:format("~s/" ++ FP, [WD, ColumnId]),
    DA = gen_server:call(BS, {get, distribution_algorithm}),
    TN = io_lib:format("ts_~s_col~2..0w", [DA, ColumnId]),
    S1 = io_lib:format("SELECT reset_ts('~s');", [TN]),
    S2 = io_lib:format("COPY ~s FROM '~s';", [TN, FN]),
    CR = io_lib:format("rm ~s", [FN]),
    io:fwrite("~p\n", [uops_perform_sql(uops_get_connection(), S1)]),
    io:fwrite("~p\n", [uops_perform_sql(uops_get_connection(), S2)]),
    io:fwrite("~s\n", [uops_perform_cmd(CR)]).

uops_load_sit_fl(FileName) ->
    uoa_sns_publish("start loading string id file " ++ FileName),
    ColumnId = put(column_id, get(column_id) + 1),
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    SI = gen_server:call(BS, {get, name_of_string_id_table}),
    CL = io_lib:format("col~2..0w", [ColumnId]),
    WD = string:trim(os:cmd("pwd")),
    TS = io_lib:format("~s_~s", [SI, CL]),
    FS = io_lib:format("~s/~s", [WD, FileName]),
    S1 = io_lib:format("SELECT reset_sip('~s', '~s');", [SI, CL]),
    S2 = io_lib:format("DROP INDEX IF EXISTS ~s_ix_str_h1k;", [TS]),
    S3 = io_lib:format("COPY ~s FROM '~s';", [TS, FS]),
    S4 = io_lib:format("SELECT index_si_str_h1k('~s', '~s');", [SI, CL]),
    CR = io_lib:format("rm ~s", [FS]),
    io:fwrite("~p\n", [uops_perform_sql(uops_get_connection(), S1)]),
    io:fwrite("~p\n", [uops_perform_sql(uops_get_connection(), S2)]),
    io:fwrite("~p\n", [uops_perform_sql(uops_get_connection(), S3)]),
    io:fwrite("~s\n", [uops_perform_cmd(CR)]),
    io:fwrite("~p\n", [uops_perform_sql(uops_get_connection(), S4)]).

uops_load_encoded_column_table(Node) ->
    %% put(debug, true),
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    DL = gen_server:call(BS, {get, data_server_nodes}),
    TL = gen_server:call(BS, {get, name_of_triple_tables}),
    DS = ui_resolve_node_synonym(Node),
    WD = string:trim(os:cmd("pwd")),
    F = fun ({N, C}) when N =:= DS -> {true, C}; (_) -> false end,
    case lists:filtermap(F, DL) of
	[] -> CIL = [];
	CIL -> ok
    end,
    case lists:filtermap(F, TL) of
	[] -> TEL = [];
	TEL -> ok
    end,
    case CIL ++ TEL of
	[] -> R = io_lib:format("illegal data server: ~s\n", [Node]);
	[CI, TE] ->
	    SF = "SELECT count(*) FROM pg_class" ++
		 " WHERE relkind = 'r' AND relname like '~s';",
	    SQ = io_lib:format(SF, [TE]),
	    case uops_perform_sql(uops_get_connection(), SQ) of
		{ok, _, [{<<"0">>}]} ->
		    R1 = lists:flatten(uoa_bucket_get_encoded_column_table(Node)),
		    FE = io_lib:format("~s/bak/tse_col~2..0w.tsv", [WD, CI]),
		    S1 = io_lib:format("SELECT reset_tse('~s');", [TE]),
		    S2 = io_lib:format("COPY ~s FROM '~s';", [TE, FE]),
		    S3 = io_lib:format("SELECT index_tse('~s');", [TE]),
		    CR = io_lib:format("rm ~s", [FE]),
		    io:fwrite("~p\n", [uops_perform_sql(uops_get_connection(), S1)]),
		    io:fwrite("~p\n", [uops_perform_sql(uops_get_connection(), S2)]),
		    io:fwrite("~s\n", [uops_perform_cmd(CR)]),
		    io:fwrite("~p\n", [uops_perform_sql(uops_get_connection(), S3)]),
		    %% FM = "(~s) finish psql load encoded column table",
		    %% uoa_sns_publish(io_lib:format(FM, [Node])),
		    R = R1 ++ io_lib:format("table ~s was loaded on ~s.\n", [TE, Node]);
		_ ->
		    R = io_lib:format("table ~s was already created on ~s.\n", [TE, Node])
	    end,
	    ui_batch_pull_queue(DS, 'load-ds')
    end,
    epgsql:close(put(psql_connection, undefined)),
    R.

uops_load_encoded_column_table_force(Node) ->
    %% put(debug, true),
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    DL = gen_server:call(BS, {get, data_server_nodes}),
    TL = gen_server:call(BS, {get, name_of_triple_tables}),
    DS = ui_resolve_node_synonym(Node),
    WD = string:trim(os:cmd("pwd")),
    F = fun ({N, C}) when N =:= DS -> {true, C}; (_) -> false end,
    case lists:filtermap(F, DL) of
	[] -> CIL = [];
	CIL -> ok
    end,
    case lists:filtermap(F, TL) of
	[] -> TEL = [];
	TEL -> ok
    end,
    case CIL ++ TEL of
	[] -> R = io_lib:format("illegal data server: ~s\n", [Node]);
	[CI, TE] ->
	    R1 = lists:flatten(uoa_bucket_get_encoded_column_table_force(Node)),
	    FE = io_lib:format("~s/bak/tse_col~2..0w.tsv", [WD, CI]),
	    S1 = io_lib:format("SELECT reset_tse('~s');", [TE]),
	    S2 = io_lib:format("COPY ~s FROM '~s';", [TE, FE]),
	    S3 = io_lib:format("SELECT index_tse('~s');", [TE]),
	    CR = io_lib:format("rm ~s", [FE]),
	    io:fwrite("~p\n", [uops_perform_sql(uops_get_connection(), S1)]),
	    io:fwrite("~p\n", [uops_perform_sql(uops_get_connection(), S2)]),
	    io:fwrite("~s\n", [uops_perform_cmd(CR)]),
	    io:fwrite("~p\n", [uops_perform_sql(uops_get_connection(), S3)]),
	    R = R1 ++ io_lib:format("table ~s was loaded on ~s.\n", [TE, Node]),
	    ui_batch_pull_queue(DS, 'load-ds')
    end,
    epgsql:close(put(psql_connection, undefined)),
    R.

%% save
uops_save(fail, _) ->
    "failed connecting postgre sql server.";
uops_save(_, ["encoded", "column", "tables"]) ->
    %% put(debug, true),
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    DS = gen_server:call(BS, {get, data_server_nodes}),
    lists:map(fun uops_save_ct_ds/1, DS),
    epgsql:close(put(psql_connection, undefined)),
    uoa_sns_publish("finish psql save column tables"),
    "";
uops_save(_, ["string", "id", "tables"]) ->
    %% put(debug, true),
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    DS = gen_server:call(BS, {get, data_server_nodes}),
    lists:map(fun uops_save_sit_ds/1, DS),
    epgsql:close(put(psql_connection, undefined)),
    uoa_sns_publish("finish psql save string id tables"),
    "";
uops_save(_, ["predicate", "string", "id", "tables"]) ->
    uops_save_psi();
uops_save(_, _) ->
    "usage: psql save {encoded column|string id|predicate string id} tables\n".

uops_save_psi() ->
    BSN = lists:nth(1, get(b3s_state_nodes)),
    BS = {b3s_state, BSN},
    SI = {string_id, BSN},
    TD = gen_server:call(BS, {get, triple_distributor_pid}),
    PSI = pred_string_id,
    put(PSI, #{}),
    FK = fun (PS, _) ->
		 case gen_server:call(SI, {find, PS}) of
		     {found, I} ->
			 put(PSI, maps:put(PS, I, get(PSI)));
		     E ->
			 io:fwrite("~p\n", [E])
		 end
	 end,
    TN = gen_server:call(BS, {get, name_of_string_id_table}),
    gen_server:call(SI, {put, sid_table_name, TN}),
    case gen_server:call(BS, {get, pred_clm}) of
	undefined -> pred_clm_not_defined;
	PC ->
	    maps:map(FK, PC)
    end,
    gen_server:call(BS, {put, PSI, get(PSI)}),
    gen_server:call(TD, {put_property, PSI, get(PSI)}),
    epgsql:close(put(psql_connection, undefined)),
    "".

uops_save_ct_ds({_, ColumnId}) ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    DA = gen_server:call(BS, {get, distribution_algorithm}),
    WD = string:trim(os:cmd("pwd")),
    TE = io_lib:format("tse_~s_col~2..0w", [DA, ColumnId]),
    FE = io_lib:format("~s/bak/tse_col~2..0w.tsv", [WD, ColumnId]),
    S1 = io_lib:format("COPY ~s TO '~s';", [TE, FE]),
    io:fwrite("~p\n", [uops_perform_sql(uops_get_connection(), S1)]).

uops_save_sit_ds({_, ColumnId}) ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    DA = gen_server:call(BS, {get, distribution_algorithm}),
    WD = string:trim(os:cmd("pwd")),
    TS = io_lib:format("si_~s_col~2..0w", [DA, ColumnId]),
    FS = io_lib:format("~s/bak/si_col~2..0w.tsv", [WD, ColumnId]),
    S1 = io_lib:format("COPY ~s TO '~s';", [TS, FS]),
    io:fwrite("~p\n", [uops_perform_sql(uops_get_connection(), S1)]).

%% encode
uops_encode(fail, _) ->
    "failed connecting postgre sql server.";
uops_encode(C, ["column", "tables"]) ->
    %% put(debug, true),
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    DS = gen_server:call(BS, {get, data_server_nodes}),
    DA = gen_server:call(BS, {get, distribution_algorithm}),
    SQ = io_lib:format("SELECT reset_si('si_~s');", [DA]),

    io:fwrite("~p\n", [uops_perform_sql(C, SQ)]),
    RL = lists:map(fun uops_encode_ds/1, DS),

    epgsql:close(put(psql_connection, undefined)),
    uoa_sns_publish("finish preparing to encode column tables."),
    uops_encode_report(RL, []);
uops_encode(_, _) ->
    "usage: psql encode column tables\n".

uops_encode_ds({_, ColumnId}) ->
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    DA = gen_server:call(BS, {get, distribution_algorithm}),
    TS = io_lib:format("ts_~s_col~2..0w", [DA, ColumnId]),
    TE = io_lib:format("tse_~s_col~2..0w", [DA, ColumnId]),
    SI = io_lib:format("si_~s", [DA]),
    CL = io_lib:format("col~2..0w", [ColumnId]),
    S1 = io_lib:format("SELECT reset_tse('~s');", [TE]),
    S2 = io_lib:format("SELECT reset_sip('~s', '~s');", [SI, CL]),
    S3 = io_lib:format("SELECT encode_triples('~s', '~s', '~s', '~s');", [TS, TE, SI, CL]),
    %% uoa_sns_publish("start encoding column table " ++ TS),
    io:fwrite("~p\n", [uops_perform_sql(uops_get_connection(), S1)]),
    io:fwrite("~p\n", [uops_perform_sql(uops_get_connection(), S2)]),
    S3.
    %% io:fwrite("~p\n", [uops_perform_sql(uops_get_connection(), S3)]).

uops_encode_report([Sql | Rest], R) ->
    uops_encode_report(Rest, R ++ Sql ++ "\n");
uops_encode_report([], R) ->
    H = "\n* perform following SQL statements using psql:\n\n",
    F = "\npsql can be invoked by 'psql b3s b3s'.\n",
    H ++ R ++ F.

%% describe
uops_describe(fail, _) ->
    "failed connecting postgre sql server.";
uops_describe(C, ["table", Str]) ->
    %% put(debug, true),
    SF = "SELECT relname FROM pg_class" ++
	 " WHERE relkind = 'r' AND relname like '%~s%';",
    SQ = io_lib:format(SF, [Str]),
    PS = uops_perform_sql(C, SQ),
    case PS of
 	{ok, _, RBL} ->
	    F = fun ({B}) -> binary_to_list(B); (_) -> [] end,
	    RLL = lists:sort(lists:map(F, RBL)),
	    R = string:join(RLL, "\n") ++ "\n";
	_ ->
	    R = []
    end,
    epgsql:close(put(psql_connection, undefined)),
    R;
uops_describe(_, _) ->
    "usage: psql describe table <search string>\n".

%% 
%% remote operation functions
%% 
ui_operate_remote([]) ->
    uor_help();
ui_operate_remote([_]) ->
    uor_help();
ui_operate_remote(["load", "all", "encoded", "column", "tables"]) ->
    uor_all_load_encoded_column_tables();
ui_operate_remote(["load", "all", "encoded", "column", "tables", "force"]) ->
    uor_all_load_encoded_column_tables_force();
ui_operate_remote(["restart", "all", "data", "servers"]) ->
    uor_all_restart_data_servers();
ui_operate_remote(["restart", "data", "servers" | Args]) ->
    uor_restart_data_servers(Args);
ui_operate_remote(["all" | Args]) ->
    BSN = get(b3s_state_nodes),
    BS = {b3s_state, lists:nth(1, BSN)},
    DS = gen_server:call(BS, {get, data_server_nodes}),
    F = fun ({NA, _}) ->
		timer:sleep(500),
		put(b3s_state_nodes, BSN),
		NL = atom_to_list(NA),
		io:fwrite("~s", [ui_operate_remote([NL | Args])])
	end,
    FS = fun ({_, CA}, {_, CB}) when CA < CB -> true; (_, _) -> false end,
    lists:map(F, lists:sort(FS, DS)),
    "";
ui_operate_remote([Node | Args]) ->
    FS = lists:nth(1, get(b3s_state_nodes)),
    GC = "gen_server:call(~p,\n                {perform_ui, ~p})\n",
    case {ui_resolve_node_synonym(Node), get(debug)} of
	{ND, true} ->
	    NS = {node_state, ND},
	    io:fwrite(GC, [NS, Args]);
	{FS, _} ->
	    io_lib:format("node ~s is not remote.\n", [FS]);
	{ND, _} ->
	    NS = {node_state, ND},
	    io:fwrite("* ~s\n", [string:join([Node | Args], " ")]),
	    case (catch gen_server:call(NS, {perform_ui, Args})) of
		{'EXIT', _} ->
		    io_lib:format("process ~p is busy.\n", [NS]);
		R when is_list(R) -> R;
		R -> io_lib:format("~p\n", [R])
	    end
    end.

uor_help() ->
    "usage: remote {<node>|all} <UI command statement>|" ++
    "load all encoded column tables|" ++
    "restart data servers <nodes>|" ++
    "restart all data servers\n".

uor_all_load_encoded_column_tables_force() ->
    PS = "all psql load encoded column table S force",
    uor_all_load_encoded_column_tables_skel(PS).

uor_all_load_encoded_column_tables() ->
    PS = "all psql load encoded column table S",
    uor_all_load_encoded_column_tables_skel(PS).

uor_all_load_encoded_column_tables_skel(PS) ->
    %% put(debug, true),
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    DS = gen_server:call(BS, {get, data_server_nodes}),
    J = 'load-ds',
    M = "pushed ~p to node_state:ui_queued_jobs\n",
    F = fun ({N, _}) ->
		ui_batch_push_queue(N, J),
		R = io_lib:format(M, [{N, J}]),
		lists:flatten(R)
	end,
    R1 = lists:map(F, DS),
    PL = string:split(PS, " ", all),
    R2 = lists:flatten(ui_operate_remote(PL)),
    lists:flatten(string:join(R1, "") ++ string:join(R2, "")).

uor_restart_data_servers([Node]) ->
    uards_process_ds(ui_resolve_node_synonym(Node));
uor_restart_data_servers([Node | Rest]) ->
    uor_restart_data_servers([Node]),
    uor_restart_data_servers(Rest);
uor_restart_data_servers([]) ->
    "usage: remote restart data servers <node1>[, <node2>, ...]\n".

uor_all_restart_data_servers() ->
    %% put(debug, true),
    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    DS = gen_server:call(BS, {get, data_server_nodes}),
    F = fun ({N, _}) -> uards_process_ds(N) end,
    lists:map(F, DS),
    "".

uards_process_ds(Node) ->
    N = ui_resolve_node_synonym(Node),
    uti_unregister(N, false, false),
    ui_batch_push_queue(N, 'boot-ds'),
    rpc:call(N, os, cmd, ["/home/ubuntu/bbd002.sh"]),
    %% A = list_to_atom(lists:nth(2, string:split(atom_to_list(N), "@"))),
    %% S = "../aws/bin/ssh-execute.sh ~s sudo /home/ubuntu/bbd002.sh",
    %% C = io_lib:format(S, [A]),
    %% %% io:fwrite("~s\n", [C]),
    %% os:cmd(C),
    io:fwrite("restarting data server node of ~s.\n", [Node]),
    "".

%% 
%% memory investigation functions
%% 
ui_operate_memory(["s"]) ->
    uom_summary();
ui_operate_memory(["summary"]) ->
    uom_summary();
ui_operate_memory(["m" | A]) ->
    uom_message(A);
ui_operate_memory(["memory" | A]) ->
    uom_message(A);
ui_operate_memory(_) ->
    "usage: memory {summary | message [on | off]}\n".

% summary
uom_summary() ->
    HD = " Node    |  Total |  Proc. |   Atom |   Bin. |   Ets",
    DL = "---------+--------+--------+--------+--------+--------",
    FA = " | ~5wM | ~5wM | ~5wK | ~5wK | ~5wK",
    FB = " DS~2..0w-~2..0w" ++ FA,
    FF = " FS     " ++ FA,
    FM = fun (N) ->
		 K = 1000,
		 [round(rpc:call(N, erlang, memory, [total]) / K / K),
		  round(rpc:call(N, erlang, memory, [processes_used]) / K / K),
		  round(rpc:call(N, erlang, memory, [atom_used]) / K),
		  round(rpc:call(N, erlang, memory, [binary]) / K),
		  round(rpc:call(N, erlang, memory, [ets]) / K)]
	 end,
    FD = fun (N) ->
		 io_lib:format(FF, FM(N))
	 end,
    FC = fun ({C, RM}) ->
		 FR = fun ({R, N}) ->
			      io_lib:format(FB, [C, R] ++ FM(N))
		      end,
		 string:join(lists:map(FR, maps:to_list(RM)), "\n")
	 end,

    BS = {b3s_state, lists:nth(1, get(b3s_state_nodes))},
    FN = gen_server:call(BS, {get, front_server_nodes}),
    CR = maps:to_list(gen_server:call(BS, {get, clm_row_conf})),
    R1 = lists:map(FD, FN),
    R2 = lists:map(FC, CR),
    string:join([HD, DL] ++ R1 ++ R2, "\n") ++ "\n\n".

% message
uom_message([]) ->
    L = [b3s_state, node_state, benchmark, query_tree,
	 tp_query_node],
    F = fun (P) -> io:fwrite("will apply: ~w\n", [P]) end,
    uomm_apply_function(ui_all_nodes(), L, F);
uom_message(["on"]) ->
    L = [b3s_state, node_state, benchmark, query_tree,
	 tp_query_node],
    F1 = fun (X) -> gen_server:call(X, {put, mq_debug, true}) end,
    uomm_apply_function(ui_all_nodes(), L, F1),
    io:fwrite("turn process message logging mode on\n", []);
uom_message(["off"]) ->
    L = [b3s_state, node_state, benchmark, query_tree,
	 tp_query_node],
    F1 = fun (X) ->
		 gen_server:call(X, {put, mq_debug,  false}),
		 gen_server:call(X, {put, mq_count,  undefined}),
		 gen_server:call(X, {put, mq_maxlen, undefined})
	 end,
    uomm_apply_function(ui_all_nodes(), L, F1),
    io:fwrite("turn process message logging mode off\n", []);
uom_message(Arg) ->
    io:fwrite("unknown option: ~s\n", [Arg]),
    "".

uomm_apply_function(Nodes, Modules, Func) ->
    F = fun (Node) ->
		N = ui_resolve_node_synonym(Node),
		case (catch rpc:call(N, supervisor, which_children, [b3s])) of
		{'EXIT', E} ->
			io:fwrite("  ~p\n", [E]);
		R ->
			uaf_each_process(R, sets:from_list(Modules), Modules, N, Func)
		end
	end,
    lists:map(F, Nodes).

uaf_each_process([], _, _, _, _) ->
    "";
uaf_each_process([{Id, _, _, [M]} | R], ModuleSet, ML, N, F) ->
    case sets:is_element(M, ModuleSet) of
	true  -> F({Id, N});
	false -> ok
    end,
    uaf_each_process(R, ModuleSet, ML, N, F);
uaf_each_process(_, _, _, _, _) ->
    "".

%% batch supporting functions

% ui_batch_push_queue/2 <node> <job>
% @doc Push a job queue item to node_state:ui_queued_jobs.
% @spec ui_batch_push_queue(Node::atom(), Job::atom()) -> ok|already_pushed
ui_batch_push_queue(Node, Job) ->
    BSN = lists:nth(1, get(b3s_state_nodes)),
    NS  = {node_state, BSN},
    UQJ = ui_queued_jobs,
    NR  = ui_resolve_node_synonym(Node),
    DL  = list_to_atom(lists:nth(1, string:split(atom_to_list(NR), "@"))),
    case gen_server:call(NS, {get, UQJ}) of
	undefined ->
	    gen_server:call(NS, {put, UQJ, [{DL, Job}]}),
	    ok;
	L ->
	    LL = lists:append(L, [{DL, Job}]),
	    gen_server:call(NS, {put, UQJ, LL}),
	    ok
    end.

ui_batch_push_queue_t() ->
    BSN = lists:nth(1, get(b3s_state_nodes)),
    NS = {node_state, BSN},
    UQJ = ui_queued_jobs,
    N1 = node_a,
    N2 = node_b,
    J1 = qwe,
    J2 = asd,
    R1 = [{N1, J1}],
    R2 = R1 ++ [{N1, J2}],
    R3 = R2 ++ [{N2, J2}],
    R4 = R3 ++ [{N2, J2}],
    {inorder,
     [
      ?_assertMatch([],        gen_server:call(NS, {get, UQJ})),
      ?_assertMatch(ok,        ui_batch_push_queue(N1, J1)),
      ?_assertMatch(R1,        gen_server:call(NS, {get, UQJ})),
      ?_assertMatch(ok,        ui_batch_push_queue(N1, J2)),
      ?_assertMatch(R2,        gen_server:call(NS, {get, UQJ})),
      ?_assertMatch(ok,        ui_batch_push_queue(N2, J2)),
      ?_assertMatch(R3,        gen_server:call(NS, {get, UQJ})),
      ?_assertMatch(ok,        ui_batch_push_queue(N2, J2)),
      ?_assertMatch(R4,        gen_server:call(NS, {get, UQJ}))
    ]}.

% ui_batch_pull_queue/2 <node> <job>
% @doc Pull a job queue item from node_state:ui_queued_jobs.
% @spec ui_batch_pull_queue(Node::atom(), Job::atom()) ->
%       ok|not_found|completed
ui_batch_pull_queue(Node, Job) ->
    BSN = lists:nth(1, get(b3s_state_nodes)),
    NS  = {node_state, BSN},
    UQJ = ui_queued_jobs,
    CJ  = [{completed, Job}],
    DL  = list_to_atom(lists:nth(1, string:split(atom_to_list(Node), "@"))),
    put(ui_batch_pull_queue_done, false),
    case gen_server:call(NS, {get, UQJ}) of
	undefined ->
	    not_found;
	[] ->
	    not_found;
	L ->
	    FJ  = fun ({_, J}) when J == Job -> true; (_) -> false end,
	    FNJ = fun ({N, J}) when N == DL andalso J == Job ->
			  case get(ui_batch_pull_queue_done) of
			      false ->
				  put(ui_batch_pull_queue_done, true),
				  case lists:delete({N, J}, L) of
				      [] ->
					  gen_server:call(NS, {put, UQJ, CJ}),
					  {true, completed};
				      LL ->
					  case lists:filtermap(FJ, LL) of
					      [] ->
						  gen_server:call(NS, {put, UQJ, CJ ++ LL}),
						  {true, completed};
					      _ ->
						  gen_server:call(NS, {put, UQJ, LL}),
						  {true, ok}
					  end
				  end;
			      _ -> false
			  end;
		      (_) -> false end,
	    case lists:filtermap(FNJ, L) of
		[R] -> R;
		_ -> not_found
	    end
    end.

ui_batch_pull_queue_t() ->
    BSN = lists:nth(1, get(b3s_state_nodes)),
    NS = {node_state, BSN},
    UQJ = ui_queued_jobs,
    N1 = node_a,
    N2 = node_b,
    J1 = qwe,
    J2 = asd,
    R1 = [{N1, J1}],
    R2 = [{completed, J1}],
    R3 = [{N1, J2}, {N2, J1}, {N2, J2}],
    R4 = R2 ++ [{N1, J2}, {N2, J2}],
    R5 = R1 ++ R1,
    {inorder,
     [
      ?_assertMatch(ok,        gen_server:call(NS, {put, UQJ, undefined})),
      ?_assertMatch(not_found, ui_batch_pull_queue(N1, J1)),
      ?_assertMatch(ok,        gen_server:call(NS, {put, UQJ, []})),
      ?_assertMatch(not_found, ui_batch_pull_queue(N1, J1)),
      ?_assertMatch(ok,        ui_batch_push_queue(N1, J1)),
      ?_assertMatch(R1,        gen_server:call(NS, {get, UQJ})),
      ?_assertMatch(completed, ui_batch_pull_queue(N1, J1)),
      ?_assertMatch(R2,        gen_server:call(NS, {get, UQJ})),

      ?_assertMatch(ok,        gen_server:call(NS, {put, UQJ, undefined})),
      ?_assertMatch(ok,        ui_batch_push_queue(N1, J1)),
      ?_assertMatch(ok,        ui_batch_push_queue(N1, J1)),
      ?_assertMatch(R5,        gen_server:call(NS, {get, UQJ})),
      ?_assertMatch(ok,        ui_batch_pull_queue(N1, J1)),
      ?_assertMatch(R1,        gen_server:call(NS, {get, UQJ})),
      ?_assertMatch(completed, ui_batch_pull_queue(N1, J1)),
      ?_assertMatch(R2,        gen_server:call(NS, {get, UQJ})),

      ?_assertMatch(ok,        gen_server:call(NS, {put, UQJ, undefined})),
      ?_assertMatch(ok,        ui_batch_push_queue(N1, J1)),
      ?_assertMatch(ok,        ui_batch_push_queue(N1, J2)),
      ?_assertMatch(ok,        ui_batch_push_queue(N2, J1)),
      ?_assertMatch(ok,        ui_batch_push_queue(N2, J2)),
      ?_assertMatch(ok,        ui_batch_pull_queue(N1, J1)),
      ?_assertMatch(R3,        gen_server:call(NS, {get, UQJ})),
      ?_assertMatch(completed, ui_batch_pull_queue(N2, J1)),
      ?_assertMatch(R4,        gen_server:call(NS, {get, UQJ})),
      ?_assertMatch(not_found, ui_batch_pull_queue(N1, J1))
    ]}.

% ui_batch_complete_queue/1 <job>
% @doc Clear the complete semaphore of job from node_state:ui_queued_jobs.
% @spec ui_batch_complete_queue(Job::atom()) -> completed|not_found
ui_batch_complete_queue(Job) ->
    BSN = lists:nth(1, get(b3s_state_nodes)),
    NS  = {node_state, BSN},
    UQJ = ui_queued_jobs,
    CJ  = {completed, Job},
    FCJ = fun ({completed, J}) when J == Job -> true; (_) -> false end,
    case gen_server:call(NS, {get, UQJ}) of
	undefined ->
	    not_found;
	[] ->
	    not_found;
	L ->
	    case lists:filtermap(FCJ, L) of
		[] -> not_found;
		_ ->
		    LL = lists:delete(CJ, L),
		    gen_server:call(NS, {put, UQJ, LL}),
		    completed
	    end
    end.

ui_batch_complete_queue_t() ->
    BSN = lists:nth(1, get(b3s_state_nodes)),
    NS = {node_state, BSN},
    UQJ = ui_queued_jobs,
    N1F = 'node_a@qwe.asd.zxc',
    N2F = 'node_b@rty.fgh.zxc',
    N1 = 'node_a',
    N2 = 'node_b',
    J1 = qwe,
    J2 = asd,
    R1 = [{N1, J2}, {N2, J2}],
    {inorder,
     [
      ?_assertMatch(ok,        gen_server:call(NS, {put, UQJ, undefined})),
      ?_assertMatch(not_found, ui_batch_complete_queue(J1)),
      ?_assertMatch(ok,        gen_server:call(NS, {put, UQJ, []})),
      ?_assertMatch(not_found, ui_batch_complete_queue(J1)),
      ?_assertMatch(ok,        ui_batch_push_queue(N1F, J1)),
      ?_assertMatch(ok,        ui_batch_push_queue(N1F, J2)),
      ?_assertMatch(ok,        ui_batch_push_queue(N2F, J1)),
      ?_assertMatch(ok,        ui_batch_push_queue(N2F, J2)),
      ?_assertMatch(ok,        ui_batch_pull_queue(N1F, J1)),
      ?_assertMatch(completed, ui_batch_pull_queue(N2F, J1)),
      ?_assertMatch(completed, ui_batch_complete_queue(J1)),
      ?_assertMatch(R1,        gen_server:call(NS, {get, UQJ})),
      ?_assertMatch(not_found, ui_batch_complete_queue(J1))
    ]}.

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
      {generator, fun ui_batch_push_queue_t/0},
      {generator, fun ui_batch_pull_queue_t/0},
      {generator, fun ui_batch_complete_queue_t/0},
      ?_assertMatch(ok, b3s:stop())
     ]};

ut_site(_) ->
    [].

%% ====> END OF LINE <====

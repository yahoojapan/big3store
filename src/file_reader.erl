%%
%% File reader process of simple triplestore application.
%%
%% @copyright 2013-2019 UP FAMNIT and Yahoo Japan Corporation
%% @version 0.3
%% @since August, 2013
%% @author Kiyoshi Nitta <knitta@yahoo-corp.jp>
%% 
%% @see triple_distributor
%% 
%% @doc A process for reading a tsv triple dump file and generating
%% data streams of the file. One process is invoked for one file. This
%% module is implemented as an erlang <A
%% href="http://www.erlang.org/doc/man/gen_server.html">gen_server</A>
%% process.
%% 
%% <table bgcolor="lemonchiffon">
%% <tr><th>Section Index</th></tr>
%% <tr><td>{@section handle_call (synchronous) message API}</td></tr>
%% <tr><td>{@section handle_cast (asynchronous) message API}</td></tr>
%% <tr><td>{@section property list}</td></tr>
%% </table>
%% 
%% == handle_call (synchronous) message API ==
%% 
%% (LINK: {@section handle_call (synchronous) message API})
%% 
%% === {start, FileName, Destination, Message} ===
%% 
%% Prepare to generate a stream from specified
%% Filename::string(). Variable Destination is a tuple
%% {ProcessId::pid(), NodeId::node()} that indicates a {@link
%% triple_distributor} process in distributed environment. Variable
%% Message is an atom() that specifies whether the stream message is
%% investigate_stream or store_stream. It returns ok if the process
%% was successfully started. Otherwise, it returns {error,
%% Reason::term()}. If succeeded, it turns the wait property from true
%% to false so that this file_reader process can accept {@section
%% @{empty, From@}} messages to send stream messages. A stream message
%% will have a form of {Message, From, Triple}, where Message is
%% either of investigate_stream or store_stream, From is a tuple
%% {ProcessId::pid(), NodeId::node()} that indicates the file_reader
%% process in distributed environment, and Triple is {@link
%% tp_query_node:qn_triple()} | end_of_stream. This request is
%% implemented by {@link hc_start/4}. (LINK: {@section @{start,
%% FileName, Destination, Message@}})
%% 
%% === {get_property, Name} ===
%% 
%% Return the value of specified property name. Variable Name is an
%% atom(). (LINK: {@section @{get_property, Name@}}).
%% 
%% == handle_cast (asynchronous) message API ==
%% 
%% (LINK: {@section handle_cast (asynchronous) message API})
%% 
%% === {empty, From} ===
%% 
%% Send a stream message, if the process has been started by {@section
%% @{start, FileName, Destination, Message@}} call and the wait
%% property is false. Variable From is a tuple {ProcessId::pid(),
%% NodeId::node()} that indicates a process in distributed
%% environment. A stream message will have a form of {Message, From,
%% Triple}, where Message is either of investigate_stream or
%% store_stream, From is a tuple {ProcessId::pid(), NodeId::node()}
%% that indicates the file_reader process in distributed environment,
%% and Triple is {@link tp_query_node:qn_triple()} |
%% end_of_stream. This request is implemented by {@link
%% hc_empty/1}. (LINK: {@section @{empty, From@}})
%% 
%% == property list ==
%% 
%% (LINK: {@section property list})
%% 
%% The gen_server process uses following properties holded by {@link
%% fr_state()}. These properties can be accessed using {@section
%% @{get_property, Name@}} synchronous messages.
%% 
%% <table border="3">
%% <tr><th>Name</th><th>Type</th><th>Description</th></tr>
%% 
%% <tr> <td>created</td> <td>boolean()</td> <td>true denotes that
%% process dictionary was created and used. false denotes that
%% completely new process.</td> </tr>
%% 
%% <tr> <td>pid</td> <td>pid()</td> <td>local id of the file_reader
%% process.</td> </tr>
%% 
%% <tr> <td>file_name</td> <td>string()</td> <td>path string to be
%% read.</td> </tr>
%% 
%% <tr> <td>dest_proc</td> <td>pid()</td> <td>process id of stream
%% destination.</td> </tr>
%% 
%% <tr> <td>dest_node</td> <td>node()</td> <td>node id of stream
%% destination.</td> </tr>
%% 
%% <tr> <td>msg_type</td> <td>atom()</td> <td>type of stream
%% message.</td> </tr>
%% 
%% <tr> <td>io_device</td> <td>io_device()</td> <td>I/O device pointer
%% of opened file.</td> </tr>
%% 
%% <tr> <td>triple_count</td> <td>integer()</td> <td>number of sent
%% triples.</td> </tr>
%% 
%% <tr> <td>request_start_time</td> <td>calendar:datetime()</td>
%% <td>Date and time when the file_reader process was initialized by
%% {@section @{start, FileName, Destination, Message@}}.</td> </tr>
%% 
%% <tr> <td>process_sec_total</td> <td>integer()</td> <td>total
%% seconds after process creation.</td> </tr>
%% 
%% <tr> <td>process_sec_latest</td> <td>integer()</td> <td>total
%% seconds after request_start_time.</td> </tr>
%% 
%% <tr> <td>number_of_triple_total</td> <td>integer()</td> <td>total
%% number of processed triples after process creation.</td> </tr>
%% 
%% <tr> <td>number_of_triple_latest</td> <td>integer()</td> <td>total
%% number of processed triples after request_start_time.</td> </tr>
%% 
%% <tr> <td>wait</td> <td>boolean()</td> <td>indicate whether the
%% process is in wait state or not.</td> </tr>
%% 
%% <tr> <td>store_report_frequency</td> <td>integer()</td>
%% <td>Report frequency of storing process.</td> </tr>
%%
%% </table>
%% 
%% @type fr_state() = maps:map(). A Reference pointer of the map
%% object that manages properties for operating the gen_server
%% process.
%% 
-module(file_reader).
-behavior(gen_server).
-export([child_spec/1,
	 init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3
	]).
-include_lib("stdlib/include/qlc.hrl").
-include_lib("eunit/include/eunit.hrl").

%% ======================================================================
%% 
%% gen_server behavior
%% 

%% 
%% init/1
%% 
%% @doc Initialize a file_reader process.
%% 
%% @spec init([pid()]) -> {ok, fr_state()}
%% 
init([Id]) ->
    process_flag(trap_exit, true),
    put(created,                 true),
    put(wait,                    true),
    put(process_sec_total,       0),
    put(number_of_triple_total,  0),
    put(pid,                     Id),
    BS = gen_server:call(node_state, {get, b3s_state_pid}),
    SRF = gen_server:call(BS, {get, store_report_frequency}),
    put(store_report_frequency,  SRF),
    R = {ok, hc_save_pd()},
    info_msg(init, [], R, -1),
    R.

%% 
%% handle_call/3
%% 
%% @doc Handle synchronous query requests.
%% 
%% @spec handle_call(term(), {pid(), term()}, fr_state()) -> {reply,
%% term(), fr_state()}
%% 

%% start
handle_call({start, FileName, Destination, Message}, From, State) ->
    hc_restore_pd(get(created), State),
    R = hc_start(FileName, Destination, Message, From),
    {reply, R, hc_save_pd()};

%% get property
handle_call({get_property, all}, _, State) ->
    hc_restore_pd(get(created), State),
    {reply, get(), hc_save_pd()};

handle_call({get_property, Name}, _, State) ->
    hc_restore_pd(get(created), State),
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
%% @spec handle_cast(term(), fr_state()) -> {noreply, fr_state()}
%% 

%% empty
handle_cast({empty, From}, State) ->
    hc_restore_pd(get(created), State),
    hc_empty(From),
    {noreply, hc_save_pd()};

%% default
handle_cast(Request, State) ->
    R = {unknown_request, Request},
    error_msg(handle_cast, [Request, State], R),
    {noreply, State}.

%% 
%% handle_info/2
%% 
%% @doc Handle exceptional query requests. 
%% 
%% @spec handle_info(term(), fr_state()) -> {noreply, fr_state()}
%% 
handle_info(_Info, State) ->
    {noreply, State}.

%% 
%% terminate/2
%% 
%% @doc Process termination. 
%% 
%% @spec terminate(term(), fr_state()) -> none()
%% 
terminate(Reason, State) ->
    info_msg(terminate, [Reason, State], terminate, 0),
    ok.

%% 
%% code_change/3
%% 
%% @doc Process code change action. 
%% 
%% @spec code_change(term(), fr_state(), term()) -> {ok, fr_state()}
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

%% 
%% @doc Restore process dictionaries from state map structure.
%% 
%% @spec hc_restore_pd([{atom(), term()}] | undefined, fr_state()) -> ok
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
%% @spec hc_save_pd() -> fr_state()
%% 
hc_save_pd() ->
    maps:from_list(get()).

%% ======================================================================
%% 
%% API
%% 

%% 
%% @doc Return child spec for this process. It can be used in
%% supervisor:init/0 callback implementation.
%% 
%% @spec child_spec(Id::atom()) -> supervisor:child_spec()
%% 
child_spec(Id) ->
    GSOpt = [{local, Id}, file_reader, [Id], []],
    StartFunc = {gen_server, start_link, GSOpt},
    Restart = permanent,
    Shutdwon = 1000,
    Type = worker,
    Modules = [file_reader],

    {Id, StartFunc, Restart, Shutdwon, Type, Modules}.

%% ======================================================================
%% 
%% handle call/cast implementation
%% 

%% @doc Process a token of line.
hcsspl_process_token([], TokLst, _, _) ->
    TokLst;
hcsspl_process_token([Tok | Rst], TokLst, TokN, LinN) ->
    %% R = {processing_token, Tok},
    %% A = [[Tok | Rst], TokLst, TokN, LinN],
    %% N = list_to_atom("hcsspl_process_token <" ++ Tok ++ ">"),
    %% info_msg(N, A, R, 100),

    FixedT = hcsspl_fix_token(Tok, TokN, LinN),
    NxtLst = lists:append(FixedT, TokLst),
    hcsspl_process_token(Rst, NxtLst, TokN + 1, LinN).

%% @doc Fix token string.
hcsspl_fix_token(Tok, TokN, LinN) ->
    LN = string:len(Tok),
    C1 = string:chr(Tok, $<),
    C2 = string:chr(Tok, $>),
    C3 = string:chr(Tok, $\n),
    C4 = string:chr(Tok, $"),
    C5 = string:rchr(Tok, $"),
    S1 = string:str(Tok, "^^"),
    hcsspl_sel_token(Tok, LN, C1, C2, C3, C4, C5, S1, TokN, LinN, no_class).

%% @doc Select appropriate portion of token string.
hcsspl_sel_token(T, LN, C1, C2, C3, C4, C5, S1, TokN, LinN, _)
  when S1 > 0 ->
    T1 = string:substr(T, 1, S1-1),
    T2 = string:substr(T, S1+2, LN-S1-1),
    ST1 = hcsspl_sel_token(T1, S1, C1, C2, C3,
			   C4, C5, 0, TokN, LinN, literal),
    ST2 = hcsspl_sel_token(T2, LN-S1-1, C1-S1-1, C2-S1-1, C3-S1-1,
			   C4-S1-1, C5-S1-1, 0, TokN, LinN, type),
    lists:append(ST2, ST1);
hcsspl_sel_token(T, LN, C1, C2, C3, C4, C5, S1, TokN, LinN, CLS)
  when C3 > 0 ->
    TT = string:substr(T, 1, LN-1),
    hcsspl_sel_token(TT, LN-1, C1, C2, 0, C4, C5, S1, TokN, LinN, CLS);
hcsspl_sel_token(Tok, LN, C1, C2, C3, C4, C5, S1, TokN, LinN, no_class) ->
    case string:to_float(Tok) of
	{TokFlt, []} ->
	    hcsspl_sel_token(TokFlt, LN, C1, C2, C3,
			     C4, C5, S1, TokN, LinN, float);
	_ ->
	    hcsspl_sel_token(Tok, LN, C1, C2, C3,
			     C4, C5, S1, TokN, LinN, try_int)
    end;
hcsspl_sel_token(Tok, LN, C1, C2, C3, C4, C5, S1, TokN, LinN, try_int) ->
    case string:to_integer(Tok) of
	{TokInt, []} ->
	    hcsspl_sel_token(TokInt, LN, C1, C2, C3,
			     C4, C5, S1, TokN, LinN, integer);
	_ ->
	    hcsspl_sel_token(Tok, LN, C1, C2, C3,
			     C4, C5, S1, TokN, LinN, unknown)
    end;
hcsspl_sel_token(Tok, LN, C1, C2, C3, C4, C5, S1, TokN, LinN, _CLS) ->
    T = Tok,
%    T = {_CLS, Tok},
    R = {selected_token, T},
    N = "file_reader:hcsspl_sel_token",
    F = "~s(~p): ~p.~n",
    A = [N, [Tok, LN, C1, C2, C3, C4, C5, S1, TokN, LinN], R],
    im_cond(F, A, 100),
    [T].

%% 
%% @doc Prepare to generate a stream from specified
%% Filename::string().
%% 
%% @spec hc_start(FileName::string(), Destination::{ProcId::pid(),
%% NodeId::node()}, Message::atom(), From::{ProcId::pid(),
%% NodeId::node()}) -> ok | {error, Reason::term()}
%% 
hc_start(FileName, Destination, Message, From) ->
    put(file_name,               FileName),
    put(dest_proc,               element(1, Destination)),
    put(dest_node,               element(2, Destination)),
    put(msg_type,                Message),
    put(request_start_time,      calendar:local_time()),
    put(process_sec_latest,      0),
    put(number_of_triple_latest, 0),
    put(file_reader_eof, false),

    R = hcs_wait(get(wait)),
    A = [FileName, Destination, Message, From],
    info_msg(hc_start, A, R, 100),
    R.

hcs_wait(false) ->
    E = {error, {already_started, hc_save_pd()}},
    error_msg(hcs_wait, [false], E),
    E;
hcs_wait(true) ->
    X = file:open(get(file_name), read),
    R = hcs_open(X),
    info_msg(hcs_wait, [true], R, 100),
    R.

%% @doc Open data file.
hcs_open({error, Reason}) ->
    E = {error, {Reason, hc_save_pd()}},
    error_msg(hcs_open, [{error, Reason}], E),
    E;
hcs_open({ok, IoDevice}) ->
    put(wait,         false),
    put(io_device,    IoDevice),
    put(triple_count, 0),

    M = {opened_file, get(file_name)},
    %% R = gen_server:call(stop_watch, {start, M}),
    info_msg(hcs_open, [{ok, IoDevice}], M, 50).

hc_start_test_() ->
    B3S  = {b3s, node()},
    RP   = repeater,
    ND   = node(),
    RPT  = {RP, ND},
    IS   = investigate_stream,
    GP   = get_property,
    FN   = file_name,
    DP   = dest_proc,
    DN   = dest_node,
    MT   = msg_type,
    WT   = wait,
    UD   = undefined,
    ER   = error,

    FR01 = file_reader_01,
    CS01 = child_spec(FR01),
    FN01 = "ygtsv/yagoGeonamesClasses-h1k.tsv",
    M01  = {start, FN01, RPT, IS},
    FN02 = "ygtsv/qwe-h1k.tsv",
    M02  = {start, FN02, RPT, IS},
    {inorder,
     [
      ?_assertMatch(ok,      b3s:start()),
      ?_assertMatch(ok,      b3s:bootstrap()),
      ?_assertMatch({ok, _}, supervisor:start_child(B3S, CS01)),
      ?_assertMatch(UD,      gen_server:call(FR01, {GP, FN})),
      ?_assertMatch(UD,      gen_server:call(FR01, {GP, DP})),
      ?_assertMatch(UD,      gen_server:call(FR01, {GP, DN})),
      ?_assertMatch(UD,      gen_server:call(FR01, {GP, MT})),
      ?_assertMatch(true,    gen_server:call(FR01, {GP, WT})),
      ?_assertMatch({ER, _}, gen_server:call(FR01, M02)),
      ?_assertMatch(FN02,    gen_server:call(FR01, {GP, FN})),
      ?_assertMatch(true,    gen_server:call(FR01, {GP, WT})),
      ?_assertMatch(ok,      gen_server:call(FR01, M01)),
      ?_assertMatch(FN01,    gen_server:call(FR01, {GP, FN})),
      ?_assertMatch(RP,      gen_server:call(FR01, {GP, DP})),
      ?_assertMatch(ND,      gen_server:call(FR01, {GP, DN})),
      ?_assertMatch(IS,      gen_server:call(FR01, {GP, MT})),
      ?_assertMatch(false,   gen_server:call(FR01, {GP, WT})),
      ?_assertMatch({ER, _}, gen_server:call(FR01, M01)),
      ?_assertMatch(ok,      b3s:stop())
     ]}.

%% 
%% @doc Send a stream message.
%% 
%% @spec hc_empty(From::{ProcId::pid(), NodeId::node()}) -> ok
%% 
hc_empty(From) ->
    {Proc, Node} = From,
    P = get(dest_proc),
    N = get(dest_node),
    W = get(wait),
    hce_confirm(Proc, Node, P, N, W).

hce_confirm(Proc, Node, P, N, true) ->
    A = [Proc, Node, P, N, true],
    error_msg(hce_confirm, A, not_started);
hce_confirm(P, N, P, N, _) ->
    Line = io:get_line(get(io_device), ''),
    SRF = get(store_report_frequency),
    hce_process_line(Line, get(triple_count), SRF);
hce_confirm(Proc, Node, P, N, W) ->
    A = [Proc, Node, P, N, W],
    error_msg(hce_confirm, A, changed_destination),

    put(dest_proc, Proc),
    put(dest_node, Node),
    Line = io:get_line(get(io_device), ''),
    SRF = get(store_report_frequency),
    hce_process_line(Line, get(triple_count), SRF).

hce_process_line(eof, _, _) ->
    put(wait, true),
    file:close(get(io_device)),
    hce_record_progress(file_reader_finished),
    hce_send_message(get(msg_type), end_of_stream);
hce_process_line({error, ErrorDescription}, C, SRF) ->
    A = [{error, ErrorDescription}, C, SRF],
    error_msg(hce_process_line, A, read_error),

    put(wait, true),
    file:close(get(io_device)),
    hce_record_progress(file_reader_finished),
    hce_send_message(get(msg_type), end_of_stream);
hce_process_line(Line, C, SRF) when C rem SRF == 0 ->
    hce_record_progress(progress_report),
    TokLst = hce_line_to_tok(Line),
    hce_send_message(get(msg_type), TokLst);
hce_process_line(Line, _, _) ->
    TokLst = hce_line_to_tok(Line),
    hce_send_message(get(msg_type), TokLst).

hce_record_progress(file_reader_finished) ->
    RST   = request_start_time,
    PST   = process_sec_total,
    PSL   = process_sec_latest,
    NOTT  = number_of_triple_total,
    NOTL  = number_of_triple_latest,
    RSTv  = get(RST),
    PSTv  = get(PST),
    NOTTv = get(NOTT),

    Start      = calendar:datetime_to_gregorian_seconds(RSTv),
    CLT        = calendar:local_time(),
    Current    = calendar:datetime_to_gregorian_seconds(CLT),
    Difference = Current - Start,
    Num        = get(triple_count),

    put(PST,  PSTv + Difference),
    put(PSL,  Difference),
    put(NOTT, NOTTv + Num),
    put(NOTL, Num),
    put(file_reader_eof, true),

    M = {file_reader_finished, get(file_name), Num},
    gen_server:cast(stop_watch, {record, M}),
    info_msg(hce_record_progress, [file_reader_finished], M, 10);
hce_record_progress(progress_report) ->
    M = {storing, get(file_name), get(triple_count)},
    gen_server:cast(stop_watch, {record, M}),
    info_msg(hce_record_progress, [progress_report], M, 10).

hce_line_to_tok(Line) ->
    info_msg(hce_line_to_tok, [Line], processing, 100),

    Num    = get(triple_count) + 1,
    Tok    = string:tokens(Line, "\t"),
    TL1    = hcsspl_process_token(Tok, [], 0, Num),
    ChrTab = string:chr(Line, $\t),
    TL2    = hltt_tab(ChrTab, TL1),
    TL3    = lists:reverse(TL2),
    Id     = lists:nth(1, TL3),
    Sbj    = lists:nth(2, TL3),
    Prd    = lists:nth(3, TL3),
    Obj    = lists:nth(4, TL3),
    {Id, Sbj, Prd, Obj}.

hce_line_to_tok_test_() ->
    put(triple_count, 0),
    L01 = "	<Northwood_University-West_Baden>	<wasCreatedOnDate>	\"1966-##-##\"^^xsd:date	1966.0000",
    R01 = {[],
	   "<Northwood_University-West_Baden>",
	   "<wasCreatedOnDate>",
	   "\"1966-##-##\""
	  },
    {inorder,
     [
      ?_assertMatch(ok,  b3s:start()),
      ?_assertMatch(ok,  b3s:bootstrap()),
      ?_assertMatch(R01, hce_line_to_tok(L01)),
      ?_assertMatch(ok,  b3s:stop())
     ]}.

hltt_tab(1, TokLst) ->
    lists:append(TokLst, [[]]);
hltt_tab(_, TokLst) ->
    TokLst.

%% extend here for adding new stream message types.
hce_send_message(investigate_stream, Data) ->
    put(triple_count, get(triple_count) + 1),
    Mes = {investigate_stream, {get(pid), node()}, Data},
    DP = {get(dest_proc), get(dest_node)},
    gen_server:cast(DP, Mes),

    A = [investigate_stream, Data],
    info_msg(hce_send_message, A, {DP, Mes}, 100);
hce_send_message(store_stream, Data) ->
    put(triple_count, get(triple_count) + 1),
    Mes = {store_stream, {get(pid), node()}, Data},
    DP = {get(dest_proc), get(dest_node)},
    gen_server:cast(DP, Mes),

    A = [store_stream, Data],
    info_msg(hce_send_message, A, {DP, Mes}, 100).

hc_empty_test_() ->
    ND   = node(),
    FT   = hc_empty_test_p,
    FTT  = {FT, ND},
    B3S  = {b3s, ND},
    RP   = repeater,
    RPT  = {RP, ND},
    IS   = investigate_stream,
    SS   = store_stream,
    GP   = get_property,
    TC   = triple_count,
    NTL  = number_of_triple_latest,
    NTT  = number_of_triple_total,
    RHC  = recorded_handle_cast,

    FR01 = file_reader_01,
    CS01 = child_spec(FR01),
    FN01 = "ygtsv/yagoGeonamesClasses-h1k.tsv",
    M01  = {start, FN01, RPT, IS},
    M02  = {empty, RPT},
    M03  = {empty, FTT},
    M04  = {start, FN01, RPT, SS},

    I01  = "<id_z2h3a0_1m6_zq43us>",
    S01  = "<geoclass_first-order_administrative_division>",
    P01  = "rdfs:subClassOf",
    O01  = "<yagoGeoEntity>",
    T01  = {I01, S01, P01, O01},
    R01  = {RHC, {IS, {FR01, ND}, T01}},
    R02  = {RHC, {SS, {FR01, ND}, T01}},
    {timeout, 10,
     {inorder,
      [
       ?_assertMatch(ok,      b3s:start()),
       ?_assertMatch(ok,      b3s:bootstrap()),
       ?_assertMatch(true,    register(FT, self())),
       ?_assertMatch({ok, _}, supervisor:start_child(B3S, CS01)),
       ?_assertMatch(ok,      gen_server:call(FR01, M01)),
       ?_assertMatch(0,       gen_server:call(FR01, {GP, TC})),
       ?_assertMatch(ok,      gen_server:cast(FR01, M02)),
       ?_assertMatch(1,       gen_server:call(FR01, {GP, TC})),
       ?_assertMatch(R01,     gen_server:call(RP, repeat)),
       ?_assertMatch(ok,      het_iterate_cast(1000, FR01, M03)),
       ?_assertMatch(ok,      timer:sleep(1000)),
       ?_assertMatch(673,     gen_server:call(FR01, {GP, TC})),
       ?_assertMatch(672,     gen_server:call(FR01, {GP, NTL})),
       ?_assertMatch(672,     gen_server:call(FR01, {GP, NTT})),
       ?_assertMatch(ok,      gen_server:cast(FR01, M02)),
       ?_assertMatch(ok,      gen_server:call(FR01, M04)),
       ?_assertMatch(0,       gen_server:call(FR01, {GP, TC})),
       ?_assertMatch(ok,      gen_server:cast(FR01, M02)),
       ?_assertMatch(1,       gen_server:call(FR01, {GP, TC})),
       ?_assertMatch(0,       gen_server:call(FR01, {GP, NTL})),
       ?_assertMatch(672,     gen_server:call(FR01, {GP, NTT})),
       ?_assertMatch(R02,     gen_server:call(RP, repeat)),
       ?_assertMatch(ok,      b3s:stop())
      ]}}.

het_iterate_cast(0, _, _) ->
    ok;
het_iterate_cast(N, Pid, Message) ->
    gen_server:cast(Pid, Message),
    receive
	{_, {_, _, end_of_stream}} ->
	    A = [N, Pid, Message],
	    info_msg(het_iterate_cast, A, end_of_stream, 50),
	    het_iterate_cast(0, Pid, Message);
	M ->
	    A = [N, Pid, Message],
	    info_msg(het_iterate_cast, A, M, 100),
	    het_iterate_cast(N - 1, Pid, Message)
    end.

%% ====> END OF LINE <====

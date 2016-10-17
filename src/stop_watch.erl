%%
%% Stop_Watch process for testing and debugging.
%%
%% @copyright 2013-2016 UP FAMNIT and Yahoo! Japan Corporation
%% @version 0.3
%% @since September, 2013
%% @author Kiyoshi Nitta <knitta@yahoo-corp.jp>
%% 
%% @doc A server for recording lap events and report elapsed
%% time. This module is implemented as an erlang <A
%% href="http://www.erlang.org/doc/man/gen_server.html">gen_server</A>
%% process.
%% 
%% == handle_call (synchronous) message API ==
%% 
%% (LINK: {@section handle_call (synchronous) message API})
%% 
%% === {start, Label} ===
%% 
%% Starts the stopwatch with Label::string(). It returns {started,
%% Label::string(), LocalTime::<A
%% href="http://www.erlang.org/doc/man/calendar.html#type-datetime1970">calendar:datetime1970()</A>}}. This
%% request is implemented by {@link hc_start/2}. (LINK: {@section
%% @{start, Label@}})
%% 
%% === {record, Label} ===
%% 
%% Records the event with Label::string(). It returns {recorded,
%% Label::string(), LocalTime::<A
%% href="http://www.erlang.org/doc/man/calendar.html#type-datetime1970">calendar:datetime1970()</A>}. This
%% request is implemented by {@link hc_record/2}. (LINK: {@section
%% @{record, Label@}})
%% 
%% === report ===
%% 
%% Reports the number of request and elapsed time after receiving
%% {@section @{start, Label@}} request. It returns {{number_of_rec,
%% Num::integer()}, {elapsed_microsec, Elapsed::integer()}}. This
%% request is implemented by {@link hc_report/1}. (LINK: {@section
%% report})
%% 
%% == handle_cast (asynchronous) message API ==
%% 
%% (LINK: {@section handle_cast (asynchronous) message API})
%% 
%% === {record, Label} (cast) ===
%% 
%% Records the event with Label::string(). This request is implemented
%% by {@link hc_record/2}. (LINK: {@section @{record, Label@} (cast)})
%% 
%% @type sw_state() = [{start_time, {string(), erlang:timestamp()}} |
%% {rec_history, [{string(), erlang:timestamp()}]}]. State structure
%% of stop_watch gen_server.
%% 
-module(stop_watch).
-behavior(gen_server).
-export([child_spec/0,
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
%% @doc Initialize a stop_watch process.
%% 
%% @spec init([]) -> {ok, sw_state()}
%% 
init([]) ->
    NewState =
	[
	 {start_time, {"init", os:timestamp()}},
	 {rec_history, []}
	],
    process_flag(trap_exit, true),

    R = {ok, NewState},
    info_msg(init, [], R, -1),
    R.

%% 
%% handle_call/3
%% 
%% @doc Handle synchronous query requests.
%% 
%% @spec handle_call(term(), {pid(), term()}, sw_state()) -> {reply,
%% term(), sw_state()}
%% 

%% Start measuring time.
handle_call({start, Label}, _, State) ->
    hc_start(Label, State);

%% Record an event.
handle_call({record, Label}, _, State) ->
    hc_record(Label, State);

%% Report elapsed time for all recorded events.
handle_call(report, _, State) ->
    hc_report(State);

%% Default reply.
handle_call(Request, From, State) ->
    R = {unknown_request, Request},
    error_msg(handle_call, [Request, From, State], R),
    {reply, R, State}.

handle_call_test_() ->
    SW = stop_watch,
    G01 = qwe,
    R01 = {unknown_request, G01},
    {inorder,
     [
      ?_assertMatch(ok,  b3s:start()),
      ?_assertMatch(R01, gen_server:call(SW, G01)),
      ?_assertMatch(ok,  b3s:stop())
     ]}.

%% 
%% handle_cast/2
%% 
%% @doc Handle asynchronous query requests. 
%% 
%% @spec handle_cast(term(), sw_state()) -> {noreply, sw_state()}
%% 

%% Record an event.
handle_cast({record, Label}, State) ->
    {reply, _, NewState} = hc_record(Label, State),
    {noreply, NewState};

%% default
handle_cast(_Request, State) ->
    {noreply, State}.

handle_cast_test_() ->
    {inorder,
     [
      ?_assertMatch(ok,  b3s:start()),
      ?_assertMatch(ok,  b3s:stop())
     ]}.

%% 
%% handle_info/2
%% 
%% @doc Handle exceptional query requests. 
%% 
%% @spec handle_info(term(), sw_state()) -> {noreply, sw_state()}
%% 
handle_info(_Info, State) ->
    {noreply, State}.

%% 
%% terminate/2
%% 
%% @doc Process termination. 
%% 
%% @spec terminate(term(), sw_state()) -> none()
%% 
terminate(Reason, State) ->
    info_msg(terminate, [Reason, State], ok, -1),
    ok.

%% 
%% code_change/3
%% 
%% @doc Process code change action. 
%% 
%% @spec code_change(term(), sw_state(), term()) -> {ok, sw_state()}
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
%% @doc Fetch an attribute value from the server state.
%% 
%% @spec fetch_state_value(atom(), sw_state()) -> term()
%% 
fetch_state_value(Name, State) ->
    fetch_state_value(Name, State, lists:keyfind(Name, 1, State)).
fetch_state_value(Name, _, {Name, Value}) ->
    Value;
fetch_state_value(Name, State, Err) ->
    F = "stop_watch:fetch_state_value(~w, ~w): ~w~n",
    error_logger:error_msg(F, [Name, State, Err]),
    Err.

%% 
%% @doc Update attribute values of the server state.
%% 
%% @spec update_state_value(sw_state(), sw_state()) -> sw_state()
%% 
update_state_value([], State) ->
    State;
update_state_value([{Key, Value} | Rest], State) ->
    NewState = lists:keyreplace(Key, 1, State, {Key, Value}),
    update_state_value(Rest, NewState);
update_state_value([A | Rest], State) ->
    E = not_key_value,
    F = "stop_watch:update_state_value(~w, ~w): ~w~n",
    error_logger:error_msg(F, [[A | Rest], State, {E, A}]),
    State.

%% 
%% @doc Start measuring time.
%% 
%% @spec hc_start(string(), sw_state()) -> {reply, term(), sw_state()}
%% 
hc_start(Label, State) ->
    Now = os:timestamp(),
    Update = [
	      {start_time,  {Label, Now}},
	      {rec_history, []}
	     ],
    NewState = update_state_value(Update, State),
    LocalTime = calendar:now_to_local_time(Now),
    Reply = {reply, {started, Label, LocalTime}, NewState},
    info_msg(hc_start, [Label, State], Reply, 100),
    Reply.

hc_start_test_() ->
    SW = stop_watch,
    L01 = "qwe",
    G01 = {start, L01},
    R01 = started,
    {inorder,
     [
      ?_assertMatch(ok,            b3s:start()),
      ?_assertMatch({R01, L01, _}, gen_server:call(SW, G01)),
      ?_assertMatch(ok,            b3s:stop())
     ]}.

%% 
%% @doc Record an event.
%% 
%% @spec hc_record(string(), sw_state()) -> {reply, term(), sw_state()}
%% 
hc_record(Label, State) ->
    Now = os:timestamp(),
    RH = rec_history,
    LapHist = fetch_state_value(RH, State),
    Update = [{rec_history, [{Label, Now} | LapHist]}],
    NewState = update_state_value(Update, State),
    LocalTime = calendar:now_to_local_time(Now),
    Reply = {reply, {recorded, Label, LocalTime}, NewState},
    info_msg(hc_record, [Label, State], Reply, 100),
    Reply.

hc_record_test_() ->
    SW = stop_watch,
    L01 = "qwe",
    G01 = {record, L01},
    L02 = "asd",
    G02 = {record, L02},
    L03 = "zxc",
    G03 = {record, L03},
    L04 = "test store start",
    G04 = {start, L04},
    R01 = recorded,
    R02 = started,
    {inorder,
     [
      ?_assertMatch(ok,            b3s:start()),
      ?_assertMatch({R01, L01, _}, gen_server:call(SW, G01)),
      ?_assertMatch({R01, L02, _}, gen_server:call(SW, G02)),
      ?_assertMatch({R01, L03, _}, gen_server:call(SW, G03)),
      ?_assertMatch({R02, L04, _}, gen_server:call(SW, G04)),
      ?_assertMatch({R01, L01, _}, gen_server:call(SW, G01)),
      ?_assertMatch(ok,            b3s:stop())
     ]}.

%% 
%% @doc Report elapsed time for all recorded events.
%% 
%% @spec hc_report(sw_state()) -> {reply, term(), sw_state()}
%% 
hc_report(State) ->
    RecHist = lists:reverse(fetch_state_value(rec_history, State)),
    {Label, StartTime} = fetch_state_value(start_time, State),
    SLT = calendar:now_to_local_time(StartTime),
    R = {0, Label, {datetime, SLT}, {lap, 0}, {total, 0}},
    info_msg(hc_report, reporting, R, 0),

    Report = hcr_proc_a_record(RecHist, StartTime, 0, 0),
    Reply = {reply, {reported, Report}, State},
    info_msg(hc_report, [State], Reply, 100),
    Reply.

hcr_proc_a_record([], _, Num, Elapsed) ->
    {{number_of_rec, Num}, {elapsed_microsec, Elapsed}};
hcr_proc_a_record([{Label, RecTime} | Rest], StartTime, Num, Elapsed) ->
    A = [[{Label, RecTime} | Rest], StartTime, Num, Elapsed],
    info_msg(hcr_proc_a_record, A, enter, 100),

    RLT = calendar:now_to_local_time(RecTime),
    LapTime = timer:now_diff(RecTime, StartTime),
    N = Num + 1,
    E = Elapsed + LapTime,
    R = {N, Label, {datetime, RLT}, {lap, LapTime}, {total, E}},
    info_msg(hcr_proc_a_record, reporting, R, 0),
    hcr_proc_a_record(Rest, RecTime, N, E).

hc_report_test_() ->
    SW = stop_watch,
    L01 = "qwe",
    G01 = {record, L01},
    L02 = "asd",
    G02 = {record, L02},
    L03 = "zxc",
    G03 = {record, L03},
    L04 = "test store start",
    G04 = {start, L04},
    G05 = report,
    R01 = recorded,
    R02 = started,
    R03 = reported,
    {inorder,
     [
      ?_assertMatch(ok,            b3s:start()),
      ?_assertMatch({R01, L01, _}, gen_server:call(SW, G01)),
      ?_assertMatch({R01, L02, _}, gen_server:call(SW, G02)),
      ?_assertMatch({R01, L03, _}, gen_server:call(SW, G03)),
      ?_assertMatch({R03, _},      gen_server:call(SW, G05)),
      ?_assertMatch({R02, L04, _}, gen_server:call(SW, G04)),
      ?_assertMatch({R01, L01, _}, gen_server:call(SW, G01)),
      ?_assertMatch({R03, _},      gen_server:call(SW, G05)),
      ?_assertMatch(ok,            b3s:stop())
     ]}.

%% ======================================================================
%% 
%% API
%% 

%% 
%% @doc Return child spec for this process. It can be used in
%% supervisor:init/0 callback implementation.
%% 
%% @spec child_spec() -> supervisor:child_spec()
%% 
child_spec() ->
    Id = stop_watch,
    GSOpt = [{local, Id}, Id, [], []],
    StartFunc = {gen_server, start_link, GSOpt},
    Restart = permanent,
    Shutdwon = 1000,
    Type = worker,
    Modules = [stop_watch],

    {Id, StartFunc, Restart, Shutdwon, Type, Modules}.

%% ====> END OF LINE <====

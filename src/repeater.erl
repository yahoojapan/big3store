%%
%% Repeater process for testing and debugging.
%%
%% @copyright 2013-2016 UP FAMNIT and Yahoo! Japan Corporation
%% @version 0.3
%% @since August, 2013
%% @author Kiyoshi Nitta <knitta@yahoo-corp.jp>
%% 
%% @doc A server for testing and debugging inter-process
%% messages. This module is implemented as an erlang <A
%% href="http://www.erlang.org/doc/man/gen_server.html">gen_server</A>
%% process.
%% 
%% == handle_call (synchronous) message API ==
%% 
%% Although this gen_server reminds any called messages, following
%% requests returns specific results. (LINK: {@section handle_call
%% (synchronous) message API})
%% 
%% === num_request ===
%% 
%% Returns the number total number of request.
%% (LINK: {@section num_request})
%% 
%% === repeat ===
%% 
%% Returns the last received request.
%% (LINK: {@section repeat}})
%% 
%% == handle_cast (asynchronous) message API ==
%% 
%% This server reminds any type of casted messages. (LINK: {@section
%% handle_cast (asynchronous) message API})
%% 
%% @type r_state() = [{last_request, term()} | {start_time,
%% calendar:datetime()} | {request_time, calendar:datetime()} |
%% {num_call, integer()} | {num_cast, integer()} | {num_request,
%% integer()}]. State structure of repeater gen_server.
%% 
-module(repeater).
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
%% @doc Initialize a repeater process.
%% 
%% @spec init([]) -> {ok, r_state()}
%% 
init([]) ->
    NewState =
	[
	 {last_request, []},
	 {start_time, calendar:local_time()},
	 {request_time, calendar:local_time()},
	 {num_call, 0},
	 {num_cast, 0},
	 {num_request, 0}
	],
    process_flag(trap_exit, true),

    info_msg(init, [], {started, NewState}, 0),
    {ok, NewState}.

%% 
%% handle_call/3
%% 
%% @doc Handle synchronous query requests.
%% 
%% @spec handle_call(term(), {pid(), term()}, r_state()) -> {reply,
%% term(), r_state()}
%% 

%% Default reply.
handle_call(num_request, _, State) ->
    R = fetch_state_value(num_request, State),
    {reply, R, State};
handle_call(repeat, From, State) ->
    R        = fetch_state_value(last_request, State),
    NR       = num_request,
    NumReq   = fetch_state_value(NR, State),
    Update   = [{NR, NumReq + 1}],
    NewState = update_state_value(Update, State),
    {ok, DL} = application:get_env(b3s, debug_level),
    hc_info_msg({R, NewState}, repeat, From, State, DL),
    {reply, R, NewState};
handle_call(Request, From, State) ->
    R        = {recorded_handle_call, Request},
    LR       = last_request,
    NR       = num_request,
    NC       = num_call,
    RT       = request_time,
    NumReq   = fetch_state_value(NR, State),
    NumCall  = fetch_state_value(NC, State),
    Update   = 
	[
	 {LR, R},
	 {NR, NumReq + 1},
	 {NC, NumCall + 1},
	 {RT, calendar:local_time()}
	],
    NewState = update_state_value(Update, State),
    {ok, DL} = application:get_env(b3s, debug_level),
    hc_info_msg({R, NewState}, Request, From, State, DL),
    {reply, R, NewState}.

hc_info_msg(Result, Request, no_from, State, D) when D > 50 ->
    M = "repeater:handle_cast",
    F = "~s(~p, ~p): ~p.~n",
    A = [M, Request, State, Result],
    error_logger:info_msg(F, A);
hc_info_msg(Result, Request, From, State, D) when D > 50 ->
    M = "repeater:handle_call",
    F = "~s(~p, ~p, ~p): ~p.~n",
    A = [M, Request, From, State, Result],
    error_logger:info_msg(F, A);
hc_info_msg(_, _, _, _, _) ->
    t.

handle_call_test_() ->
    RP = repeater,
    G01 = repeat,
    G02 = {qwe, asd, zxc},
    G03 = {rty, fgh, vbn},
    R01 = {recorded_handle_call, G02},
    R02 = {recorded_handle_call, G03},
    {inorder,
     [
      ?_assertMatch(ok,  b3s:start()),
      ?_assertMatch([],  gen_server:call(RP, G01)),
      ?_assertMatch(R01, gen_server:call(RP, G02)),
      ?_assertMatch(R01, gen_server:call(RP, G01)),
      ?_assertMatch(R02, gen_server:call(RP, G03)),
      ?_assertMatch(R02, gen_server:call(RP, G03)),
      ?_assertMatch(R02, gen_server:call(RP, G01)),
      ?_assertMatch(ok,  b3s:stop())
     ]}.

%% 
%% handle_cast/2
%% 
%% @doc Handle asynchronous query requests. 
%% 
%% @spec handle_cast(term(), r_state()) -> {noreply, r_state()}
%% 
handle_cast(Request, State) ->
    R        = {recorded_handle_cast, Request},
    LR       = last_request,
    NR       = num_request,
    NC       = num_cast,
    RT       = request_time,
    NumReq   = fetch_state_value(NR, State),
    NumCast  = fetch_state_value(NC, State),
    Update   =
	[
	 {LR, R},
	 {NR, NumReq + 1},
	 {NC, NumCast + 1},
	 {RT, calendar:local_time()}
	],
    NewState = update_state_value(Update, State),
    {ok, DL} = application:get_env(b3s, debug_level),
    hc_info_msg({R, NewState}, Request, no_from, State, DL),
    {noreply, NewState}.

handle_cast_test_() ->
    RP = repeater,
    G01 = repeat,
    G02 = {qwe, asd, zxc},
    G03 = {rty, fgh, vbn},
    R01 = {recorded_handle_cast, G02},
    R02 = {recorded_handle_cast, G03},
    {inorder,
     [
      ?_assertMatch(ok,  b3s:start()),
      ?_assertMatch([],  gen_server:call(RP, G01)),
      ?_assertMatch(ok,  gen_server:cast(RP, G02)),
      ?_assertMatch(R01, gen_server:call(RP, G01)),
      ?_assertMatch(ok,  gen_server:cast(RP, G03)),
      ?_assertMatch(ok,  gen_server:cast(RP, G03)),
      ?_assertMatch(R02, gen_server:call(RP, G01)),
      ?_assertMatch(ok,  b3s:stop())
     ]}.

%% 
%% handle_info/2
%% 
%% @doc Handle exceptional query requests. 
%% 
%% @spec handle_info(term(), r_state()) -> {noreply, r_state()}
%% 
handle_info(_Info, State) ->
    {noreply, State}.

%% 
%% terminate/2
%% 
%% @doc Process termination. 
%% 
%% @spec terminate(term(), r_state()) -> none()
%% 
terminate(Reason, State) ->
    info_msg(terminate, [Reason, State], terminate, 0),
    ok.

%% 
%% code_change/3
%% 
%% @doc Process code change action. 
%% 
%% @spec code_change(term(), r_state(), term()) -> {ok, r_state()}
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
%% @spec fetch_state_value(atom(), r_state()) -> term()
%% 
fetch_state_value(Name, State) ->
    fetch_state_value(Name, State, lists:keyfind(Name, 1, State)).
fetch_state_value(Name, _, {Name, Value}) ->
    Value;
fetch_state_value(Name, State, Err) ->
    error_msg(fetch_state_value, [Name, State], {error, Err}),
    Err.

%% 
%% @doc Update attribute values of the server state.
%% 
%% @spec update_state_value(r_state(), r_state()) -> r_state()
%% 
update_state_value([], State) ->
    State;
update_state_value([{Key, Value} | Rest], State) ->
    NewState = lists:keyreplace(Key, 1, State, {Key, Value}),
    update_state_value(Rest, NewState);
update_state_value([A | Rest], State) ->
    E = not_key_value,
    error_msg(fetch_state_value, [[A | Rest], State], {E, A}),
    State.

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
    Id = repeater,
    GSOpt = [{local, Id}, Id, [], []],
    StartFunc = {gen_server, start_link, GSOpt},
    Restart = permanent,
    Shutdwon = 1000,
    Type = worker,
    Modules = [repeater],

    {Id, StartFunc, Restart, Shutdwon, Type, Modules}.

%% ====> END OF LINE <====

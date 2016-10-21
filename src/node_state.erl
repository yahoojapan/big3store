%%
%% Manage global resources of one node.
%%
%% @copyright 2014-2016 UP FAMNIT and Yahoo Japan Corporation
%% @version 0.3
%% @since August, 2014
%% @author Kiyoshi Nitta <knitta@yahoo-corp.jp>
%% 
%% @see b3s
%% @see b3s_state
%% 
%% @doc Manage global resources of one node. This is a gen_server
%% process running on each node that belongs to the distributed
%% big3store system.
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
%% <tr> <td>b3s_state_pid</td> <td>{@type ns_pid()}</td> <td>global id
%% of b3s state process. (not cloned)</td> </tr>
%% 
%% <tr> <td>clm_row_conf</td> <td>maps:map()</td> <td>mapping from
%% {@type node_state:ns_column_id()} to {@type
%% node_state:ns_rows()}.</td> </tr>
%% 
%% <tr> <td>pred_clm</td> <td>maps:map()</td> <td>mapping from {@link
%% tp_query_node:qn_predicate()} to {@type
%% node_state:ns_column_id()}.</td> </tr>
%% 
%% <tr> <td>tree_id_format</td> <td>string()</td> <td>format string
%% for generating {@link query_tree:qt_id()}.</td> </tr>
%% 
%% <tr> <td>tree_id_current</td> <td>integer()</td> <td>seed integer
%% for generating {@link query_tree:qt_id()}.</td> </tr>
%% 
%% <tr> <td>start_date_time</td> <td>calendar:datetime()</td>
%% <td>started date and time of the process.</td> </tr>
%% 
%% <tr> <td>update_date_time</td> <td>calendar:datetime()</td>
%% <td>updated date and time of process properties.</td> </tr>
%% 
%% <tr> <td>db_interface_cursor</td> <td>{@type
%% db_interface:di_bdbnif_cursor()}</td> <td>session information for a
%% table.</td> </tr>
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
%% </table>
%% 
%% === {get, PropertyName} ===
%% 
%% This message takes PropertyName::atom() as an argument and returns
%% the value Result::term() of the specified global property managed
%% by the node_state process. It returns undefined, if the property
%% was not defined yet. (LINK: {@section @{get, PropertyName@}})
%% 
%% === {put, PropertyName, Value} ===
%% 
%% This message takes PropertyName::atom() and Value::term() as
%% arguments. If it successfully puts the value to the property, it
%% returns ok. Otherwise, it returns {error, Reason::term()}. (LINK:
%% {@section @{put, PropertyName, Value@}})
%% 
%% == handle_cast (asynchronous) message API ==
%% 
%% (LINK: {@section handle_cast (asynchronous) message API})
%% 
%% <table border="3">
%% 
%% <tr> <th>Message</th> <th>Args</th> <th>Returns</th>
%% <th>Description</th> </tr>
%% 
%% <tr> <td></td> <td></td> <td></td> <td></td> </tr>
%% 
%% </table>
%% 
%% @type ns_column_id() = integer(). A column identifier of natual
%% number.
%% 
%% @type ns_rows() = maps:map(). A mapping object from {@type
%% ns_row_id()} to node().
%% 
%% @type ns_row_id() = integer(). A row identifier of natual number.
%% 
%% @type ns_node_location() = {ns_column_id(), ns_row_id()}. Column
%% and row representation of a node location. It can be resolved to
%% node() using the map structure obtained from clm_row_conf
%% property. (See {@section property list})
%% 
%% @type ns_pid() = {atom(), node()}. A process identifier in
%% distributed environment. The first element atom() must be a locally
%% registered name of pid().
%% 
%% @type ns_pid_list() = [{atom(), node()}]. List of {@type ns_pid()}.
%% 
%% @type ns_state() = maps:map().
%% 
-module(node_state).
-behavior(gen_server).
-include_lib("eunit/include/eunit.hrl").
-export(
   [
    child_spec/0,
    init/1, handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3,
    error_msg/4, info_msg/5
   ]).

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
    Id        = node_state,
    GSOpt     = [{local, Id}, Id, [], []],
    StartFunc = {gen_server, start_link, GSOpt},
    Restart   = permanent,
    Shutdwon  = 1000,
    Type      = worker,
    Modules   = [node_state],

    {Id, StartFunc, Restart, Shutdwon, Type, Modules}.

%% ======================================================================
%% 
%% gen_server behavior
%% 

%% 
%% init/1
%% 
%% @doc Initialize a triple_distributor process.
%% 
%% @spec init([]) -> {ok, ns_state()}
%% 
init([]) ->
    process_flag(trap_exit, true),

    State = #{
      created          => true,
      pid              => self(),
      node_state_pid   => {node_state, node()},
      start_date_time  => calendar:local_time(),
      update_date_time => calendar:local_time(),
      tree_id_current  => 0
     },

    info_msg(?MODULE, init, [], State, -1),
    {ok, State}.

%% 
%% handle_call/3
%% 
%% @doc Handle synchronous query requests.
%% 
%% @spec handle_call(term(), pid(), ns_state()) -> {reply, term(),
%% ns_state()}
%% 

handle_call({get, all}, _, State) ->
    hc_restore_pd(erlang:get(created), State),
    {reply, erlang:get(), State};

handle_call({get, PropertyName}, _, State) ->
    hc_restore_pd(erlang:get(created), State),
    {reply, erlang:get(PropertyName), State};

handle_call({put, PropertyName, Value}, From, State) ->
    hc_restore_pd(erlang:get(created), State),
    erlang:put(PropertyName, Value),
    erlang:put(update_date_time, calendar:local_time()),
    NewState = hc_save_pd(),
    A = [{put, PropertyName, Value}, From, State],
    info_msg(?MODULE, handle_call, A, NewState, 50),
    {reply, ok, NewState};

%% default
handle_call(Request, From, State) ->
    R = {unknown_request, Request},
    error_msg(?MODULE, handle_call, [Request, From, State], R),
    {reply, R, State}.

%% 
%% handle_cast/2
%% 
%% @doc Handle asynchronous query requests. 
%% 
%% @spec handle_cast(term(), ns_state()) -> {noreply, ns_state()}
%% 

%% default
handle_cast(Request, State) ->
    R = {unknown_request, Request},
    error_msg(?MODULE, handle_cast, [Request, State], R),
    {noreply, State}.

%% 
%% @doc Restore process dictionaries from state map structure.
%% 
%% @spec hc_restore_pd([{atom(), term()}] | undefined, ns_state()) -> ok
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
%% @spec hc_save_pd() -> ns_state()
%% 
hc_save_pd() ->
    maps:from_list(get()).

%% 
%% handle_info/2
%% 
%% @doc Handle exceptional query requests. 
%% 
%% @spec handle_info(term(), ns_state()) -> {noreply, ns_state()}
%% 
handle_info(_Info, State) ->
    {noreply, State}.

%% 
%% terminate/2
%% 
%% @doc Process termination. 
%% 
%% @spec terminate(term(), ns_state()) -> none()
%% 
terminate(Reason, State) ->
    info_msg(?MODULE, terminate, [Reason, State], terminate_normal, 0),
    ok.

%% 
%% code_change/3
%% 
%% @doc Process code change action. 
%% 
%% @spec code_change(term(), ns_state(), term()) -> {ok, ns_state()}
%% 
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ======================================================================
%% 
%% handle call/cast implementation
%% 

%% ======================================================================
%% 
%% utility
%% 

%% 
%% @doc Report an error issue to the error_logger.
%% 
%% @spec error_msg(atom(), atom(), term(), term()) -> ok
%% 
error_msg(ModName, FunName, Argument, Result) ->
    Arg = [ModName, FunName, Argument, Result],
    Fmt = "~w:~w(~p): ~p.~n",
    error_logger:error_msg(Fmt, Arg).

%% 
%% @doc Report an information issue to the error_logger if current
%% debug level is greater than ThresholdDL.
%% 
%% @spec info_msg(atom(), atom(), term(), term(), integer()) -> ok
%% 
info_msg(ModName, FunName, Argument, Result, ThresholdDL) ->
    Arg = [os:timestamp(), ModName, FunName, Argument, Result],
    Fmt = "timestamp: ~w~n~w:~w(~p): ~p.~n",
    im_cond(Fmt, Arg, ThresholdDL).

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

%% ======================================================================
%% 
%% @doc Unit tests.
%% 
ns_test_() ->
    application:load(b3s),
    nt_site(b3s_state:get(test_mode)).

nt_site(local2) ->
    NDS    = node(),
    NodStr = atom_to_list(NDS),
    NDC    = list_to_atom("b3ss02" ++ string:sub_string(NodStr, 7)),
    NSS    = node_state,
    NSC    = {node_state, NDC},
    BSS    = {b3s_state, NDS},
    BSC    = {b3s_state, NDC},
    CRC    = clm_row_conf,
    BSP    = b3s_state_pid,
    UND    = undefined,

    RMS = #{1 => NDS},
    RMC = #{1 => NDC},
    CM1 = #{1 => RMS, 2 => RMC},
    R01 = [NDS, NDC],

    {inorder,
     [
      ?_assertMatch(ok,  b3s:start()),
      ?_assertMatch(ok,  b3s:bootstrap()),
      ?_assertMatch(UND, gen_server:call(NSS, {get, qwe})),
      ?_assertMatch(ok,  gen_server:call(NSS, {put, qwe, asd})),
      ?_assertMatch(asd, gen_server:call(NSS, {get, qwe})),
      ?_assertMatch(UND, gen_server:call(NSC, {get, qwe})),
      ?_assertMatch(ok,  gen_server:call(NSC, {put, qwe, asd})),
      ?_assertMatch(asd, gen_server:call(NSC, {get, qwe})),
      ?_assertMatch(ok,  gen_server:call(NSC, {put, BSP, UND})),

      ?_assertMatch(UND, gen_server:call(NSC, {get, BSP})),
      ?_assertMatch(ok,  gen_server:call(BSS, {put, CRC, CM1})),
      ?_assertMatch(R01, gen_server:call(BSS, propagate)),
      ?_assertMatch(BSS, gen_server:call(NSS, {get, BSP})),
      ?_assertMatch(BSS, gen_server:call(NSC, {get, BSP})),

      ?_assertMatch(ok,  gen_server:call(BSS, {clone, NDC})),
      ?_assertMatch(R01, gen_server:call(BSC, propagate)),
      ?_assertMatch(BSC, gen_server:call(NSS, {get, BSP})),
      ?_assertMatch(BSC, gen_server:call(NSC, {get, BSP})),

      ?_assertMatch(ok,        b3s:stop())
     ]};

nt_site(yjr6) ->
    NW     = 'b3ss02@wild.rlab.miniy.yahoo.co.jp',
    NC     = 'b3ss02@circus.rlab.miniy.yahoo.co.jp',
    NI     = 'b3ss02@innocents.rlab.miniy.yahoo.co.jp',
    NE     = 'b3ss02@erasure.rlab.miniy.yahoo.co.jp',
    NL     = 'b3ss02@loveboat.rlab.miniy.yahoo.co.jp',
    NN     = 'b3ss02@nightbird.rlab.miniy.yahoo.co.jp',

    ND     = node(),
    BS     = b3s_state,
    NS     = node_state,
    BSP    = b3s_state_pid,
    CRC    = clm_row_conf,
    PRC    = pred_clm,
    UND    = undefined,

    BSS    = {BS, ND},
    BSN    = {BS, NN},
    NSW    = {NS, NW},
    NSC    = {NS, NC},
    NSI    = {NS, NI},
    NSE    = {NS, NE},
    NSL    = {NS, NL},
    NSN    = {NS, NN},

    RM1    = #{1 => NW, 2 => NC},
    RM2    = #{1 => NI, 2 => NE},
    RM3    = #{1 => NL, 2 => NN},
    CM1    = #{1 => RM1, 2 => RM2, 3 => RM3},
    R01    = [NL, NN, NW, NC, NE, NI],

    DF1    = #{",)>" => 2,
	       "<actedIn>" => 1,
	       "<byTransport>" => 3,
	       "<created>" => 3,
	       "<dealsWith>" => 3,
	       "<diedIn>" => 2,
	       "<diedOnDate>" => 1,
	       "<directed>" => 3,
	       "<edited>" => 3,
	       "<endedOnDate>" => 3,
	       "<exports>" => 1,
	       "<extractionSource>" => 1,
	       "<extractionTechnique>" => 3,
	       "<graduatedFrom>" => 2,
	       "<happenedIn>" => 2,
	       "<happenedOnDate>" => 1,
	       "<hasAcademicAdvisor>" => 2,
	       "<hasAirportCode>" => 1,
	       "<hasArea>" => 3,
	       "<hasBudget>" => 1,
	       "<hasCapital>" => 1,
	       "<hasChild>" => 2,
	       "<hasConfidence>" => 1,
	       "<hasCurrency>" => 3,
	       "<hasDuration>" => 1,
	       "<hasEconomicGrowth>" => 1,
	       "<hasExpenses>" => 2,
	       "<hasExport>" => 1,
	       "<hasFamilyName>" => 2,
	       "<hasGDP>" => 3,
	       "<hasGender>" => 1,
	       "<hasGeonamesClassId>" => 2,
	       "<hasGeonamesEntityId>" => 3,
	       "<hasGini>" => 2,
	       "<hasGivenName>" => 3,
	       "<hasGloss>" => 1,
	       "<hasHeight>" => 3,
	       "<hasISBN>" => 2,
	       "<hasImdb>" => 1,
	       "<hasImport>" => 3,
	       "<hasInflation>" => 2,
	       "<hasLanguageCode>" => 1,
	       "<hasLatitude>" => 2,
	       "<hasLength>" => 1,
	       "<hasLongitude>" => 1,
	       "<hasMotto>" => 3,
	       "<hasMusicalRole>" => 1,
	       "<hasNumber>" => 2,
	       "<hasNumberOfPeople>" => 1,
	       "<hasNumberOfThings>" => 2,
	       "<hasOfficialLanguage>" => 2,
	       "<hasPages>" => 1,
	       "<hasPopulationDensity>" => 1,
	       "<hasPoverty>" => 3,
	       "<hasPredecessor>" => 3,
	       "<hasRevenue>" => 3,
	       "<hasSuccessor>" => 1,
	       "<hasSynsetId>" => 2,
	       "<hasTLD>" => 2,
	       "<hasThreeLetterLanguageCode>" => 3,
	       "<hasUnemployment>" => 3,
	       "<hasWebsite>" => 2,
	       "<hasWeight>" => 2,
	       "<hasWikipediaArticleLength>" => 1,
	       "<hasWikipediaUrl>" => 3,
	       "<hasWonPrize>" => 3,
	       "<hasWordnetDomain>" => 2,
	       "<holdsPoliticalPosition>" => 2,
	       "<imports>" => 1,
	       "<influences>" => 3,
	       "<isAffiliatedTo>" => 1,
	       "<isCitizenOf>" => 1,
	       "<isConnectedTo>" => 2,
	       "<isInterestedIn>" => 3,
	       "<isKnownFor>" => 2,
	       "<isLeaderOf>" => 3,
	       "<isLocatedIn>" => 2,
	       "<isMarriedTo>" => 2,
	       "<isPoliticianOf>" => 1,
	       "<isPreferredMeaningOf>" => 1,
	       "<linksTo>" => 2,
	       "<livesIn>" => 3,
	       "<occursSince>" => 3,
	       "<occursUntil>" => 2,
	       "<owns>" => 1,
	       "<participatedIn>" => 3,
	       "<playsFor>" => 3,
	       "<startedOnDate>" => 3,
	       "<wasBornIn>" => 3,
	       "<wasBornOnDate>" => 1,
	       "<wasCreatedOnDate>" => 2,
	       "<wasDestroyedOnDate>" => 2,
	       "<worksAt>" => 2,
	       "<wroteMusicFor>" => 1,
	       "owl:disjointWith" => 3,
	       "owl:equivalentClass" => 2,
	       "owl:sameAs" => 3,
	       "rdf:type" => 1,
	       "rdfs:comment" => 2,
	       "rdfs:domain" => 1,
	       "rdfs:label" => 2,
	       "rdfs:range" => 3,
	       "rdfs:subClassOf" => 3,
	       "rdfs:subPropertyOf" => 1,
	       "skos:prefLabel" => 2},

    {inorder,
     [
      ?_assertMatch(ok,  b3s:start()),
      ?_assertMatch(ok,  gen_server:call(BSS, {put, CRC, CM1})),
      ?_assertMatch(R01, sets:to_list(
			   sets:from_list(
			     gen_server:call(BSS, propagate)))),
      ?_assertMatch(BSS, gen_server:call(NSW, {get, BSP})),
      ?_assertMatch(BSS, gen_server:call(NSC, {get, BSP})),
      ?_assertMatch(BSS, gen_server:call(NSI, {get, BSP})),
      ?_assertMatch(BSS, gen_server:call(NSE, {get, BSP})),
      ?_assertMatch(BSS, gen_server:call(NSL, {get, BSP})),
      ?_assertMatch(BSS, gen_server:call(NSN, {get, BSP})),
      ?_assertMatch(ok,  gen_server:call(BSS, {clone, NN})),
      ?_assertMatch(ok,  gen_server:call(BSN, {put, PRC, DF1})),
      ?_assertMatch(R01, sets:to_list(
			   sets:from_list(
			     gen_server:call(BSN, propagate)))),
      ?_assertMatch(UND, gen_server:call(BSS, {get, PRC})),
      ?_assertMatch(DF1, gen_server:call(BSN, {get, PRC})),
      ?_assertMatch(BSN, gen_server:call(NSW, {get, BSP})),
      ?_assertMatch(BSN, gen_server:call(NSC, {get, BSP})),
      ?_assertMatch(BSN, gen_server:call(NSI, {get, BSP})),
      ?_assertMatch(BSN, gen_server:call(NSE, {get, BSP})),
      ?_assertMatch(BSN, gen_server:call(NSL, {get, BSP})),
      ?_assertMatch(BSN, gen_server:call(NSN, {get, BSP})),
      ?_assertMatch(DF1, gen_server:call(NSW, {get, PRC})),
      ?_assertMatch(DF1, gen_server:call(NSC, {get, PRC})),
      ?_assertMatch(DF1, gen_server:call(NSI, {get, PRC})),
      ?_assertMatch(DF1, gen_server:call(NSE, {get, PRC})),
      ?_assertMatch(DF1, gen_server:call(NSL, {get, PRC})),
      ?_assertMatch(DF1, gen_server:call(NSN, {get, PRC})),
      ?_assertMatch(ok,  b3s:stop())
     ]};

nt_site(_) ->
    [].

%% ====> END OF LINE <====

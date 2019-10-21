%%
%% Interface functions for accessing DB layer
%%
%% @copyright 2014-2019 UP FAMNIT and Yahoo Japan Corporation
%% @version 0.3
%% @since May, 2014
%% @author Kiyoshi Nitta <knitta@yahoo-corp.jp>
%% @author Iztok Savnik <iztok.savnik@famnit.upr.si>
%% 
%% @see query_node
%% @see tp_query_node
%% @see triple_distributor
%% 
%% @doc This module provides interface functions for accessing DB
%% layer.
%% 
%% Implementation method of triple tables can be changed by constant
%% variable DB_IMPLEMENTATION that is defined in "record.hrl". If the
%% constant has value 'epgsql', the table will be built on a
%% PostgreSQL server. This option is highly recommended for storing
%% large-scale triple dataset. In this option, this module uses
%% server-side functions defined in "../plpgsql/encode.plpgsql".
%% 
%% For mnesia_qlc, mnesia_imo, bdb_port_driver, and epgsql
%% implementation, this module uses following process dictionary keys:
%%
%% <ul>
%%   <li>di_cursor__</li>
%%   <li>di_ets__</li>
%% </ul>
%%
%% The caller process (maybe {@link tp_query_node} using {@link
%% db_next/0}) must not use this key in the process dictionary.
%%
%% @type di_pattern() = 'All' | 'Tid' | 'Sbj' | 'Prd' | 'Obj' |
%% 'SbjPrd' | 'SbjObj' | 'PrdObj' | 'SbjPrdObj'. This type means an
%% index pattern.
%% 
%% @type di_data() = query_node:qn_triple() | end_of_stream.
%%
%% @type di_serial() = integer(). This type means the number of
%% records in a table.
%%
%% @type di_table_name() = atom().
%%
%% @type di_triple() = {di_table_name(), query_node:qn_id(),
%% query_node:qn_subject(), query_node:qn_predicate(),
%% query_node:qn_object()}.
%%
-module(db_interface).
-export(
   [

    db_implementation/0, db_open_tp/1, db_close_tp/0, db_next/0, db_next/1,
    db_next_block/1, db_write/1, db_init/0, db_init/1, db_close/0,
    db_disconnect/0, db_add_index/0, db_del_index/0, db_put_map/2,
    db_get_map/1, dot_get_tn/0, dot_tp_to_qh/1, dotbpd_init/1,
    dttq_conv_var/2, dipp_squery/1,
    dip_get_connection/0, dwp_escape/1

   ]).
-include_lib("stdlib/include/qlc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("record.hrl").

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
%% @doc This function returns an identifier of database implementation
%% method.
%% 
%% @spec db_implementation() -> atom()
%% 
db_implementation() ->
    ?DB_IMPLEMENTATION.

db_implementation_test_() ->
    [
      ?_assertMatch(?DB_IMPLEMENTATION, db_implementation())
    ].

%% 
%% @doc Open access method for a triple table that exists in the same
%% column/row location.
%% 
%% @spec db_open_tp(query_node:qn_triple_pattern()) -> ok | fail
%% 
db_open_tp(TriplePattern) ->
    dot_choose(?DB_IMPLEMENTATION, TriplePattern).

dot_choose(epgsql, TriplePattern) ->
    db_open_tp_postgres(TriplePattern).

dot_get_tn() ->
    BS = gen_server:call(node_state, {get, b3s_state_pid}),
    L = gen_server:call(BS, {get, name_of_triple_tables}),
    case lists:keyfind(node(), 1, L) of
	false -> undefined;
	{_, Tab} ->  Tab
    end.

dotbpd_init(SharedLib) ->
    Port = open_port({spawn, SharedLib}, []),
    dotbpdi_loop(Port).

dotbpdi_loop(Port) ->
    receive
	{call, Caller, Msg} ->
	    Port ! {self(), {command, dotbpdi_encode(Msg)}},
	    receive
		{Port, {data, Data}} ->
		    Caller ! {dotbpd, dotbpdi_decode(Data)}
	    end,
	    dotbpdi_loop(Port);
	stop ->
	    Port ! {self(), close},
	    receive
		{Port, closed} ->
		    exit(normal)
	    end;
	{'EXIT', Port, Reason} ->
	    io:format("~p ~n", [Reason]),
	    exit(port_terminated)
    end.

dotbpdi_encode({foo, X})  -> [1, X];
dotbpdi_encode({bar, Y})  -> [2, Y];
dotbpdi_encode({baz, Z})  -> [3, Z];
dotbpdi_encode({putc, A}) -> [4, A];
dotbpdi_encode(getc)      -> [5];
dotbpdi_encode(hello)     -> [6].

dotbpdi_decode([1, 1, Int])    -> Int;
dotbpdi_decode([2, Len | Str]) -> {Len, Str}.

%% 
%% @doc Convert a triple pattern to qlc's query handler.
%% 
%% @spec dot_tp_to_qh(query_node:qn_triple_pattern()) -> qlc:query_handle()
%% 
dot_tp_to_qh(TriplePattern) ->
    Tab = dot_get_tn(),
    dttq_do(dttq_conv_var(2, TriplePattern),
	    dttq_conv_var(3, TriplePattern),
	    dttq_conv_var(4, TriplePattern), TriplePattern, Tab).

dttq_do(false, false, false, TP, Tab) ->
    MatchHead = {'_', '_', element(2, TP), element(3, TP), element(4, TP)},
    MatchSpec = [{MatchHead, [], ['$_']}],
    Opt = [{n_objects, 1}, {traverse, {select, MatchSpec}}],
    qlc:q([X || X <- mnesia:table(Tab, Opt)]);
dttq_do(true, false, false, TP, Tab) ->
    MatchHead = {'_', '_', '_', element(3, TP), element(4, TP)},
    MatchSpec = [{MatchHead, [], ['$_']}],
    Opt = [{n_objects, 1}, {traverse, {select, MatchSpec}}],
    qlc:q([X || X <- mnesia:table(Tab, Opt)]);
dttq_do(false, true, false, TP, Tab) ->
    MatchHead = {'_', '_', element(2, TP), '_', element(4, TP)},
    MatchSpec = [{MatchHead, [], ['$_']}],
    Opt = [{n_objects, 1}, {traverse, {select, MatchSpec}}],
    qlc:q([X || X <- mnesia:table(Tab, Opt)]);
dttq_do(false, false, true, TP, Tab) ->
    MatchHead = {'_', '_', element(2, TP), element(3, TP), '_'},
    MatchSpec = [{MatchHead, [], ['$_']}],
    Opt = [{n_objects, 1}, {traverse, {select, MatchSpec}}],
    qlc:q([X || X <- mnesia:table(Tab, Opt)]);
dttq_do(false, true, true, TP, Tab) ->
    MatchHead = {'_', '_', element(2, TP), '_', '_'},
    MatchSpec = [{MatchHead, [], ['$_']}],
    Opt = [{n_objects, 1}, {traverse, {select, MatchSpec}}],
    qlc:q([X || X <- mnesia:table(Tab, Opt)]);
dttq_do(true, false, true, TP, Tab) ->
    MatchHead = {'_', '_', '_', element(3, TP), '_'},
    MatchSpec = [{MatchHead, [], ['$_']}],
    Opt = [{n_objects, 1}, {traverse, {select, MatchSpec}}],
    qlc:q([X || X <- mnesia:table(Tab, Opt)]);
dttq_do(true, true, false, TP, Tab) ->
    MatchHead = {'_', '_', '_', '_', element(4, TP)},
    MatchSpec = [{MatchHead, [], ['$_']}],
    Opt = [{n_objects, 1}, {traverse, {select, MatchSpec}}],
    qlc:q([X || X <- mnesia:table(Tab, Opt)]);
dttq_do(true, true, true, _, Tab) ->
    qlc:q([X || X <- mnesia:table(Tab)]).

dttq_conv_var(Nth, TriplePattern) ->
    dcv_check(element(Nth, TriplePattern)).

dcv_check(E) when not is_list(E) ->
    false;
dcv_check(E) ->
    ChrQ  = string:chr(E, $?),
    ChrU  = string:chr(E, $_),
    ChrC  = string:chr(E, $:),
    ChrLS = string:chr(E, $[),
    ChrRS = string:chr(E, $]),
    dcv_do(ChrQ, ChrU, ChrC, ChrLS, ChrRS).

dcv_do(1, _, _, _, _) ->
    true;
dcv_do(_, 1, 2, _, _) ->
    true;
dcv_do(_, _, _, 1, 2) ->
    true;
dcv_do(_, _, _, _, _) ->
    false.

%% cursor structure access (get)
dotbn_get_cursor() ->
    DIC = gen_server:call(node_state, {get, db_interface_cursor}),
    dotbngc_check_dic(DIC).

dotbngc_check_dic({M, Serial}) ->
    dotbngc_check_map(is_map(M), M, Serial);
dotbngc_check_dic(E) ->
    A = {error, {illegal_structure, db_interface_cursor, E}},
    N = dotbn_get_cursor,
    error_msg(N, [], A),
    fail.

dotbngc_check_map(true, M, Serial) ->
    dotbngc_perform(maps:is_key(get(self), M), M, Serial);
dotbngc_check_map(_, E, Serial) ->
    A = {error, {no_map_set, db_interface_cursor, E, Serial}},
    N = dotbn_get_cursor,
    error_msg(N, [], A),
    fail.

dotbngc_perform(true, M, Serial) ->
    L = tuple_to_list(maps:get(get(self), M)),
    list_to_tuple(lists:append(L, [Serial]));
dotbngc_perform(_, E, Serial) ->
    A = {error, {no_process_entry, get(self), E, Serial}},
    N = dotbn_get_cursor,
    error_msg(N, [], A),
    fail.

%% cursor structure access (put)
dotbn_put_cursor({BHS, Pattern, Result, Serial}) ->
    DIC = gen_server:call(node_state, {get, db_interface_cursor}),
    dotbnpc_check_dic(DIC, {BHS, Pattern, Result, Serial});
dotbn_put_cursor(E) ->
    A = {error, {illegal_argument, E}},
    N = dotbn_put_cursor,
    error_msg(N, [], A),
    fail.

dotbnpc_check_dic({M, _}, Cursor) ->
    dotbnpc_check_map(is_map(M), M, Cursor);
dotbnpc_check_dic(E, Cursor) ->
    A = {error, {illegal_dic, E}},
    N = dotbn_put_cursor,
    error_msg(N, [], A),
    dotbnpc_perform(maps:new(), Cursor).

dotbnpc_check_map(true, M, Cursor) ->
    dotbnpc_perform(M, Cursor);
dotbnpc_check_map(_, E, Cursor) ->
    A = {error, {illegal_map, E}},
    N = dotbn_put_cursor,
    error_msg(N, [], A),
    dotbnpc_perform(maps:new(), Cursor).

dotbnpc_perform(M, {BHS, Pattern, Result, Serial}) ->
    NewDic = {maps:put(get(self), {BHS, Pattern, Result}, M), Serial},
    gen_server:call(node_state, {put, db_interface_cursor, NewDic}).

%% cursor structure access (close)
dotbn_close_cursor() ->
    dotbn_close_cursor(dotbn_get_cursor()).

dotbn_close_cursor({BHS, Pattern, Result, Serial}) ->
    DIC = gen_server:call(node_state, {get, db_interface_cursor}),

    A = [{BHS, Pattern, Result, Serial}],
    R = {db_interface_cursor, DIC},
    info_msg(dotbn_close_cursor, A, R, 50),

    dotbncc_check_dic(DIC, {BHS, Pattern, Result, Serial});
dotbn_close_cursor(fail) ->
    info_msg(dotbn_close_cursor, [fail], cursor_not_defined, 50);
dotbn_close_cursor(E) ->
    A = {error, {illegal_argument, E}},
    N = dotbn_close_cursor,
    error_msg(N, [], A),
    fail.

dotbncc_check_dic({M, C}, Cursor) ->
    A = [{M, C}, Cursor],
    R = {is_map, is_map(M)},
    info_msg(dotbncc_check_dic, A, R, 50),

    dotbncc_check_map(is_map(M), M, Cursor);
dotbncc_check_dic(E, Cursor) ->
    A = {error, {illegal_dic, E, Cursor}},
    N = dotbn_close_cursor,
    error_msg(N, [], A),
    fail.

dotbncc_check_map(true, M, Cursor) ->
    dotbncc_perform(M, Cursor);
dotbncc_check_map(_, E, Cursor) ->
    A = {error, {illegal_map, E, Cursor}},
    N = dotbn_close_cursor,
    error_msg(N, [], A),
    fail.

dotbncc_perform(M, {_, _, _, Serial}) ->
    NewDic = {maps:remove(get(self), M), Serial},
    gen_server:call(node_state, {put, db_interface_cursor, NewDic}).

%% cursor structure access (get_numdat)
dotbn_get_numdat() ->
    DIC = gen_server:call(node_state, {get, db_interface_cursor}),
    dotbngn_check_dic(DIC).

dotbngn_check_dic({_, Serial}) ->
    Serial;
dotbngn_check_dic(E) ->
    A = {error, {illegal_structure, db_interface_cursor, E}},
    N = dotbn_get_numdat,
    error_msg(N, [], A),
    0.

%% cursor structure access (test: get, put, and close)
dotbn_gc_pc_test() ->
    DIC = {a, b, c, d},

    put(self, '2-1-1'),
    {inorder,
     [
      ?_assertMatch(0,    dotbn_get_numdat()),
      ?_assertMatch(fail, dotbn_get_cursor()),
      ?_assertMatch(ok,   dotbn_close_cursor()),
      ?_assertMatch(fail, dotbn_put_cursor(abc)),
      ?_assertMatch(fail, dotbn_get_cursor()),
      ?_assertMatch(ok,   dotbn_put_cursor(DIC)),
      ?_assertMatch(DIC,  dotbn_get_cursor()),
      ?_assertMatch(d,    dotbn_get_numdat())
     ]}.

db_open_tp_postgres(TriplePattern) ->
    dotp_perform(dip_get_connection(), TriplePattern).

dotp_perform(fail, _) ->
    fail;
dotp_perform(C, TriplePattern) ->
    F = "SELECT count(*) FROM pg_cursors WHERE name ='~s';",
    Q = lists:flatten(io_lib:format(F, [dotpp_cons_cid()])),
    R = dipp_squery({C, Q}),
    info_msg(db_open_tp_postgres, [get(self), {tp,TriplePattern}, {query,Q}, {result,R}, {connection, C}], check_cursor, 50),
    dotpp_close(R,  C, TriplePattern).

dotpp_close(1, C, TriplePattern) ->
    Q = lists:flatten(io_lib:format("CLOSE ~s;", [dotpp_cons_cid()])),
    dotpp_switch(?STRING_ID_CODING_METHOD,
    dipp_squery({C, Q}), C, TriplePattern);
dotpp_close(0, C, TriplePattern) ->
    dotpp_switch(?STRING_ID_CODING_METHOD, ok, C, TriplePattern);
dotpp_close(fail, _, _) ->
    fail.

dotpp_switch(string_integer, ok, C, TriplePattern) ->
    Tid = dttq_conv_var(1, TriplePattern),
    Sbj = dttq_conv_var(2, TriplePattern),
    Prd = dttq_conv_var(3, TriplePattern),
    Obj = dttq_conv_var(4, TriplePattern),
    dotpp_where_si(Tid, Sbj, Prd, Obj, C, TriplePattern);
dotpp_switch(_, fail, _ , _) ->
    fail.

dotpp_where_si(false, _Sbj, _Prd, _Obj, C, TP) ->
    case element(1, TP) of
	{I, code} -> ok;
	I -> ok
    end,
    F = "WHERE tid='~w'",
    dotpp_declare(lists:flatten(io_lib:format(F, [I])), C, TP);
dotpp_where_si(true, false, true, true, C, TP) ->
    case element(2, TP) of
	{S, code} -> ok;
	S -> ok
    end,
    F = "WHERE s='~w'",
    dotpp_declare(lists:flatten(io_lib:format(F, [S])), C, TP);
dotpp_where_si(true, true, false, true, C, TP) ->
    case element(3, TP) of
	{P, code} -> ok;
	P -> ok
    end,
    F = "WHERE p='~w'",
    dotpp_declare(lists:flatten(io_lib:format(F, [P])), C, TP);
dotpp_where_si(true, true, true, false, C, TP) ->
    O = element(4, TP),
    F = "WHERE ",
    L = F ++ dotpp_where_obj(O),
    dotpp_declare(lists:flatten(L), C, TP);
dotpp_where_si(true, false, false, true, C, TP) ->
    case element(2, TP) of
	{S, code} -> ok;
	S -> ok
    end,
    case element(3, TP) of
	{P, code} -> ok;
	P -> ok
    end,
    F = "WHERE s='~w' AND p='~w'",
    dotpp_declare(lists:flatten(io_lib:format(F, [S, P])), C, TP);
dotpp_where_si(true, false, true, false, C, TP) ->
    case element(2, TP) of
	{S, code} -> ok;
	S -> ok
    end,
    O = element(4, TP),
    F = "WHERE s='~w' AND ",
    L = io_lib:format(F, [S]) ++ dotpp_where_obj(O),
    dotpp_declare(lists:flatten(L), C, TP);
dotpp_where_si(true, true, false, false, C, TP) ->
    case element(3, TP) of
	{P, code} -> ok;
	P -> ok
    end,
    O = element(4, TP),
    F = "WHERE p='~w' AND ",
    L = io_lib:format(F, [P]) ++ dotpp_where_obj(O),
    dotpp_declare(lists:flatten(L), C, TP);
dotpp_where_si(true, false, false, false, C, TP) ->
    case element(2, TP) of
	{S, code} -> ok;
	S -> ok
    end,
    case element(3, TP) of
	{P, code} -> ok;
	P -> ok
    end,
    O = element(4, TP),
    F = "WHERE s='~w' AND p='~w' AND ",
    L = io_lib:format(F, [S, P]) ++ dotpp_where_obj(O),
    dotpp_declare(lists:flatten(L), C, TP);
dotpp_where_si(_Tid, _Sbj, _Prd, _Obj, C, TP) ->
    dotpp_declare("", C, TP).

%% 
%% @doc This function constructs SQL sub-clause that can be
%% concatenated after WHERE using object literals.
%% 
dotpp_where_obj({ObjVal, code}) ->
    io_lib:format("o='~w'", [ObjVal]);
dotpp_where_obj({ObjVal, integer}) ->
    io_lib:format("o='~w'", [ObjVal]);
dotpp_where_obj({ObjVal, real}) ->
    io_lib:format("orl='~g'", [ObjVal]);
dotpp_where_obj({ObjVal, datetime}) ->
    GS = calendar:datetime_to_gregorian_seconds(ObjVal),
    io_lib:format("o='~w'", [GS]);
dotpp_where_obj({ObjVal, string}) ->
    io_lib:format("o='~s'", [ObjVal]);
dotpp_where_obj({ObjVal, undefined}) ->
    io_lib:format("o='~w'", [ObjVal]);
dotpp_where_obj(Obj) ->
    io_lib:format("o='~w'", [Obj]).

dotpp_declare(WhereClause, C, TriplePattern) ->
    NTT = dot_get_tn(),
    Tab = atom_to_list(NTT),
    Cid = dotpp_cons_cid(),

    F = "DECLARE ~s CURSOR FOR SELECT * FROM ~s ~s;",
    Q = lists:flatten(io_lib:format(F, [Cid, Tab, WhereClause])),
    A = [get(self), {where,WhereClause}, {connection,C}, {tp,TriplePattern}, {sql,Q}],
    info_msg(dotpp_declare, A, issue_declare_cursor, 50),

    put(table_name, list_to_atom(Tab)),
    dipp_squery({C, "BEGIN;"}),
    dipp_squery({C, Q}).

dotpp_cons_cid() ->
    {Qnid, _} = get(self),
    L = string:tokens(atom_to_list(Qnid), "-"),
    string:join(["cursor" | L], "_").

%% 
%% @doc This function closes cursor and commits transaction. 
%% 
%% @spec db_close_tp() -> ok | fail
%% 
db_close_tp() ->
    C = dip_get_connection(), 
    info_msg(db_close_tp, [get(self), {connection, C}], closing_tp, 50),

    %% close cursor
    Q = lists:flatten(io_lib:format("CLOSE ~s;", [dotpp_cons_cid()])),
    dipp_squery({C, Q}),

    %% commit transaction
    Q1 = "COMMIT;",
    case dipp_squery({C, Q1}) of
	ok ->
            %% connection stays open!
	    %% put(di_cursor__, {false, C}),
	    info_msg(db_close_tp, [get(self), {ok, Q1}], commit_ok, 50);
	_ ->
	    info_msg(db_close_tp, [get(self), {failed, Q1}], commit_failed, 50),
	    fail
    end.

%% 
%% @doc Retrieve next triple form DB. The string_id implementation of
%% this function uses process dictionary key "__string_id_mp" that
%% stores several re:mp() matching patterns for parsing datetime
%% representations.
%% 
%% @spec db_next() -> di_triple() | end_of_stream | fail
%% 
db_next() ->
    dn_choose(?DB_IMPLEMENTATION).

db_next(Cursor) ->
    put(di_cursor__, Cursor),
    dn_choose(?DB_IMPLEMENTATION).

dn_choose(epgsql) ->
    db_next_postgres().

db_next_postgres() ->
    dnp_perform(dip_get_connection()).

dnp_perform(fail) ->
    fail;
dnp_perform(C) ->
    Q = lists:flatten(io_lib:format("FETCH ~s;", [dotpp_cons_cid()])),
    R = dipp_squery({C, Q}),
%    info_msg(dnp_perform, [C], [{sql, Q}, {result, R}], 50),
    dnpp_format(R, ?STRING_ID_CODING_METHOD).

dnpp_format(fail, _) ->
    end_of_stream;
dnpp_format(0, _) ->
    end_of_stream;
dnpp_format({0, _, []}, _) ->
    end_of_stream;
dnpp_format({_, [{_, TidB, SbjB, PrdB, ObjB, RB, TB}]}, string_integer) ->
    Tid = binary_to_integer(TidB),
    Sbj = binary_to_integer(SbjB),
    Prd = binary_to_integer(PrdB),
    Obj = dnpp_format_obj(ObjB, RB, TB),
    {get(table_name), Tid, Sbj, Prd, Obj};
dnpp_format({_, [{_, TidB, SbjB, PrdB, ObjB, _, _}]}, _) ->
    Tid = binary_to_list(TidB),
    Sbj = binary_to_list(SbjB),
    Prd = binary_to_list(PrdB),
    Obj = binary_to_list(ObjB),
    {get(table_name), Tid, Sbj, Prd, Obj};
dnpp_format({Col, Row}, _) ->
    error_msg(db_next_postgres, [], {unextected_fetch_result, {Col, Row}}),
    fail.

%% @doc This function decodes a Postgres triple table record into
%% {@type query_node:qn_triple()}.
dnpp_format_obj(ObjB, _RealB, <<"1">>) ->
    {binary_to_integer(ObjB), code};
dnpp_format_obj(ObjB, _RealB, <<"2">>) ->
    {binary_to_integer(ObjB), integer};
dnpp_format_obj(_ObjB, RealB, <<"3">>) ->
    case (catch binary_to_float(RealB)) of
	{'EXIT', E} ->
	    A = [_ObjB, RealB, <<"3">>],
	    M = {binary_to_float_failed_recovering___, E},
	    error_msg(dnpp_format_obj, A, M),

	    L = binary_to_list(RealB),
	    RealL = re:replace(L,"[0-9]+","&.0",[{return,list}]),
	    {list_to_float(RealL), real};
	_ ->
	    {binary_to_float(RealB), real}
    end;
dnpp_format_obj(ObjB, _RealB, <<"4">>) ->
    Obj = binary_to_integer(ObjB),
    {calendar:gregorian_seconds_to_datetime(Obj), datetime};
dnpp_format_obj(ObjB, _RealB, <<"5">>) ->
    {binary_to_list(ObjB), string};
dnpp_format_obj(ObjB, _RealB, <<"0">>) ->
    {binary_to_list(ObjB), undefined}.

%% 
%% @doc This function retrieves next maximum Num triples from DB
%% table. It returns a list that contains result triples. The last
%% element of the result will be end_of_stream. It returns fail if an
%% error occured. The string_id implementation of this function uses
%% process dictionary key "__string_id_mp" that stores several re:mp()
%% matching patterns for parsing datetime representations.
%% 
%% @spec db_next_block(Num::interger()) -> [di_triple() |
%% end_of_stream] | fail
%% 
db_next_block(Num) ->
    dnb_choose(?DB_IMPLEMENTATION, Num).

dnb_choose(epgsql, Num) ->
    db_next_block_postgres(Num).

%% @doc This function is a version of postgres block return API
%% returning a list.
db_next_block_postgres(Num) ->
    %% info_msg(db_next_block_postgres, [Num], entered, 50), %
    dnbp_perform(dip_get_connection(), Num).

dnbp_perform(fail, Num) ->
    error_msg(db_next_block_postgres, [Num], no_connection),
    fail;
dnbp_perform(C, Num) ->
    F = "FETCH ~p FROM ~s;",
    Q = lists:flatten(io_lib:format(F, [Num, dotpp_cons_cid()])),
    %% info_msg(dnbp_perform, [C, Num], {sql, Q}, 50), %
    dnbp_enqueue(dipp_squery({C, Q}), Num).

dnbp_enqueue(fail, _) ->
    [end_of_stream];
dnbp_enqueue(0, _) ->
    [end_of_stream];
dnbp_enqueue({_Num, _Cols, Rows}, MaxNum) ->
    Q = dnbp_perform(Rows, [], MaxNum, 0, ?STRING_ID_CODING_METHOD),
    %% A = [{_Num, _Cols, Rows}, MaxNum],			%
    %% R = {squery_result, Q},			%
    %% info_msg(dnbp_enqueue, A, R, 50),		%
    Q;
dnbp_enqueue({_Cols, Rows}, MaxNum) ->
    dnbp_perform(Rows, [], MaxNum, 0, ?STRING_ID_CODING_METHOD);
dnbp_enqueue(R, N) ->
    error_msg(dnbp_enqueue, [R, N], unextected_fetch_result),
    fail.

dnbp_perform([], R, N, N, _) ->
    R;
dnbp_perform([], R, _, _, _) ->
    lists:append(R, [end_of_stream]);
dnbp_perform([{_, TidB, SbjB, PrdB, ObjB, RB, TB} | Rest],
	     R, MN, N, string_integer) ->
    Tid = binary_to_integer(TidB),
    Sbj = binary_to_integer(SbjB),
    Prd = binary_to_integer(PrdB),
    Obj = dnpp_format_obj(ObjB, RB, TB),
    Triple = {get(table_name), Tid, Sbj, Prd, Obj},

    dnbp_perform(Rest, [Triple | R], MN, N + 1, string_integer);
dnbp_perform([{_, TidB, SbjB, PrdB, ObjB, _, _} | Rest], R, MN, N, CM) ->
    Tid = binary_to_list(TidB),
    Sbj = binary_to_list(SbjB),
    Prd = binary_to_list(PrdB),
    Obj = binary_to_list(ObjB),
    Triple = {get(table_name), Tid, Sbj, Prd, Obj},

    dnbp_perform(Rest, [Triple | R], MN, N + 1, CM).

%% 
%% @doc This function writes a triple to a database table. The name of
%% table is fetched from name_of_triple_tables property of {@link
%% node_state}. It is originally defined as an <a
%% href='b3s.html#environment_variables'> application environment
%% variable</a>.
%% 
%% @spec db_write(Triple::di_triple()) -> ok | fail
%% 
db_write(Triple) ->
    dw_choose(?DB_IMPLEMENTATION, Triple).

dw_choose(epgsql, Triple) ->
    dw_postgres(Triple).

dw_transaction({atomic, ok}) ->
    info_msg(dw_transaction, [{atomic, ok}], ok, 80);
dw_transaction(E) ->
    R = fail,
    error_msg(dw_transaction, [E], R),
    R.

dw_postgres(Triple) ->
    dwp_perform(dip_get_connection(), Triple).

dwp_perform(fail, _) ->
    fail;
dwp_perform(C, {Tab, Id, Sbj, Prd, {ObjVal, ObjTyp}}) ->
    T = {Tab, Id, Sbj, Prd, {ObjVal, ObjTyp}},
    case ObjTyp of
	code ->
	    Val = "(~w, ~w, ~w, ~w, 0, 1)",
	    Rec = [Tab, Id, Sbj, Prd, ObjVal];
	integer ->
	    Val = "(~w, ~w, ~w, ~w, 0, 2)",
	    Rec = [Tab, Id, Sbj, Prd, ObjVal];
	real ->
	    Val = "(~w, ~w, ~w, 0,  ~g, 3)",
	    Rec = [Tab, Id, Sbj, Prd, ObjVal];
	datetime ->
	    Val = "(~w, ~w, ~w, ~w, 0, 4)",
	    OV  = calendar:datetime_to_gregorian_seconds(ObjVal),
	    Rec = [Tab, Id, Sbj, Prd, OV];
	%% string ->
	%%     Val = "(~w, ~w, ~w, '~s', 0, 5)",
	%%     Rec = [Tab, Id, Sbj, Prd, ObjVal];
	undefined ->
	    Val = "(~w, ~w, ~w, ~w, 0, 0)",
	    Rec = [Tab, Id, Sbj, Prd, ObjVal]
    end,

    Fld = "(tid, s, p, o, orl, oty)",
    Fmt = "INSERT INTO ~s " ++ Fld ++ " VALUES " ++ Val ++ ";",
    Sql = lists:flatten(io_lib:format(Fmt, Rec)),

    case (dipp_squery({C, Sql})) of
	1 -> ok;
	_ ->
	    R = [{record, Rec}, {sql_statement, Sql}],
	    error_msg(dbn_confirm, [C, T], R),
	    fail
    end;
dwp_perform(C, Triple) ->
    {Tab, Id, Sbj, Prd, Obj} = Triple,
    Fld = "(tid, s, p, o, orl, oty)",
    Val = "('~s', '~s', '~s', '~s', 0, 0)",
    Fmt = "INSERT INTO ~s " ++ Fld ++ " VALUES " ++ Val ++ ";",
    Rec = [Tab, dwp_escape(Id),
	   dwp_escape(Sbj), dwp_escape(Prd), dwp_escape(Obj)],
    Sql = lists:flatten(io_lib:format(Fmt, Rec)),

    case (dipp_squery({C, Sql})) of
	1 -> ok;
	_ ->
	    R = [{record, Rec}, {sql_statement, Sql}],
	    error_msg(dbn_confirm, [C, Triple], R),
	    fail
    end.

dwp_escape(Str) ->
    dwpe_squote(string:chr(Str, $'), Str).
    %% S01 = dwpe_squote(string:chr(Str, $'), Str).
    %% dwpe_de_dquote(string:chr(S01, $"), S01).

%% dwpe_de_dquote(0, Str) ->
%%     Str;
%% dwpe_de_dquote(1, Str) ->
%%     I = string:chr(string:sub_string(Str, 2), $"),
%%     dwpedd_perform(I, length(Str) - 1, Str);
%% dwpe_de_dquote(_, Str) ->
%%     Str.

%% dwpedd_perform(0, _, Str) ->
%%     Str;
%% dwpedd_perform(I, I, Str) ->
%%     string:sub_string(Str, 2, I);
%% dwpedd_perform(_, _, Str) ->
%%     Str.

dwpe_squote(0, Str) ->
    Str;
dwpe_squote(_N, Str) ->
    R = re:replace(Str,"'","''",[{return,list}, global]),
%    R = re:replace(Str,"'","\\\\'",[{return,list}, global]),
%    info_msg(dwpe_squote, [_N, Str], {result, R}, 50),
    R.

dwp_escape_test_() ->
    application:load(b3s),
    [
      %% ?_assertMatch("q''w''e", dwp_escape("\"q'w'e\"")),
      ?_assertMatch("\"q''w''e", dwp_escape("\"q'w'e")),
      ?_assertMatch("q''w''e", dwp_escape("q'w'e")),
      ?_assertMatch("q''we", dwp_escape("q'we")),
      ?_assertMatch("qwe", dwp_escape("qwe"))
    ].

%% 
%% @doc This Function initializes a triple table. It is only prepared
%% for non-distributed environment. It will be useful for unit test
%% initialization. The name of table is fetched from
%% name_of_triple_tables property of {@link node_state}. It is
%% originally defined as an <a href='b3s.html#environment_variables'>
%% application environment variable</a>.
%% 
%% @spec db_init() -> ok | fail
%% 
db_init() ->
    di_choose(?DB_IMPLEMENTATION).

di_choose(epgsql) ->
    db_init_postgres().

di_delete({atomic, ok}, TabNam, NodeList, RI) ->
    di_try_create(TabNam, NodeList, RI);
di_delete({aborted, {no_exists, TabNam}}, TabNam, NodeList, RI) ->
    di_try_create(TabNam, NodeList, RI);
di_delete(E, TabNam, NodeList, RI) ->
    R = fail,
    error_msg(di_delete, [E, TabNam, NodeList, RI], R),
    R.

di_try_create(TabNam, NodeList, RecInfo) ->
    Attrs = {attributes, RecInfo},
    TabDef = [Attrs, {ram_copies, NodeList}],
    %% TabDef = [Attrs, {disc_copies, NodeList}],
    %% TabDef = [Attrs, {disc_only_copies, NodeList}],
    di_create(mnesia:create_table(TabNam, TabDef), TabDef).

di_create({atomic, ok}, _) ->
    ok;
di_create(E, TabDef) ->
    R = fail,
    error_msg(di_create, [E, TabDef], R),
    R.

%% postgres implementation
db_init_postgres() ->
    info_msg(db_init_postgres, [], entered, 50),
    dip_perform(dip_get_connection()).

dip_perform(fail) ->
    fail;
dip_perform(C) ->
    F = "SELECT count(*) FROM pg_cursors WHERE name ='~s';",
    Q = lists:flatten(io_lib:format(F, [dotpp_cons_cid()])),
    info_msg(db_init_postgres, [], {query, Q}, 50),
    dipp_close(dipp_squery({C, Q}),  C).

dipp_close(1, C) ->
    Q = lists:flatten(io_lib:format("CLOSE ~s;", [dotpp_cons_cid()])),
    info_msg(db_init_postgres, [], {query, Q}, 50),
    dip_reconstruct(dipp_squery({C, Q}), C);
dipp_close(0, C) ->
    info_msg(db_init_postgres, [], already_closed, 50),
    dip_reconstruct(ok, C);
dipp_close(fail, _) ->
    fail.

dip_reconstruct(ok, C) ->
    info_msg(dip_reconstruct, [C], entererd, 50),

    NTT = dot_get_tn(),
    Tab = atom_to_list(NTT),
    %% Q01 = "CREATE TABLE " ++ Tab ++ " (" ++
    %% 	"    sid integer PRIMARY KEY DEFAULT nextval('serial')," ++
    %% 	"    tid text," ++
    %% 	"    s   text NOT NULL CHECK (s <> '')," ++
    %% 	"    p   text NOT NULL CHECK (p <> '')," ++
    %% 	"    o   text," ++
    %% 	"    orl real,"   ++ % object real
    %% 	"    oty smallint" ++ % object type; undefined(0) / code(1) / integer(2) / real(3) / datetime(4) / string(5)
    %% 	"  );",
    %% Q02 = "DROP TABLE IF EXISTS " ++ Tab ++ " CASCADE;",
    %% Q03 = "CREATE SEQUENCE serial;",
    %% Q04 = "DROP SEQUENCE IF EXISTS serial CASCADE;",
    %% Q05 = "COMMIT;",
    %% Q06 = "BEGIN;",
    %% L = [{C, Q02}, {C, Q04}, {C, Q03}, {C, Q01}, {C, Q05}, {C, Q06}],

    Q01 = "SELECT reset_tse('" ++ Tab ++ "');",
    L = [{C, Q01}],
    lists:foreach(fun dipp_squery/1, L),
    info_msg(dip_reconstruct, [C], {performed, L}, 50);
dip_reconstruct(fail, _) ->
    fail.

%% 
%% @doc This function executes a simple SQL statement on postgres.
%% 
%% @spec dipp_squery({pid(), string()}) -> ok | term() | fail
%% 
dipp_squery({C, Q}) ->
    info_msg(dipp_squery, [get(self), C, Q], run_query, 50),
    dippsq_confirm(epgsql:squery(C, Q), C, Q).

dippsq_confirm({ok, Num}, _C, _Q) ->
    %% info_msg(dipp_squery, [_C, _Q], {ok, Num}, 50),
    Num;
dippsq_confirm({ok, [], []}, _C, _Q) ->
    %% info_msg(dipp_squery, [_C, _Q], {ok, [], []}, 50),
    ok;
dippsq_confirm({ok, _Col, []}, _C, _Q) ->
    0;
dippsq_confirm({ok, Col, [{Fld1, Fld2}]}, _C, _Q) ->
    {Col, [{Fld1, Fld2}]};
dippsq_confirm({ok, _Col, [{B}]}, _C, _Q) ->
    %% info_msg(dipp_squery, [_C, _Q], {ok, _Col, [{B}]}, 50),
    list_to_integer(binary_to_list(B));
dippsq_confirm({ok, Col, RowList}, _C, _Q) ->
    {length(RowList), Col, RowList};
dippsq_confirm({ok, 1, Col, Row}, _C, _Q) ->
    %% info_msg(dipp_squery, [_C, _Q], {ok, 1, Col, Row}, 50),
    {Col, Row};
dippsq_confirm({ok, Num, Col, Row}, _C, _Q) ->
    %% info_msg(dipp_squery, [_C, _Q], {ok, 1, Col, Row}, 50),
    {Num, Col, Row};
dippsq_confirm({error, {error, error, CodeB, TextB, []}}, C, Q) ->
    Code = list_to_atom(binary_to_list(CodeB)),
    Text = binary_to_list(TextB),
    io:fwrite(" (~s) ~ts.~n", [Code, Text]),
    error_msg(dipp_squery, [C, Q], {error, Code}),
    fail;
dippsq_confirm({error, {error, error, CodeB, TextB,
			[{position, PoSB}]}}, C, Q) ->
    Code = list_to_atom(binary_to_list(CodeB)),
    Text = binary_to_list(TextB),
    Pos  = binary_to_integer(PoSB),
    io:fwrite(" (~s) ~ts, pos(~w).~n", [Code, Text, Pos]),
    error_msg(dipp_squery, [C, Q], {error, Code}),
    fail;
dippsq_confirm({error, E}, C, Q) ->
    error_msg(dipp_squery, [C, Q], {error, E}),
    fail.

%% 
%% @doc This function returns a postgres connection. It opens the
%% connection if not opened. It uses process dictionary entry
%% 'di_cursor__' for storing a structure
%% {OpenedTransaction::boolean(), Connection::pid()}. If a transaction
%% was started, OpenedTransaction is true. Otherwise, false. Database
%% handler returned from epgsql is stored in Connection.
%% 
%% @spec dip_get_connection() -> pid() | fail
%% 
dip_get_connection() ->
    dgc_pdic(get(di_cursor__)).

dgc_pdic(undefined) ->
    %% dgc_node_state(gen_server:call(node_state, {get, db_interface_cursor}));
    dgc_node_state(undefined);
dgc_pdic({true, C}) ->
    C;
dgc_pdic({false, C}) ->
    Q = "BEGIN;",
    case dipp_squery({C, Q}) of
	ok ->
            dipp_squery({C, "COMMIT;"}),
	    put(di_cursor__, {true, C}),
	    C;
	E ->
	    error_msg(dip_get_connection, [], {error, {open_transaction, E}}),
	    fail
    end.

dgc_node_state(undefined) ->
    BS  = gen_server:call(node_state, {get, b3s_state_pid}),
    LHS = gen_server:call(BS, {get, epgsql_host}),
    USN = gen_server:call(BS, {get, epgsql_user}),
    PWD = gen_server:call(BS, {get, epgsql_pass}),
    OPT = [{port, gen_server:call(BS, {get, epgsql_port})}],
    info_msg(dip_get_connection, [get(self), {epgsql_host,LHS}, {epgsql_user,USN}, {epgsql_port,OPT}], connect_to_pgsql, 50),
    dgc_connect(epgsql:connect(LHS, USN, PWD, OPT)).

%%dgc_node_state(C) ->
%%    dgc_alive(is_process_alive(C), C).
%%
%%dgc_alive(true, C) ->
%%    dgc_connect({ok, C});
%%dgc_alive(false, _) ->
%%    dgc_node_state(undefined).
%%
dgc_connect({ok, C}) ->
    put(di_cursor__, {true, C}),
    C;
%%    %% gen_server:call(node_state, {put, db_interface_cursor, C}),
%%    Q = "BEGIN;",
%%    case dipp_squery({C, Q}) of
%%	ok ->
%%	    put(di_cursor__, {true, C}),
%%	    C;
%%	E ->
%%	    error_msg(dip_get_connection, [], {error, {open_transaction, E}}),
%%	    fail
%%    end;
dgc_connect(E) ->
    error_msg(dip_get_connection, [get(self), {error,E}], epgsql_connect_error),
    fail.

%% 
%% @doc This function initializes triple table copies on listed
%% nodes. It performs rpc:call/4 internally. CAUTION: While it
%% rebuilds mnesia schema structure, it destroys all existing
%% tables. The name of table is fetched from name_of_triple_tables
%% property of {@link node_state}. It is originally defined as an <a
%% href='b3s.html#environment_variables'> application environment
%% variable</a>.
%% 
%% @spec db_init(NodeList::[node()]) -> ok | fail
%% 
db_init([]) ->
    R = fail,
    error_msg(db_init, [[]], {no_node_to_process, R}),
    R;
db_init(NodeList) ->
    di_choose(?DB_IMPLEMENTATION, NodeList).

di_choose(epgsql, NodeList) ->
    diep(lists:usort(NodeList)).

diep(NodeList) ->
    Fini = fun (X) -> rpc:call(X, db_interface, db_init, []) end,
    Rini = lists:map(Fini, NodeList),

    R = [{implementation, epgsql}, {db_init, Rini}],
    info_msg(db_init, [NodeList], R, 50).

%% 
%% @doc This function closes the triple table safely as a permanent
%% resource. Usually, it dumps the table contents into a file. The
%% name of table is fetched from name_of_triple_tables property of
%% {@link node_state}. It is originally defined as an <a
%% href='b3s.html#environment_variables'> application environment
%% variable</a>.
%% 
%% @spec db_close() -> ok | fail
%% 
db_close() ->
    dc_choose(?DB_IMPLEMENTATION).

dc_choose(epgsql) ->
    db_close_postgres().

dc_confirm({atomic, ok}, _) ->
    ok;
dc_confirm(E, TabLst) ->
    error_msg(dc_confirm, [E, TabLst], failed_mnesia_dump_tables).

%% postgres implementation
db_close_postgres() ->
    dcp_perform(dip_get_connection()).

dcp_perform(fail) ->
    fail;
dcp_perform(C) ->
    info_msg(db_close_postgres, [], {connection, C}, 50),
    %% gen_server:call(node_state, {put, db_interface_cursor, undefined}),
    Q = "COMMIT;",
    case dipp_squery({C, Q}) of
	ok ->
	    put(di_cursor__, {false, C}),
	    info_msg(db_close_postgres, [], {ok, Q}, 50);
	_ ->
	    info_msg(db_close_postgres, [], {failed, Q}, 50),
	    fail
    end.

%% 
%% @doc This function closes and disconnects a database connection to
%% the triple table. The name of table is fetched from
%% name_of_triple_tables property of {@link node_state}. It is
%% originally defined as an <a href='b3s.html#environment_variables'>
%% application environment variable</a>.
%% 
%% @spec db_disconnect() -> ok | fail
%% 
db_disconnect() ->
    dd_choose(?DB_IMPLEMENTATION).

dd_choose(epgsql) ->
    db_disconn_postgres().

%% postgres implementation
db_disconn_postgres() ->
    ddp_perform(dip_get_connection()).

ddp_perform(fail) ->
    fail;
ddp_perform(C) ->
    info_msg(db_disconn_postgres, [], {connection, C}, 50),
    erase(di_cursor__),
    %% gen_server:call(node_state, {put, db_interface_cursor, undefined}),
    Q = "COMMIT;",
    case dipp_squery({C, Q}) of
	ok ->
	    info_msg(db_disconn_postgres, [], {ok, Q}, 50),
	    epgsql:close(C),
	    info_msg(db_disconn_postgres, [], {disconnected, C}, 50);
	_ ->
	    info_msg(db_disconn_postgres, [], {failed, Q}, 50),
	    fail
    end.

%% 
%% @doc This function builds indices of triple table on the local
%% node. The name of table is fetched from name_of_triple_tables
%% property of {@link node_state}. It is originally defined as an <a
%% href='b3s.html#environment_variables'> application environment
%% variable</a>. Mnesia application must be started.
%% 
%% @spec db_add_index() -> ok | {error, Reason::term()}
%% 
db_add_index() ->
    dx_choose(?DB_IMPLEMENTATION).

dx_choose(epgsql) ->
    db_add_index_postgres().

db_add_index_postgres() ->
    daip_perform(dip_get_connection()).

daip_perform(fail) ->
    fail;
daip_perform(C) ->
    info_msg(daip_perform, [C], entererd, 50),

    NTT = dot_get_tn(),
    Tab = atom_to_list(NTT),
    %% F01 = "CREATE INDEX ~s ON ~s (~s);",
    %% F02 = "CREATE INDEX ~s ON ~s (~s, ~s);",
    %% F03 = "CREATE INDEX ~s ON ~s (~s, ~s, ~s);",
    %% Q11 = lists:flatten(io_lib:format(F01, ["ix_tid",   Tab, "tid"])),
    %% Q12 = lists:flatten(io_lib:format(F01, ["ix_s",     Tab, "s"])),
    %% Q13 = lists:flatten(io_lib:format(F01, ["ix_p",     Tab, "p"])),
    %% Q14 = lists:flatten(io_lib:format(F01, ["ix_o",     Tab, "o"])),
    %% Q15 = lists:flatten(io_lib:format(F02, ["ix_s_p",   Tab, "s", "p"])),
    %% Q16 = lists:flatten(io_lib:format(F02, ["ix_s_o",   Tab, "s", "o"])),
    %% Q17 = lists:flatten(io_lib:format(F02, ["ix_p_o",   Tab, "p", "o"])),
    %% Q18 = lists:flatten(io_lib:format(F03, ["ix_s_p_o", Tab, "s", "p", "o"])),
    %% L = [{C, Q11}, {C, Q12}, {C, Q13}, {C, Q14},
    %% 	 {C, Q15}, {C, Q16}, {C, Q17}, {C, Q18}],

    Q01 = "SELECT index_tse('" ++ Tab ++ "');",
    L = [{C, Q01}],
    lists:foreach(fun dipp_squery/1, L),
    info_msg(dip_reconstruct, [C], {performed, L}, 50).

%% 
%% @doc This function remove indices of triple table on the local
%% node. The name of table is fetched from name_of_triple_tables
%% property of {@link node_state}. It is originally defined as an <a
%% href='b3s.html#environment_variables'> application environment
%% variable</a>. Mnesia application must be started.
%% 
%% @spec db_del_index() -> ok | {error, Reason::term()}
%% 
%% @deprecated Instead of using this function, implementation depended
%% tools (i.e. psql) should be used.
%% 
db_del_index() ->
    ddx_choose(?DB_IMPLEMENTATION).

ddx_choose(epgsql) ->
    ok.

%% 
%% @doc This function saves a map structure into a table.
%% 
%% @spec db_put_map(maps:map(), atom()) -> ok | {error, Reason::term()}
%% 
db_put_map(Map, TableName) ->
    dpm_choose(?DB_IMPLEMENTATION, Map, TableName).

dpm_choose(epgsql, Map, TableName) ->
    RI = record_info(fields, key_value),
    R1 = mnesia:delete_table(TableName),
    R2 = di_delete(R1, TableName, [node()], RI),
    dpmmq_init_table(R2, maps:size(Map), Map, TableName).

dpmmq_init_table(fail, Size, Map, TableName) ->
    A = [fail, Size, Map, TableName],
    E = {error, init_table_failed},
    error_msg(dpmmq_init_table, A, E),
    E;
dpmmq_init_table(ok, Size, Map, TableName) when Size == 0 ->
    A = [ok, Size, Map, TableName],
    E = {error, map_has_no_data},
    error_msg(dpmmq_init_table, A, E),
    E;
dpmmq_init_table(ok, Size, Map, TableName) ->
    A = [ok, Size, Map, TableName],
    F = fun () -> dpmmq_write(Map, TableName) end,
    case dw_transaction(mnesia:sync_transaction(F)) of
	ok ->
	    info_msg(dpmmq_init_table, A, successfully_saved, 60),
	    TabLst = [TableName],
	    dc_confirm(mnesia:dump_tables(TabLst), TabLst);
	fail ->
	    E = {error, write_failed},
	    error_msg(dpmmq_init_table, A, E),
	    E
    end.

dpmmq_write(Map, TableName) ->
    F = fun({K, V}) ->
		KV = {TableName, K, V},
		R  = mnesia:write(TableName, KV, write),
		info_msg(dpmmq_write_nnf, [Map, TableName, KV], R, 60),
		R
	end,
    RL = lists:map(F, maps:to_list(Map)),
    case lists:usort(RL) of
	[ok] -> ok;
	_    -> RL
    end.

%% 
%% @doc This function returns a map structure saved in specified
%% table.
%% 
%% @spec db_get_map(atom()) -> maps:map() | {error, Reason::term()}
%% 
db_get_map(TableName) ->
    dgm_choose(?DB_IMPLEMENTATION, TableName).

dgm_choose(epgsql, TableName) ->
    R = mnesia:table_info(TableName, size),
    dgmmq_check_table(R, TableName).

dgmmq_check_table(0, TableName) ->
    {error, {no_data_or_table, TableName}};
dgmmq_check_table(_, TableName) ->
    F = fun () ->
		K = mnesia:first(TableName),
		dgmmq_read_record(K, TableName, #{})
	end,
    dgmmq_transaction(mnesia:sync_transaction(F)).

dgmmq_read_record('$end_of_table', _, Map) ->
    Map;
dgmmq_read_record(Key, TableName, Map) ->
    [{TableName, Key, Val}] = mnesia:read(TableName, Key),
    NewMap = maps:put(Key, Val, Map),
    NewKey = mnesia:next(TableName, Key),
    dgmmq_read_record(NewKey, TableName, NewMap).

dgmmq_transaction({atomic, Map}) ->
    A = [{atomic, Map}],
    info_msg(dgmmq_transaction, A, successfully_retrieved, 60),
    Map;
dgmmq_transaction({aborted, Reason}) ->
    A = [{aborted, Reason}],
    E = {error, Reason},
    error_msg(dgmmq_transaction, A, E),
    E.

%% ===================================================================
%% 
%% @doc Unit tests.
%% 
dbi_test_() ->
    dt_implementation(?DB_IMPLEMENTATION).

dt_implementation(epgsql) ->
    application:load(b3s),
    dt_epg(b3s_state:get(test_mode)).

dt_epg(local1) ->
    {inorder,
     [
      ?_assertMatch(ok, b3s:start()),
      ?_assertMatch(ok, b3s:bootstrap()),
      {generator, fun()-> dt_epg_test01() end},
      {generator, fun()-> dt_epg_test02() end},
      ?_assertMatch(ok, b3s:stop())
     ]};
dt_epg(_) ->
    [].

dt_epg_test02() ->
    case ?STRING_ID_CODING_METHOD of
	string_integer -> dt_epg_test02_string_integer();
	_              -> dt_epg_test02_string()
    end.

dt_epg_test02_string_integer() ->
    BS = gen_server:call(node_state, {get, b3s_state_pid}),
    Tab = dot_get_tn(),
    ET  = fun (X) -> string_id:encode_triple(X) end,
    ETT = fun ({T, I, S, P, O}) ->
		  X = string_id:encode_triple({I, S, P, O}),
		  list_to_tuple([T | tuple_to_list(X)])
	  end,
    ETP = fun (X) -> string_id:encode_triple_pattern(X) end,

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
    T01A = ET({I01, S01, P01, O01}),
    T01 = ETT({Tab, I01, S01, P01, O01}),

    I02 = "<triple_id_0002>",
    S02 = "<Japanese>",
    O02 = "<fishes>",
    T02A = ET({I02, S02, P01, O02}),
    T02 = ETT({Tab, I02, S02, P01, O02}),

    I03 = "<triple_id_0003>",
    S03 = "<Slovenian>",
    O03 = "<potatoes>",
    T03A = ET({I03, S03, P01, O03}),
    T03 = ETT({Tab, I03, S03, P01, O03}),

    I04 = "<triple_id_0004>",
    S04 = "<Japan>",
    P04 = "<hasGDP>",
    O04 = "5869000000000",
    T04A = ET({I04, S04, P04, O04}),
    T04 = ETT({Tab, I04, S04, P04, O04}),

    I05 = "<triple_id_0005>",
    P05 = "<hasArea>",
    O05 = "3.77944E11",
    T05A = ET({I05, S04, P05, O05}),
    T05 = ETT({Tab, I05, S04, P05, O05}),

    I06 = "<triple_id_0006>",
    P06 = "<wasCreatedOnDate>",
    O06 = "660-02-11",
    T06A = ET({I06, S04, P06, O06}),
    T06 = ETT({Tab, I06, S04, P06, O06}),

    TP01 = ETP({I01,  "?s", "?p", "?o"}),
    TP02 = ETP({I02,  "?s", "?p", "?o"}),
    TP03 = ETP({I03,  "?s", "?p", "?o"}),
    TP04 = ETP({I04,  "?s", "?p", "?o"}),
    TP05 = ETP({I05,  "?s", "?p", "?o"}),
    TP06 = ETP({I06,  "?s", "?p", "?o"}),

    TP11 = ETP({"?id", "<Chinese>", "<eat>", "<vegetables>"}),
    TP12 = ETP({"?id", "?sbj",      "<eat>", "<fishes>"}),
    TP13 = ETP({"?id", "<Chinese>", "?prd",  "<vegetables>"}),
    TP14 = ETP({"?id", "<Chinese>", "<eat>", "?obj"}),
    TP15 = ETP({"?id", "<Chinese>", "?prd",  "?obj"}),
    TP16 = ETP({"?id", "?sbj",      "<eat>", "?obj"}),
    TP17 = ETP({"?id", "?sbj",      "?prd",  "<potatoes>"}),
    TP18 = ETP({"?id", "?sbj",      "?prd",  "?obj"}),
    TP19 = ETP({"_:",  "_:sbj",     "_:prd", "_:obj"}),
    TP20 = ETP({"?id", "<Chinese>", "<eat>", "<fishes>"}),
    TP21 = ETP({"?id", "<Chinese>", "?prd",  "<fished>"}),

    EOS = end_of_stream,
    R00 = [EOS],
    R01 = [T03, T02, T01, EOS],
    R02 = [T02, T01],
    R03 = [T03],
    R04 = [T02],
    R05 = [T01],
    R06 = [T03, T02, T01],
    R07 = [T03, EOS],
    QnId = '1-1-1',

    put(self, {QnId, node()}),
    erase(di_cursor__),
    {inorder,
     [
      ?_assertMatch(ok,  gen_server:call(db_writer, db_init)),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T01A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T02A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T03A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T04A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T05A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T06A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T02A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T03A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T02A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T03A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T02A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T03A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T02A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T03A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T02A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T03A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T02A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T03A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T02A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T03A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, db_close)),

      ?_assertMatch(ok,  db_open_tp(TP01)),
      ?_assertMatch(T01, db_next()),
      ?_assertMatch(EOS, db_next()),
      ?_assertMatch(ok,  db_init()),
      ?_assertMatch(ok,  db_open_tp(TP01)),
      ?_assertMatch(EOS, db_next()),

      ?_assertMatch(ok,  db_init()),
      ?_assertMatch(ok,  db_add_index()),
      ?_assertMatch(ok,  db_write(T01)),
      ?_assertMatch(ok,  db_write(T02)),
      ?_assertMatch(ok,  db_write(T03)),
      ?_assertMatch(ok,  db_write(T04)),
      ?_assertMatch(ok,  db_write(T05)),
      ?_assertMatch(ok,  db_write(T06)),
      ?_assertMatch(ok,  db_close()),

      ?_assertMatch(ok,  db_open_tp(TP01)),
      ?_assertMatch(T01, db_next()),
      ?_assertMatch(EOS, db_next()),
      ?_assertMatch(EOS, db_next()),
      ?_assertMatch(ok,  db_open_tp(TP02)),
      ?_assertMatch(T02, db_next()),
      ?_assertMatch(EOS, db_next()),
      ?_assertMatch(ok,  db_open_tp(TP03)),
      ?_assertMatch(T03, db_next()),
      ?_assertMatch(EOS, db_next()),
      ?_assertMatch(ok,  db_open_tp(TP04)),
      ?_assertMatch(T04, db_next()),
      ?_assertMatch(EOS, db_next()),
      ?_assertMatch(ok,  db_open_tp(TP05)),
      ?_assertMatch(T05, db_next()),
      ?_assertMatch(EOS, db_next()),
      ?_assertMatch(ok,  db_open_tp(TP06)),
      ?_assertMatch(T06, db_next()),
      ?_assertMatch(EOS, db_next()),
      ?_assertMatch(ok,  db_open_tp(TP12)),
      ?_assertMatch(T02, db_next()),
      ?_assertMatch(EOS, db_next()),
      ?_assertMatch(ok,  db_open_tp(TP13)),
      ?_assertMatch(T01, db_next()),
      ?_assertMatch(EOS, db_next()),
      ?_assertMatch(ok,  db_open_tp(TP14)),
      ?_assertMatch(T01, db_next()),
      ?_assertMatch(EOS, db_next()),
      ?_assertMatch(ok,  db_open_tp(TP15)),
      ?_assertMatch(T01, db_next()),
      ?_assertMatch(EOS, db_next()),
      ?_assertMatch(ok,  db_open_tp(TP16)),
      ?_assertMatch(T01, db_next()),
      ?_assertMatch(T02, db_next()),
      ?_assertMatch(T03, db_next()),
      ?_assertMatch(EOS, db_next()),
      ?_assertMatch(ok,  db_open_tp(TP17)),
      ?_assertMatch(T03, db_next()),
      ?_assertMatch(EOS, db_next()),
      ?_assertMatch(ok,  db_open_tp(TP18)),
      ?_assertMatch(T01, db_next()),
      ?_assertMatch(T02, db_next()),
      ?_assertMatch(T03, db_next()),
      ?_assertMatch(T04, db_next()),
      ?_assertMatch(T05, db_next()),
      ?_assertMatch(T06, db_next()),
      ?_assertMatch(EOS, db_next()),
      ?_assertMatch(ok,  db_open_tp(TP19)),
      ?_assertMatch(T01, db_next()),
      ?_assertMatch(T02, db_next()),
      ?_assertMatch(T03, db_next()),
      ?_assertMatch(T04, db_next()),
      ?_assertMatch(T05, db_next()),
      ?_assertMatch(T06, db_next()),
      ?_assertMatch(EOS, db_next()),
      ?_assertMatch(ok,  db_open_tp(TP21)),
      ?_assertMatch(EOS, db_next()),
      ?_assertMatch(ok,  db_open_tp(TP11)),
      ?_assertMatch(T01, db_next()),
      ?_assertMatch(EOS, db_next()),
      ?_assertMatch(ok,  db_open_tp(TP20)),
      ?_assertMatch(EOS, db_next()),

      ?_assertMatch(ok,  db_open_tp(TP16)),
      ?_assertMatch(R01, db_next_block(5)),
      ?_assertMatch(R00, db_next_block(5)),
      ?_assertMatch(ok,  db_open_tp(TP16)),
      ?_assertMatch(R06, db_next_block(3)),
      ?_assertMatch(R00, db_next_block(3)),
      ?_assertMatch(ok,  db_open_tp(TP16)),
      ?_assertMatch(R02, db_next_block(2)),
      ?_assertMatch(R07, db_next_block(2)),
      ?_assertMatch(R00, db_next_block(2)),
      ?_assertMatch(ok,  db_open_tp(TP16)),
      ?_assertMatch(R05, db_next_block(1)),
      ?_assertMatch(R04, db_next_block(1)),
      ?_assertMatch(R03, db_next_block(1)),
      ?_assertMatch(R00, db_next_block(1)),

      ?_assertMatch(ok,  timer:sleep(750)),
      ?_assertMatch(ok,  db_close())
     ]}.

dt_epg_test02_string() ->
    Tab = dot_get_tn(),

    I01 = "<triple_id_0001>",
    S01 = "<Chinese>",
    P01 = "<eat>",
    O01 = "<vegetables>",
    T01A = {I01, S01, P01, O01},
    T01 = {Tab, I01, S01, P01, O01},

    I02 = "<triple_id_0002>",
    S02 = "<Japanese>",
    O02 = "<fishes>",
    T02A = {I02, S02, P01, O02},
    T02 = {Tab, I02, S02, P01, O02},

    I03 = "<triple_id_0003>",
    S03 = "<Slovenian>",
    O03 = "<potatoes>",
    T03A = {I03, S03, P01, O03},
    T03 = {Tab, I03, S03, P01, O03},

    TP01 = {I01,  "?s", "?p", "?o"},
    TP02 = {I02,  "?s", "?p", "?o"},
    TP03 = {I03,  "?s", "?p", "?o"},

    TP11 = {"?id", "<Chinese>", "<eat>", "<vegetables>"},
    TP12 = {"?id", "?sbj",      "<eat>", "<fishes>"},
    TP13 = {"?id", "<Chinese>", "?prd",  "<vegetables>"},
    TP14 = {"?id", "<Chinese>", "<eat>", "?obj"},
    TP15 = {"?id", "<Chinese>", "?prd",  "?obj"},
    TP16 = {"?id", "?sbj",      "<eat>", "?obj"},
    TP17 = {"?id", "?sbj",      "?prd",  "<potatoes>"},
    TP18 = {"?id", "?sbj",      "?prd",  "?obj"},
    TP19 = {"_:",  "_:sbj",     "_:prd", "_:obj"},
    TP20 = {"?id", "<Chinese>", "<eat>", "<fishes>"},
    TP21 = {"?id", "<Chinese>", "?prd",  "<fished>"},

    EOS = end_of_stream,
    R00 = [EOS],
    R01 = [T03, T02, T01, EOS],
    R02 = [T02, T01],
    R03 = [T03],
    R04 = [T02],
    R05 = [T01],
    R06 = [T03, T02, T01],
    R07 = [T03, EOS],
    %% R01 = [{1, T01}, {2, T02}, {3, T03}, {max_key, 3}],
    %% R02 = [{1, T01}, {2, T02}, {max_key, 2}],
    %% R03 = [{1, T03}, {max_key, 1}],
    %% R04 = [{1, T02}, {max_key, 1}],
    %% R05 = [{1, T01}, {max_key, 1}],
    QnId = '1-1-1',

    put(self, {QnId, node()}),
    {inorder,
     [
      ?_assertMatch(ok,  gen_server:call(db_writer, db_init)),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T01A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T02A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T03A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T02A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T03A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T02A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T03A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T02A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T03A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T02A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T03A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T02A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T03A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T02A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T03A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T02A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, {db_write, T03A})),
      ?_assertMatch(ok,  gen_server:call(db_writer, db_close)),

      ?_assertMatch(ok,  db_open_tp(TP01)),
      ?_assertMatch(T01, db_next()),
      ?_assertMatch(EOS, db_next()),
      ?_assertMatch(ok,  db_init()),
      ?_assertMatch(ok,  db_open_tp(TP01)),
      ?_assertMatch(EOS, db_next()),

      ?_assertMatch(ok,  db_init()),
      ?_assertMatch(ok,  db_add_index()),
      ?_assertMatch(ok,  db_write(T01)),
      ?_assertMatch(ok,  db_write(T02)),
      ?_assertMatch(ok,  db_write(T03)),
      ?_assertMatch(ok,  db_close()),

      ?_assertMatch(ok,  db_open_tp(TP01)),
      ?_assertMatch(T01, db_next()),
      ?_assertMatch(EOS, db_next()),
      ?_assertMatch(EOS, db_next()),
      ?_assertMatch(ok,  db_open_tp(TP02)),
      ?_assertMatch(T02, db_next()),
      ?_assertMatch(EOS, db_next()),
      ?_assertMatch(ok,  db_open_tp(TP03)),
      ?_assertMatch(T03, db_next()),
      ?_assertMatch(EOS, db_next()),
      ?_assertMatch(ok,  db_open_tp(TP12)),
      ?_assertMatch(T02, db_next()),
      ?_assertMatch(EOS, db_next()),
      ?_assertMatch(ok,  db_open_tp(TP13)),
      ?_assertMatch(T01, db_next()),
      ?_assertMatch(EOS, db_next()),
      ?_assertMatch(ok,  db_open_tp(TP14)),
      ?_assertMatch(T01, db_next()),
      ?_assertMatch(EOS, db_next()),
      ?_assertMatch(ok,  db_open_tp(TP15)),
      ?_assertMatch(T01, db_next()),
      ?_assertMatch(EOS, db_next()),
      ?_assertMatch(ok,  db_open_tp(TP16)),
      ?_assertMatch(T02, db_next()),
      ?_assertMatch(T03, db_next()),
      ?_assertMatch(T01, db_next()),
      ?_assertMatch(EOS, db_next()),
      ?_assertMatch(ok,  db_open_tp(TP17)),
      ?_assertMatch(T03, db_next()),
      ?_assertMatch(EOS, db_next()),
      ?_assertMatch(ok,  db_open_tp(TP18)),
      ?_assertMatch(T01, db_next()),
      ?_assertMatch(T02, db_next()),
      ?_assertMatch(T03, db_next()),
      ?_assertMatch(EOS, db_next()),
      ?_assertMatch(ok,  db_open_tp(TP19)),
      ?_assertMatch(T01, db_next()),
      ?_assertMatch(T02, db_next()),
      ?_assertMatch(T03, db_next()),
      ?_assertMatch(EOS, db_next()),
      ?_assertMatch(ok,  db_open_tp(TP21)),
      ?_assertMatch(EOS, db_next()),
      ?_assertMatch(ok,  db_open_tp(TP11)),
      ?_assertMatch(T01, db_next()),
      ?_assertMatch(EOS, db_next()),
      ?_assertMatch(ok,  db_open_tp(TP20)),
      ?_assertMatch(EOS, db_next()),

      ?_assertMatch(ok,  db_open_tp(TP18)),
      ?_assertMatch(R01, db_next_block(5)),
      ?_assertMatch(R00, db_next_block(5)),
      ?_assertMatch(ok,  db_open_tp(TP18)),
      ?_assertMatch(R06, db_next_block(3)),
      ?_assertMatch(R00, db_next_block(3)),
      ?_assertMatch(ok,  db_open_tp(TP18)),
      ?_assertMatch(R02, db_next_block(2)),
      ?_assertMatch(R07, db_next_block(2)),
      ?_assertMatch(R00, db_next_block(2)),
      ?_assertMatch(ok,  db_open_tp(TP18)),
      ?_assertMatch(R05, db_next_block(1)),
      ?_assertMatch(R04, db_next_block(1)),
      ?_assertMatch(R03, db_next_block(1)),
      ?_assertMatch(R00, db_next_block(1)),
      %% ?_assertMatch(R01, ets:tab2list(db_next_block(5))),
      %% ?_assertMatch(EOS, db_next_block(5)),
      %% ?_assertMatch(ok,  db_open_tp(TP18)),
      %% ?_assertMatch(R01, ets:tab2list(db_next_block(3))),
      %% ?_assertMatch(EOS, db_next_block(3)),
      %% ?_assertMatch(ok,  db_open_tp(TP18)),
      %% ?_assertMatch(R02, ets:tab2list(db_next_block(2))),
      %% ?_assertMatch(R03, ets:tab2list(db_next_block(2))),
      %% ?_assertMatch(EOS, db_next_block(2)),
      %% ?_assertMatch(ok,  db_open_tp(TP18)),
      %% ?_assertMatch(R05, ets:tab2list(db_next_block(1))),
      %% ?_assertMatch(R04, ets:tab2list(db_next_block(1))),
      %% ?_assertMatch(R03, ets:tab2list(db_next_block(1))),
      %% ?_assertMatch(EOS, db_next_block(1)),

      %% ?_assertMatch(ok,  db_open_tp(TP18)),
      %% {generator, fun()-> dt_epg_test02a(R01, db_next_block(3)) end},
      %% ?_assertMatch(EOS, db_next_block(3)),

      ?_assertMatch(ok,  timer:sleep(750)),
      ?_assertMatch(ok,  db_close())
     ]}.

%% dt_epg_test02a(R01, Tab) ->
%%     [{1, T01}, {2, T02}, {3, T03}, {MK, 3}] = R01,
%%     EOT = '$end_of_table',

%%     {inorder,
%%      [
%%       ?_assertMatch(1,   ets:first(Tab)),
%%       ?_assertMatch(T01, ets:lookup_element(Tab, 1, 2)),
%%       ?_assertMatch(2,   ets:next(Tab, 1)),
%%       ?_assertMatch(T02, ets:lookup_element(Tab, 2, 2)),
%%       ?_assertMatch(3,   ets:next(Tab, 2)),
%%       ?_assertMatch(T03, ets:lookup_element(Tab, 3, 2)),
%%       ?_assertMatch(MK,  ets:next(Tab, 3)),
%%       ?_assertMatch(3,   ets:lookup_element(Tab, MK, 2)),
%%       ?_assertMatch(EOT, ets:next(Tab, MK))
%%      ]}.

dt_epg_test01() ->
    BS  = gen_server:call(node_state, {get, b3s_state_pid}),
    LHS = gen_server:call(BS, {get, epgsql_host}),
    USN = gen_server:call(BS, {get, epgsql_user}),
    PWD = gen_server:call(BS, {get, epgsql_pass}),
    OPT = [{port, b3s_state:get(epgsql_port)}],
    {ok, C} = epgsql:connect(LHS, USN, PWD, OPT),
    C01 = "cursor_1_1_1",
    C02 = "cursor_test02",
    C03 = "cursor_test03",

    I01 = "<triple_id_0001>",
    S01 = "<Chinese>",
    P01 = "<eat>",
    O01 = "<vegetables>",

    I02 = "<triple_id_0002>",
    S02 = "<Japanese>",
    O02 = "<fishes>",

    I03 = "<triple_id_0003>",
    S03 = "<Slovenian>",
    O03 = "<potatoes>",

    NTT = dot_get_tn(),
    Tab = atom_to_list(NTT),
    Q01 = "CREATE TABLE " ++ Tab ++ " (" ++
	"    sid integer PRIMARY KEY DEFAULT nextval('serial')," ++
	"    tid text," ++
	"    s   text NOT NULL CHECK (s <> '')," ++
	"    p   text NOT NULL CHECK (p <> '')," ++
	"    o   text NOT NULL CHECK (o <> ''));",
    Q02 = "DROP TABLE IF EXISTS " ++ Tab ++ ";",
    Q03 = "CREATE SEQUENCE serial;",
    Q04 = "DROP SEQUENCE IF EXISTS serial;",
    F01 = "INSERT INTO ~s (tid, s, p, o) VALUES ('~s', '~s', '~s', '~s');",
    Q05 = lists:flatten(io_lib:format(F01, [Tab, I01, S01, P01, O01])),
    Q06 = lists:flatten(io_lib:format(F01, [Tab, I02, S02, P01, O02])),
    Q07 = lists:flatten(io_lib:format(F01, [Tab, I03, S03, P01, O03])),
    Q08 = "SELECT count(*) FROM " ++ Tab ++ ";",
    Q09 = "DECLARE " ++ C01 ++" CURSOR FOR SELECT * FROM " ++ Tab ++ ";",
    Q10 = "BEGIN;",
    Q11 = "COMMIT;",
    Q12 = "FETCH " ++ C01 ++ ";",
    F02 = "CREATE INDEX ~s ON ~s (~s);",
    F03 = "CREATE INDEX ~s ON ~s (~s, ~s);",
    F04 = "CREATE INDEX ~s ON ~s (~s, ~s, ~s);",
    Q13 = lists:flatten(io_lib:format(F02, ["ix_tid",   Tab, "tid"])),
    Q14 = lists:flatten(io_lib:format(F02, ["ix_s",     Tab, "s"])),
    Q15 = lists:flatten(io_lib:format(F02, ["ix_p",     Tab, "p"])),
    Q16 = lists:flatten(io_lib:format(F02, ["ix_o",     Tab, "o"])),
    Q17 = lists:flatten(io_lib:format(F03, ["ix_s_p",   Tab, "s", "p"])),
    Q18 = lists:flatten(io_lib:format(F03, ["ix_s_o",   Tab, "s", "o"])),
    Q19 = lists:flatten(io_lib:format(F03, ["ix_p_o",   Tab, "p", "o"])),
    Q20 = lists:flatten(io_lib:format(F04, ["ix_s_p_o", Tab, "s", "p", "o"])),
    Q21 = "DECLARE " ++ C01 ++" CURSOR FOR SELECT * FROM " ++ Tab
	++ " WHERE tid = '" ++ I01 ++ "';",
    Q22 = "CLOSE " ++ C01 ++ ";",
    Q23 = "DECLARE " ++ C02 ++" CURSOR FOR SELECT * FROM " ++ Tab
	++ " WHERE tid = '" ++ I02 ++ "';",
    Q24 = "FETCH " ++ C02 ++ ";",
    Q25 = "CLOSE " ++ C02 ++ ";",
    Q26 = "DECLARE " ++ C03 ++" CURSOR FOR SELECT * FROM " ++ Tab
	++ " WHERE tid = '" ++ I03 ++ "';",
    Q27 = "FETCH " ++ C03 ++ ";",
    Q28 = "CLOSE " ++ C03 ++ ";",

    R01 = {ok, [], []},
    R02 = {ok, 1},
    %% R03 = {ok, [{column, <<"count">>, int8, 8, -1, 0}], [{<<"3">>}]},
    R03 = {ok, [{column, <<"count">>, int8, 20, 8, -1, 0}], [{<<"3">>}]},
    CL1 = {column, <<"sid">>, int4, 23,  4, -1, 0},
    CL2 = {column, <<"tid">>, text, 25, -1, -1, 0},
    CL3 = {column, <<"s">>,   text, 25, -1, -1, 0},
    CL4 = {column, <<"p">>,   text, 25, -1, -1, 0},
    CL5 = {column, <<"o">>,   text, 25, -1, -1, 0},
    Col = [CL1, CL2, CL3, CL4, CL5],
    L2B = fun (X) -> list_to_binary(X) end,
    %% R04 = {ok, 1, Col, [{<<"1">>, L2B(I01), L2B(S01), L2B(P01), L2B(O01)}]},
    %% R05 = {ok, 1, Col, [{<<"2">>, L2B(I02), L2B(S02), L2B(P01), L2B(O02)}]},
    %% R06 = {ok, 1, Col, [{<<"3">>, L2B(I03), L2B(S03), L2B(P01), L2B(O03)}]},
    R04 = {ok, 1, Col, [{<<"1">>, L2B(I01), L2B(S01), L2B(P01), L2B(O01)}]},
    R05 = {ok, 1, Col, [{<<"2">>, L2B(I02), L2B(S02), L2B(P01), L2B(O02)}]},
    R06 = {ok, 1, Col, [{<<"3">>, L2B(I03), L2B(S03), L2B(P01), L2B(O03)}]},
    %% R07 = {ok, 0},
    R07 = {ok, 0, Col, []},

    ESQ = fun(X) ->
		  R = epgsql:squery(C, X),
		  io:format("SQL: ~p~n", [X]),
		  io:format("R: ~p~n", [R]),
		  R
	  end,

    {inorder,
     [
      ?_assertMatch(R01, ESQ(Q02)),
      ?_assertMatch(R01, ESQ(Q04)),
      ?_assertMatch(R01, ESQ(Q03)),
      ?_assertMatch(R01, ESQ(Q01)),
      ?_assertMatch(R02, ESQ(Q05)),
      ?_assertMatch(R02, ESQ(Q06)),
      ?_assertMatch(R02, ESQ(Q07)),
      ?_assertMatch(R03, ESQ(Q08)),
      ?_assertMatch(R01, ESQ(Q10)),
      ?_assertMatch(R01, ESQ(Q09)),
      ?_assertMatch(R04, ESQ(Q12)),
      ?_assertMatch(R05, ESQ(Q12)),
      ?_assertMatch(R06, ESQ(Q12)),
      ?_assertMatch(R07, ESQ(Q12)),
      ?_assertMatch(R01, ESQ(Q22)),
      %% ?_assertMatch(R01, ESQ(Q11)),
      ?_assertMatch(R01, ESQ(Q13)),
      ?_assertMatch(R01, ESQ(Q14)),
      ?_assertMatch(R01, ESQ(Q15)),
      ?_assertMatch(R01, ESQ(Q16)),
      ?_assertMatch(R01, ESQ(Q17)),
      ?_assertMatch(R01, ESQ(Q18)),
      ?_assertMatch(R01, ESQ(Q19)),
      ?_assertMatch(R01, ESQ(Q20)),
      %% ?_assertMatch(R01, ESQ(Q10)),
      ?_assertMatch(R01, ESQ(Q21)),
      ?_assertMatch(R01, ESQ(Q23)),
      ?_assertMatch(R01, ESQ(Q26)),
      ?_assertMatch(R04, ESQ(Q12)),
      ?_assertMatch(R05, ESQ(Q24)),
      ?_assertMatch(R06, ESQ(Q27)),
      ?_assertMatch(R07, ESQ(Q12)),
      ?_assertMatch(R07, ESQ(Q24)),
      ?_assertMatch(R01, ESQ(Q22)),
      ?_assertMatch(R01, ESQ(Q25)),
      ?_assertMatch(R01, ESQ(Q28)),
      ?_assertMatch(R01, ESQ(Q11)),
      ?_assertMatch(ok,  epgsql:close(C))
     ]}.

%% ====> END OF LINE <====

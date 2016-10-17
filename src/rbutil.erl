%% -*- Erlang -*-
%%
%% rbutil module
%%
%% @copyright 2013-2016 UP FAMNIT and Yahoo! Japan Corporation
%% @version 0.3
%% @since August, 2013
%% @author Kiyoshi Nitta <knitta@yahoo-corp.jp>
%% 
%% @doc Utility functions for rb and test related tools.
%% 

-module(rbutil).
-export([

	 rb_grep/1, rb_list/0, rb_list/1, rb_show/1,
	 rb_filter_by_date/1,

	 test_halt/0, test_halt/1
	]).

%% ======================================================================
%% 
%% utility
%% 

%% 
%% @doc test and halt. (to be called from command line)
%% 
test_halt() ->
    M = [ glds, relation_element, asn, snapshot, ontsem ],
    O = [ verbose ],
    eunit:test(M, O),
    halt().

test_halt([A]) ->
    {ok, T, _} = erl_scan:string(A),
    {ok, M} = erl_parse:parse_term(T),
    O = [ verbose ],
    eunit:test(M, O),
    halt().

%% 
%% @doc Grep <A
%% href="http://www.erlang.org/doc/man/rb.html">report
%% browser</A>'s log entries. For command line calls.
%% 
rb_grep(S) ->
    rb:start(),
    rb:grep(S),
    halt().

%% 
%% @doc Filter <A
%% href="http://www.erlang.org/doc/man/rb.html">report
%% browser</A>'s log entries generated within specified time. For
%% command line calls.
%% 
rb_filter_by_date([Num, Unit]) ->
    C = calendar:local_time(),
    CS = calendar:datetime_to_gregorian_seconds(C),
    N = Num,
    case Unit of
	"d" ->
	    DS = CS - list_to_integer(N) * 60 * 60 * 24;
	"h" ->
	    DS = CS - list_to_integer(N) * 60 * 60;
	"m" ->
	    DS = CS - list_to_integer(N) * 60;
	"s" ->
	    DS = CS - list_to_integer(N);
	_ ->
	    DS = CS - list_to_integer(N)
    end,

    rb:start(),
    D = calendar:gregorian_seconds_to_datetime(DS),
    rb:filter([], {D, from}),
    halt().

%% 
%% @doc List <A
%% href="http://www.erlang.org/doc/man/rb.html">report
%% browser</A>'s all log entries. For command line calls.
%% 
rb_list() ->
    rb:start(),
    rb:list(),
    halt().

%% 
%% @doc List <A
%% href="http://www.erlang.org/doc/man/rb.html">report
%% browser</A>'s all log entries. For command line calls.
%% 
rb_list([A]) ->
    rb:start(),
    rb:list(list_to_atom(A)),
    halt().

%% 
%% @doc Show <A
%% href="http://www.erlang.org/doc/man/rb.html">report
%% browser</A>'s log entries. For command line calls.
%% 
rb_show([A]) ->
    rb:start(),
    case A of
	"crash_report" ->
	    rb:show(list_to_atom(A));
	"supervisor_report" ->
	    rb:show(list_to_atom(A));
	"error" ->
	    rb:show(list_to_atom(A));
	"progress" ->
	    rb:show(list_to_atom(A));
	"info_report" ->
	    rb:show(list_to_atom(A));
	"info_msg" ->
	    rb:show(list_to_atom(A));
	_ ->
	    rb:show()
    end,
    halt().

%% ====> END OF LINE <====

%%
%% Query Node
%%
%% @copyright 2015-2019 UP FAMNIT and Yahoo Japan Corporation
%% @version 0.3
%% @since September, 2015
%% @author Iztok Savnik <iztok.savnik@famnit.upr.si>
%% 
%% @see b3s
%% @see tp_query_node
%% @see join_query_node
%% @see query_tree
%% 
%% @doc This module provides functions commonly shared by query node
%% modules.
%% 
%% == streams ==
%% 
%% (LINK: {@section streams})
%%
%% The main function of query_node module is to provide interface to streams of 
%% graphs implemented among the pairs of query nodes. Each pair of query nodes 
%% is connected by N empty messages where N is defined by environment variable 
%% *num_of_empty_msgs*. Empty messages functions as wagons of circular 
%% "train" forming the stream between the query nodes. Child query node is allowed 
%% to send a block to its parent only if it holds an empty message. 
%%
%% The streams are implemented by means of process queues holding messages from 
%% other processes. The query node queues have two modes of work. Firstly, they 
%% serve as standard queues where one can add the message to the beginning and 
%% take it from the end of queue. 
%% Secondly, data messages include blocks composed of lists of graphs. To be 
%% able to process graphs in iterative manner, either when adding graphs to queue,
%% or, when retrieving graphs from queue, we need graph-based interface. 
%%
%% Graphs added to queue are first gathered in buffers which are moved into the queue
%% when full. From the opposite side, when reading from queue, data messages are 
%% first unpacked and the block 
%% is stored into the buffer. Graphs can then be iterativelly pulled from buffer---if 
%% buffer is empty then another message is retrieved from queue.
%% 
%% == operations select and project ==
%% 
%% (LINK: {@section operations select and project})
%%
%% Each query node that is a part of some graph-pattern includes in addition to its 
%% primary operation as, for example, join or triple-pattern access methods, 
%% also the fuctionality of select and project operations. 
%%
%% Module query_node provides the functions for processing operations project and select. 
%% The function {@link project_prepare/1} prepares the environment 
%% for processing projections using function {@link eval_project/1}. The function 
%% {@link eval_select/1} evaluates selection on current graph.
%%
%% @type qn_opsym() = tp | join | leftjoin | union | differ. 
%% Atom describing query operation can be one of presented. 
%%
%% @type qn_id() = string() | integer().
%% @type qn_subject() = string() | integer().
%% @type qn_predicate() = string() | integer().
%% @type qn_object_value() = integer() | real() | calendar:datetime() | string().
%% @type qn_object_type() = undefined | code | integer | real | datetime | string.
%% @type qn_object() = string() | {qn_object_value(), qn_object_type()}.
%% @type qn_var() = string().
%% 
%% @type qn_term() = qn_var() | string().
%% Query term used for expressing selection conditions.
%%
%% @type qn_triple() = {qn_id(), qn_subject(), qn_predicate(), qn_object()}.
%% Every triple has id. Graphs can be represented as sets of triples. Relationship
%% between triples and graphs is therefore expressed by means of reification, i.e., sets
%% of triples relating triple ids with graphs. This model allows for expressing 
%% semantically more complex relationships, as, for instance, in CycL.
%%
%% @type qn_triple_pattern() = {qn_id() | qn_var(), qn_subject() | qn_var(), qn_predicate() | qn_var(), qn_object() | qn_var()}.
%% Triple patterns are triples (with id) that include variables. Type of triple 
%% pattern follows format of qn_triple(). 
%%
%% @type qn_graph() = maps:map(). Mapping form {@link qn_id()}
%% to {@link qn_triple()}.
%% 
%% @type qn_graph_pattern() = maps:map(). Mapping form {@link
%% qn_id()} to {@link qn_triple_pattern()}.
%% 
%% @type qn_attribute() = qn_subject() | qn_predicate() | qn_object().
%% Query node attribute is triple component, S, P or O.
%% 
%% @type qn_var_val_map() = [{qn_var(), string()}]. Mapping between 
%% variables and their values. 
%%
%% @type qn_project_list() = [qn_var()].
%% Project list is a list of variables to be eliminated from graph pattern. 
%%
%% @type qn_binary_operation() = equal | less | lesseq | greater | greatereq | land | lor 
%% 
%% @type qn_unary_operation() = lnot.
%%
%% @type qn_select_predicate() = {qn_unary_operation(), qn_term() | qn_select_predicate()} |
%%                               {qn_term() | qn_select_predicate(), qn_binary_operation(), qn_term() | qn_select_predicate()}.
%% Selection predicate is an expression composed of pairs of terms connected 
%% by some operation. 
%%
%% @type qn_side() = outer | inner.
%% The side of query node is its position in relation to parent.
%%

-module(query_node).
-export(
   [
    queue_prepared/1,
    eval_project/1,
    project_prepare/1,
    difference_lists/2,
    eval_attribute/1,
    eval_select/1,
    queue_block_end/1
   ]).
-include_lib("eunit/include/eunit.hrl").

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

%% ======================================================================
%% 
%% query node queue manipulation

%% 
%% @doc Initializes queue Queue of type Type by setting appropriate PD entries.
%%
%% Input queues accept queue_write() to enter complete messages including blocks 
%% of elements into the queue, and, queue_get() to retrieve elements that 
%% constitute message blocks.
%%
%% Output queues use queue_put() to enter elements into the queue, and, 
%% queue_read() to read messages including blocks of elements from queue. 
%%
%% Queues of type plain are simple queues that use queue_write() to enter messages 
%% and queue_read() to retrieve messages from queues.
%% 
%% @spec queue_init(Queue::atom(), Type::(input|output|none), MsgHead::atom()) -> ok|fail
%% 
queue_init(Queue, Type, MsgHead) when (Type == input) or (Type == output) or (Type == plain) ->

    QL = atom_to_list(Queue),
    Q = list_to_atom(string:concat("queue_",QL)),
    B = list_to_atom(string:concat("buff_",QL)),
    C = list_to_atom(string:concat("cnt_",QL)),
    P = list_to_atom(string:concat("pid_",QL)),
    M = list_to_atom(string:concat("msg_",QL)),

    %% create main queue and buff
    put(Q,queue:new()),            % init queue
    put(B,[]),                     % init buffer
    put(M, MsgHead),               % init msg head

    %% create special PD entries for queues
    case Type of 
       input  -> put(P,undefined);
       output -> put(C,0);
       plain   -> ok
    end,
    info_msg(queue_init, [get(self), {queue,Queue}, {Q,get(Q)}, {B,get(B)}, {C,get(C)}, {M,get(M)}, {P,get(P)}, get(state)], init_done, 50);

queue_init(Queue, Type, MsgHead) -> 
    error_msg(queue_init, [get(self), {queue,Queue}, {type,Type}, {msg_head,MsgHead}, {all,get()}, get(state)], wrong_queue),
    fail.

%% 
%% @doc Check if (input|output) queue Queue is empty and return results as boolean(). 
%% Input queue is empty then there are no more triples in the queue. 
%% Output queue is empty if there are no complete messages prepared to be sent.
%% 
%% @spec queue_empty(Queue::atom()) -> boolean() 
%% 
queue_empty(Queue) ->
    %% gen atoms for buff and queue
    Q = list_to_atom(string:concat("queue_",atom_to_list(Queue))),
    B = list_to_atom(string:concat("buff_",atom_to_list(Queue))),

    %% true if empty queue and buff
    Res = queue:is_empty(get(Q)) and (get(B) =:= []),
    info_msg(queue_empty, [get(self), {queue,Queue}, {B, get(B)}, {Q, get(Q)}, {return,Res}, get(state)], check_empty, 50),
    Res.

%% 
%% @doc Check if queue Queue incudes messages prepared to be read from or 
%% sent to remote process. 
%% 
%% @spec queue_prepared(Queue::atom()) -> boolean() 
%% 
queue_prepared(Queue) ->
    %% gen atom for queue
    Q = list_to_atom(string:concat("queue_",atom_to_list(Queue))),
    B = list_to_atom(string:concat("buff_",atom_to_list(Queue))),
    Res = not queue:is_empty(get(Q)),
    info_msg(queue_prepared, [get(self), {queue,Queue}, {B, get(B)}, {Q, get(Q)},  {return,Res}, get(state)], check_prepared, 50),
    Res.

%% 
%% @doc Write message Msg to queue named Queue. Functions queue_write and 
%% queue_read deal with queue of messages solevly, and do not touch individual 
%% graphs that can be included in messages.
%%
%% @spec queue_write(Queue::atom(), Msg::term()) -> ok 
%% 
queue_write(Queue, Msg) ->
    Queue0 = list_to_atom(string:concat("queue_",atom_to_list(Queue))),
    %% insert Msg into queue 
    Q = get(Queue0),
    case Q of 
    undefined -> put(Queue0, queue:in(Msg, queue:new()));
    _         -> put(Queue0, queue:in(Msg, Q))
    end, 
    info_msg(queue_write, [get(self), {queue,Queue}, {msg,Msg}, {Queue0,get(Queue0)}, get(state)], write_done, 50),
    ok.

%%
%% @doc Read message from queue Queue. Functions queue_write and 
%% queue_read deal with queue of messages solely, and do not touch individual 
%% graphs that can be included in messages.
%% 
%% @spec queue_read(Queue::atom()) -> Msg::term()
%% 
queue_read(Queue) ->
    Queue0 = list_to_atom(string:concat("queue_",atom_to_list(Queue))),
    info_msg(queue_read, [get(self), {queue,Queue}, {Queue0,get(Queue0)}, get(state)], read_enter, 50), 
    %% retrieve Msg from queue
    {{value, Msg}, Q} = queue:out(get(Queue0)),
    put(Queue0, Q),
    info_msg(queue_read, [get(self), {queue,Queue}, {Queue0,get(Queue0)}, {return,Msg}, get(state)], read_done, 50), 
    Msg.

%% 
%% @doc Determines if Queue buffer holding a block of triples is at the end. 
%% Method queue_get() must be called before calling queue_block_end() 
%% to intialize the queue buffer. 
%% 
%% @spec queue_block_end(Queue::atom()) -> boolean()
%%
queue_block_end(Queue) ->

    %% set names of dictionary variables
    QL = atom_to_list(Queue),
    B = list_to_atom(string:concat("buff_",QL)),
    BV = get(B),                    % get buffer
 
    %% check buffer if it contains no messages
    case BV of 
    [] -> Ret = true;
    _  -> Ret = fail
    end,
    info_msg(queue_block_end, [get(self), {queue,Queue}, {B,get(B)}, {return,Ret}, get(state)], check_block_end_done, 50), 
    Ret.

%% 
%% @doc Read a triple Triple from input queue Queue. Input queue is a queue 
%% that receives data messages from some other process. Data messages include 
%% lists of Graphs. Access to graphs received from some other process is 
%% provided by using function queue_get() and queue_put(). There are two 
%% input queues in join query node: from_outer and from_inner.
%% 
%% @spec queue_get(Queue::atom()) -> {From::node_state:ns_pid(), Graph::qn_graph()} | fail
%%
queue_get(Queue) ->

    %% set names of dictionary variables
    QL = atom_to_list(Queue),
    Q = list_to_atom(string:concat("queue_",QL)),
    B = list_to_atom(string:concat("buff_",QL)),
    P = list_to_atom(string:concat("pid_",QL)),
    M = list_to_atom(string:concat("msg_",QL)),

    %% read dictionary variables
    BV = get(B),                    % get buffer
    PV = get(P),                    % get pid of sender
    MV = get(M),                    % get message name
    EQ = queue:is_empty(get(Q)),
 
    %% retrieve graph      
    case {BV, EQ} of 
    {[], true} ->  %% no more graphs in queue 
                   Ret = fail;
    {[], false} -> %% empty buffer but not empty queue
                   {MV, F, [H|T]} = queue_read(Queue),
                   put(P, F),
                   put(B, T),
                   Ret = {F, H};
    {[H|T], _}  -> %% data is in buffer
                   put(B, T),
                   Ret = {PV, H};
    _           -> Ret = fail
    end,
    info_msg(queue_get, [get(self), {queue,Queue}, {Q,get(Q)}, {B,get(B)}, {M,get(M)}, {P,get(P)}, {return,Ret}, get(state)], get_done, 50), 
    Ret.

%% 
%% @doc Write graph Graph to output queue Queue. Output queue of query 
%% nodes is a queue that gathers graphs from local process and then 
%% packs list of graphs into messages to be sent to remote process. 
%% There is only one output queue of join query node: to_parent.
%% 
%% @spec queue_put(Queue::atom(), Graph::qn_graph()) -> ok|fail
%%
queue_put(Queue, Graph) ->

    %% set names of dictionary variables
    QL = atom_to_list(Queue),
    Q = list_to_atom(string:concat("queue_",QL)),
    B = list_to_atom(string:concat("buff_",QL)),
    C = list_to_atom(string:concat("cnt_",QL)),
    M = list_to_atom(string:concat("msg_",QL)),

    %% read dictiounary variables
    CS = get(block_size),            % get block size
    BV = get(B),                     % get buffer
    CN = get(C),                     % get counter
    MV = get(M),                     % get message head

    %% insert graph to queue
    case {BV, (CN >= (CS-1))} of 
    {[], _} ->    %% buff is empty
                  put(B, [Graph|[]]),
                  put(C, 1),
                  Ret = ok;
    {L, false} -> %% buff is not full
                  put(B, [Graph|L]),
                  put(C, CN+1),
                  Ret = ok;
    {L, true}  -> %% buff full, store it to queue
                  queue_write(Queue, {MV, get(self), lists:reverse([Graph|L])}),
                  put(B, []),
                  put(C, 0), 
                  Ret = ok;
    _          -> Ret = fail
    end,
    info_msg(queue_put, [get(self), {queue,Queue}, {graph,Graph}, {Q,get(Q)}, {B,get(B)}, {M,get(M)}, {C,get(C)}, {return,Ret}, get(state)], put_done, 50), 
    Ret.

%% 
%% @doc Flush output queue. Output queue buffer is packed and inserted into
%% queue. Queue buffer is set empty and counter of graphs in the buffer is 
%% set to 0.
%% 
%% @spec queue_flush(Queue::atom()) -> ok|fail
%%
queue_flush(Queue) ->
    %% set names of dictionary variables
    QL = atom_to_list(Queue),
    Q = list_to_atom(string:concat("queue_",QL)),
    B = list_to_atom(string:concat("buff_",QL)),
    C = list_to_atom(string:concat("cnt_",QL)),
    M = list_to_atom(string:concat("msg_",QL)),

    %% flush buffer to queue
    BV = get(B),
    MV = get(M),
    case BV of 
    [] -> ok;
    _  -> queue_write(Queue, {MV, get(self), lists:reverse(BV)})   
    end,
    put(B, []),
    put(C, 0), 
    info_msg(queue_flush, [get(self), {queue,Queue}, {Queue,get(Q)}, {B,get(B)}, {M,get(M)}, {C,get(C)}, get(state)], flush_done, 50), 
    ok.


%%
%% @doc Test function for join query node queues. 
%%
queue_test_() ->
    b3s:start(),
    b3s:stop(),
    b3s:start(),
    b3s:bootstrap(),
    {inorder,
     [
%     ?_assertMatch(ok, b3s:start()),
      {generator, fun()-> queue_t1() end},
%     {generator, fun()-> queue_t2() end},
      ?_assertMatch(ok, b3s:stop())
     ]}.

queue_t1() ->
    info_msg(queue_t1, [get(self)], start, 50),

    {ok, N1} = application:get_env(b3s, block_size),
    put(block_size, N1),
    put(self, testpid0),

    %% init queues
    queue_init(from_db, input, db_block),
    queue_init(to_parent, output, data_outer),
    queue_init(from_parent, plain, empty),

    T1 = {triple_store, "id23", "tokyo", "isLocatedIn",	"japan"},
    T2 = {triple_store, "id24", "kyoto", "isLocatedIn",	"japan"},
    T3 = {triple_store, "id25", "osaka", "isLocatedIn",	"japan"},
    T4 = {triple_store, "id26", "koper", "isLocatedIn",	"slovenia"},
    T5 = {triple_store, "id27", "ljubljana", "isLocatedIn","slovenia"},

    G1 = maps:put("1", T1, maps:new()),
    G2 = maps:put("1", T2, maps:new()),
    G3 = maps:put("1", T3, maps:new()),
    G4 = maps:put("1", T4, maps:new()),
    G5 = maps:put("1", T5, maps:new()),
    M1 = {db_block, testpid1, [G1, G2, G3, G4, G5]},
    M2 = {data_outer, testpid0, [G1, G2, G3, G4, G5]},
    M3 = {data_outer, testpid0, [G5, G4, G3, G2, G1]},
    M4 = {data_outer, testpid0, [G1, G2, G3]},
%    M7 = {data_outer, testpid0, [G5]},

    R1 = {testpid1, G1},
    R2 = {testpid1, G2},
    R3 = {testpid1, G3},
    R4 = {testpid1, G4},
    R5 = {testpid1, G5},
%    R6 = {testpid2, G3},
%    R7 = {testpid2, G3},
%    R8 = {testpid2, G4},
%    R9 = {testpid2, G5},
    
    {inorder,
     [
      ?_assertMatch(true,     queue_empty(to_parent)),
      ?_assertMatch(true,     queue_empty(from_parent)),
      ?_assertMatch(true,     queue_empty(from_db)),
      %
      ?_assertMatch(ok,       queue_write(from_db, M1)),
      ?_assertMatch(R1,       queue_get(from_db)),
      ?_assertMatch(R2,       queue_get(from_db)),
      ?_assertMatch(R3,       queue_get(from_db)),
      ?_assertMatch(R4,       queue_get(from_db)),
      ?_assertMatch(R5,       queue_get(from_db)),
      ?_assertMatch(true,     queue_empty(from_db)),
      %
      ?_assertMatch(ok,       queue_put(to_parent, G1)),
      ?_assertMatch(ok,       queue_put(to_parent, G2)),
      ?_assertMatch(ok,       queue_put(to_parent, G3)),
      ?_assertMatch(ok,       queue_put(to_parent, G4)),
      ?_assertMatch(ok,       queue_put(to_parent, G5)),
      ?_assertMatch(M2,       queue_read(to_parent)),
      ?_assertMatch(ok,       queue_put(to_parent, G1)),
      ?_assertMatch(ok,       queue_put(to_parent, G2)),
      ?_assertMatch(ok,       queue_put(to_parent, G3)),
      ?_assertMatch(ok,       queue_put(to_parent, G4)),
      ?_assertMatch(ok,       queue_put(to_parent, G5)),
      ?_assertMatch(ok,       queue_put(to_parent, G5)),
      ?_assertMatch(ok,       queue_put(to_parent, G4)),
      ?_assertMatch(ok,       queue_put(to_parent, G3)),
      ?_assertMatch(ok,       queue_put(to_parent, G2)),
      ?_assertMatch(ok,       queue_put(to_parent, G1)),
      ?_assertMatch(ok,       queue_put(to_parent, G1)),
      ?_assertMatch(ok,       queue_put(to_parent, G2)),
      ?_assertMatch(ok,       queue_put(to_parent, G3)),
      ?_assertMatch(M2,       queue_read(to_parent)),
      ?_assertMatch(M3,       queue_read(to_parent)),
      ?_assertMatch(ok,       queue_flush(to_parent)),
      ?_assertMatch(M4,       queue_read(to_parent))
     ]}.

%% ======================================================================
%% 
%% select and project operation can be part of every query node

%%
%% eval_project/1
%% 
%% @doc Compute projection of graph by retaining values of varables specified 
%% by the list PL and deleting triples that do not contain these variables.
%%
%% eval_project(PL::query_tree:qn_project_list()) -> maps:map()
%%
eval_project(none) -> ok;

eval_project(_) -> 

    %% construct new graph to return¸
    PO = get(project_out),
    F1 = fun (E, M) ->
             maps:remove(E, M)
         end,
    G = lists:foldl(F1, get(gp_val), PO),
    put(gp_val, G),
    ok.

%%
%% project_prepare/0
%% 
%% @doc From list of variables that are retained in the projected graph 
%% prepare list of variables to be projected out. Input list of variables 
%% is the parameter PL. Output is stored as PD entry project_out.
%%
%% project_prepare(PL::query_tree:qn_project_list()) -> ok|fail
%%
project_prepare(PL) -> 
    case PL of 
    none -> put(project_out, []);

    _ -> %% first get id-s of al tp-s 
         VP = get(vars_pos),
         L = lists:flatten(maps:values(VP)),

         %% get id-s of tp-s from gp including vars from PL
         F2 = fun (V) ->
                  maps:get(V, VP)
              end,
         %%  map list of vars to list of pairs {qn_id,int}
         L1 = lists:flatmap(F2, PL),
            
         %% now extract qn_id-s ie. 1st component of each pair
            F3 = fun (P) ->
       	             element(1, P)
            end,
         LA = lists:usort(lists:map(F3, L)),
         L2 = lists:usort(lists:map(F3, L1)),
         L3 = difference_lists(LA, L2),
         info_msg(hc_eval, [get(self), {project_list,PL}, {all_qids,LA}, {project_qids,L2}, {project_out,L3}, get(state)], comp_project_out, 50),
         put(project_out, L3)
    end.

%% 
%% @doc Compute difference between two lists.
%%
%% difference_list(L1,L2) -> L3
difference_lists([], _) -> [];

difference_lists([X|L], L1) -> 
    XInBoth = lists:member(X, L1),
    case XInBoth of
    true -> 
        difference_lists(L, L1);
    false -> 
        L2 = difference_lists(L, L1), 
        [X|L2]
    end.

%% 
%% @doc Convert an instance of qn_attribute() to a value of a given Erlang type. 
%%
%% eval_attribute(Atr::qn_attribute())) -> string() | integer() | real() | timedate()
eval_attribute(Atr) -> 
   case Atr of 
     {V,code} -> V;
     {V,integer} -> V;
     {V,timedate} -> V;
     {V,real} -> V;
     {V,string} -> V;
     _  -> Atr
   end.

%%
%% eval_select/1
%% 
%% @doc Compute selection predicate on a given graph Graph and return 
%% result as boolean(). Predicate is parameter SelectPred. 
%%
%% eval_select(SelectPred::query_tree:qn_select_predicate()) -> any()
%%
eval_select(S) when is_atom(S) -> 
    info_msg(select, [get(self), {expr,S}, {select_pred,get(select_pred)}, get(state)], select_atom_expr, 50), 
    S == none;

eval_select(S) when is_integer(S) -> 
    info_msg(select, [get(self), {expr,S}, {select_pred,get(select_pred)}, get(state)], select_int_expr, 50), 
    S;

eval_select({V,Type}) when (Type == integer) or (Type == real) or (Type == timedate) or 
                           (Type == code) or (Type == string) -> 
    info_msg(select, [get(self), {expr,{V,Type}}, {select_pred,get(select_pred)}, get(state)], select_typed_expr, 50), 
    V; 

%% selection predicate is a string S.
eval_select(S) when is_list(S) -> 
    info_msg(select, [get(self), {expr,S}, {qnode,get(qnode)}, {select_pred,get(select_pred)}, get(state)], select_list_expr, 50), 

    %% check if variable
    IsVar = string:chr(S, $?) =:= 1, 

    case get(qnode) of 
    join -> %% return either var value or constant
            case IsVar of 
            true  -> %% get position and tuple
                     [{NodeId, Pos}|_] = maps:get(S, get(vars_pos)),
                     Tuple = maps:get(NodeId, get(gp_val)),

                     %% Pos+1 since first component is table-name
                     E = element(Pos+1, Tuple),
                     info_msg(select, [get(self), {select_pred,get(select_pred)}, {variable,S}, {value,E}, get(state)], variable_value, 50),
                     eval_attribute(E);
            %% return string constant if not var
            false -> S
            end;

    tp   -> %% return either var value or constant
            case IsVar of 
            true  -> Pos = maps:get(S, get(vars)),
  	   	     E = element(Pos+1, get(tp_val)),
                     info_msg(select, [get(self), {select_pred,get(select_pred)}, {variable,S}, {value,E}, get(state)], variable_value, 50),
                     eval_attribute(E);
            %% return string constant if not var
            false -> S
            end
    end;

%% selection predicate includes comparison ops
eval_select({S1, equal, S2}) -> 
    VS1 = eval_select(S1), 
    VS2 = eval_select(S2),
    VS1 == VS2;

eval_select({S1, less, S2}) -> 
    VS1 = eval_select(S1), 
    VS2 = eval_select(S2),
    VS1 < VS2;

eval_select({S1, lesseq, S2}) -> 
    VS1 = eval_select(S1), 
    VS2 = eval_select(S2),
    VS1 =< VS2;

eval_select({S1, greater, S2}) -> 
    VS1 = eval_select(S1), 
    VS2 = eval_select(S2),
    VS1 > VS2;

eval_select({S1, greatereq, S2}) -> 
    VS1 = eval_select(S1), 
    VS2 = eval_select(S2),
    VS1 >= VS2;

eval_select({S1, land, S2}) -> 
    VS1 = eval_select(S1), 
    VS2 = eval_select(S2),
    VS1 and VS2; 

eval_select({S1, lor, S2}) -> 
    VS1 = eval_select(S1), 
    VS2 = eval_select(S2),
    VS1 or VS2;

eval_select({lnot, S1}) -> 
    VS1 = eval_select(S1), 
    not VS1;

eval_select(Expr) ->
    error_msg(select, [get(self), {expr,Expr}, {all,get()}, get(state)], illegal_select_expression),
    fail.

%% ===================================================================
%% 
%% @doc Unit tests.
%% 
qn_test_() ->
    application:load(b3s),
    qt_site(b3s_state:get(test_mode)).

qt_site(local1) ->
    {inorder,
     [
      ?_assertMatch(ok, b3s:start()),
      ?_assertMatch(ok, b3s:bootstrap()),
      ?_assertMatch(ok, b3s:stop())
     ]};

qt_site(_) ->
    [].

%% ====> END OF LINE <====

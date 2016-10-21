%%
%% Common header for record and constant definition
%%
%% @copyright 2015 UP FAMNIT and Yahoo Japan Corporation
%% @version 0.3
%% @since September, 2015
%% @author Kiyoshi Nitta <knitta@yahoo-corp.jp>
%% @author Iztok Savnik <iztok.savnik@famnit.upr.si>
%% 
%% This file should be included in module source files that refer
%% record definitions or constants.
%% 

-record(triple_store, {id,s,p,o}).
-record(key_value, {k,v}).
-record(person, {name,surname,address,age}).
-record(string_id_map, {id, str}).

-define(STRING_ID_CODING_METHOD, string_integer).
-define(DB_IMPLEMENTATION, epgsql).

%% ====> END OF LINE <====

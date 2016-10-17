## How to use command line interface ##

This document instructs how to use big3store command line
interface. It is invoked by entring following command.

    $ make ui-main

* Copyright (C) 2016 UP FAMNIT and Yahoo! Japan Corporation
* Version 0.3
* Since: January, 2016
* Author: Iztok Savnik <iztok.savnik@famnit.upr.si>
* Author: Kiyoshi Nitta <knitta@yahoo-corp.jp>

### 1. System management

Some system management operations can be performed from user interface.

    config [help | modules [short|NodeList]]

This command reports the values of important parameters of big3store system
by inquiring alive *b3s_state* process. It has sub commands
`help` and `modules`. Sub command `help` prints a short help message
of `config` command. Sub command `modules` lists loaded Erlang modules
for checking whether each module was compiled into fully optimized
native code or not. Sub command `modules` can take `short` option for
listing only optimized modules. Sub command `modules` can take
`NodeList` option for listing modules on specified erlang nodes
separated by a space character. While a node should be expressed in
"NodeName@FQDN" form, it can also be a synonym listed below.

| Synonym | Expanded to |
|-|-|
| FS | front server node |
| DS1 | 1st data server node |
| DS2 | 2nd data server node |
| : | : |
| DSn | nth data server node |

    debug_level [NodeName] DebugLevel

This command changes `debug_level` parameter value of all front and
data server nodes to `DebugLevel`. It must be an integer. Larger
`debug_level` value prints more precise messages. The default value
is 11. Value 51 will produce large amount of messages that might
affect query execution performance of big3store. It should only be
used for debugging.

    gc NodeName ProcessName Command [Argument1 [Argument2]]

This command executes Erlang's standard library function
[*genserver:call/2*](http://erlang.org/doc/man/gen_server.html#call-2).
While argument `NodeName` should be expressed in "NodeName@FQDN" form,
it can be a synomym listed at *config* command. Argument `ProcessName`
specifies a process on the node. It can be a synonym listed below.

| Synonym | Expanded to |
|-|-|
| BS | b3s_state |
| NS | node_state |
| TD | triple_distributor |
| SW | stop_watch |

Argument `Command` specifies a command message sent to the process. It
can be a synonym listed below.

| Synonym | Expanded to |
|-|-|
| G | get |
| P | put |
| GP | get_property |
| PP | put_property |
| I | info |
| TI | table_info |

    eps [[NodeList] | all [ModuleList]]

This command lists Erlang processes. It lists processes on specified
nodes if `NodeList` were provided. Node names can be synonyms listed
at *config* command. This command lists processes of specified modules
with `all ModuleList` option.

    boot

This command executes b3s:bootstrap/0 function. It must be executed
 after starting front and data server Erlang nodes.

    inv [est MaxRecords | stat | save | kill-finished | kill-all]

This command executes, monitors, and manages the investigation process
for loading triple data into big3store. It starts the investigation
process with no option. Files to be processed are specified by *dfs*
command. It reports estimated time of completion with `est MaxRecords`
option. Parameter `MaxRecord` must be a number of records to be
processed. It reports the progress of files with `stat` option. It
saves statistics information to permanent storage area with `save`
option. This option must be invoked after completing the investigation
process successfully. This command kills finished investigation Erlang
processes with `kill-finished` option. It kills all investigation
Erlang processes with `kill-all` option. This option should be used
carefully, because it terminates investigation processes that is still
running. An example is described in
[load-triples.md](load-triples.md).

    dst

This command starts the distribution process for loading triple data
into big3store. Files to be processed are specified by *dfs*
command. It usually generates several TSV files. An example is
described in [load-triples.md](load-triples.md). Command `inv` can
also be used for monitoring and managing Erlang processes invoked by
command `dst`.

    dfs [add DataFiles | rem {DataFile | DataNumber} | clr]

This command maintains a list of files to be loaded. They are TSV data
files processed by *inv* or *dst* commands. It lists numbered
registered files with no option. Sub command `add DataFiles` adds
specified files to the list. Sub command `rem {DataFile | DataNumber}`
removes a file from the list. Parameter `DataNumber` is a number
displayed invoking *dfs* command with no option. Sub command `clr`
clears the list.

    mnesia NodeName {info | table_info} [ArgumentList]

This command reports mnesia status of specified Erlang node. While
argument `NodeName` should be expressed in "NodeName@FQDN" form, it
can be a synonym listed at *config* command. Sub command `info` or `I`
executes *mnesia:info/0*. Sub command `table_info` or `TI` executes
*mnesia:table_info/2* which takes a table name as its 1st argument and
an information key as its 2nd argument. They must be surrounded by
single quotes (') for indicating that they are atoms of Erlang data
type.

    kill NodeName ProcessList

This command kills processes on a specified Erlang node. While
argument `NodeName` should be expressed in "NodeName@FQDN" form, it
can be a synonym listed at *config* command.

### 2. Execute ad-hoc queries

User interface is based on Unix environment. Basic operations of
Unix shell are provided. User interface allows invocation of operations
`ls` (and `ll`), `vi`, `less`, and `mv`. 

The concept of *active query* is used to define the query that 
can be manipulated by means of basic interface commands. For instance, 
active query can be edited by typing command `edit`. Active query can 
be executed using command `exec`. Active query can be linked to some 
file by using the operation `active`.

#### 2.1 Operations on active query ####

    active [ file ] (abbr., "a")

This command either displays the active query, if no parameters 
are given, or, sets active query to file `file` specified as 
parameter. 

    edit [ file ] (abbr., "e")

The command `edit` (also `vi`) activates editor `vi` to edit
the active query, or, starts editing session on file `file`. 
The edditing session has to be propery concluded, 
so that active query file is saved.

    exec [ file ] (abbr., "!")

The command `exec` executes the active query file, if no parameter 
is given, or, query file `file`, if parameter is given. The result of 
the query is displayed in activation environmemnt.

#### 2.2 Definition of queries

The query is Erlang term defined as an instance of type 
`qt_query()`. Syntax rules that define type 
`qt_query()` are presented as follows. 

    qt_query() = qt_bin_query() | qt_tp_query()
    qt_bin_query() = {qn_opsyn(), qt_query(), qt_query(), qn_select_predicate(), qn_project_list()}
    qt_tp_query() = {qn_opsyn(), qn_triple_pattern(), qn_select_predicate(), qn_project_list()}

    qn_opsym() = tp | join | leftjoin | union | differ. 
    qn_triple_pattern() = {qn_id() | qn_var(), qn_subject() | qn_var(), qn_predicate() | qn_var(), qn_object() | qn_var()}.

    qn_select_predicate() = {qn_unary_operation(), qn_term() | qn_select_predicate()} | {qn_term() | qn_select_predicate(), qn_binary_operation(), qn_term() | qn_select_predicate()}.
    qn_project_list() = [qn_var()].
    qn_object_value() = integer() | real() | calendar:datetime() | string().
    qn_object_type() = undefined | code | integer | real | datetime | string.

    qn_id() = string() | integer().
    qn_subject() = string() | integer().
    qn_predicate() = string() | integer().
    qn_object() = string() | {qn_object_value(), qn_object_type()}.

    qn_var() = string().
    qn_term() = qn_var() | string().
    qn_binary_operation() = equal | less | lesseq | greater | greatereq | land | lor 
    qn_unary_operation() = lnot.

The detailed presentation of `qt_query()` type is given in 
[module query_tree.erl](../../doc/query_tree.html#Type_qt_query()).
Queries writen as instances of `qt_type()` can be interpreted in Erlang directly.
Example of query composed of two triple patterns and no selection 
predicate as well as no projection list is written as follows.

    {join, {tp, { "?i1", "?y", "livesIn", "?x" }, none, none}, 
           {tp, { "?i2", "slovenia", "hasCapital", "?x" }, none, none},
           none, none}.

The above query finds all persons (`?y`) that live in some city (`?x`) that is 
the capital of Slovenia. Note that `?i1` and `?i2` denote the variables that 
stand for triple identifiers.

The following example represents a query that includes three triple-patterns,
a selection predicate and a projection list. 

    {join, {join, {tp, { "?i1", "?x", "livesIn", "ljubljana" }, none, none},
                  {tp, { "?i2", "?x", "graduatedFrom", "ul" }, none, none},
                  none, none },
            {tp, {"?i3", "?x", "age", "?y"}, none, none},
            {{"?y", less, 30}, land, {"?y", greatereq, 20}}, 
            ["?y"]},

The query finds all persons (`?x`) that live in Ljubljana and have 
graduated from University of Ljubljana (identifier "`ul`"), and have 
age that is greater or equal to 20 and less than 30. The triples including 
variable `?y` are returned.

#### 2.3 Unix operations

    ls (ll) [ dir ]

The operation `ls` lists files in the current directory, if no 
parameter is specified, or, selected directory `dir`. The operation 
`ll` lists files in current directory in row-by-row manner. 

    cat [ file ]

The operation `cat` uses Unix `cat` command to display 
either the active query, or, selected file `file`.

    less [ file ]

The operation `less` uses Unix `more` command to display 
either the active query, or, selected file `file`.

    cp file1 file2

The operation `cp` copies file `file1` to the file named `file2`.

    mv file1 file2

The operation `mv` moves file `file1` to the file named `file2`.

    vi [ file ]

The operation `vi` activates editor `vi` either on active query,
or, on selected file `file`.

### 3. Execute predefined benchmark tasks

Big3store has predefined benchmark tasks. User interface provides
commands for executing and measuring them.

    sb BenchmarkTaskName

This command starts a benchmark task. However, it only spawns the
task. Command *cb* should be used for checking the termination of the
task. Command *rb* should be used for reporting its precise
result. Command *kill* (described above) should be used for
terminating unnecessary benchmark task processes on the front server.

    cb {all | BenchmarkTaskNameList}

This command reports a short summary of executions including elapsed
time and number of result triples. Sub-command `all` reports about all
alive benchmark task processes. Otherwise, it reports about specified
processes. Command *sb* and *bb* invokes one Erlang process for the
same benchmark task. Therefore, this command reports the latest
execution of each benchmark task.

    rb BenchmarkTaskName

This command reports precise results of a specified benchmark task
including result triples, related system properties, and elapsed time
of sub-processes.

    bb BenchmarkTaskNameList

This command executes multiple benchmark tasks sequentially. It waits
until all benchmark tasks are completed. It reports a short summary of
executions including elapsed time and number of result triples. It can
also be used for executing the same benchmark task multiple times by
repeating the task name, because it reports average and middle average
of elapsed times of each execution in the summary. The middle average
is calculated by omitting the best and the worst values.

====> END OF LINE <====

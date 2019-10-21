## How to use command line interface ##

This document instructs how to use big3store command line interface
tool. It is invoked by entering following command.

    $ make ui-main

You can see summarized help message by typing `help`. Those details
are described below.

* Copyright (C) 2016-2019 UP FAMNIT and Yahoo Japan Corporation
* Version 0.3
* Since: January, 2016
* Author: Iztok Savnik <iztok.savnik@famnit.upr.si>
* Author: Kiyoshi Nitta <knitta@yahoo-corp.jp>

### 1. System management

Following is a list of system management commands of the user
interface.

| Command | Alias | Description |
|-|-|-|
| config              | c   | report important parameters |
| debug_level         |     | set debug level of nodes |
| gen_server_call     | gc  | invoke genserver:call/2 |
| gen_server_call_get | gcg | invoke genserver:call/2 |
| eval                | ev  | evaluate an erlang expression |
| erlang_ps           | eps | list erlang processes |
| boot                |     | perform bootstrap process |
| investigate         | inv | manage investigation process of loading triples |
| distribute          | dst | manage distribution process of loading triples |
| data_files          | dfs | manage files to be loaded |
| mnesia              |     | report mnesia table status / information |
| kill                |     | kill erlang processes |
| aws                 |     | perform aws operations |
| local               |     | perform local server operations |
| property            | pr  | perform property management operations |
| psql                |     | perform postgresql operations |
| remote              |     | perform UI commands on remote data severs |
| memory              | m   | perform memory investigations |

Some system management operations can be performed from user
interface.

    config [help | down | debug | modules [short|NodeList]]

This command reports the values of important parameters of big3store
system by inquiring alive *b3s_state* process. It has sub commands
`help`, `down`, `debug`, and `modules`. Sub command `help` prints a
short help message of `config` command. Sub command `down` prints a
list of down data servers. Sub command `debug` prints a list of debug
levels for all managed nodes. Sub command `modules` lists loaded
Erlang modules for checking whether each module was compiled into
fully optimized native code or not. Sub command `modules` can take
`short` option for listing only optimized modules. Sub command
`modules` can take `NodeList` option for listing modules on specified
erlang nodes separated by a space character. While a node should be
expressed in *NodeName@FQDN* form, it can also be a synonym listed
below.

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
`debug_level` value leave more precise messages to the elog
storage. The default value is 11. Value 51 will produce large amount
of messages that might affect query execution performance of
big3store. It should only be used for debugging. See [Erlang doc's
Report Browser
section](http://erlang.org/doc/apps/sasl/error_logging.html#report-browser)
for learning more detail about logs.

    gen_server_call NodeName ProcessName Command [Argument1 [Argument2]]

This command executes Erlang's standard library function
[*genserver:call/2*](http://erlang.org/doc/man/gen_server.html#call-2).
While argument `NodeName` should be expressed in *NodeName@FQDN* form,
it can be a synonym listed at `config` command. Argument `ProcessName`
specifies a process on the node. It can be a synonym listed below.

| Synonym | Expanded to |
|-|-|
| BS | b3s_state |
| NS | node_state |
| TD | triple_distributor |
| SW | stop_watch |
| SI | string_id |

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

    gen_server_call_get ProcessDictionaryProperty
                        NodeName ProcessName Command [Argument1 [Argument2]]

This command executes Erlang's standard library function
[*genserver:call/2*](http://erlang.org/doc/man/gen_server.html#call-2)
like `gen_server_call` command and stores the execution result to a
local process dictionary of Erlang. While argument `NodeName` should
be expressed in *NodeName@FQDN* form, it can be a synonym listed at
`config` command. Argument `ProcessName` specifies a process on the
node. It can be a synonym listed at `gen_server_call`
command. Argument `Command` specifies a command message sent to the
process. It can be a synonym listed at `gen_server_call` command. The
stored value can be used in `eval` command using *get/1* primitive
function. While the process dictionary belongs to the user interface
process, the contents will be destroyed when the user interface was
restarted.

    eval ErlangExpression

This command evaluates an arbitrary Erlang expression.

    erlang_ps [[NodeList] | all [ModuleList]]

This command lists Erlang processes. It lists processes on specified
nodes if `NodeList` were provided. Node names can be synonyms listed
at `config` command. This command lists processes of specified modules
with `all ModuleList` option.

    boot

This command executes b3s:bootstrap/0 function. It must be executed
after starting front server Erlang node.

    investigate [est MaxRecords | stat | save | kill-finished | kill-all
                 reuse pred_freq | is finished]

This command executes, monitors, and manages the investigation process
for loading triple data into big3store. It starts the investigation
process with no option. Files to be processed are specified by
`data_files` command. It reports estimated time of completion with
`est MaxRecords` option. Parameter `MaxRecord` must be a number of
records to be processed. It reports the progress of files with `stat`
option. It saves statistics information to permanent storage area with
`save` option. This option must be invoked after completing the
investigation process successfully. This command kills finished
investigation Erlang processes with `kill-finished` option. It kills
all investigation Erlang processes with `kill-all` option. This option
should be used carefully, because it terminates investigation
processes that is still running. An example could be found in
[load-triples.md](load-triples.md). While the investigation process
usually takes very long time, following two sub commands might
help. Sub command `reuse pred-freq` use existing predicate frequency
statistics for calculating the mapping function. Therefore, it never
reads any source file. Sub command `is finished` waits until all
reading processes terminated. It is convenient in batch executions.

    distribute [rebuild encoded column dump files | is finished]

This command starts the distribution process for loading triple data
into big3store. Files to be processed are specified by `data_files`
command. It usually generates several TSV files. An example is
described in [load-triples.md](load-triples.md). Command `investigate`
could also be used for monitoring and managing Erlang processes
invoked by command `distribution`. Sub command `rebuild encoded column
dump files` performs the distribution process directly on encoded
column dump files, while the initial loading procedure requires the
encoding process after the distribution process. This sub command
requires string-id mapping table already created and full set of
encoded column dump files stored in `bak/bak` directory. Sub command
`is finished` waits until all reading processes terminated. It is
convenient in batch executions.

    data_files [add DataFiles | rem {DataFile | DataNumber} | clr]

This command maintains a list of files to be loaded. They are TSV data
files processed by `investigate` or `distribute` commands. It lists
numbered registered files with no option. Sub command `add DataFiles`
adds specified files to the list. Sub command `rem {DataFile |
DataNumber}` removes a file from the list. Parameter `DataNumber` is a
number displayed invoking `data_files` command with no option. Sub
command `clr` clears the list.

    mnesia NodeName {info | table_info} [ArgumentList]

This command reports mnesia status of specified Erlang node. While
argument `NodeName` should be expressed in *NodeName@FQDN* form, it
can be a synonym listed at `config` command. Sub command `info` or `I`
executes *mnesia:info/0*. Sub command `table_info` or `TI` executes
*mnesia:table_info/2* which takes a table name as its 1st argument and
an information key as its 2nd argument. They must be surrounded by
single quotes (') for indicating that they are atoms of Erlang data
type.

    kill {NodeName ProcessList | all query nodes}

This command kills processes on a specified Erlang node. While
argument `NodeName` should be expressed in *NodeName@FQDN* form, it
can be a synonym listed at `config` command. Sub command `all query
nodes` kills all processes of query nodes.

    aws Command [Args...]

This command performs various aws operations. While it has a large
number of sub commands, they are described separately below. Just
typing `aws` prints a short help message of sub commands.

    aws run NodeName1 NumberOfRows1 [NodeName2 NumberOfRows2...]

This sub command starts data server node instances. Argument
`NodeName[0-9]+` specifies a symbol for identifying the
column. Argument `NumberOfRows[0-9]+` specifies the number of rows
assigned for the column.

    aws describe {instance [NodeName, ...] | image}

This sub command describes specified type of AWS resources. Resource
type `instance` shows instance ids, private addresses, and public
addresses of front and data server instances. It can take additional
*NodeName* arguments for restricting nodes. Resource type `image`
shows AMI ids and snapshot ids of saved images created by `aws create
image` command.

    aws terminate {NodeName | InstanceId | all |
                   data servers | front server | quick and no save}

This sub command terminates AWS EC2 instances. All instances save
Erlang logs to an AWS S3 bucket when they terminate excepting the last
option `quick and no save`. If `NodeName` was provided as an argument,
the instance running corresponding node is terminated. If `InstanceId`
was provided as an argument, the instance of the id is terminated. If
`all` was provided as an argument, all big3store instances are
terminated. If `data servers` was provided as an argument, all data
server instances are terminated. If `front server` was provided as an
argument, front server instance is terminated. If `quick and no save`
was provided as an argument, all big3store instances are terminated
not leaving logs to the bucket. This option might be useful when
necessary logs or results have been already copied somewhere manually.

    aws bucket {list [SearchTerm ..] |
                save predicate dictionaries |
                save column tables |
                save encoded column tables |
                save string id tables |
                save benchmark Task .. |
                save elog |
                save elog and terminate |
		get column tables |
		get encoded column table NodeName |
		get string id tables |
		load predicate dictionaries}

This sub command provides operations on an AWS S3 bucket. The bucket
must be created manually. Its specifications must be set to aws.cf
correctly. Operation `list` lists contents of the bucket. The
operation can take additional search terms for restricting files which
contain all terms in their file names. Operation `save` stores various
big3store resources to the permanent storage of AWS. Resource
`predicate dictionaries` stores *b3s_state* process's `pred_clm`,
`pred_freq`, and `pred_string_id` properties to `dat` directory of the
bucket. Resources `column tables`, `encoded column tables`, and
`string id tables` store dumped table TSV files to `dat` directory of
the bucket. Their file names are automatically generated using values
of properties `distribution_algorithm` and `data_server_nodes`. Those
TSV files must be generated using `aws psql` commands in
advance. Resource `benchmark` stores log files of specified benchmark
tasks to `spool` directory of the bucket. Resource `elog` stores
Erlang log file of the running node to `spool` directory of the
bucket. It can have option `and terminate` which also terminates the
instance after storing the log file. Operation `get` retrieves various
big3store resources from the bucket. Resources `column tables`,
`encoded column tables`, and `string id tables` retrieve dumped table
TSV files from `dat` directory of the bucket. Their file names are
automatically generated using values of properties
`distribution_algorithm` and `data_server_nodes`. The operation will
fail if appropriate number of data servers had not been successfully
invoked. Operation `load` retrieves various big3store resources from
the bucket and loads them to appropriate big3store processes. Resource
`predicate dictionaries` retrieves *b3s_state* process's `pred_clm`,
`pred_freq`, and `pred_string_id` properties from `dat` directory of
the bucket and loads them to the process.

    aws metadata

This sub command prints some metadata of AWS EC2 bucket.

    aws sns publish Message

This sub command sends specified string as an AWS sns
message. Parameters `aws_sns_region` and `aws_sns_arn` of
*big3store/aws/cf/aws.cf* must be set for its successful operation.

    aws servers.cf

This sub command writes public and private addresses of front and data
server instances to the file named *big3store/aws/cf/servers.cf*.

    aws create {image Name | template front server}

This sub command creates various AWS EC2 resources. Resource `image`
creates a machine image (Amazon Machine Image; AMI) of the instance
executing the user interface tool. The name of the image must be given
as an argument. Resource `template front server` creates a template
for invoking the front server instance of the recently created
image. Because `aws create image` command reboots the instance, the
template creation must be done after the reboot. It will be
automatically executed when the front server was booted using the
default *b3s.app.aws* application preference file.

    aws delete {image | snapshot | bucket} Name

This sub command deletes various AWS resources. Resource `image`
deletes a machine image (Amazon Machine Image; AMI) having specified
image id. Resource `snapshot` deletes a snapshot having specified
snapshot id. Ids of images and snapshots could be obtained by
executing `aws describe image` command. Resource `bucket` deletes a
file on the S3 bucket having specified path name. Path names could be
obtained by executing `aws bucket list` command.

    aws unregister {all | Node..}

This sub command cancels the registrations of specified data server
nodes. It removes those nodes from `data_server_nodes` of *b3s_state*
process property, but not from `clm_row_conf`. It is convenient for
keeping the column number mapping when data server nodes were
restarted manually. Command `property construct clm_row_conf` should
be used for changing also the mapping.

    aws reboot {all | Node..}

This sub command reboots instances running specified data server
nodes. It removes those nodes from `data_server_nodes` of *b3s_state*
process property, but not from `clm_row_conf`. Each node will be
assigned to the same column number. The reboot progress could be
monitored using `config` command. Data server nodes in booting
processes are listed at `down_data_servers` property. The value
null([]) of the property indicates that all data servers have been
booted successfully.

    local Command [Args...]

This command performs various local operations for Erlang nodes. While
it has a large number of sub commands, they are described separately
below. Just typing `local` prints a short help message of sub
commands.

    local run {all | {Node Column}...}

This sub command runs data server nodes on the same local server
running the user interface and the front server. The same number of
data server nodes listed at `data_server_nodes` property of
*b3s_state* process will be invoked locally by providing `all`
argument to this command. It invokes specified nodes assigning
specified column number by providing a list of node and column number
pairs.

    local terminate {all | Node...}

This sub command terminates all or specified data server nodes on the
same local server running the user interface and the front server.

    local restart-ds Node...

This sub command restart specified data server nodes on the same local
server running the user interface and the front server.

    property

This command performs various property operations of alive Erlang
processes. While it has a large number of sub commands, they are
described separately below. Just typing `property` prints a short help
message of sub commands.

    property cp SourceNode SourceProcess Property DestinationNode DestinationProcess

This sub command copies specified property from the source process to
the destination process.

    property write Node Process Property Path

This sub command writes the value of a specified property to a
specified file.

    property {find | f} Node Process Term...

This sub command prints values of properties, which names match all
given search terms. Abbreviation `f` can be used instead of `find`.

    property backup b3s state to {All | Node}

This sub command backups important properties of *b3s_state* process
to *node_state* processes on running data server nodes. The properties
are `clm_row_conf`, `pred_clm`, `pred_freq`, `data_server_nodes`,
`name_of_triple_tables`, `aws_node_instance_map`, and
`aws_node_fleet_map`.

    property restore b3s state from Node

This sub command restores important properties of *b3s_state* process
from a *node_state* process on specified data server node. The
properties are `clm_row_conf`, `pred_clm`, `pred_freq`,
`data_server_nodes`, `name_of_triple_tables`, `aws_node_instance_map`,
and `aws_node_fleet_map`.

    property construct clm_row_conf

This sub command recreates a mapping structure `clm_row_conf` of
*b3s_state* process on the front server using `data_server_nodes`
property. It will be useful after correcting the assignment of data
servers to columns manually using `property append data server`
command.

    property append data server Column Node

This sub command appends a mapping from a specified data server to a
specified column on `data_server_nodes` property of *b3s_state*
process on the front server.

    psql

This command performs various property operations of alive Erlang
processes. While it has a large number of sub commands, they are
described separately below. Just typing `psql` prints a short help
message of sub commands.

    psql load Data

This sub command loads various data into postgre sql tables. It loads
TSV dump files by specifying `column tables` or `string id tables` as
data. Those files must be located at `bak` directory and have name
`ts_<alg>_<col>.tsv` or `si_<col>.tsv` where `<alg>` is a distribution
algorithm name and `<col>` is a 2 digits integer indicating a column
number. Those files will be removed after loading them. An AWS SNS
message will be sent after completing load processes if parameters
`aws_sns_region` and `aws_sns_arn` of *big3store/aws/cf/aws.cf* were
set correctly. It loads TSV encoded column dump file of a data server
by specifying `column tables Node` as data. The file will be retrieved
from the AWS bucket and removed after loading it. It will not perform
the loading process if the postgre sql table was already created. The
process could be performed by providing `force` option even if the
table exists.

    psql encode column tables

This sub command starts the encoding procedure on postgresql
tables. See [load-triples.md](load-triples.md) for the detail of the
encoding process. While this procedure takes long time, it is
recommended to perform the procedure directly using the postgres
client interface, by which the progress and estimated finish time are
printed periodically. An AWS SNS message will be sent after completing
the encode processes if parameters `aws_sns_region` and `aws_sns_arn`
of *big3store/aws/cf/aws.cf* were set correctly.

    psql save Data tables

This sub command saves various data in postgre sql tables into TSV
dump files on the local storage. If `encoded column` or `string id`
was specified as data, it saves encoded triple tables of all columns
or string-id mapping tables which were generated by `psql encode`
process. If `predicate string id` was specified as data, it saves a
subset of the string-id mapping which only contains predicates
appeared in `pred_clm` property of *b3s_state* process on the front
server.

    psql describe table Term

This sub command list table names that contain Term as their sub
strings. Triple table names of data servers are defined at
`name_of_triple_tables` property of *b3s_state* process on the front
server. A string-id mapping table name of the front server is defined
at `name_of_string_id_table` property.

    remote Node Command

This command performs any user interface command on data
servers. Synonym notation described at `config` command section above
can be used for specifying the data server node. If `all` was given as
Node, it performs the command on all data server nodes. Some
expressions perform special tasks. They are described below.

    remote load all encoded column tables [force]

This command makes all data servers to load encoded column tables from
the AWS bucket. Without `force` option, it doesn't start loading
processes if triple tables were already created.

    remote restart all data servers

This command restarts Erlang nodes of all data servers. While it
clears `data_server_nodes` property of *b3s_state* process of the
front server, it makes no change to `clm_row_conf` property.

    remote restart data servers Nodes

This command restarts Erlang nodes of specified data servers. While it
removes corresponding entries from `data_server_nodes` property of
*b3s_state* process of the front server, it makes no change to
`clm_row_conf` property.

    memory summary

This command reports heap memory statuses of all big3store managed
Erlang nodes. If some nodes consumed too much heap memory, they should
be restarted using `remote restart data servers` or `aws restart`
commands.

### 2. Execute ad hoc queries

Following is a list of ad hoc query execution commands of the user
interface.

| Command | Alias | Description |
|-|-|-|
| active | a | prints active query |
| edit   | e, vi | edit active query |
| exec   | ! | execute active query file |
| ls     |   | run unix 'ls' |
| cat    |   | run unix 'cat' |
| less   |   | run unix 'less' |
| cp     |   | run unix 'cp' |
| mv     |   | run unix 'mv' |
| df     |   | run unix 'df' |
| ps     |   | run unix 'ps' |
| top    |   | run unix 'top' |

User interface is based on Unix environment. Basic operations of Unix
shell are provided. User interface allows invocation of operations
`ls` (and `ll`), `vi`, `less`, and `mv`.

The concept of *active query* is used to define the query that can be
manipulated by means of basic interface commands. For instance, active
query can be edited by typing command `edit`. Active query can be
executed using command `exec`. Active query can be linked to some file
by using the operation `active`.

#### 2.1 Operations on active query ####

    active [ file ] (abbr., "a")

This command either displays the active query, if no parameters are
given, or, sets active query to file *file* specified as parameter.

    edit [ file ] (abbr., "e" or "vi")

The command `edit` activates editor `vi` to edit the active query, or,
starts editing session on file *file*.  The editing session has to be
properly concluded, so that active query file is saved.

    exec [ file ] (abbr., "!")

The command `exec` executes the active query file, if no parameter is
given, or, query file *file*, if parameter is given. The result of the
query is displayed in activation environment.

#### 2.2 Definition of queries

The query is Erlang term defined as an instance of type
`qt_query()`. Syntax rules that define type `qt_query()` are presented
as follows.

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

The detailed presentation of `qt_query()` type is given in [module
query_tree.erl](../../doc/query_tree.html#Type_qt_query()).  Queries
written as instances of `qt_type()` can be interpreted in Erlang
directly. Example of query composed of two triple patterns and no
selection predicate as well as no projection list is written as
follows.

    {join, {tp, { "?i1", "?y", "livesIn", "?x" }, none, none}, 
           {tp, { "?i2", "slovenia", "hasCapital", "?x" }, none, none},
           none, none}.

The above query finds all persons (`?y`) that live in some city (`?x`)
that is the capital of Slovenia. Note that `?i1` and `?i2` denote the
variables that stand for triple identifiers.

The following example represents a query that includes three
triple-patterns, a selection predicate and a projection list.

    {join, {join, {tp, { "?i1", "?x", "livesIn", "ljubljana" }, none, none},
                  {tp, { "?i2", "?x", "graduatedFrom", "ul" }, none, none},
                  none, none },
            {tp, {"?i3", "?x", "age", "?y"}, none, none},
            {{"?y", less, 30}, land, {"?y", greatereq, 20}}, 
            ["?y"]},

The query finds all persons (`?x`) that live in Ljubljana and have
graduated from University of Ljubljana (identifier "`ul`"), and have
age that is greater or equal to 20 and less than 30. The triples
including variable `?y` are returned.

#### 2.3 Unix operations

    ls (ll) [ dir ]

The operation `ls` lists files in the current directory, if no
parameter is specified, or, selected directory `dir`. The operation
`ll` lists files in current directory in row-by-row manner.

    cat [ file ]

The operation `cat` uses Unix `cat` command to display either the
active query, or, selected file *file*.

    less [ file ]

The operation `less` uses Unix `more` command to display either the
active query, or, selected file *file*.

    cp file1 file2

The operation `cp` copies file *file1* to the file named *file2*.

    mv file1 file2

The operation `mv` moves file *file1* to the file named *file2*.

    vi [ file ]

The operation `vi` activates editor `vi` either on active query, or,
on selected file *file*.

    df [ options ]

The operation `df` uses Unix `df` command to display disk usage of
running host server. It can take options which `df` command can
take. It can be used with `remote` command for observing the remote
status.

    ps [ terms ] 

The operation `ps` uses Unix `ps` command to display processes of
running host server. It can take arbitrary number of terms for
restricting processes appearing those terms. It can be used with
`remote` command for observing the remote status.

    top [ integer options ]

The operation `top` uses Unix `top` command to display active
processes of running host server. It can take an integer for
restricting the number of processes to be displayed. It can also take
options as the second argument which `top` command can take. It can be
used with `remote` command for observing the remote status.

### 3. Execute predefined benchmark tasks

Following is a list of benchmark task execution commands of the user
interface.

| Command | Alias | Description |
|-|-|-|
| start_benchmark  | sb | start a benchmark task |
| batch_benchmark  | bb | execute benchmark tasks sequentially |
| report_benchmark | rb | report full result of a benchmark task |
| check_benchmark  | cb | report summary reports of all benchmark tasks |

Big3store has predefined benchmark tasks. User interface provides
commands for executing and measuring them.

    sb BenchmarkTaskName

This command starts a benchmark task. However, it only spawns the
task. Command `check_benchmark` should be used for checking the
termination of the task. Command `report_benchmark` should be used for
reporting its precise result. Command `erlang_ps` and `kill`
(described above) should be used for investigating and terminating
unnecessary benchmark task processes on the front server.

    cb {all | BenchmarkTaskNameList}

This command reports a short summary of executions including elapsed
time and number of result triples. Sub-command `all` reports about all
alive benchmark task processes. Otherwise, it reports about specified
processes. Command `start_benchmark` and `batch_benchmark` invokes one
Erlang process for the same benchmark task. Therefore, this command
reports the latest execution of each benchmark task.

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

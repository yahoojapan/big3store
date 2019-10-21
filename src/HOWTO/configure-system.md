## How to Configure System ##

This document instructs how to configure a big3store system. It can
run on a single server and on plural servers. These system
configurations are determined by parameters defined in Erlang
application default file. Precise meanings of those parameters will be
described. Before configuring a big3store system, it must be
installed (see [install.md](install.md)).

* Copyright (C) 2016-2019 UP FAMNIT and Yahoo Japan Corporation
* Version 0.3
* Since: January, 2016
* Author: Kiyoshi Nitta <knitta@yahoo-corp.jp>

### 1. Setup Application Default File ###

While most of configuration parameters are defined in the Erlang
application default file (b3s.app), some parameters must be set in
Makefile. These configuration tasks can be performed by following
procedures below.

* (P-1-1) Modify b3s.app.local? files.

* (P-1-2) Modify Makefile.

* (P-1-3) Perform unit tests.

* (P-1-4) Modify b3s.app file.

Procedure (P-1-3) is executed by entering following command.

    $ make test-all

Term 'MANDATED' means that corresponding parameter must be modified
to fit the user's site.

#### b3s.app parameters ####

Configurable parameters are set in 'env' variable of b3s.app file. The
value of 'env' is a list of two-element tuples, each of which consists
of parameter name and its value. The meaning of these parameters are
described below. Some parameters can be overwritten by command line
arguments. Most of the following parameters are held in b3s_state
process. They can be dynamically modified using
[command line user interface tool](user-interface.md).

##### b3s_mode

This parameter specifies the running mode of big3store Erlang
application. A b3s application runs in front server mode by setting
this parameter value to **front_server**. A b3s application runs in
data server mode by setting this parameter value to **data_server**.

##### test_mode

This parameter controls unit test functions. It is used appropriately
from unit test targets written in Makefile.

##### write_mode

This parameter specifies the actual behavior of write requests in
performing triple distribution. The triple_distributor process
immediately stores triples to permanent triple store tables of data
servers by setting this parameter value to **on_the_fly**. This mode
is prepared for testing. The triple_distributor process writes column
dump TSV files instead of storing to the tables by setting this
parameter value to **postgres_bulk_load**.

##### benchmark_task

This parameter provides the name of default predefined benchmark
task. These benchmark tasks can be executed from the command line user
interface (see [user-interface.md](user-interface.md)).

##### front_server_nodes (MANDATED)

This parameter specifies a list of Erlang node names of front
servers. At least one front server must be set.

##### data_server_nodes (MANDATED)

This parameter defines a configuration of data servers. It specifies a
list of two element columns, each of which consists of data server
Erlang node name and corresponding column number. At least one data
server must be set. An example of plural data server configuration
can be found in 'b3s.app.sample5' file.

##### name_of_triple_tables (MANDATED)

This parameter specifies a list of two element columns, each of which
consists of data server Erlang node and corresponding triple store
table names. These triple table names should be different to each other.

##### b3s_state_nodes (MANDATED)

This parameter specifies a list of Erlang node names, on which
*b3s_state* processes are invoked. At least one node must be set.

##### triple_distributor_nodes (MANDATED)

This parameter specifies a list of Erlang node names, on which
*triple_distributor* processes are invoked. At least one node must be
set.

##### num_of_empty_msgs

This parameter specifies number of empty messages that are sent from
receiver processes of query execution pipelines.

##### block_size

This parameter specifies size of blocks that are transferred in query
execution pipelines.

##### distribution_algorithm

This parameter specifies the algorithm for distributing triples into
columns. It affects triple loading and the behavior of query execution.
Triples are distributed based on predicate value by setting
this parameter value to **predicate_based**. Triples are distributed
randomly by setting this parameter value to **random**.

##### name_of_triple_table (OBSOLETE)

This parameter provides default name of triple table for all column
data servers. It is overwritten by parameter *name_of_triple_tables* in
operating b3s application. It remains only for executing some old unit
tests successfully.

##### name_of_pred_clm_table

This parameter specifies name of permanent mnesia table that stores
mapping from predicates to column numbers. This table is only required
when parameter *distribution_algorithm* is set to **predicate_based**.

##### name_of_pred_freq_table

This parameter specifies name of permanent mnesia table that stores
predicate frequency of given triple data set. This table is only
required when parameter *distribution_algorithm* is set to
**predicate_based**.

##### name_of_string_id_table (MANDATED)

This parameter specifies name of mapping table from URI or literal
strings to integer ids. The table is created during the data loading
process described in [load-triples.md](load-triples.md). It is used
for encoding queries or decoding results.

##### store_report_frequency

This parameter specifies how frequently progress report messages are
produced on the storing stage. A progress message is produced periodically
after processing every *n* triples, if the value of this parameter was set
to *n*.

##### triple_id_skel

This parameter specifies skeleton string for generating unique triple
ids assigned to triples that have no triple id URIs. While most
published triples have their own ids, some triples do not. The triple
loader of big3store assigns unique ids to them. The string provided by
this parameter is used for generating such ids.

##### data_files

This parameter specifies path names of TSV files to be loaded. They
can be changed using the command line user interface (see
[user-interface.md](user-interface.md)).

##### epgsql_host

This parameter specifies a string holding hostname of PostgreSQL
server. This parameter is required for running data
servers. While a data server Erlang node and corresponding PostgreSQL
server usually run on the same server machine, this parameter have
"localhost" value in most cases.

##### epgsql_user (MANDATED)

This parameter specifies a string holding PostgreSQL user name. The user
should be the one registered in
[the installation process of PostgreSQL server and epgsql](install.md).

##### epgsql_pass (MANDATED)

This parameter specifies a string storing PostgreSQL user password. The
user should be the one registered in
[the installation process of PostgreSQL server and epgsql](install.md).

##### epgsql_port (MANDATED)

This parameter specifies port number of PostgreSQL server. The port
should be the one defined in
[the installation process of PostgreSQL server and epgsql](install.md).

##### result_record_max

This parameter specifies the maximum number of result triples in
benchmark result reports.

##### debug_level

This parameter controls volume of log messages. The higher value
produces more messages. Almost all messages are suppressed by setting
this parameter value to *0*. Statements producing log messages can be
find by following command.

    $ grep info_msg *.erl

The value *101* produces the most precise messages. While they are
convenient for debugging, this may slow down execution performance of
big3store.

#### Makefile parameters ####

Some variables defined in Makefile must be set correctly for executing
big3store successfully. They are listed below.

##### EPGDIR (MANDATED)

This variable should be set to the path where epgsql library is
installed in [this installation process](install.md).

##### HEAP (MANDATED)

This variable should be adjusted by assigning as much as possible 
RAM of server machines to Erlang emulator heaps. As explained in
[the official manual](http://erlang.org/doc/man/erl.html), the unit of
the number is kiloword. A word is 2 bytes in 16-bit architecture or 4
bytes in 32-bit. While larger heap area may bring better performance
to front and data server Erlang nodes, certain area should be kept for
PostgreSQL processes on data server machines. The section 2 of
[installation instruction](install.md) explains how to configure size
of memory used by a PostgreSQL server.

##### HSTNAM

This variable must be set to fully qualified domain name that includes
domain information in the printed name. While the default definition
may work correctly on most platforms, it must be set manually if
hostname command doesn't produce domain information with -f option.

#### Test Data

The unit test of big3store requires YAGO dataset that is disclosed
under a Creative Commons Attribution 3.0 License by the YAGO team of
the Max-Planck Institute for Informatics on the following site.

    https://www.mpi-inf.mpg.de/departments/databases-and-information-systems/research/yago-naga/yago/downloads/

Because YAGO dataset contains large number of triples, only extracted
subset was actually used in the unit tests. They are included in the
big3store distribution package (src/ygtsv).

### 2. Start big3store System ###

It requires several steps for starting a big3store system. At least
one front server Erlang nodes must be invoked. The invocation
operations can be performed using make commands. The bootstrap
procedure must be executed to start necessary processes on those
nodes. This operation can be performed using make command or command
line user interface tool of big3store. PostgreSQL servers must be
started before executing queries on all data server machines.

* (P-2-1) Start a front server.

* (P-2-2) Start data servers.

* (P-2-3) Perform the bootstrap process.

* (P-2-4) Start PostgreSQL servers.

(P-2-1) A front server Erlang node can be invoked by entering the
following command from a shell on a physical server machine reserved
for running a front server.

    $ make start-fs

(P-2-2) A data server Erlang node can be invoked by entering the
following command from each shell on one of physical servers machine
reserved for running data servers. This operation should be iterated
until invoking all data servers configured in the application default
file.

    $ make start-ds

These invocation processes of data servers could also be performed
using `aws` or `local` command of [command line user interface
tool](user-interface.md).

(P-2-3) When front and data servers are running, the bootstrap process
can be performed by entering the following command from any server
machine.

    $ make bootstrap

This process could also be performed using `boot` command of [command
line user interface tool](user-interface.md).

    $ make ui-main
    big3store=# boot

(P-2-4) PostgreSQL servers can be invoked by using the shell script
provided by epgsql distribution. Every data server machine should
invoke each PostgreSQL server by entering following command.

    $ cd $(EPGSQLDIR)
    $ start_test_db.sh

Some of the above tasks could be performed using shell scripts that
were prepared for the AWS computing environment. Refer
[aws.md](aws.md) for reading their detailed descriptions. They use
special makefile targets that might be useful in separated uses. Some
of those targets are described below.

    $ make start-fs-aws

This target copies `b3s.app.aws` to `b3s.app` and invokes a front
server on an Erlang node which name is `fs`. The front server is
designed to be used for performing query and benchmark executions on
the AWS environment.

    $ make start-fs-aws-boot-ds

This target copies `b3s.app.aws.boot_ds` to `b3s.app` and invokes a
front server on an Erlang node which name is `fs`. The front server is
designed to be used for performing query and benchmark executions on
the AWS environment. While it is almost identical to `start-fs-aws`,
it automatically invokes data server instances on AWS EC2 after the
front server was booted. The Erlang application preference file
`b3s.app.aws.boot_ds` includes several typical configurations as
comments.

    $ make start-fs-prep

This target copies `b3s.app.prep` to `b3s.app` and invokes a front
server on an Erlang node which name is `fs`. The front server is
designed to be used in the preparation process for loading triples
described at [load-triples.md](load-triples.md) and [aws.md](aws.md).

    $ make start-ds-aws DS=ds01 FSHN=123.45.67.89

This target copies `b3s.app.ds` to `b3s.app` and invokes a data server
on an Erlang node which name is `ds01`. The data server will try to
connect the front server running at `123.45.67.89`.

    $ make term FS=fs DS=ds01

This target terminates a front server node having name `fs` and a data
server node having name `ds01` if they are running.

It might be useful to integrate the *readline* feature with the user
interface for editing the statement and refering the history of
executions. Following targets provide it if `rlwrap` command was
installed.

    $ make sui[23]*

Because Erlang node names on the same server must be different, these
targets invoke the user interface tools on different nodes. You can
simultaneously invoke plural user interface tools on terminals of the
front server machine by using different targets.

====> END OF LINE <====

## How to Load Triples ##

This document instructs how to load large amount of triples into a
big3store instance of distributed configuration. Before loading
triples, a big3store system must be installed (see
[install.md](install.md)), configured to be suited for your system
environment, and started successfully (see
[configure-system.md](configure-system.md)). Concrete examples are
provided for tasks described below. These examples enable to load
whole YAGO2s TSV data into the big3store system that is configured to
run on 5 server machines. This loading task uses several commands
provided by the user interface tool. Detailed command descriptions can
be referred from [user-interface.md](user-interface.md).

* Copyright (C) 2016-2019 UP FAMNIT and Yahoo Japan Corporation
* Version 0.3
* Since: January, 2016
* Author: Kiyoshi Nitta <knitta@yahoo-corp.jp>

### 1. Generate Column Dump Files ###

The first task for loading triples is to generate a TSV triple dump
file for each column. The task can be performed by following
procedures below.

* (P-1-1) Investigate source files and create a distribution function.

* (P-1-2) Perform distribution and generate column dump files.

* (P-1-3) Save distribution function permanently.

#### example ####

It will take about 6 hours for processing all triples in YAGO2s on a
256GB memory machine. This task must be performed on one server
machine sequentially.

First, please confirm again that the application default file is
appropriately set and that big3store system is started. Major
configuration parameters can be displayed using *config* command of
big3store user interface. This command requires that a *b3s_state*
process runs on the front server. You can perform the bootstrap
process from the user interface using *boot* command. Detailed command
descriptions can be referred from
[user-interface.md](user-interface.md).

    $ cd $(B3S)/src
    $ make ui-main
	big3store=# config

                    front_server_nodes : ['b3ss01@nightbird.rlab.miniy.yahoo.co.jp']
                     data_server_nodes : {'b3ss02@circus.rlab.miniy.yahoo.co.jp',
                                          5}
                                         {'b3ss02@innocents.rlab.miniy.yahoo.co.jp',
                                          4}
                                         {'b3ss02@erasure.rlab.miniy.yahoo.co.jp',
                                          3}
                                         {'b3ss02@loveboat.rlab.miniy.yahoo.co.jp',
                                          2}
                                         {'b3ss02@nightbird.rlab.miniy.yahoo.co.jp',
                                          1}
                 name_of_triple_tables : {'b3ss02@circus.rlab.miniy.yahoo.co.jp',
                                          yago2s_5x1a_small_c5}
                                         {'b3ss02@innocents.rlab.miniy.yahoo.co.jp',
                                          yago2s_5x1a_small_c4}
                                         {'b3ss02@erasure.rlab.miniy.yahoo.co.jp',
                                          yago2s_5x1a_small_c3}
                                         {'b3ss02@loveboat.rlab.miniy.yahoo.co.jp',
                                          yago2s_5x1a_small_c2}
                                         {'b3ss02@nightbird.rlab.miniy.yahoo.co.jp',
                                          yago2s_5x1a_small_c1}
                       b3s_state_nodes : ['b3ss01@nightbird.rlab.miniy.yahoo.co.jp']
              triple_distributor_nodes : ['b3ss01@nightbird.rlab.miniy.yahoo.co.jp']
                     num_of_empty_msgs : 10
                            block_size : 100
                name_of_pred_clm_table : pc_yago2s_5x1a_small
               name_of_pred_freq_table : pf_yago2s_5x1a_small
               name_of_string_id_table : string_id_5x1a_small

            'b3ss01@night:debug_level' : 11
            'b3ss02@circu:debug_level' : 11
            'b3ss02@innoc:debug_level' : 11
            'b3ss02@erasu:debug_level' : 11
            'b3ss02@loveb:debug_level' : 11
            'b3ss02@night:debug_level' : 11

                            ====> END OF LINE <====

List of data files to be stored can be maintained by *dfs* command
interactively. Note that all YAGO2s TSV files are stored in
$(B3S)/src/ygtsv-full.

    big3store=# dfs
    usage: dfs [add | rem | clr]

                                     1 : "ygtsv-full/yagoStatistics.tsv"
                                     2 : "ygtsv-full/yagoSchema.tsv"
                                     3 : "ygtsv-full/yagoGeonamesGlosses.tsv"
                                     4 : "ygtsv-full/yagoGeonamesClassIds.tsv"
                                     5 : "ygtsv-full/yagoGeonamesClasses.tsv"
                                     6 : "ygtsv-full/yagoSimpleTaxonomy.tsv"
                                     7 : "ygtsv-full/yagoWordnetIds.tsv"
                                     8 : "ygtsv-full/yagoWordnetDomains.tsv"
                                     9 : "ygtsv-full/yagoGeonamesEntityIds.tsv"
                                    10 : "ygtsv-full/yagoDBpediaClasses.tsv"
                                    11 : "ygtsv-full/yagoTaxonomy.tsv"
                                    12 : "ygtsv-full/yagoMultilingualClassLabels.tsv"
                                    13 : "ygtsv-full/yagoDBpediaInstances.tsv"
                                    14 : "ygtsv-full/yagoMetaFacts.tsv"
                                    15 : "ygtsv-full/yagoImportantTypes.tsv"
                                    16 : "ygtsv-full/yagoLiteralFacts.tsv"
                                    17 : "ygtsv-full/yagoFacts.tsv"
                                    18 : "ygtsv-full/yagoSimpleTypes.tsv"
                                    19 : "ygtsv-full/yagoMultilingualInstanceLabels.tsv"
                                    20 : "ygtsv-full/yagoTypes.tsv"
                                    21 : "ygtsv-full/yagoLabels.tsv"
                                    22 : "ygtsv-full/yagoGeonamesData.tsv"
                                    23 : "ygtsv-full/yagoWikipediaInfo.tsv"
                                    24 : "ygtsv-full/yagoTransitiveType.tsv"
                                    25 : "ygtsv-full/yagoSources.tsv"

                            ====> END OF LINE <====                             
    big3store=# dfs rem 25
    removing data file ygtsv-full/yagoSources.tsv from data_files list...
    okbig3store=# dfs
    usage: dfs [add | rem | clr]

                                     1 : "ygtsv-full/yagoStatistics.tsv"
                                     2 : "ygtsv-full/yagoSchema.tsv"
                                     3 : "ygtsv-full/yagoGeonamesGlosses.tsv"
                                     4 : "ygtsv-full/yagoGeonamesClassIds.tsv"
                                     5 : "ygtsv-full/yagoGeonamesClasses.tsv"
                                     6 : "ygtsv-full/yagoSimpleTaxonomy.tsv"
                                     7 : "ygtsv-full/yagoWordnetIds.tsv"
                                     8 : "ygtsv-full/yagoWordnetDomains.tsv"
                                     9 : "ygtsv-full/yagoGeonamesEntityIds.tsv"
                                    10 : "ygtsv-full/yagoDBpediaClasses.tsv"
                                    11 : "ygtsv-full/yagoTaxonomy.tsv"
                                    12 : "ygtsv-full/yagoMultilingualClassLabels.tsv"
                                    13 : "ygtsv-full/yagoDBpediaInstances.tsv"
                                    14 : "ygtsv-full/yagoMetaFacts.tsv"
                                    15 : "ygtsv-full/yagoImportantTypes.tsv"
                                    16 : "ygtsv-full/yagoLiteralFacts.tsv"
                                    17 : "ygtsv-full/yagoFacts.tsv"
                                    18 : "ygtsv-full/yagoSimpleTypes.tsv"
                                    19 : "ygtsv-full/yagoMultilingualInstanceLabels.tsv"
                                    20 : "ygtsv-full/yagoTypes.tsv"
                                    21 : "ygtsv-full/yagoLabels.tsv"
                                    22 : "ygtsv-full/yagoGeonamesData.tsv"
                                    23 : "ygtsv-full/yagoWikipediaInfo.tsv"
                                    24 : "ygtsv-full/yagoTransitiveType.tsv"

                            ====> END OF LINE <====
    big3store=# dfs add ygtsv-full/yagoSources.tsv
    adding data file(s) from data_files list...
    ["ygtsv-full/yagoSources.tsv"]
    okbig3store=# dfs
    usage: dfs [add | rem | clr]

                                     1 : "ygtsv-full/yagoStatistics.tsv"
                                     2 : "ygtsv-full/yagoSchema.tsv"
                                     3 : "ygtsv-full/yagoGeonamesGlosses.tsv"
                                     4 : "ygtsv-full/yagoGeonamesClassIds.tsv"
                                     5 : "ygtsv-full/yagoGeonamesClasses.tsv"
                                     6 : "ygtsv-full/yagoSimpleTaxonomy.tsv"
                                     7 : "ygtsv-full/yagoWordnetIds.tsv"
                                     8 : "ygtsv-full/yagoWordnetDomains.tsv"
                                     9 : "ygtsv-full/yagoGeonamesEntityIds.tsv"
                                    10 : "ygtsv-full/yagoDBpediaClasses.tsv"
                                    11 : "ygtsv-full/yagoTaxonomy.tsv"
                                    12 : "ygtsv-full/yagoMultilingualClassLabels.tsv"
                                    13 : "ygtsv-full/yagoDBpediaInstances.tsv"
                                    14 : "ygtsv-full/yagoMetaFacts.tsv"
                                    15 : "ygtsv-full/yagoImportantTypes.tsv"
                                    16 : "ygtsv-full/yagoLiteralFacts.tsv"
                                    17 : "ygtsv-full/yagoFacts.tsv"
                                    18 : "ygtsv-full/yagoSimpleTypes.tsv"
                                    19 : "ygtsv-full/yagoMultilingualInstanceLabels.tsv"
                                    20 : "ygtsv-full/yagoTypes.tsv"
                                    21 : "ygtsv-full/yagoLabels.tsv"
                                    22 : "ygtsv-full/yagoGeonamesData.tsv"
                                    23 : "ygtsv-full/yagoWikipediaInfo.tsv"
                                    24 : "ygtsv-full/yagoTransitiveType.tsv"
                                    25 : "ygtsv-full/yagoSources.tsv"

                            ====> END OF LINE <====

Procedure (P-1-1) can be started by entering *inv* command. It will
process all files listed by *dfs* command sequentially.

	big3store=# inv
    * investigating[1](ygtsv-full/yagoStatistics.tsv)... done. {{2016,3,1},{15,24,23}}
    {start,{repeater,'b3ss01@nightbird.rlab.miniy.yahoo.co.jp'},
           investigate_stream,"ygtsv-full/yagoStatistics.tsv",file_reader_1,
           'b3ss01@nightbird.rlab.miniy.yahoo.co.jp'}
    ok
    * investigating[2](ygtsv-full/yagoSchema.tsv)... done. {{2016,3,1},{15,24,23}}
    {start,{repeater,'b3ss01@nightbird.rlab.miniy.yahoo.co.jp'},
           investigate_stream,"ygtsv-full/yagoSchema.tsv",file_reader_2,
           'b3ss01@nightbird.rlab.miniy.yahoo.co.jp'}
    ok
    * investigating[3](ygtsv-full/yagoGeonamesGlosses.tsv)... done. {{2016,3,1},{15,24,23}}
    {start,{repeater,'b3ss01@nightbird.rlab.miniy.yahoo.co.jp'},
           investigate_stream,"ygtsv-full/yagoGeonamesGlosses.tsv",file_reader_3,
           'b3ss01@nightbird.rlab.miniy.yahoo.co.jp'}
    ok
    * investigating[4](ygtsv-full/yagoGeonamesClassIds.tsv)... done. {{2016,3,1},{15,24,23}}
    {start,{repeater,'b3ss01@nightbird.rlab.miniy.yahoo.co.jp'},
           investigate_stream,"ygtsv-full/yagoGeonamesClassIds.tsv",file_reader_4,
           'b3ss01@nightbird.rlab.miniy.yahoo.co.jp'}
    ok
    * investigating[5](ygtsv-full/yagoGeonamesClasses.tsv)... done. {{2016,3,1},{15,24,23}}
    {start,{repeater,'b3ss01@nightbird.rlab.miniy.yahoo.co.jp'},
           investigate_stream,"ygtsv-full/yagoGeonamesClasses.tsv",file_reader_5,
           'b3ss01@nightbird.rlab.miniy.yahoo.co.jp'}
    ok
    * investigating[6](ygtsv-full/yagoSimpleTaxonomy.tsv)... done. {{2016,3,1},{15,24,23}}
    {start,{repeater,'b3ss01@nightbird.rlab.miniy.yahoo.co.jp'},
           investigate_stream,"ygtsv-full/yagoSimpleTaxonomy.tsv",file_reader_6,
           'b3ss01@nightbird.rlab.miniy.yahoo.co.jp'}
    ok

The progress of these processes can be checked by entering *inv stat* command.

    big3store=# inv stat
    file_reader_1       106 triples,    0 seconds, eof(true), <ygtsv-full/yagoStatistics.tsv>.
    file_reader_2       349 triples,    0 seconds, eof(true), <ygtsv-full/yagoSchema.tsv>.
    file_reader_3       611 triples,    0 seconds, eof(true), <ygtsv-full/yagoGeonamesGlosses.tsv>.
    file_reader_4       672 triples,    0 seconds, eof(true), <ygtsv-full/yagoGeonamesClassIds.tsv>.
    file_reader_5       672 triples,    0 seconds, eof(true), <ygtsv-full/yagoGeonamesClasses.tsv>.
    file_reader_6      6576 triples,    0 seconds, eof(true), <ygtsv-full/yagoSimpleTaxonomy.tsv>.

If all *eof* values became 'true', procedure (P-1-1) was
completed. Created distribution function can be saved permanently by
entring *inv save* command (P-1-3).

    big3store=# inv save
    pred_clm: ok
    pred_freq: ok

You can display existing mnesia tables on the front server by using
*mnesia FS I* command, where *FS* means front server and *I* means
'info'. You can find table names defined in configuration parameters
*name_of_pred_clm_table* and *name_of_pred_freq_table*. Query and
benchmark executions require these tables.

    big3store=# mnesia FS I
    ---> Processes holding locks <--- 
    ---> Processes waiting for locks <--- 
    ---> Participant transactions <--- 
    ---> Coordinator transactions <---
    ---> Uncertain transactions <--- 
    ---> Active tables <--- 
    pf_yago2s_5x1a_small: with 11       records occupying 713      words of mem
    pc_yago2s_5x1a_small: with 11       records occupying 713      words of mem
    schema         : with 7        records occupying 1108     words of mem
    pf_yago2s_5x1a : with 105      records occupying 3993     words of mem
    pc_yago2s_5x1a : with 105      records occupying 3993     words of mem
    pred_clm       : with 31       records occupying 1399     words of mem
    pred_freq      : with 31       records occupying 1399     words of mem
    ===> System info in version "4.12", debug level = none <===
    opt_disc. Directory "/home/knitta/wrk/bb/b3s/src/b3s/erlang/bak" is used.
    use fallback at restart = false
    running db nodes   = ['b3ss01@nightbird.rlab.miniy.yahoo.co.jp']
    stopped db nodes   = [] 
    master node tables = []
    remote             = []
    ram_copies         = [pc_yago2s_5x1a,pc_yago2s_5x1a_small,pf_yago2s_5x1a,
                          pf_yago2s_5x1a_small,pred_clm,pred_freq]
    disc_copies        = [schema]
    disc_only_copies   = []
    [{'b3ss01@nightbird.rlab.miniy.yahoo.co.jp',disc_copies}] = [schema]
    [{'b3ss01@nightbird.rlab.miniy.yahoo.co.jp',ram_copies}] = [pred_freq,
                                                                pred_clm,
                                                                pc_yago2s_5x1a,
                                                                pf_yago2s_5x1a,
                                                                pc_yago2s_5x1a_small,
                                                                pf_yago2s_5x1a_small]
    12 transactions committed, 0 aborted, 0 restarted, 12 logged to disc
    0 held locks, 0 in queue; 0 local transactions, 0 remote
    0 transactions waits for other nodes: []
    ok

Procedure (P-1-2) can be executed by entering *dst* command. The first
statement "gc FS TD PP ..." modifies a parameter value of the
*triple_distributor* process on the front server.

    big3store=# gc FS TD PP column_file_path /tmp/ts_yago2s_random_col~w.tsv
    performing genserver:call({triple_distributor,'b3ss01@circus.rlab.miniy.yahoo.co.jp'}, {put_property,column_file_path,[47,116,109,112,47,116,115,95,121,97,103,111,50,115,95,114,97,110,100,111,109,95,99,111,108,126,119,46,116,115,118]})...

    ok
    big3store=# gc FS TD GP column_file_path
    performing genserver:call({triple_distributor,'b3ss01@circus.rlab.miniy.yahoo.co.jp'}, {get_property,column_file_path})...

    "/tmp/ts_yago2s_random_col~w.tsv"
    big3store=# dst
    * distributing[1](ygtsv-full/yagoStatistics.tsv)... done. {{2016,3,1},{15,49,47}}
    {start,{repeater,'b3ss01@nightbird.rlab.miniy.yahoo.co.jp'},
           store_stream,"ygtsv-full/yagoStatistics.tsv",file_reader_1,
           'b3ss01@nightbird.rlab.miniy.yahoo.co.jp'}
    ok
    * distributing[2](ygtsv-full/yagoSchema.tsv)... done. {{2016,3,1},{15,49,47}}
    {start,{repeater,'b3ss01@nightbird.rlab.miniy.yahoo.co.jp'},
           store_stream,"ygtsv-full/yagoSchema.tsv",file_reader_2,
           'b3ss01@nightbird.rlab.miniy.yahoo.co.jp'}
    ok
    * distributing[3](ygtsv-full/yagoGeonamesGlosses.tsv)... done. {{2016,3,1},{15,49,47}}
    {start,{repeater,'b3ss01@nightbird.rlab.miniy.yahoo.co.jp'},
           store_stream,"ygtsv-full/yagoGeonamesGlosses.tsv",file_reader_3,
           'b3ss01@nightbird.rlab.miniy.yahoo.co.jp'}
    ok
    * distributing[4](ygtsv-full/yagoGeonamesClassIds.tsv)... done. {{2016,3,1},{15,49,47}}
    {start,{repeater,'b3ss01@nightbird.rlab.miniy.yahoo.co.jp'},
           store_stream,"ygtsv-full/yagoGeonamesClassIds.tsv",file_reader_4,
           'b3ss01@nightbird.rlab.miniy.yahoo.co.jp'}
    ok
    * distributing[5](ygtsv-full/yagoGeonamesClasses.tsv)... done. {{2016,3,1},{15,49,47}}
    {start,{repeater,'b3ss01@nightbird.rlab.miniy.yahoo.co.jp'},
           store_stream,"ygtsv-full/yagoGeonamesClasses.tsv",file_reader_5,
           'b3ss01@nightbird.rlab.miniy.yahoo.co.jp'}
    ok
    * distributing[6](ygtsv-full/yagoSimpleTaxonomy.tsv)... done. {{2016,3,1},{15,49,47}}
    {start,{repeater,'b3ss01@nightbird.rlab.miniy.yahoo.co.jp'},
           store_stream,"ygtsv-full/yagoSimpleTaxonomy.tsv",file_reader_6,
           'b3ss01@nightbird.rlab.miniy.yahoo.co.jp'}
    ok

The progress of these processes can also be checked by entring *inv
stat* command. If you know the total number of triples, you can check
the estimated finish time by entering *inv est <number>* command. You
can find column dump file in specified file path.

    big3store=# inv stat
    file_reader_1          106 triples,     0 seconds, eof(true), <ygtsv-full/yagoStatistics.tsv>.
    file_reader_2          349 triples,     0 seconds, eof(true), <ygtsv-full/yagoSchema.tsv>.
    file_reader_3          611 triples,     0 seconds, eof(true), <ygtsv-full/yagoGeonamesGlosses.tsv>.
    file_reader_4          672 triples,     0 seconds, eof(true), <ygtsv-full/yagoGeonamesClassIds.tsv>.
    file_reader_5          672 triples,     0 seconds, eof(true), <ygtsv-full/yagoGeonamesClasses.tsv>.
    file_reader_6         6576 triples,    34 seconds, eof(true), <ygtsv-full/yagoSimpleTaxonomy.tsv>.
    file_reader_7            0 triples,     0 seconds, eof(false), <ygtsv-full/yagoWordnetIds.tsv>.
    file_reader_8            0 triples,     0 seconds, eof(false), <ygtsv-full/yagoWordnetDomains.tsv>.
    file_reader_9            0 triples,     0 seconds, eof(false), <ygtsv-full/yagoGeonamesEntityIds.tsv>.
    file_reader_10           0 triples,     0 seconds, eof(false), <ygtsv-full/yagoDBpediaClasses.tsv>.
    file_reader_11           0 triples,     0 seconds, eof(false), <ygtsv-full/yagoTaxonomy.tsv>.
    file_reader_12           0 triples,     0 seconds, eof(false), <ygtsv-full/yagoMultilingualClassLabels.tsv>.
    file_reader_13           0 triples,     0 seconds, eof(false), <ygtsv-full/yagoDBpediaInstances.tsv>.
    file_reader_14           0 triples,     0 seconds, eof(false), <ygtsv-full/yagoMetaFacts.tsv>.
    file_reader_15           0 triples,     0 seconds, eof(false), <ygtsv-full/yagoImportantTypes.tsv>.
    file_reader_16           0 triples,     0 seconds, eof(false), <ygtsv-full/yagoLiteralFacts.tsv>.
    file_reader_17           0 triples,     0 seconds, eof(false), <ygtsv-full/yagoFacts.tsv>.
    file_reader_18           0 triples,     0 seconds, eof(false), <ygtsv-full/yagoSimpleTypes.tsv>.
    file_reader_19           0 triples,     0 seconds, eof(false), <ygtsv-full/yagoMultilingualInstanceLabels.tsv>.
    file_reader_20           0 triples,     0 seconds, eof(false), <ygtsv-full/yagoTypes.tsv>.
    file_reader_21           0 triples,     0 seconds, eof(false), <ygtsv-full/yagoLabels.tsv>.
    file_reader_22           0 triples,     0 seconds, eof(false), <ygtsv-full/yagoGeonamesData.tsv>.
    file_reader_23           0 triples,     0 seconds, eof(false), <ygtsv-full/yagoWikipediaInfo.tsv>.
    file_reader_24           0 triples,     0 seconds, eof(false), <ygtsv-full/yagoTransitiveType.tsv>.
    file_reader_25           0 triples,     0 seconds, eof(false), <ygtsv-full/yagoSources.tsv>.
    big3store=# inv est 244800067
    [2016/03/10 18:27:18] started.
    [2016/03/10 18:28:31]    227891/244800067 (  0.09%)
    [2016/03/11 16:14:14] estimated.

### 2. Load Column Dump Files ###

While there are several methods for loading TSV data into PostgreSQL
tables, using COPY command of psql is one of the most efficient way
for performing the task. It is recommended to drop indices before
loading.

* (P-2-1) Create or reset tables for storing column dump files.

* (P-2-2) Load column dump files into those tables.

#### example ####

Procedure (P-2-1) should be executed using function *reset_ts* defined
in 'encode.plpgsql'. This function creates only the basic table
structure. There is no need to build indeces on fields of this table.

    $ cd $(B3S)/src
    $ psql -h $(EpgsqlDir) -p $(PortNumber) $(UserName)
    # \i plpgsql/encode.plpgsql
	# SELECT reset_ts('ts_yago2s_col1');
	# SELECT reset_ts('ts_yago2s_col2');
	# SELECT reset_ts('ts_yago2s_col3');
	# SELECT reset_ts('ts_yago2s_col4');
	# SELECT reset_ts('ts_yago2s_col5');
    # \d ts_yago2s_col1
                             Table "public.ts_yago2s_col1"
     Column |  Type   |                       Modifiers
    --------+---------+--------------------------------------------------------
     sid    | integer | not null default nextval('ts_yago2s_col1_serial'::regclass)
     tid    | text    | 
     s      | text    | not null
     p      | text    | not null
     o      | text    | not null
    Indexes:
        "ts_yago2s_col1_pkey" PRIMARY KEY, btree (sid)

Procedure (P-2-2) should be executed using *COPY* command of psql.

    # COPY ts_yago2s_col1 FROM '/tmp/ts_yago2s_col1.tsv;
    # COPY ts_yago2s_col2 FROM '/tmp/ts_yago2s_col2.tsv;
    # COPY ts_yago2s_col3 FROM '/tmp/ts_yago2s_col3.tsv;
    # COPY ts_yago2s_col4 FROM '/tmp/ts_yago2s_col4.tsv;
    # COPY ts_yago2s_col5 FROM '/tmp/ts_yago2s_col5.tsv;

### 3. Execute Server Side Encoding Functions ###

Triples stored in above tables can be encoded using server side
functions written in PL/pgSQL. In order to have efficient execution
of encoding procedure, some PostgreSQL server parameter should be corrected (see
[install.md](install.html)). If 'postgresql.conf' file was modified,
the PostgreSQL server must be restarted. Note that these processes
should be executed not parallelly but sequentially. They must be
executed on the front server.

* (P-3-1) Create or reset tables for storing encoded triples.

* (P-3-2) Create or reset tables for storing string-id mappings.

* (P-3-3) Perform encoding process.

* (P-3-4) Export all encoded column tables.

* (P-3-5) Compress exported column dump files.

#### example ####

Procedure (P-3-1) should be executed using function *reset_tse*
defined in 'encode.plpgsql'. This function only creates basic table
structure. Indices should be built using a function described in
(P-4-5) example on each data server.

    $ cd $(B3S)/src
    $ psql -h $(EpgsqlDir) -p $(PortNumber) $(UserName)
    # \i plpgsql/encode.plpgsql
    # SELECT reset_tse('tse_yago2s_col1');
    # SELECT reset_tse('tse_yago2s_col2');
    # SELECT reset_tse('tse_yago2s_col3');
    # SELECT reset_tse('tse_yago2s_col4');
    # SELECT reset_tse('tse_yago2s_col5');
	# \d tse_yago2s_col1
                          Table "public.tse_yago2s_col1"
     Column |   Type   |                          Modifiers
    --------+----------+--------------------------------------------------------------
     sid    | integer  | not null default nextval('tse_yago2s_col1_serial'::regclass)
     tid    | bigint   | 
     s      | bigint   | not null
     p      | bigint   | not null
     o      | bigint   | 
     orl    | real     | 
     oty    | smallint | 
    Indexes:
        "tse_yago2s_col1_pkey" PRIMARY KEY, btree (sid)

Procedure (P-3-2) should be executed using functions *reset_si* and
*reset_sip* defined in 'encode.plpgsql'. Function *reset_si* resets a
root table for accessing string-id mappings. It also prepares default
partition table of it. Function *reset_sip* resets a partition table
after creating a root string-id mapping table. While the encoding
process requires *str* always to be indexed, cost for adding a new
string-id pair may become quite large for string-id
mapping table. Therefore, partitioning will be effective for reducing
such costs.

    # SELECT reset_si('si_yago2s');
    # SELECT reset_sip('si_yago2s', 'col1');
    # SELECT reset_sip('si_yago2s', 'col2');
    # SELECT reset_sip('si_yago2s', 'col3');
    # SELECT reset_sip('si_yago2s', 'col4');
    # SELECT reset_sip('si_yago2s', 'col5');
    # \d
                        List of relations
     Schema |             Name              |   Type   | Owner
    --------+-------------------------------+----------+--------
     public | si_yago2s                     | table    | knitta
     public | si_yago2s_col1                | table    | knitta
     public | si_yago2s_col2                | table    | knitta
     public | si_yago2s_col3                | table    | knitta
     public | si_yago2s_col4                | table    | knitta
     public | si_yago2s_col5                | table    | knitta
     public | si_yago2s_spool               | table    | knitta
    :

Procedure (P-3-3) can be performed by calling function
*encode_triples*. It will take about 10-15 hours for processing 1 of 5
columns which are partitioned from YAGO2s.

    # SELECT encode_triples('ts_yago2s_col1', 'tse_yago2s_col1', 'si_yago2s', 'col1');
    # SELECT encode_triples('ts_yago2s_col2', 'tse_yago2s_col2', 'si_yago2s', 'col2');
    # SELECT encode_triples('ts_yago2s_col3', 'tse_yago2s_col3', 'si_yago2s', 'col3');
    # SELECT encode_triples('ts_yago2s_col4', 'tse_yago2s_col4', 'si_yago2s', 'col4');
    # SELECT encode_triples('ts_yago2s_col5', 'tse_yago2s_col5', 'si_yago2s', 'col5');

Procedure (P-3-4) should be executed using *COPY* command of psql.

    # COPY tse_yago2s_col1 TO '/tmp/tse_yago2s_col1.tsv;
    # COPY tse_yago2s_col2 TO '/tmp/tse_yago2s_col2.tsv;
    # COPY tse_yago2s_col3 TO '/tmp/tse_yago2s_col3.tsv;
    # COPY tse_yago2s_col4 TO '/tmp/tse_yago2s_col4.tsv;
    # COPY tse_yago2s_col5 TO '/tmp/tse_yago2s_col5.tsv;

Procedure (P-3-5) should be executed using *bzip2* (or similar) 
utility.

    $ bzip2 /tmp/tse_yago2s_col1.tsv
    $ bzip2 /tmp/tse_yago2s_col2.tsv
    $ bzip2 /tmp/tse_yago2s_col3.tsv
    $ bzip2 /tmp/tse_yago2s_col4.tsv
    $ bzip2 /tmp/tse_yago2s_col5.tsv

### 4. Load Column Dump File on each Data Server Machine ###

* (P-4-1) Transfer each column dump file to corresponding server.

* (P-4-2) Decompress the transferred file.

* (P-4-3) Create or reset a table for storing encoded triples.

* (P-4-4) Load column dump files into those tables.

* (P-4-5) Build indices on the table.

#### example ####

Procedure (P-4-1) can be executed using *scp* command. You can choose
any other file transferring tool. Following example is executed on a
front server.

    $ scp -p /tmp/tse_yago2s_col1.tsv.bz2 ds1.b3s.org:spool
    $ scp -p /tmp/tse_yago2s_col2.tsv.bz2 ds2.b3s.org:spool
    $ scp -p /tmp/tse_yago2s_col3.tsv.bz2 ds3.b3s.org:spool
    $ scp -p /tmp/tse_yago2s_col4.tsv.bz2 ds4.b3s.org:spool
    $ scp -p /tmp/tse_yago2s_col5.tsv.bz2 ds5.b3s.org:spool

Procedure (P-4-2) will be executed on each data server machine. It may
be convenient to decompress it to a file having short path name.

	$ bzcat spool/ts_yago2s_col1.tsv.bz2 > /tmp/ts1.tsv

Procedure (P-4-3) should be executed using function *reset_tse*
defined in 'encode.plpgsql'. This function only creates basic table
structure.

    $ cd $(B3S)/src
    $ psql -h $(EpgsqlDir) -p $(PortNumber) $(UserName)
    # \i plpgsql/encode.plpgsql
    # SELECT reset_tse('yago2s_5x1a_c1');

Procedure (P-4-4) should be executed using *COPY* command of psql.

    # COPY yago2s_5x1a_c1 FROM '/tmp/ts1.tsv';

Procedure (P-4-5) should be executed using function *index_tse*
defined in 'encode.plpgsql'.

    # SELECT index_tse('yago2s_5x1a_c1');
    # \d yago2s_5x1a_c1
      Table "public.yago2s_5x1a_c1"
     Column |   Type   | Modifiers 
    --------+----------+-----------
     sid    | integer  | not null
     tid    | bigint   | 
     s      | bigint   | not null
     p      | bigint   | not null
     o      | bigint   | 
     orl    | real     | 
     oty    | smallint | 
    Indexes:
        "yago2s_5x1a_c1_pkey" PRIMARY KEY, btree (sid)
        "yago2s_5x1a_c1_ix_o" btree (o)
        "yago2s_5x1a_c1_ix_orl" btree (orl)
        "yago2s_5x1a_c1_ix_oty" btree (oty)
        "yago2s_5x1a_c1_ix_p" btree (p)
        "yago2s_5x1a_c1_ix_po" btree (p, o)
        "yago2s_5x1a_c1_ix_s" btree (s)
        "yago2s_5x1a_c1_ix_so" btree (s, o)
        "yago2s_5x1a_c1_ix_sp" btree (s, p)
        "yago2s_5x1a_c1_ix_spo" btree (s, p, o)
        "yago2s_5x1a_c1_ix_tid" btree (tid)
    #
    $ rm /tmp/ts.tsv

===> END OF LINE <===

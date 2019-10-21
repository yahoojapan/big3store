## Getting Started on AWS ##

This document instructs how to start big3store distributed experiments
using YAGO on Amazon Web Service (AWS). Most tasks will be performed
using the user interface tool included in the big3store
distribution. Detailed command descriptions can be referred from
[user-interface.md](user-interface.md).

* Copyright (C) 2019 UP FAMNIT and Yahoo Japan Corporation
* Version 0.4
* Since: May, 2019
* Author: Kiyoshi Nitta <knitta@yahoo-corp.jp>

### 0. Prepare AWS environment ###

An activated AWS account is required. Several account information must
be reflected to big3store's parameter file and saved in a AWS
bucket. The tasks consists of following 4 steps.

* (P-0-1) Get an AWS account
* (P-0-2) Download the big3store source
* (P-0-3) Install and configure AWS CLI
* (P-0-4) Configure AWS parameters
* (P-0-5) Register the active big3store archive file

Concrete examples are shown in following sebsections.

Here is the summary of AWS EC2 instances invoked for performing the
tasks described in this document.

| role         | task        | type      | number | disk (GB) |
|-|-|-|-|-|
| installer    | preparation | t2.micro  |    1   |   10      |
| front server | preparation | m1.large  |    1   |   80      |
| front server | benchmark   | t2.micro  |    1   |   50      |
| data server  | benchmark   | m3.medium |    5   |   30      |

Column 'role' shows the role of instances. The *installer* is a role
for starting a front server. The *front server* is a role for
performing preparation, bootstrap, benchmark, and query execution
tasks. The *data server* is a role for storing and retrieving actual
triple data. Column 'task' shows the task for which those instances
works. The *preparation* is a task for building the experiment
environment and loading data into big3store. In this document, tsv
format of YAGO2s data set is partitioned into 5 columns and
loaded. The *benchmark* is a task for performing sets of benchmark
tasks or ad hoc queries. Column 'type' shows recommended AWS EC2
instance type. Column 'number' shows the number of invoked
instances. Column 'disk' shows recommended disk size of each instance
reuired for performing tasks.

#### (P-0-1) Get an AWS account ####

You can create and activate a new AWS account by following the
instruction of the link. Note that you might wait for a while until
completing the activation after registring credit card information.

https://aws.amazon.com/premiumsupport/knowledge-center/create-and-activate-aws-account

#### (P-0-2) Download the big3store source ####

The source archive of big3store must be downloaded on a file system of
a linux machine (installer). You can use any type of machine. One of
the most convenient choice is using a t2.micro instance of Amazon
linux 2 operating system. Following page will help to perform the
task.

https://aws.amazon.com/ec2/getting-started

After createing an EC2 instance and connecting to it, you can download
big3store source files on the machine by typing following commands.

    $ sudo yum install -y git
    $ git clone https://github.com/big3store/big3store.git

#### (P-0-3) Install and configure AWS CLI ####

Script files in `aws/bin` require AWS CLI. It should be installed and
configured correctly. See [this
instruction](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html)
for the detail. The easiest way for completing them is shown below as
an example.

    $ sudo yum install python
    $ curl -O https://bootstrap.pypa.io/get-pip.py
    $ python get-pip.py --user
    $ export PATH=~/.local/bin:$PATH
    $ pip install awscli --upgrade --user
    $ aws configure
    AWS Access Key ID [None]: AKIAIOSFODNN7EXAMPLE
    AWS Secret Access Key [None]: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
    Default region name [None]: us-east-1
    Default output format [None]: text

You have to provide your account's `AWS Access Key ID` and `AWS Secret
Access Key` at the last command. See [this
instruction](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html#cli-quick-configuration)
for obtaining them.

#### (P-0-4) Configure AWS parameters ####

You can find AWS configuration file aws.cf at the path
big3store/aws/cf. Please look through all parameters. The file
contains precise description of each parameter. You must at least
update following parameters in this time.

* aws_ec2_key_name
* aws_ec2_iam_fleet_role
* aws_ec2_instance_profile_arn
* aws_ec2_instance_profile_name
* aws_ec2_security_group_name
* aws_ec2_security_group_id
* aws_sns_arn

    $ cd ~/big3store/aws/cf
    $ vi aws.cf

While an AWS security group could be created using AWS console, you
can create it using following command. This command automatically add
access permission from the working instance to bigstore front and data
servers. It should be used when you change the instance for performing
the big3store installation task.

    $ ~/big3store/aws/bin/create-security-group.sh

#### (P-0-5) Register the active big3store archive file ####

An AWS S3 bucket that can save important files permanently must be
created first. While the task could be performed using the [AWS
console](https://aws.amazon.com/console/?nc1=h_ls), following command
could be used instead. A successful operation will return the
following message. Confirm that the succeeded bucket and region names
were properly set in the AWS configuration file aws.cf at the path
big3store/aws/cf.

    $ aws s3 mb s3://b3s-us-east-1-knitta --region us-east-1
    make_bucket: b3s-us-east-1-knitta

Following command stores necessary big3store source and configuration
files to an archive file on the AWS S3 bucket. This command should be
used when you change the configuration.

    $ ~/big3store/aws/bin/setup-bucket.sh

Stored archive files could be confirmed using following command.

    $ ~/big3store/aws/bin/s3-ls.sh | grep oss
    2019-08-23 15:53:08    2396939 spool/ossb3s-20190823155256.tbz
    2019-09-03 15:05:37    2399447 spool/ossb3s-20190903150527.tbz

An unnecessary archive file could be removed using following comand.

    $ ~/big3store/aws/bin/s3-rm.sh spool/ossb3s-20190823155256.tbz
    delete: s3://b3s-us-east-1-knitta/spool/ossb3s-20190823155256.tbz

### 1. Generate Column Dump Files ###

| Input       | source triple data in tsv format of YAGO2s |
| Output      | column dump tsv files |
| Time        | about 10 hours on m1.large for processing YAGO2s |
| Volume Size | 80GB on the front server |

The first task for loading triples is to generate a TSV triple dump
file for each column. The task can be performed by following
procedures below.

* (P-1-1) Investigate source files and create a distribution function.
* (P-1-2) Perform distribution and generate column dump files.

All tasks should be performed from a front server. It can be invoked
by following command.

    $ ~/big3store/aws/bin/run-front-server-prep.sh

After waiting several seconds, following command will report the ip
address of the front server.

    $ ~/big3store/aws/bin/describe-front-server-instances.sh
    i-0990ace83a09d1424	t2.micro	ec2-100-26-98-240.compute-1.amazonaws.com
    running

You can login to the front server instance using following
command. The ssh key and the ip address must be corrected
appropriately.

    $ ssh -i id_rsa.20190908a ubuntu@ec2-100-26-98-240.compute-1.amazonaws.com

While performing following preparation tasks, all AWS EC2 cpu credits
might be consumed. Therefore, the unlimited burst mode of the front
server instance should be turned on for continuing the tasks
normally. See [Burstable Performance
Instances](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/burstable-performance-instances.html)
for more detail.

#### (P-1-1) Investigate source files and create a distribution function ####

| Input       | source triple data in tsv format of YAGO2s |
| Output      | predicate dictionaries |
| Time        | about 3 hours on m1.large for processing YAGO2s |
| Volume Size | 25GB on the front server |

Here, entire source triple files must be obtained and placed under
~/big3store/src. Source triple files must be tab separated text (TSV)
format.

    $ cd ~/big3store/src
    $ mkdir yg2s
    $ cd yg2s
    $ wget http://resources.mpi-inf.mpg.de/yago-naga/yago2.5/yago2s_tsv.7z
    $ 7za x yago2s_tsv.7z

Relative path for each file must be listed at 'data_files' property of
"b3s.app.prep" application configuration file. You should also decide
the number of columns to be partitioned and modify 'data_server_nodes'
and 'name_of_triple_tables' properties. Sample definitions for 5, 10,
25, and 50 columns are included as commants in "b3s.app.prep". You
might change 'distribution_algorithm' property for selecting the
distribution algorithm. If you choose "predicate_based" value for it,
you should change 'name_of_pred_clm_table' property for including the
total number of columns. Properties 'name_of_pred_clm_table',
'name_of_pred_freq_table', and 'name_of_pred_string_id' should contain
the short string for showing the triple data set name (ex. 'yg2s' for
YAGO2S).

    $ cd ~/big3store/src
    $ vi b3s.app.prep

Next, a front server for the preparation task should be invoked by
following make target.

    $ sudo make start-fs-prep

You can confirm parameters of the front server Erlang node.

    $ make ps

If you find some wrong parameters, you can terminate the node and
start another front server node. You could confirm by the second make
target that no big3store Erlang node is running.

    $ sudo make term FS=fs
    $ make ps
    $ sudo make start-fs-prep

Then, you can start the big3store user interface tool. If you want to
use 'rlwarp' editing feature, 'sui2' or 'sui3' target should be used
instead.

    $ make sui

While the actual prompt of the user insterface is like 'ui2 08:14 [4]
big3store=#', it will be just written as 'ui# ' in this document. The
first task is booting necessary processes.

    ui# boot

You can confirm important configuration parameters by following user
interafce command if the boot process succeeded.

    ui# config

You might confirm the source triple data csv files to be investigated
here. If no data file was set, you should correct 'data_files'
property in 'b3s.app.prep' and restart the front server node.

    ui# data_files

The main investigation task could be started by following user
interface command. This task is required when you chosen
'predicate_based' value for 'distribution_algorithm' property. You can
skip reading until (P-1-2) if you chosen other values.

    ui# investigate

This task will take about 3 hours on m1.large instance for processing
whole YAGO2s triples (244,800,042 triples). Its progress could be
reported by following command.

    ui# investigate est 244800042

Following command waits until completing the task. Note that you will
not be able to enter more user interface commands until the
investigation process terminated.

    ui# investigate is finished

You will receive an AWS SNS notification when finished by typing
following statement while waiting.

    ui# aws sns publish investigation task was finished

The distribution function was created on an Erlang process. Because it
will disappear when the front server was terminated, it should be
saved in some permanent storages. You can store it in an AWS S3 bucket
by following command.

    ui# aws bucket save predicate dictionaries

The investigation and distribution tasks print progress messages
periodically to the user interface terminal. Its frequency could be
adjusted by setting 'store_report_frequency' property of 'b3s_state'
process on the front server. The property could also be set from
"b3s.app" application configuration file. Its default value is 10,000.

The investigation and distribution tasks invoke 'file_reader'
processes simultaneously. The starting time and precise progress could
be observed using following command (ex. file reader 1).

    ui# gen_server_call FS file_reader_1 GP all

#### (P-1-2) Perform distribution and generate column dump files ####

| Input       | source triple data in tsv format of YAGO2s |
| Input       | predicate dictionaries |
| Output      | column dump tsv files |
| Time        | 6.7 hours on m1.large for processing YAGO2s |
| Volume Size | 50GB on the front server |

The distribution task reads source triple data csv files and write
partitioned csv files. This task will take about 6.7 hours on m1.large
instance for processing whole YAGO2s triples (244,800,042 triples).

    ui# distribute

Its progress could be reported by following command.

    ui# investigate est 244800042

Following command waits until completing the task. Note that you will
not be able to enter more user interface commands until the
distribution process terminated.

    ui# distribute is finished

You will receive an AWS SNS notification when finished by typing
following statement while waiting.

    ui# aws sns publish distribution task was finished

While all triple data were copied in column csv files, it is safe to
save them anywhere. Front server instances will be created as AWS spot
instances by big3store scripts. They might terminate suddenly
according to changes of spot prices. Their disk images will be also
lost in that case. Following command saves the result of the
distribution process to an AWS bucket.

    ui# aws bucket save column tables

Those column dump csv files on the local file system are located at
'~/big3store/bak'. The location path could be changed by modifying
property 'column_file_path' of 'triple_distributor' process on the
front server. Its default value is "bak/column-~2..0w.tsv". You can
change and confirm it by following commands.

    ui# gen_server_call FS TD PP column_file_path /tmp/column-~2..0w.tsv
    ui# property find FS TD col
    [{column_file_path,"/tmp/column-~2..0w.tsv"}]

### 2. Execute Server Side Encoding Functions ###

| Input       | predicate dictionaries |
| Input       | column dump tsv files |
| Output      | encoded column dump tsv files |
| Output      | string id mapping table dump tsv files |
| Output      | front server AMI for the operation |
| Time        | 4-5 days on m1.large for processing YAGO2s |
| Volume Size | 80GB on the front server |

Most triple stores encode URI strings to integer codes for saving
memory and disk resources. Big3store also takes the same
approach. While big3store performs this task using PostgreSQL,
partitioned column triple data csv files must be loaded in psql
tables. Following user interface command executes appropreate SQL
statement on connected PostgreSQL server.

    ui# psql load column tables

The encoding task reads all triples from loaded tables and write
encoded triples and mappings into newly created tables. If you set up
'aws_sns_arn' parameter correctly at (P-0-3), you will receive a
notification message when the task finished. This task will take 4-5
days on m1.large instance for processing whole YAGO2s triples
(244,800,042 triples).

    ui# psql encode column tables

While the process takes very long time for completing, the progress
messages are only printed when the process was executed
manually. Refer [load-triples.md](load-triples.md) for executing
manual operations.

The result tables must be saved to a permanent storage. Following
commands store all result tables of the encoding task to an AWS S3
bucket.

    ui# psql save encoded column tables
    ui# aws bucket save encoded column tables
    ui# psql save string id tables
    ui# aws bucket save string id tables

Because it takes several hours for loading string-id mapping table on
a front server, it is convenient for creating an Amazon Machine Image
(AMI) in this timing.

    ui# aws create image b3sfs-20190531a

After entering above command, the front server instance will be
rebooted immediately. A front server Erlang node using 'b3s.app.aws'
application preference file will be invoked and booted automatically
in a few minutes. It also create an AWS EC2 launch template including
the ami id of the image. You can start front server instances using
the template. Following script file starts an front server instance
using the template not using spot instance request.

    $ big3store/aws/bin/run-front-server-non-spot.sh

Files genrated in this section could be confirmed using following
commands.

    ui$ bucket list dat
    2019-09-19 15:29:28        424 bin/boot-b3s-data-server.sh
    2018-08-20 12:31:14          0 dat/
    2019-03-28 14:08:47       2374 dat/pc_yg2s_col5.txt
    2019-04-12 10:32:52       2781 dat/pf_yg2s.txt
    2019-04-12 10:32:53       3044 dat/psi_yg2s.txt
    2019-02-17 23:11:02 1232084748 dat/string_id_triplestore.7z
    2019-02-20 14:33:43  283377131 dat/tse_predicate_based_01_of_05.7z
    2019-02-17 21:09:35  285745279 dat/tse_predicate_based_02_of_05.7z
    2019-02-17 21:24:41  251627182 dat/tse_predicate_based_03_of_05.7z
    2019-02-17 21:43:42  310568297 dat/tse_predicate_based_04_of_05.7z
    2019-02-17 22:06:53  356511059 dat/tse_predicate_based_05_of_05.7z

    ui$ aws describe image
    2019-09-20T00:15:18.000Z	ami-0b7aba1e7f3d76777	b3s-front-server
    2019-09-20T00:13:25.000Z	ami-0b7aba1e7f3d76777	b3sfs-20190920a
    				snap-089b6c5881283ebd2	50
    2019-09-20T00:13:42.827Z	snap-089b6c5881283ebd2	50
    2019-09-09T06:28:09.632Z	snap-071cb097b2290e450	50

### 3. Load Column Dump File on each Data Server Machine ###

| Input       | predicate dictionaries |
| Input       | encoded column dump tsv files |
| Input       | front server AMI for the operation |
| Time        | about 1 hour on m3.medium data servers |
| Volume Size | 50GB on the front server |
| Volume Size | 30GB on a data server |

Following command starts data server instances and loads appropreate
data on each data server.

#### ex. 5 columns configuration:

    ui# aws run dsa 1 dsb 1 dsc 1 dsd 1 dse 1

You could also start data servers when a front server was started
using the above image. Before creating a front server image,
'b3s.app.aws.boot_ds' file should be modified. When a front server
instance was started using the image, a front server Erlang node using
'b3s.app.aws.boot_ds' is automatically started and booted. You can
specify any user interface statements to 'ui_run_command_boot_fs'
property. They are executed sequentially when the front server was
booted. Statements start with 'aws run ...' for invoking 5, 10, 25,
and 50 column data servers are already written as comments. You should
uncomment only an appropreate one. It will take about 1 hour for all
data servers to load allocated column dump file from the AWS S3
bucket, while data servers perform loading tasks simultaneously. If
you set up 'aws_sns_arn' parameter correctly at (P-0-3), you will
receive a notification message when all data servers completed
loading. Alive data servers are listed at 'data_server_nodes' section
of the report printed by the following command.

    ui# config

If you have built encoded column dump files for 5 columns
configuration and started 5 data servers, you will find 5 data servers
having different column numbers at 'data_server_nodes'. Otherwise,
data servers failed booting. While there are some cases that data
servers failed booting, the most often case is that some data servers
have the same column number. Following procedure will adjust the
configuratio. For example, imagine that you found 2 data servers
having the same column number '2'. Following command will remove one
of them from 'data_server_nodes'.

    ui# aws unregister DS2

Then, you can find the unregistered node name at 'down_data_servers'
section of the 'config' report. Let the name as
'dsk@ip-172-31-86-195.ec2.internal' in this example. Definition of the
column number must be corrected.

    ui# property append data server 5 dsk@ip-172-31-86-195.ec2.internal
    ui# property constructed clm_row_conf
    ui# remote restart data servers DS5

You have to wait a few minutes until data server DS5 wakes
up. Following command prints the restarted time of the node.

    ui# property find DS5 NS start

If the above command returns the correct time, the remained task is to
start table loading processes on the all data servers.

    ui# gen_server_call FS BS propagate
    ui# property backup b3s state to all
    ui# remote load all encoded column tables force

You have to wait at most 1 hour for loading all tables. You might hope
monitoring their progresses using following commands.

    ui# remote all top 1
    ui# remote all df -h .

If you find specific data servers which seem to fail loading,
following command will be useful.

    ui# remote DS5 psql load encoded column table S force

### 4. Perform Benchmark Tasks ###

Benchmark tasks can be executed using following command.

    ui# batch_benchmark task_yg_0001 task_yg_0002 task_yg_0004 task_yg_0013 task_yg_0014 task_yg_0015 task_yg_0004m task_yg_0013m task_yg_0014m

While you will see the summary report on the user interface console,
you can get more detailed execution report of specific task. Example:
task_yg_0004

    ui# report_benchmark task_yg_0004

Following command reports memory usage of each data server nodes.

    ui# memory summary

If some data servers consume much memory, following command can
restart Erlang beam interpreters. Exampe: DS2 and DS4

    ui# remote restart data servers DS2 DS4

Benchmark performances may vary on hot or cold statuses of PostgreSQL
servers on data servers. Note that they are hot status when data
servers have just loaded encoded column tables. The easiest method for
performing cold start experiments might be rebooting data servers.

    ui# aws reboot all

You have to wait for a while that all data servers become ready. You
can monitor the activation status of data servers using following
command.

    ui# config

You might find some nodes are listed at 'down_data_servers' section of
the above command. If the list doesn't become '[]' (null) by waiting
several minutes, you have to restart those nodes. While you can
restart them manually, following user interface command reboots data
server instances. Exampe: DS2 and DS4

    ui# aws reboot DS2 DS4

Sometimes, you might have to login data server instances manually and
restart data server Erlang nodes. Followng command produce the server
address list file at big3store/aws/cf/servers.cf.

    ui# aws servers.cf

You can connect to DS2 instance by following shell script.

    $ . ~/big3store/aws/cf/servers.cf
    $ . ~/big3store/aws/cf/aws.cf
    $ ssh -i $aws_ec2_key_name ubuntu@$ds_c02r01

If the front server behaves strangely, one convenient solution is
rebooting. Following commands can reserve configuration information
during the reboot. Because the information is saved on the data
servers, they must keep running. You should copy a node name among
those data server nodes printed by the 2nd command below.

    ui# property backup b3s state to all
    ui# remote all property find S NS node_state
    $ (cd ~/big3store/src; sudo kill -9 <front server node process>; make start-fs-aws)
    ui# boot
    ui# property restore b3s state from <a data server node>

### 5. Session Logs of Operations ###

This section demonstrates exact operations and tool or system outputs
using logs of sessions after the sucsessful installation of 25 columns
and 1 row random distriburion. Following commands confirm the launch
template and image existences. The latest launch template for starting
the front server refers image 'ami-0d17477102b269872'. The image
existence is confirmed.

    pts/3 13:32 knitta@ip-172-31-32-126[39] pwd
    /home/knitta/wrk/b3s/oss/big3store/aws/bin
    pts/3 13:32 knitta@ip-172-31-32-126[40] describe-launch-templates.sh
    describe-launch-templates.sh
    2019-07-04T21:32:42.000Z	ami-0d17477102b269872	b3s-front-server
    pts/3 13:33 knitta@ip-172-31-32-126[41] describe-images.sh
    describe-images.sh
    2019-07-04T06:48:17.000Z	ami-0d17477102b269872	b3sfs-20190704b
    snap-03bd9547c02c97056	50

Following commands confirm the encoded column dump files of 25 columns
partition. They are stored at 'dat' directory of the repository.

    pts/3 13:36 knitta@ip-172-31-32-126[44] pwd
    /home/knitta/wrk/b3s/oss/big3store/aws/bin
    pts/3 13:36 knitta@ip-172-31-32-126[45] s3-ls.sh | grep of_25 | grep tse_random
    2019-04-16 07:55:18   82534301 dat/tse_random_01_of_25.7z
    2019-04-16 07:59:19   82578601 dat/tse_random_02_of_25.7z
    2019-04-16 08:03:22   82522346 dat/tse_random_03_of_25.7z
    2019-04-16 08:07:28   82461925 dat/tse_random_04_of_25.7z
    2019-04-16 08:11:31   82521778 dat/tse_random_05_of_25.7z
    2019-04-16 08:15:33   82533391 dat/tse_random_06_of_25.7z
    2019-04-16 08:19:35   82559510 dat/tse_random_07_of_25.7z
    2019-04-16 08:23:37   82410714 dat/tse_random_08_of_25.7z
    2019-04-16 08:27:42   82555971 dat/tse_random_09_of_25.7z
    2019-04-16 08:31:42   82482985 dat/tse_random_10_of_25.7z
    2019-04-16 08:35:42   82594074 dat/tse_random_11_of_25.7z
    2019-04-16 08:39:44   82433978 dat/tse_random_12_of_25.7z
    2019-04-16 08:43:47   82553088 dat/tse_random_13_of_25.7z
    2019-04-16 08:47:50   82583551 dat/tse_random_14_of_25.7z
    2019-04-16 08:51:57   82562336 dat/tse_random_15_of_25.7z
    2019-04-16 08:56:00   82562715 dat/tse_random_16_of_25.7z
    2019-04-16 09:00:01   82493030 dat/tse_random_17_of_25.7z
    2019-04-16 09:04:06   82464418 dat/tse_random_18_of_25.7z
    2019-04-16 09:08:09   82621932 dat/tse_random_19_of_25.7z
    2019-04-16 09:12:13   82566691 dat/tse_random_20_of_25.7z
    2019-04-16 09:16:15   82584888 dat/tse_random_21_of_25.7z
    2019-04-16 09:20:17   82598124 dat/tse_random_22_of_25.7z
    2019-04-16 09:24:19   82543482 dat/tse_random_23_of_25.7z
    2019-04-16 09:28:21   82627560 dat/tse_random_24_of_25.7z
    2019-04-16 09:32:21   82519258 dat/tse_random_25_of_25.7z

Note that following 'big3store/src/b3s.app.aws.boot_ds' file included
in 'ami-0d17477102b269872' image has following definition (show only
key properties). Property 'distriburion_algorithm' must match with the
file name of the above encoded column dump files. Property
'ui_run_command_boot_fs' is a list of user interface commands. They
are sequentially executed in its order when the front server was
booted successfully. Command 'aws run ..' starts 25 data server
instances. This example command starts a row instance for each column.

    :
    {distribution_algorithm,   random},
    :
    {ui_run_command_boot_fs, [
			      "aws run dsa 1 dsb 1 dsc 1 dsd 1 dse 1 dsf 1 dsg 1 dsh 1 dsi 1 dsj 1 dsk 1 dsl 1 dsm 1 dsn 1 dso 1 dsp 1 dsq 1 dsr 1 dss 1 dst 1 dsu 1 dsv 1 dsw 1 dsx 1 dsy 1",
			      "inv save",
			      "aws sns publish front server booted"
			     ]},
    :

Following command starts a front server instance. When the instance
was started successfully, a big3store front server Erlang node is
automatically started and booted using 'b3s.app.aws.boot_ds'
application preference file.

    pts/3 11:05 knitta@ip-172-31-32-126[9] pwd
    /home/knitta/wrk/b3s/oss/big3store/aws/bin
    pts/3 11:06 knitta@ip-172-31-32-126[10] ./run-front-server-non-spot.sh
    739193317138	r-018fb8140e3801c0b
    INSTANCES	0	x86_64		False	xen	ami-0d17477102b269872	i-0d384d969abe9ebdc	t2.micro	id_rsa.20190311a	2019-07-10T02:06:09.000Z	ip-172-31-95-122.ec2.internal	172.31.95.122		/dev/sda1	ebs	True		subnet-bd41c193	hvm	vpc-6bd50411
    CAPACITYRESERVATIONSPECIFICATION	open
    CPUOPTIONS	1	1
    IAMINSTANCEPROFILE	arn:aws:iam::739193317138:instance-profile/b3s	AIPAI2KYR4PM6GSGTPGCS
    MONITORING	disabled
    NETWORKINTERFACES		12:e4:5d:ce:4b:7e	eni-0ab70287c6208ebe5	739193317138	ip-172-31-95-122.ec2.internal	172.31.95.122	True	in-use	subnet-bd41c193	vpc-6bd50411
    ATTACHMENT	2019-07-10T02:06:09.000Z	eni-attach-0fca5a066bcee8dad	True	0	attaching
    GROUPS	sg-03e13e3b88a0103c7	big3store
    PRIVATEIPADDRESSES	True	ip-172-31-95-122.ec2.internal	172.31.95.122
    PLACEMENT	us-east-1a		default
    SECURITYGROUPS	sg-03e13e3b88a0103c7	big3store
    STATE	0	pending
    STATEREASON	pending	pending
    TAGS	aws:ec2launchtemplate:id	lt-0140cf5e4de21ac21
    TAGS	ServiceName	b3s
    TAGS	ModuleName	b3s-front-server
    TAGS	aws:ec2launchtemplate:version	1
    
    for monitoring the progress:
    tail -f /var/log/cloud-init-output.log
    
    i-0d384d969abe9ebdc	ip-172-31-95-122.ec2.internal	ec2-52-54-210-46.compute-1.amazonaws.com
    
    pts/3 11:08 knitta@ip-172-31-32-126[13] ./describe-front-server-instances.sh
    i-0d384d969abe9ebdc	ip-172-31-95-122.ec2.internal	ec2-52-54-210-46.compute-1.amazonaws.com

Then, you can connect using ssh to the instance. After logging in, you
can start a big3store user interface tool.

    pts/3 11:19 knitta@ip-172-31-32-126[30] ssh -i id_rsa.20190311a ubuntu@ec2-52-54-210-46.compute-1.amazonaws.com
    <rsa.20190311a ubuntu@ec2-52-54-210-46.compute-1.amazonaws.com
    Warning: Permanently added 'ec2-52-54-210-46.compute-1.amazonaws.com,52.54.210.46' (ECDSA) to the list of known hosts.
    Welcome to Ubuntu 18.04.2 LTS (GNU/Linux 4.15.0-1043-aws x86_64)
    
     * Documentation:  https://help.ubuntu.com
     * Management:     https://landscape.canonical.com
     * Support:        https://ubuntu.com/advantage
    
      System information as of Wed Jul 10 11:19:41 JST 2019
    
      System load:  0.0                Processes:           97
      Usage of /:   77.2% of 48.41GB   Users logged in:     0
      Memory usage: 21%                IP address for eth0: 172.31.95.122
      Swap usage:   0%
    
     * MicroK8s 1.15 is out! It has already been installed on more
       than 14 different distros. Guess which ones?
    
         https://snapcraft.io/microk8s
    
      Get cloud support with Ubuntu Advantage Cloud Guest:
        http://www.ubuntu.com/business/services/cloud
    
     * Canonical Livepatch is available for installation.
       - Reduce system reboots and improve kernel security. Activate at:
         https://ubuntu.com/livepatch
    
    119 packages can be updated.
    0 updates are security updates.
    
    
    Last login: Thu Jul  4 15:28:22 2019 from 52.197.205.177
    ubuntu@ip-172-31-95-122:~$ zsh
    pts/0 11:19 ubuntu@ip-172-31-95-122[1] cd big3store/src
    pts/0 11:20 ubuntu@ip-172-31-95-122[2] sudo make ui-main
    erl -pa ../ebin -pa /home/ubuntu/epgsql/_build/default/lib/epgsql/ebin -noshell -run user_interface main -name ui@ip-172-31-95-122.ec2.internal
    big3store v0.2
    ui 11:20 [1] big3store=# config
    
                        front_server_nodes : ['fs@ip-172-31-95-122.ec2.internal']
                         num_of_empty_msgs : 10
                                block_size : 100
                    distribution_algorithm : random
                    name_of_pred_clm_table : pc_yg2s_col25
                   name_of_pred_freq_table : pf_yg2s
                   name_of_string_id_table : string_id_triplestore
                    name_of_pred_string_id : psi_yg2s
                         down_data_servers : []
    
                                ====> END OF LINE <====                             

Just after invoking the front server, data server instances are not
listed by 'config' command. Several minutes later, the 'config' report
changes like following.

    ui 11:28 [8] big3store=# config
    
                        front_server_nodes : ['fs@ip-172-31-95-122.ec2.internal']
                         data_server_nodes : "dsg@ip-172-31-80 (1)"
                                           : "dsj@ip-172-31-84 (2)"
                                           : "dsc@ip-172-31-92 (3)"
                                           : "dsu@ip-172-31-81 (4)"
                                           : "dsx@ip-172-31-81 (5)"
                                           : "dsy@ip-172-31-85 (6)"
                                           : "dsn@ip-172-31-84 (7)"
                                           : "dsi@ip-172-31-82 (8)"
                                           : "dsd@ip-172-31-83 (9)"
                                           : "dsq@ip-172-31-94 (10)"
                                           : "dsm@ip-172-31-90 (11)"
                                           : "dsl@ip-172-31-83 (12)"
                                           : "dsp@ip-172-31-94 (13)"
                                           : "dso@ip-172-31-80 (14)"
                                           : "dsb@ip-172-31-90 (15)"
                                           : "dsh@ip-172-31-85 (16)"
                                           : "dst@ip-172-31-81 (17)"
                                           : "dsf@ip-172-31-86 (18)"
                                           : "dsv@ip-172-31-92 (19)"
                                           : "dsa@ip-172-31-81 (20)"
                                           : "dse@ip-172-31-90 (21)"
                                           : "dsk@ip-172-31-91 (22)"
                                           : "dsw@ip-172-31-94 (23)"
                                           : "dsr@ip-172-31-80 (24)"
                         num_of_empty_msgs : 10
                                block_size : 100
                    distribution_algorithm : random
                    name_of_pred_clm_table : pc_yg2s_col25
                   name_of_pred_freq_table : pf_yg2s
                   name_of_string_id_table : string_id_triplestore
                    name_of_pred_string_id : psi_yg2s
                         down_data_servers : [[{14,
                                                'dss@ip-172-31-91-140.ec2.internal'}]]
    
                                ====> END OF LINE <====                             

While this report saids that column 14 was not successfully booted,
following commands are required for fixing the issue. The last command
'remote load all ..' is automatically executed if no issue arose.

    ui 11:34 [13] big3store=# property append data server 25 dss@ip-172-31-91-140.ec2.internal
    [{'dsg@ip-172-31-80-173.ec2.internal',1},
     {'dsj@ip-172-31-84-145.ec2.internal',2},
     {'dsc@ip-172-31-92-69.ec2.internal',3},
     {'dsu@ip-172-31-81-14.ec2.internal',4},
     {'dsx@ip-172-31-81-183.ec2.internal',5},
     {'dsy@ip-172-31-85-7.ec2.internal',6},
     {'dsn@ip-172-31-84-252.ec2.internal',7},
     {'dsi@ip-172-31-82-105.ec2.internal',8},
     {'dsd@ip-172-31-83-46.ec2.internal',9},
     {'dsq@ip-172-31-94-19.ec2.internal',10},
     {'dsm@ip-172-31-90-129.ec2.internal',11},
     {'dsl@ip-172-31-83-95.ec2.internal',12},
     {'dsp@ip-172-31-94-209.ec2.internal',13},
     {'dso@ip-172-31-80-243.ec2.internal',14},
     {'dsb@ip-172-31-90-178.ec2.internal',15},
     {'dsh@ip-172-31-85-39.ec2.internal',16},
     {'dst@ip-172-31-81-4.ec2.internal',17},
     {'dsf@ip-172-31-86-179.ec2.internal',18},
     {'dsv@ip-172-31-92-118.ec2.internal',19},
     {'dsa@ip-172-31-81-94.ec2.internal',20},
     {'dse@ip-172-31-90-96.ec2.internal',21},
     {'dsk@ip-172-31-91-90.ec2.internal',22},
     {'dsw@ip-172-31-94-71.ec2.internal',23},
     {'dsr@ip-172-31-80-53.ec2.internal',24},
     {'dss@ip-172-31-91-140.ec2.internal',25}]
    ui 11:34 [15] big3store=# property construct clm_row_conf
    constructed clm_row_conf: #{1 => #{1 => 'dsg@ip-172-31-80-173.ec2.internal'},
                                2 => #{1 => 'dsj@ip-172-31-84-145.ec2.internal'},
                                3 => #{1 => 'dsc@ip-172-31-92-69.ec2.internal'},
                                4 => #{1 => 'dsu@ip-172-31-81-14.ec2.internal'},
                                5 => #{1 => 'dsx@ip-172-31-81-183.ec2.internal'},
                                6 => #{1 => 'dsy@ip-172-31-85-7.ec2.internal'},
                                7 => #{1 => 'dsn@ip-172-31-84-252.ec2.internal'},
                                8 => #{1 => 'dsi@ip-172-31-82-105.ec2.internal'},
                                9 => #{1 => 'dsd@ip-172-31-83-46.ec2.internal'},
                                10 => #{1 => 'dsq@ip-172-31-94-19.ec2.internal'},
                                11 => #{1 => 'dsm@ip-172-31-90-129.ec2.internal'},
                                12 => #{1 => 'dsl@ip-172-31-83-95.ec2.internal'},
                                13 => #{1 => 'dsp@ip-172-31-94-209.ec2.internal'},
                                14 => #{1 => 'dso@ip-172-31-80-243.ec2.internal'},
                                15 => #{1 => 'dsb@ip-172-31-90-178.ec2.internal'},
                                16 => #{1 => 'dsh@ip-172-31-85-39.ec2.internal'},
                                17 => #{1 => 'dst@ip-172-31-81-4.ec2.internal'},
                                18 => #{1 => 'dsf@ip-172-31-86-179.ec2.internal'},
                                19 => #{1 => 'dsv@ip-172-31-92-118.ec2.internal'},
                                20 => #{1 => 'dsa@ip-172-31-81-94.ec2.internal'},
                                21 => #{1 => 'dse@ip-172-31-90-96.ec2.internal'},
                                22 => #{1 => 'dsk@ip-172-31-91-90.ec2.internal'},
                                23 => #{1 => 'dsw@ip-172-31-94-71.ec2.internal'},
                                24 => #{1 => 'dsr@ip-172-31-80-53.ec2.internal'},
                                25 => #{1 => 'dss@ip-172-31-91-140.ec2.internal'}}
    ui 11:37 [19] big3store=# remote restart data servers DS25
    restarting data server node of dss@ip-172-31-91-140.ec2.internal.
    ui 11:40 [26] big3store=# remote load all encoded column tables force
    * dsg@ip-172-31-80-173.ec2.internal psql load encoded column table S force
    process {node_state,'dsg@ip-172-31-80-173.ec2.internal'} is busy.
    * dsj@ip-172-31-84-145.ec2.internal psql load encoded column table S force
    process {node_state,'dsj@ip-172-31-84-145.ec2.internal'} is busy.
    * dsc@ip-172-31-92-69.ec2.internal psql load encoded column table S force
    process {node_state,'dsc@ip-172-31-92-69.ec2.internal'} is busy.
    * dsu@ip-172-31-81-14.ec2.internal psql load encoded column table S force
    process {node_state,'dsu@ip-172-31-81-14.ec2.internal'} is busy.
    * dsx@ip-172-31-81-183.ec2.internal psql load encoded column table S force
    process {node_state,'dsx@ip-172-31-81-183.ec2.internal'} is busy.
    * dsy@ip-172-31-85-7.ec2.internal psql load encoded column table S force
    process {node_state,'dsy@ip-172-31-85-7.ec2.internal'} is busy.
    * dsn@ip-172-31-84-252.ec2.internal psql load encoded column table S force
    process {node_state,'dsn@ip-172-31-84-252.ec2.internal'} is busy.
    * dsi@ip-172-31-82-105.ec2.internal psql load encoded column table S force
    process {node_state,'dsi@ip-172-31-82-105.ec2.internal'} is busy.
    * dsd@ip-172-31-83-46.ec2.internal psql load encoded column table S force
    process {node_state,'dsd@ip-172-31-83-46.ec2.internal'} is busy.
    * dsq@ip-172-31-94-19.ec2.internal psql load encoded column table S force
    process {node_state,'dsq@ip-172-31-94-19.ec2.internal'} is busy.
    * dsm@ip-172-31-90-129.ec2.internal psql load encoded column table S force
    process {node_state,'dsm@ip-172-31-90-129.ec2.internal'} is busy.
    * dsl@ip-172-31-83-95.ec2.internal psql load encoded column table S force
    process {node_state,'dsl@ip-172-31-83-95.ec2.internal'} is busy.
    * dsp@ip-172-31-94-209.ec2.internal psql load encoded column table S force
    process {node_state,'dsp@ip-172-31-94-209.ec2.internal'} is busy.
    * dso@ip-172-31-80-243.ec2.internal psql load encoded column table S force
    process {node_state,'dso@ip-172-31-80-243.ec2.internal'} is busy.
    * dsb@ip-172-31-90-178.ec2.internal psql load encoded column table S force
    process {node_state,'dsb@ip-172-31-90-178.ec2.internal'} is busy.
    * dsh@ip-172-31-85-39.ec2.internal psql load encoded column table S force
    process {node_state,'dsh@ip-172-31-85-39.ec2.internal'} is busy.
    * dst@ip-172-31-81-4.ec2.internal psql load encoded column table S force
    process {node_state,'dst@ip-172-31-81-4.ec2.internal'} is busy.
    * dsf@ip-172-31-86-179.ec2.internal psql load encoded column table S force
    process {node_state,'dsf@ip-172-31-86-179.ec2.internal'} is busy.
    * dsv@ip-172-31-92-118.ec2.internal psql load encoded column table S force
    process {node_state,'dsv@ip-172-31-92-118.ec2.internal'} is busy.
    * dsa@ip-172-31-81-94.ec2.internal psql load encoded column table S force
    process {node_state,'dsa@ip-172-31-81-94.ec2.internal'} is busy.
    * dse@ip-172-31-90-96.ec2.internal psql load encoded column table S force
    process {node_state,'dse@ip-172-31-90-96.ec2.internal'} is busy.
    * dsk@ip-172-31-91-90.ec2.internal psql load encoded column table S force
    process {node_state,'dsk@ip-172-31-91-90.ec2.internal'} is busy.
    * dsw@ip-172-31-94-71.ec2.internal psql load encoded column table S force
    process {node_state,'dsw@ip-172-31-94-71.ec2.internal'} is busy.
    * dsr@ip-172-31-80-53.ec2.internal psql load encoded column table S force
    process {node_state,'dsr@ip-172-31-80-53.ec2.internal'} is busy.
    * dss@ip-172-31-91-140.ec2.internal psql load encoded column table S force
    process {node_state,'dss@ip-172-31-91-140.ec2.internal'} is busy.
    pushed {'dsg@ip-172-31-80-173.ec2.internal','load-ds'} to node_state:ui_queued_jobs
    pushed {'dsj@ip-172-31-84-145.ec2.internal','load-ds'} to node_state:ui_queued_jobs
    pushed {'dsc@ip-172-31-92-69.ec2.internal','load-ds'} to node_state:ui_queued_jobs
    pushed {'dsu@ip-172-31-81-14.ec2.internal','load-ds'} to node_state:ui_queued_jobs
    pushed {'dsx@ip-172-31-81-183.ec2.internal','load-ds'} to node_state:ui_queued_jobs
    pushed {'dsy@ip-172-31-85-7.ec2.internal','load-ds'} to node_state:ui_queued_jobs
    pushed {'dsn@ip-172-31-84-252.ec2.internal','load-ds'} to node_state:ui_queued_jobs
    pushed {'dsi@ip-172-31-82-105.ec2.internal','load-ds'} to node_state:ui_queued_jobs
    pushed {'dsd@ip-172-31-83-46.ec2.internal','load-ds'} to node_state:ui_queued_jobs
    pushed {'dsq@ip-172-31-94-19.ec2.internal','load-ds'} to node_state:ui_queued_jobs
    pushed {'dsm@ip-172-31-90-129.ec2.internal','load-ds'} to node_state:ui_queued_jobs
    pushed {'dsl@ip-172-31-83-95.ec2.internal','load-ds'} to node_state:ui_queued_jobs
    pushed {'dsp@ip-172-31-94-209.ec2.internal','load-ds'} to node_state:ui_queued_jobs
    pushed {'dso@ip-172-31-80-243.ec2.internal','load-ds'} to node_state:ui_queued_jobs
    pushed {'dsb@ip-172-31-90-178.ec2.internal','load-ds'} to node_state:ui_queued_jobs
    pushed {'dsh@ip-172-31-85-39.ec2.internal','load-ds'} to node_state:ui_queued_jobs
    pushed {'dst@ip-172-31-81-4.ec2.internal','load-ds'} to node_state:ui_queued_jobs
    pushed {'dsf@ip-172-31-86-179.ec2.internal','load-ds'} to node_state:ui_queued_jobs
    pushed {'dsv@ip-172-31-92-118.ec2.internal','load-ds'} to node_state:ui_queued_jobs
    pushed {'dsa@ip-172-31-81-94.ec2.internal','load-ds'} to node_state:ui_queued_jobs
    pushed {'dse@ip-172-31-90-96.ec2.internal','load-ds'} to node_state:ui_queued_jobs
    pushed {'dsk@ip-172-31-91-90.ec2.internal','load-ds'} to node_state:ui_queued_jobs
    pushed {'dsw@ip-172-31-94-71.ec2.internal','load-ds'} to node_state:ui_queued_jobs
    pushed {'dsr@ip-172-31-80-53.ec2.internal','load-ds'} to node_state:ui_queued_jobs
    pushed {'dss@ip-172-31-91-140.ec2.internal','load-ds'} to node_state:ui_queued_jobs

You have to wait about 15-20 minutes for data server nodes to load
encoded column dump files. The loading prgress could be observed using
following commands.

    ui 11:43 [29] big3store=# property find FS NS ui
    [{ui_queued_jobs,[{dss,'load-ds'},
                      {dsg,'load-ds'},
                      {dsj,'load-ds'},
                      {dsc,'load-ds'},
                      {dsu,'load-ds'},
                      {dsx,'load-ds'},
                      {dsy,'load-ds'},
                      {dsn,'load-ds'},
                      {dsi,'load-ds'},
                      {dsd,'load-ds'},
                      {dsq,'load-ds'},
                      {dsm,'load-ds'},
                      {dsl,'load-ds'},
                      {dsp,'load-ds'},
                      {dso,'load-ds'},
                      {dsb,'load-ds'},
                      {dsh,'load-ds'},
                      {dst,'load-ds'},
                      {dsf,'load-ds'},
                      {dsv,'load-ds'},
                      {dsa,'load-ds'},
                      {dse,'load-ds'},
                      {dsk,'load-ds'},
                      {dsw,'load-ds'},
                      {dsr,'load-ds'},
                      {dss,'load-ds'}]}]
    ui 11:43 [30] big3store=# remote all df .
    * dsg@ip-172-31-80-173.ec2.internal df .
    Filesystem     1K-blocks    Used Available Use% Mounted on
    /dev/xvda1      30428560 6232792  24179384  21% /
    * dsj@ip-172-31-84-145.ec2.internal df .
    Filesystem     1K-blocks    Used Available Use% Mounted on
    /dev/xvda1      30428560 5810120  24602056  20% /
    * dsc@ip-172-31-92-69.ec2.internal df .
    Filesystem     1K-blocks    Used Available Use% Mounted on
    /dev/xvda1      30428560 5378352  25033824  18% /
    * dsu@ip-172-31-81-14.ec2.internal df .
    Filesystem     1K-blocks    Used Available Use% Mounted on
    /dev/xvda1      30428560 6137028  24275148  21% /
    * dsx@ip-172-31-81-183.ec2.internal df .
    Filesystem     1K-blocks    Used Available Use% Mounted on
    /dev/xvda1      30428560 6061212  24350964  20% /
    * dsy@ip-172-31-85-7.ec2.internal df .
    Filesystem     1K-blocks    Used Available Use% Mounted on
    /dev/xvda1      30428560 6216000  24196176  21% /
    * dsn@ip-172-31-84-252.ec2.internal df .
    Filesystem     1K-blocks    Used Available Use% Mounted on
    /dev/xvda1      30428560 5895464  24516712  20% /
    * dsi@ip-172-31-82-105.ec2.internal df .
    Filesystem     1K-blocks    Used Available Use% Mounted on
    /dev/xvda1      30428560 5854764  24557412  20% /
    * dsd@ip-172-31-83-46.ec2.internal df .
    Filesystem     1K-blocks    Used Available Use% Mounted on
    /dev/xvda1      30428560 5955932  24456244  20% /
    * dsq@ip-172-31-94-19.ec2.internal df .
    Filesystem     1K-blocks    Used Available Use% Mounted on
    /dev/xvda1      30428560 5737952  24674224  19% /
    * dsm@ip-172-31-90-129.ec2.internal df .
    Filesystem     1K-blocks    Used Available Use% Mounted on
    /dev/xvda1      30428560 5786016  24626160  20% /
    * dsl@ip-172-31-83-95.ec2.internal df .
    Filesystem     1K-blocks    Used Available Use% Mounted on
    /dev/xvda1      30428560 5841296  24570880  20% /
    * dsp@ip-172-31-94-209.ec2.internal df .
    Filesystem     1K-blocks    Used Available Use% Mounted on
    /dev/xvda1      30428560 5617396  24794780  19% /
    * dso@ip-172-31-80-243.ec2.internal df .
    Filesystem     1K-blocks    Used Available Use% Mounted on
    /dev/xvda1      30428560 5620380  24791796  19% /
    * dsb@ip-172-31-90-178.ec2.internal df .
    Filesystem     1K-blocks    Used Available Use% Mounted on
    /dev/xvda1      30428560 5913420  24498756  20% /
    * dsh@ip-172-31-85-39.ec2.internal df .
    Filesystem     1K-blocks    Used Available Use% Mounted on
    /dev/xvda1      30428560 5377776  25034400  18% /
    * dst@ip-172-31-81-4.ec2.internal df .
    Filesystem     1K-blocks    Used Available Use% Mounted on
    /dev/xvda1      30428560 5458124  24954052  18% /
    * dsf@ip-172-31-86-179.ec2.internal df .
    Filesystem     1K-blocks    Used Available Use% Mounted on
    /dev/xvda1      30428560 5434544  24977632  18% /
    * dsv@ip-172-31-92-118.ec2.internal df .
    Filesystem     1K-blocks    Used Available Use% Mounted on
    /dev/xvda1      30428560 4874248  25537928  17% /
    * dsa@ip-172-31-81-94.ec2.internal df .
    Filesystem     1K-blocks    Used Available Use% Mounted on
    /dev/xvda1      30428560 5220220  25191956  18% /
    * dse@ip-172-31-90-96.ec2.internal df .
    Filesystem     1K-blocks    Used Available Use% Mounted on
    /dev/xvda1      30428560 5378060  25034116  18% /
    * dsk@ip-172-31-91-90.ec2.internal df .
    Filesystem     1K-blocks    Used Available Use% Mounted on
    /dev/xvda1      30428560 5096492  25315684  17% /
    * dsw@ip-172-31-94-71.ec2.internal df .
    Filesystem     1K-blocks    Used Available Use% Mounted on
    /dev/xvda1      30428560 4896176  25516000  17% /
    * dsr@ip-172-31-80-53.ec2.internal df .
    Filesystem     1K-blocks    Used Available Use% Mounted on
    /dev/xvda1      30428560 4767304  25644872  16% /
    * dss@ip-172-31-91-140.ec2.internal df .
    Filesystem     1K-blocks    Used Available Use% Mounted on
    /dev/xvda1      30428560 4733676  25678500  16% /
    ui 11:45 [31] big3store=# remote all top 1
    * dsg@ip-172-31-80-173.ec2.internal top 1
    top - 11:45:58 up 30 min,  0 users,  load average: 1.46, 1.03, 0.57
    Tasks:  98 total,   1 running,  62 sleeping,   0 stopped,   0 zombie
    %Cpu(s): 10.7 us,  2.2 sy,  2.4 ni, 59.2 id, 13.8 wa,  0.0 hi,  0.1 si, 11.6 st
    KiB Mem :  3846060 total,    72960 free,   216936 used,  3556164 buff/cache
    KiB Swap:        0 total,        0 free,        0 used.  3356288 avail Mem 
    
      PID USER      PR  NI    VIRT    RES    SHR S %CPU %MEM     TIME+ COMMAND
    14202 postgres  20   0  343140 166164  80092 D  5.6  4.3   2:33.83 postgres
    * dsj@ip-172-31-84-145.ec2.internal top 1
    top - 11:46:00 up 30 min,  0 users,  load average: 1.61, 1.11, 0.61
    Tasks:  99 total,   1 running,  64 sleeping,   0 stopped,   0 zombie
    %Cpu(s): 10.3 us,  2.3 sy,  2.5 ni, 56.0 id, 18.1 wa,  0.0 hi,  0.0 si, 10.8 st
    KiB Mem :  3846060 total,    72372 free,   218192 used,  3555496 buff/cache
    KiB Swap:        0 total,        0 free,        0 used.  3355024 avail Mem 
    
      PID USER      PR  NI    VIRT    RES    SHR S %CPU %MEM     TIME+ COMMAND
      862 root      20   0  288876   6232   5448 S  6.2  0.2   0:00.01 polkitd
    * dsc@ip-172-31-92-69.ec2.internal top 1
    top - 11:46:01 up 30 min,  0 users,  load average: 1.45, 1.05, 0.59
    Tasks:  99 total,   2 running,  62 sleeping,   0 stopped,   0 zombie
    %Cpu(s):  9.1 us,  2.1 sy,  2.4 ni, 57.2 id, 19.2 wa,  0.0 hi,  0.0 si,  9.9 st
    KiB Mem :  3846060 total,   130872 free,   217440 used,  3497748 buff/cache
    KiB Swap:        0 total,        0 free,        0 used.  3355764 avail Mem 
    
      PID USER      PR  NI    VIRT    RES    SHR S %CPU %MEM     TIME+ COMMAND
    14241 postgres  20   0  343104 168796  82760 R 71.4  4.4   1:43.99 postgres
    * dsu@ip-172-31-81-14.ec2.internal top 1
    top - 11:46:02 up 30 min,  0 users,  load average: 1.74, 1.13, 0.61
    Tasks: 100 total,   2 running,  63 sleeping,   0 stopped,   0 zombie
    %Cpu(s): 11.1 us,  2.4 sy,  2.4 ni, 58.6 id, 13.9 wa,  0.0 hi,  0.0 si, 11.6 st
    KiB Mem :  3846068 total,    75004 free,   216844 used,  3554220 buff/cache
    KiB Swap:        0 total,        0 free,        0 used.  3356364 avail Mem 
    
      PID USER      PR  NI    VIRT    RES    SHR S %CPU %MEM     TIME+ COMMAND
    14230 postgres  20   0  343140 166200  80132 R 27.8  4.3   2:31.71 postgres
    * dsx@ip-172-31-81-183.ec2.internal top 1
    top - 11:46:03 up 30 min,  0 users,  load average: 1.49, 1.02, 0.56
    Tasks:  99 total,   1 running,  64 sleeping,   0 stopped,   0 zombie
    %Cpu(s): 11.7 us,  2.3 sy,  2.4 ni, 57.1 id, 13.4 wa,  0.0 hi,  0.0 si, 13.0 st
    KiB Mem :  3846060 total,    75492 free,   217024 used,  3553544 buff/cache
    KiB Swap:        0 total,        0 free,        0 used.  3356200 avail Mem 
    
      PID USER      PR  NI    VIRT    RES    SHR S %CPU %MEM     TIME+ COMMAND
    14333 root      22   2   44400   4020   3524 R  4.8  0.1   0:00.02 top
    * dsy@ip-172-31-85-7.ec2.internal top 1
    top - 11:46:04 up 30 min,  0 users,  load average: 1.82, 1.19, 0.63
    Tasks: 100 total,   2 running,  63 sleeping,   0 stopped,   0 zombie
    %Cpu(s): 10.9 us,  2.3 sy,  2.5 ni, 60.1 id, 12.6 wa,  0.0 hi,  0.1 si, 11.5 st
    KiB Mem :  3846060 total,   145892 free,   218648 used,  3481520 buff/cache
    KiB Swap:        0 total,        0 free,        0 used.  3354572 avail Mem 
    
      PID USER      PR  NI    VIRT    RES    SHR S %CPU %MEM     TIME+ COMMAND
    14224 postgres  20   0  343140 166228  80156 R 68.2  4.3   2:24.30 postgres
    * dsn@ip-172-31-84-252.ec2.internal top 1
    top - 11:46:05 up 30 min,  0 users,  load average: 1.34, 1.00, 0.59
    Tasks:  99 total,   2 running,  63 sleeping,   0 stopped,   0 zombie
    %Cpu(s): 10.0 us,  2.2 sy,  2.4 ni, 59.0 id, 15.5 wa,  0.0 hi,  0.1 si, 10.8 st
    KiB Mem :  3846068 total,    89288 free,   217708 used,  3539072 buff/cache
    KiB Swap:        0 total,        0 free,        0 used.  3355432 avail Mem 
    
      PID USER      PR  NI    VIRT    RES    SHR S %CPU %MEM     TIME+ COMMAND
    14278 postgres  20   0  343140 166744  80708 R 50.0  4.3   2:08.37 postgres
    * dsi@ip-172-31-82-105.ec2.internal top 1
    top - 11:46:06 up 30 min,  0 users,  load average: 1.60, 1.07, 0.58
    Tasks: 100 total,   1 running,  65 sleeping,   0 stopped,   0 zombie
    %Cpu(s):  9.9 us,  2.2 sy,  2.5 ni, 60.2 id, 14.0 wa,  0.0 hi,  0.0 si, 11.1 st
    KiB Mem :  3846060 total,    88620 free,   218604 used,  3538836 buff/cache
    KiB Swap:        0 total,        0 free,        0 used.  3354600 avail Mem 
    
      PID USER      PR  NI    VIRT    RES    SHR S %CPU %MEM     TIME+ COMMAND
    14317 root      22   2   44400   3964   3464 R  1.1  0.1   0:00.01 top
    * dsd@ip-172-31-83-46.ec2.internal top 1
    top - 11:46:07 up 30 min,  0 users,  load average: 1.79, 1.10, 0.60
    Tasks:  99 total,   2 running,  62 sleeping,   0 stopped,   0 zombie
    %Cpu(s): 10.6 us,  2.3 sy,  2.5 ni, 59.3 id, 13.7 wa,  0.0 hi,  0.1 si, 11.5 st
    KiB Mem :  3846060 total,   283404 free,   218296 used,  3344360 buff/cache
    KiB Swap:        0 total,        0 free,        0 used.  3354928 avail Mem 
    
      PID USER      PR  NI    VIRT    RES    SHR S %CPU %MEM     TIME+ COMMAND
    14255 postgres  20   0  343140 166088  80016 R 93.8  4.3   2:23.80 postgres
    * dsq@ip-172-31-94-19.ec2.internal top 1
    top - 11:46:08 up 30 min,  0 users,  load average: 1.77, 1.10, 0.60
    Tasks:  99 total,   2 running,  62 sleeping,   0 stopped,   0 zombie
    %Cpu(s): 10.2 us,  2.2 sy,  2.5 ni, 58.3 id, 15.6 wa,  0.0 hi,  0.0 si, 11.2 st
    KiB Mem :  3846060 total,   158428 free,   218648 used,  3468984 buff/cache
    KiB Swap:        0 total,        0 free,        0 used.  3354504 avail Mem 
    
      PID USER      PR  NI    VIRT    RES    SHR S %CPU %MEM     TIME+ COMMAND
    14238 postgres  20   0  343140 166916  80880 R 71.4  4.3   2:03.48 postgres
    * dsm@ip-172-31-90-129.ec2.internal top 1
    top - 11:46:09 up 30 min,  0 users,  load average: 1.43, 0.95, 0.52
    Tasks:  99 total,   1 running,  64 sleeping,   0 stopped,   0 zombie
    %Cpu(s): 10.9 us,  2.2 sy,  2.7 ni, 59.0 id, 13.0 wa,  0.0 hi,  0.0 si, 12.2 st
    KiB Mem :  3846060 total,    92372 free,   218252 used,  3535436 buff/cache
    KiB Swap:        0 total,        0 free,        0 used.  3354916 avail Mem 
    
      PID USER      PR  NI    VIRT    RES    SHR S %CPU %MEM     TIME+ COMMAND
    14047 root      22   2 1658732  28880   5092 S  6.2  0.8   0:01.38 beam.smp
    * dsl@ip-172-31-83-95.ec2.internal top 1
    top - 11:46:10 up 30 min,  0 users,  load average: 1.52, 1.01, 0.51
    Tasks: 100 total,   1 running,  64 sleeping,   0 stopped,   0 zombie
    %Cpu(s): 10.3 us,  2.3 sy,  2.6 ni, 61.6 id, 12.2 wa,  0.0 hi,  0.0 si, 11.0 st
    KiB Mem :  3846060 total,    81580 free,   220284 used,  3544196 buff/cache
    KiB Swap:        0 total,        0 free,        0 used.  3352944 avail Mem 
    
      PID USER      PR  NI    VIRT    RES    SHR S %CPU %MEM     TIME+ COMMAND
       34 root      20   0       0      0      0 I  4.8  0.0   0:00.48 kworker/u30+
    * dsp@ip-172-31-94-209.ec2.internal top 1
    top - 11:46:11 up 30 min,  0 users,  load average: 1.90, 1.12, 0.58
    Tasks:  99 total,   2 running,  62 sleeping,   0 stopped,   0 zombie
    %Cpu(s):  9.6 us,  2.2 sy,  2.5 ni, 61.3 id, 13.7 wa,  0.0 hi,  0.0 si, 10.7 st
    KiB Mem :  3846068 total,    93704 free,   219620 used,  3532744 buff/cache
    KiB Swap:        0 total,        0 free,        0 used.  3353584 avail Mem 
    
      PID USER      PR  NI    VIRT    RES    SHR S %CPU %MEM     TIME+ COMMAND
    14275 postgres  20   0  343108 168708  82664 R 64.0  4.4   1:48.41 postgres
    * dso@ip-172-31-80-243.ec2.internal top 1
    top - 11:46:12 up 30 min,  0 users,  load average: 1.63, 1.00, 0.54
    Tasks:  99 total,   1 running,  64 sleeping,   0 stopped,   0 zombie
    %Cpu(s):  9.7 us,  2.3 sy,  2.5 ni, 62.3 id, 12.2 wa,  0.0 hi,  0.1 si, 11.0 st
    KiB Mem :  3846060 total,    80076 free,   217572 used,  3548412 buff/cache
    KiB Swap:        0 total,        0 free,        0 used.  3355660 avail Mem 
    
      PID USER      PR  NI    VIRT    RES    SHR S %CPU %MEM     TIME+ COMMAND
        1 root      20   0  159836   9188   6764 S  0.0  0.2   0:05.11 systemd
    * dsb@ip-172-31-90-178.ec2.internal top 1
    top - 11:46:13 up 30 min,  0 users,  load average: 1.40, 1.01, 0.58
    Tasks:  99 total,   1 running,  64 sleeping,   0 stopped,   0 zombie
    %Cpu(s): 11.1 us,  2.3 sy,  2.6 ni, 59.4 id, 11.6 wa,  0.0 hi,  0.1 si, 12.9 st
    KiB Mem :  3846060 total,    83044 free,   218036 used,  3544980 buff/cache
    KiB Swap:        0 total,        0 free,        0 used.  3355192 avail Mem 
    
      PID USER      PR  NI    VIRT    RES    SHR S %CPU %MEM     TIME+ COMMAND
        1 root      20   0  159836   9128   6688 S  0.0  0.2   0:06.55 systemd
    * dsh@ip-172-31-85-39.ec2.internal top 1
    top - 11:46:14 up 30 min,  0 users,  load average: 1.54, 1.04, 0.58
    Tasks:  99 total,   2 running,  62 sleeping,   0 stopped,   0 zombie
    %Cpu(s):  9.7 us,  2.2 sy,  2.6 ni, 61.2 id, 13.4 wa,  0.0 hi,  0.0 si, 10.9 st
    KiB Mem :  3846060 total,   118600 free,   218684 used,  3508776 buff/cache
    KiB Swap:        0 total,        0 free,        0 used.  3354548 avail Mem 
    
      PID USER      PR  NI    VIRT    RES    SHR S %CPU %MEM     TIME+ COMMAND
    14203 postgres  20   0  343104 168588  82556 R 81.0  4.4   1:53.29 postgres
    * dst@ip-172-31-81-4.ec2.internal top 1
    top - 11:46:15 up 30 min,  0 users,  load average: 1.92, 1.12, 0.59
    Tasks:  99 total,   1 running,  63 sleeping,   0 stopped,   0 zombie
    %Cpu(s):  9.9 us,  2.3 sy,  2.6 ni, 61.3 id, 13.0 wa,  0.0 hi,  0.1 si, 10.9 st
    KiB Mem :  3846068 total,    80200 free,   217884 used,  3547984 buff/cache
    KiB Swap:        0 total,        0 free,        0 used.  3355352 avail Mem 
    
      PID USER      PR  NI    VIRT    RES    SHR S %CPU %MEM     TIME+ COMMAND
       86 root       0 -20       0      0      0 I  3.0  0.0   0:00.36 kworker/0:1H
    * dsf@ip-172-31-86-179.ec2.internal top 1
    top - 11:46:17 up 30 min,  0 users,  load average: 1.68, 1.01, 0.55
    Tasks:  99 total,   1 running,  64 sleeping,   0 stopped,   0 zombie
    %Cpu(s):  9.0 us,  2.2 sy,  2.5 ni, 61.5 id, 13.8 wa,  0.0 hi,  0.0 si, 10.9 st
    KiB Mem :  3846060 total,    85656 free,   242480 used,  3517924 buff/cache
    KiB Swap:        0 total,        0 free,        0 used.  3330752 avail Mem 
    
      PID USER      PR  NI    VIRT    RES    SHR S %CPU %MEM     TIME+ COMMAND
    14275 root      22   2   44400   3876   3376 R  5.3  0.1   0:00.01 top
    * dsv@ip-172-31-92-118.ec2.internal top 1
    top - 11:46:17 up 30 min,  0 users,  load average: 1.78, 1.07, 0.58
    Tasks:  99 total,   2 running,  62 sleeping,   0 stopped,   0 zombie
    %Cpu(s):  8.6 us,  2.1 sy,  2.5 ni, 61.0 id, 15.8 wa,  0.0 hi,  0.0 si,  9.9 st
    KiB Mem :  3846060 total,   407100 free,   243324 used,  3195636 buff/cache
    KiB Swap:        0 total,        0 free,        0 used.  3329904 avail Mem 
    
      PID USER      PR  NI    VIRT    RES    SHR S %CPU %MEM     TIME+ COMMAND
    14309 postgres  20   0  365216 193568  83200 R 88.9  5.0   1:19.40 postgres
    * dsa@ip-172-31-81-94.ec2.internal top 1
    top - 11:46:18 up 31 min,  0 users,  load average: 1.61, 0.95, 0.53
    Tasks: 100 total,   1 running,  65 sleeping,   0 stopped,   0 zombie
    %Cpu(s):  9.3 us,  2.1 sy,  2.5 ni, 58.8 id, 16.1 wa,  0.0 hi,  0.0 si, 11.0 st
    KiB Mem :  3846060 total,   126020 free,   244268 used,  3475772 buff/cache
    KiB Swap:        0 total,        0 free,        0 used.  3328964 avail Mem 
    
      PID USER      PR  NI    VIRT    RES    SHR S %CPU %MEM     TIME+ COMMAND
        1 root      20   0  159824   9044   6620 S  0.0  0.2   0:05.59 systemd
    * dse@ip-172-31-90-96.ec2.internal top 1
    top - 11:46:19 up 30 min,  0 users,  load average: 1.37, 0.83, 0.46
    Tasks:  99 total,   1 running,  64 sleeping,   0 stopped,   0 zombie
    %Cpu(s):  9.7 us,  2.2 sy,  2.5 ni, 60.6 id, 14.2 wa,  0.0 hi,  0.1 si, 10.6 st
    KiB Mem :  3846060 total,    77744 free,   217608 used,  3550708 buff/cache
    KiB Swap:        0 total,        0 free,        0 used.  3355612 avail Mem 
    
      PID USER      PR  NI    VIRT    RES    SHR S %CPU %MEM     TIME+ COMMAND
    14270 root      22   2   44400   3868   3368 R  4.8  0.1   0:00.01 top
    * dsk@ip-172-31-91-90.ec2.internal top 1
    top - 11:46:21 up 30 min,  0 users,  load average: 1.64, 0.93, 0.52
    Tasks:  99 total,   2 running,  62 sleeping,   0 stopped,   0 zombie
    %Cpu(s):  9.4 us,  2.3 sy,  2.6 ni, 59.7 id, 15.6 wa,  0.0 hi,  0.1 si, 10.3 st
    KiB Mem :  3846060 total,   104740 free,   242564 used,  3498756 buff/cache
    KiB Swap:        0 total,        0 free,        0 used.  3330620 avail Mem 
    
      PID USER      PR  NI    VIRT    RES    SHR S %CPU %MEM     TIME+ COMMAND
    14260 postgres  20   0  365732 193664  83036 R 71.4  5.0   1:38.03 postgres
    * dsw@ip-172-31-94-71.ec2.internal top 1
    top - 11:46:21 up 30 min,  0 users,  load average: 1.73, 0.90, 0.50
    Tasks: 100 total,   3 running,  62 sleeping,   0 stopped,   0 zombie
    %Cpu(s):  8.3 us,  2.3 sy,  2.6 ni, 60.7 id, 16.6 wa,  0.0 hi,  0.1 si,  9.6 st
    KiB Mem :  3846060 total,   479096 free,   242428 used,  3124536 buff/cache
    KiB Swap:        0 total,        0 free,        0 used.  3330672 avail Mem 
    
      PID USER      PR  NI    VIRT    RES    SHR S %CPU %MEM     TIME+ COMMAND
    14232 postgres  20   0  365216 193372  83028 R 35.3  5.0   1:07.11 postgres
    * dsr@ip-172-31-80-53.ec2.internal top 1
    top - 11:46:22 up 30 min,  0 users,  load average: 2.23, 1.03, 0.57
    Tasks: 100 total,   2 running,  63 sleeping,   0 stopped,   0 zombie
    %Cpu(s):  8.1 us,  2.7 sy,  2.6 ni, 54.8 id, 21.7 wa,  0.0 hi,  0.1 si, 10.0 st
    KiB Mem :  3846060 total,   794844 free,   212220 used,  2838996 buff/cache
    KiB Swap:        0 total,        0 free,        0 used.  3361048 avail Mem 
    
      PID USER      PR  NI    VIRT    RES    SHR S %CPU %MEM     TIME+ COMMAND
    14281 postgres  20   0  328936 160500  83160 R 71.4  4.2   0:58.53 postgres
    * dss@ip-172-31-91-140.ec2.internal top 1
    top - 11:46:24 up 30 min,  0 users,  load average: 1.41, 1.37, 0.83
    Tasks:  99 total,   2 running,  64 sleeping,   0 stopped,   0 zombie
    %Cpu(s): 13.7 us,  3.4 sy,  3.6 ni, 50.1 id, 13.7 wa,  0.0 hi,  0.1 si, 15.4 st
    KiB Mem :  3846060 total,  1106328 free,   200048 used,  2539684 buff/cache
    KiB Swap:        0 total,        0 free,        0 used.  3379684 avail Mem 
    
      PID USER      PR  NI    VIRT    RES    SHR S %CPU %MEM     TIME+ COMMAND
    14553 postgres  20   0  324344 149688  82180 R 18.6  3.9   0:57.68 postgres

After receiving a SNS notification saids '[11:50:43] all table
loaded', 'ui_queued_jobs' property of 'node_state' process changes
like following. It indicates that all encoded column dump files has
been successfully loaded.

    ui 11:53 [38] big3store=# property find FS NS ui
    [{ui_queued_jobs,[]}]

While it's ready to operate big3store, postgresql servers are
hot-start states. In order to make them cold-start states, following
command is required. You have to wait a few minutes.

    ui 12:01 [41] big3store=# aws reboot all
    rebooting dsg@ip-172-31-80-173.ec2.internal.
    rebooting dsj@ip-172-31-84-145.ec2.internal.
    rebooting dsc@ip-172-31-92-69.ec2.internal.
    rebooting dsu@ip-172-31-81-14.ec2.internal.
    rebooting dsx@ip-172-31-81-183.ec2.internal.
    rebooting dsy@ip-172-31-85-7.ec2.internal.
    rebooting dsn@ip-172-31-84-252.ec2.internal.
    rebooting dsi@ip-172-31-82-105.ec2.internal.
    rebooting dsd@ip-172-31-83-46.ec2.internal.
    rebooting dsq@ip-172-31-94-19.ec2.internal.
    rebooting dsm@ip-172-31-90-129.ec2.internal.
    rebooting dsl@ip-172-31-83-95.ec2.internal.
    rebooting dsp@ip-172-31-94-209.ec2.internal.
    rebooting dso@ip-172-31-80-243.ec2.internal.
    rebooting dsb@ip-172-31-90-178.ec2.internal.
    rebooting dsh@ip-172-31-85-39.ec2.internal.
    rebooting dst@ip-172-31-81-4.ec2.internal.
    rebooting dsf@ip-172-31-86-179.ec2.internal.
    rebooting dsv@ip-172-31-92-118.ec2.internal.
    rebooting dsa@ip-172-31-81-94.ec2.internal.
    rebooting dse@ip-172-31-90-96.ec2.internal.
    rebooting dsk@ip-172-31-91-90.ec2.internal.
    rebooting dsw@ip-172-31-94-71.ec2.internal.
    rebooting dsr@ip-172-31-80-53.ec2.internal.
    rebooting dss@ip-172-31-91-140.ec2.internal.
    ui 12:02 [42] big3store=# config
    
                        front_server_nodes : ['fs@ip-172-31-95-122.ec2.internal']
                         num_of_empty_msgs : 10
                                block_size : 100
                    distribution_algorithm : random
                    name_of_pred_clm_table : pc_yg2s_col25
                   name_of_pred_freq_table : pf_yg2s
                   name_of_string_id_table : string_id_triplestore
                    name_of_pred_string_id : psi_yg2s
                         down_data_servers : [[{1,
                                                'dsg@ip-172-31-80-173.ec2.internal'}],
                                              [{2,
                                                'dsj@ip-172-31-84-145.ec2.internal'}],
                                              [{3,
                                                'dsc@ip-172-31-92-69.ec2.internal'}],
                                              [{4,
                                                'dsu@ip-172-31-81-14.ec2.internal'}],
                                              [{5,
                                                'dsx@ip-172-31-81-183.ec2.internal'}],
                                              [{6,
                                                'dsy@ip-172-31-85-7.ec2.internal'}],
                                              [{7,
                                                'dsn@ip-172-31-84-252.ec2.internal'}],
                                              [{8,
                                                'dsi@ip-172-31-82-105.ec2.internal'}],
                                              [{9,
                                                'dsd@ip-172-31-83-46.ec2.internal'}],
                                              [{10,
                                                'dsq@ip-172-31-94-19.ec2.internal'}],
                                              [{11,
                                                'dsm@ip-172-31-90-129.ec2.internal'}],
                                              [{12,
                                                'dsl@ip-172-31-83-95.ec2.internal'}],
                                              [{13,
                                                'dsp@ip-172-31-94-209.ec2.internal'}],
                                              [{14,
                                                'dso@ip-172-31-80-243.ec2.internal'}],
                                              [{15,
                                                'dsb@ip-172-31-90-178.ec2.internal'}],
                                              [{16,
                                                'dsh@ip-172-31-85-39.ec2.internal'}],
                                              [{17,
                                                'dst@ip-172-31-81-4.ec2.internal'}],
                                              [{18,
                                                'dsf@ip-172-31-86-179.ec2.internal'}],
                                              [{19,
                                                'dsv@ip-172-31-92-118.ec2.internal'}],
                                              [{20,
                                                'dsa@ip-172-31-81-94.ec2.internal'}],
                                              [{21,
                                                'dse@ip-172-31-90-96.ec2.internal'}],
                                              [{22,
                                                'dsk@ip-172-31-91-90.ec2.internal'}],
                                              [{23,
                                                'dsw@ip-172-31-94-71.ec2.internal'}],
                                              [{24,
                                                'dsr@ip-172-31-80-53.ec2.internal'}],
                                              [{25,
                                                'dss@ip-172-31-91-140.ec2.internal'}]]
    
                                ====> END OF LINE <====                             
    ui 12:05 [46] big3store=# config
    
                        front_server_nodes : ['fs@ip-172-31-95-122.ec2.internal']
                         data_server_nodes : "dsu@ip-172-31-81 (4)"
                                           : "dsg@ip-172-31-80 (1)"
                                           : "dsy@ip-172-31-85 (6)"
                                           : "dsj@ip-172-31-84 (2)"
                                           : "dsi@ip-172-31-82 (8)"
                                           : "dsl@ip-172-31-83 (12)"
                                           : "dsm@ip-172-31-90 (11)"
                                           : "dsx@ip-172-31-81 (5)"
                                           : "dsd@ip-172-31-83 (9)"
                                           : "dsq@ip-172-31-94 (10)"
                                           : "dsc@ip-172-31-92 (3)"
                                           : "dsh@ip-172-31-85 (16)"
                                           : "dsk@ip-172-31-91 (22)"
                                           : "dsv@ip-172-31-92 (19)"
                                           : "dsf@ip-172-31-86 (18)"
                                           : "dsn@ip-172-31-84 (7)"
                                           : "dss@ip-172-31-91 (25)"
                                           : "dst@ip-172-31-81 (17)"
                                           : "dse@ip-172-31-90 (21)"
                                           : "dso@ip-172-31-80 (14)"
                                           : "dsb@ip-172-31-90 (15)"
                                           : "dsr@ip-172-31-80 (24)"
                                           : "dsa@ip-172-31-81 (20)"
                                           : "dsw@ip-172-31-94 (23)"
                                           : "dsp@ip-172-31-94 (13)"
                         num_of_empty_msgs : 10
                                block_size : 100
                    distribution_algorithm : random
                    name_of_pred_clm_table : pc_yg2s_col25
                   name_of_pred_freq_table : pf_yg2s
                   name_of_string_id_table : string_id_triplestore
                    name_of_pred_string_id : psi_yg2s
                         down_data_servers : []
    
                                ====> END OF LINE <====                             

Benchmark tasks are executed using following command.

    ui 12:13 [47] big3store=# bb task_yg_0001 task_yg_0002 task_yg_0004 task_yg_0013 task_yg_0014 task_yg_0015 task_yg_0004m task_yg_0013m task_yg_0014m
    
    * preparation:
    [{error,{{error,not_found},'fs@ip-172-31-95-122.ec2.internal',task_yg_0001}},
     {ok,{task_yg_0001,'fs@ip-172-31-95-122.ec2.internal'}}]
    
    * {task_yg_0001,'fs@ip-172-31-95-122.ec2.internal'} started 'task_yg_0001'.
    [ok,{{2019,7,10},{12,15,11}}]
    | task_yg_0001             | ********** |          8 |
    
    * preparation:
    [{error,{{error,not_found},'fs@ip-172-31-95-122.ec2.internal',task_yg_0002}},
     {ok,{task_yg_0002,'fs@ip-172-31-95-122.ec2.internal'}}]
    
    * {task_yg_0002,'fs@ip-172-31-95-122.ec2.internal'} started 'task_yg_0002'.
    [ok,{{2019,7,10},{12,15,16}}]
    | task_yg_0002             |    4203728 |          1 |
    
    * preparation:
    [{error,{{error,not_found},'fs@ip-172-31-95-122.ec2.internal',task_yg_0004}},
     {ok,{task_yg_0004,'fs@ip-172-31-95-122.ec2.internal'}}]
    
    * {task_yg_0004,'fs@ip-172-31-95-122.ec2.internal'} started 'task_yg_0004'.
    [ok,{{2019,7,10},{12,15,21}}]
    | task_yg_0004             |     488643 |          1 |
    
    * preparation:
    [{error,{{error,not_found},'fs@ip-172-31-95-122.ec2.internal',task_yg_0013}},
     {ok,{task_yg_0013,'fs@ip-172-31-95-122.ec2.internal'}}]
    
    * {task_yg_0013,'fs@ip-172-31-95-122.ec2.internal'} started 'task_yg_0013'.
    [ok,{{2019,7,10},{12,15,23}}]
    | task_yg_0013             |   16297226 |          6 |
    
    * preparation:
    [{error,{{error,not_found},'fs@ip-172-31-95-122.ec2.internal',task_yg_0014}},
     {ok,{task_yg_0014,'fs@ip-172-31-95-122.ec2.internal'}}]
    
    * {task_yg_0014,'fs@ip-172-31-95-122.ec2.internal'} started 'task_yg_0014'.
    [ok,{{2019,7,10},{12,15,41}}]
    | task_yg_0014             |    3174924 |          3 |
    
    * preparation:
    [{error,{{error,not_found},'fs@ip-172-31-95-122.ec2.internal',task_yg_0015}},
     {ok,{task_yg_0015,'fs@ip-172-31-95-122.ec2.internal'}}]
    
    * {task_yg_0015,'fs@ip-172-31-95-122.ec2.internal'} started 'task_yg_0015'.
    [ok,{{2019,7,10},{12,15,47}}]
    | task_yg_0015             |   26592812 |         35 |
    
    * preparation:
    [{error,{{error,not_found},'fs@ip-172-31-95-122.ec2.internal',task_yg_0004m}},
     {ok,{task_yg_0004m,'fs@ip-172-31-95-122.ec2.internal'}}]
    
    * {task_yg_0004m,'fs@ip-172-31-95-122.ec2.internal'} started 'task_yg_0004m'.
    [ok,{{2019,7,10},{12,16,15}}]
    | task_yg_0004m            |      26632 |          1 |
    
    * preparation:
    [{error,{{error,not_found},'fs@ip-172-31-95-122.ec2.internal',task_yg_0013m}},
     {ok,{task_yg_0013m,'fs@ip-172-31-95-122.ec2.internal'}}]
    
    * {task_yg_0013m,'fs@ip-172-31-95-122.ec2.internal'} started 'task_yg_0013m'.
    [ok,{{2019,7,10},{12,16,17}}]
    | task_yg_0013m            |   13503734 |          6 |
    
    * preparation:
    [{error,{{error,not_found},'fs@ip-172-31-95-122.ec2.internal',task_yg_0014m}},
     {ok,{task_yg_0014m,'fs@ip-172-31-95-122.ec2.internal'}}]
    
    * {task_yg_0014m,'fs@ip-172-31-95-122.ec2.internal'} started 'task_yg_0014m'.
    [ok,{{2019,7,10},{12,16,33}}]
    | task_yg_0014m            |  139676808 |          3 |
    <<<benchmark batch finished. (total 3.437 minutes)>>>
    "MessageId": "7e9a6ee9-2e66-5d8d-a64a-3b76750631dd"
    
    | Task Name                | Elapsed    | Triples    |
    | ------------------------ | ----------:| ----------:|
    | task_yg_0001             | ********** |          8 |
    | task_yg_0002             |    4203728 |          1 |
    | task_yg_0004             |     488643 |          1 |
    | task_yg_0013             |   16297226 |          6 |
    | task_yg_0014             |    3174924 |          3 |
    | task_yg_0015             |   26592812 |         35 |
    | task_yg_0004m            |      26632 |          1 |
    | task_yg_0013m            |   13503734 |          6 |
    | task_yg_0014m            |  139676808 |          3 |
    
        total                 3.437265461805555 minutes
        average              22915103.078703705 microseconds
        middle average        9504641.101190476 microseconds

Following command should be used frequently for confirming the heap
memory usages of data server Erlang nodes. It reports that data server
DS01 consumes larger memories than other data server nodes.

    ui 12:38 [50] big3store=# memory summary
     Node    |  Total |  Proc. |   Atom |   Bin. |   Ets
    ---------+--------+--------+--------+--------+--------
     FS      |    27M |     8M |   562K |   226K |   432K
     DS01-01 |   983M |   968M |   402K |   587K |   362K
     DS02-01 |    22M |     8M |   383K |   364K |   360K
     DS03-01 |    22M |     8M |   383K |   421K |   360K
     DS04-01 |    22M |     8M |   383K |   409K |   360K
     DS05-01 |    22M |     8M |   383K |   408K |   360K
     DS06-01 |    23M |     8M |   383K |   366K |   360K
     DS07-01 |    22M |     8M |   383K |   380K |   360K
     DS08-01 |    22M |     8M |   383K |   421K |   360K
     DS09-01 |    23M |     9M |   383K |   408K |   360K
     DS10-01 |    23M |     9M |   383K |   360K |   360K
     DS11-01 |    22M |     8M |   383K |   376K |   360K
     DS12-01 |    23M |     9M |   383K |   379K |   360K
     DS13-01 |    22M |     8M |   383K |   438K |   360K
     DS14-01 |    22M |     8M |   383K |   385K |   360K
     DS15-01 |    23M |     8M |   383K |   407K |   360K
     DS16-01 |    23M |     9M |   383K |   421K |   360K
     DS17-01 |    23M |     9M |   383K |   368K |   360K
     DS18-01 |    23M |     8M |   383K |   441K |   360K
     DS19-01 |    23M |     9M |   383K |   381K |   360K
     DS20-01 |    23M |     8M |   383K |   425K |   360K
     DS21-01 |    22M |     8M |   383K |   380K |   360K
     DS22-01 |    22M |     7M |   383K |   416K |   360K
     DS23-01 |    23M |     8M |   383K |   358K |   360K
     DS24-01 |    23M |     9M |   383K |   366K |   360K
     DS25-01 |    24M |     9M |   383K |   419K |   360K

Following command is required for restarting Erlang nodes using much
heap memories.

    ui 12:40 [53] big3store=# remote restart data servers DS1
    restarting data server node of dsg@ip-172-31-80-173.ec2.internal.
    ui 12:42 [56] big3store=# memory summary
     Node    |  Total |  Proc. |   Atom |   Bin. |   Ets
    ---------+--------+--------+--------+--------+--------
     FS      |    28M |     8M |   562K |   243K |   432K
     DS01-01 |    18M |     6M |   314K |   131K |   356K
     DS02-01 |    23M |     9M |   383K |   406K |   360K
     DS03-01 |    24M |     9M |   383K |   457K |   360K
     DS04-01 |    23M |     8M |   383K |   453K |   360K
     DS05-01 |    23M |     8M |   383K |   455K |   360K
     DS06-01 |    22M |     8M |   383K |   407K |   360K
     DS07-01 |    23M |     8M |   383K |   424K |   360K
     DS08-01 |    23M |     8M |   383K |   467K |   360K
     DS09-01 |    23M |     8M |   383K |   447K |   360K
     DS10-01 |    22M |     8M |   383K |   398K |   360K
     DS11-01 |    23M |     8M |   383K |   678K |   360K
     DS12-01 |    23M |     8M |   383K |   674K |   360K
     DS13-01 |    23M |     9M |   383K |   741K |   360K
     DS14-01 |    22M |     8M |   383K |   632K |   360K
     DS15-01 |    23M |     8M |   383K |   759K |   360K
     DS16-01 |    23M |     8M |   383K |   772K |   360K
     DS17-01 |    23M |     8M |   383K |   411K |   360K
     DS18-01 |    23M |     8M |   383K |   534K |   360K
     DS19-01 |    23M |     8M |   383K |   577K |   360K
     DS20-01 |    23M |     8M |   383K |   672K |   360K
     DS21-01 |    23M |     8M |   383K |   687K |   360K
     DS22-01 |    22M |     8M |   383K |   515K |   360K
     DS23-01 |    24M |     9M |   383K |   504K |   360K
     DS24-01 |    22M |     8M |   383K |   514K |   360K
     DS25-01 |    24M |     9M |   383K |   465K |   360K

If you had made modifications on the front server (ex. editing
'b3s.app.aws.boot_ds'), you have to create a new front server image
using followong command.

    ui 13:06 [61] big3store=# aws create image b3sfs-20190710a
    13:07:40 ../aws/bin/create-ami.sh b3sfs-20190710a
    "aws --region us-east-1 --output text ec2 create-image --instance-id i-0d384d969abe9ebdc --name b3sfs-20190710a\naws_ec2_ami_id_front_server=ami-08a942c05fa6c0fb5\n"
    ui 13:07 [62] big3store=# Connection to ec2-52-54-210-46.compute-1.amazonaws.com closed by remote host.
    Connection to ec2-52-54-210-46.compute-1.amazonaws.com closed.

The crated image could be confirmed and maintained using following
commands.

    ui 13:11 [3] big3store=# aws describe image
    13:11:42 ../aws/bin/describe-images.sh
    13:11:43 ../aws/bin/describe-snapshots.sh
    13:11:44 ../aws/bin/describe-launch-templates.sh
    
    2019-07-10T04:09:03.000Z	ami-08a942c05fa6c0fb5	b3s-front-server
    2019-07-10T04:07:41.000Z	ami-08a942c05fa6c0fb5	b3sfs-20190710a
    				snap-06592373f30b68491	50
    2019-07-04T06:48:17.000Z	ami-0d17477102b269872	b3sfs-20190704b
    				snap-03bd9547c02c97056	50
    2019-07-10T04:07:57.394Z	snap-06592373f30b68491	50
    2019-07-04T06:48:34.389Z	snap-03bd9547c02c97056	50
    2019-07-04T06:26:55.724Z	snap-0d3a79e56b46e6f41	50
    
    ui 13:11 [5] big3store=# aws delete image ami-0d17477102b269872
    13:12:19 ../aws/bin/deregister-image.sh ami-0d17477102b269872
    2019-07-10T04:07:41.000Z	ami-08a942c05fa6c0fb5	b3sfs-20190710a
    snap-06592373f30b68491	50
    ui 13:12 [6] big3store=# aws delete snapshot snap-0d3a79e56b46e6f41
    13:12:42 ../aws/bin/delete-snapshot.sh snap-0d3a79e56b46e6f41
    snap-06592373f30b68491	50	2019-07-10T04:07:57.394Z
    snap-03bd9547c02c97056	50	2019-07-04T06:48:34.389Z
    ui 13:12 [7] big3store=# aws describe image
    13:12:47 ../aws/bin/describe-images.sh
    13:12:48 ../aws/bin/describe-snapshots.sh
    13:12:48 ../aws/bin/describe-launch-templates.sh
    
    2019-07-10T04:09:03.000Z	ami-08a942c05fa6c0fb5	b3s-front-server
    2019-07-10T04:07:41.000Z	ami-08a942c05fa6c0fb5	b3sfs-20190710a
    				snap-06592373f30b68491	50
    2019-07-10T04:07:57.394Z	snap-06592373f30b68491	50
    2019-07-04T06:48:34.389Z	snap-03bd9547c02c97056	50

Following command terminates all front and data server instances and
leaves erlang log files to the AWS bucket.

    ui 13:16 [10] big3store=# aws terminate all
    * DS4 aws bucket save elog and terminate
    ../aws/bin/cancel-spot.sh sfr-42fbb6dd-4620-4e99-bf7d-542fbf90e3f1
    * DS6 aws bucket save elog and terminate
    ../aws/bin/cancel-spot.sh sfr-ca40c4ed-7517-4cba-a314-df931c1f30d3
    * DS2 aws bucket save elog and terminate
    ../aws/bin/cancel-spot.sh sfr-976373c5-0fca-433f-8c1e-85e2519e162c
    * DS8 aws bucket save elog and terminate
    ../aws/bin/cancel-spot.sh sfr-655ea58f-8eb0-45de-844c-8d58bacce800
    * DS12 aws bucket save elog and terminate
    ../aws/bin/cancel-spot.sh sfr-f45a5d99-1466-448d-aeb1-7a410e164d32
    * DS11 aws bucket save elog and terminate
    ../aws/bin/cancel-spot.sh sfr-90e4d412-2d5b-4b65-987e-40c6bf8c50ea
    * DS5 aws bucket save elog and terminate
    ../aws/bin/cancel-spot.sh sfr-00c9903c-627f-4d24-acdd-c66737838054
    * DS9 aws bucket save elog and terminate
    ../aws/bin/cancel-spot.sh sfr-03408a7a-7469-41b4-9823-3f0bdbafd943
    * DS10 aws bucket save elog and terminate
    ../aws/bin/cancel-spot.sh sfr-fe52fd70-b335-4927-9266-6b2844b27e46
    * DS3 aws bucket save elog and terminate
    ../aws/bin/cancel-spot.sh sfr-0c2c2b6c-7f0c-4f13-9fa5-efe430fc0b04
    * DS16 aws bucket save elog and terminate
    ../aws/bin/cancel-spot.sh sfr-9babc03f-0f35-41c6-84e2-36388248d41e
    * DS22 aws bucket save elog and terminate
    ../aws/bin/cancel-spot.sh sfr-9519b891-c6f9-4ec7-ac27-0271c2245003
    * DS19 aws bucket save elog and terminate
    ../aws/bin/cancel-spot.sh sfr-b62e515a-b200-443c-9277-eeb0604220db
    * DS18 aws bucket save elog and terminate
    ../aws/bin/cancel-spot.sh sfr-f7154b07-1a9a-4c23-88d1-d51bfd22dc19
    * DS7 aws bucket save elog and terminate
    ../aws/bin/cancel-spot.sh sfr-940eb495-e9c1-41b3-8f07-bdaf2de1ffd7
    * DS25 aws bucket save elog and terminate
    ../aws/bin/cancel-spot.sh sfr-838664da-c284-4e24-ac61-b3b657ec516a
    * DS17 aws bucket save elog and terminate
    ../aws/bin/cancel-spot.sh sfr-7ab69e42-49d8-4c90-8b82-e9a901922093
    * DS21 aws bucket save elog and terminate
    ../aws/bin/cancel-spot.sh sfr-d1a12944-125f-4dff-a740-515b53702700
    * DS14 aws bucket save elog and terminate
    ../aws/bin/cancel-spot.sh sfr-747150f8-920f-43a8-86c6-abcf404cda57
    * DS15 aws bucket save elog and terminate
    ../aws/bin/cancel-spot.sh sfr-2c7e7070-3b49-4d1e-8b7c-208953b47a87
    * DS24 aws bucket save elog and terminate
    ../aws/bin/cancel-spot.sh sfr-5874dafc-bd12-4653-9039-30a74c08bd66
    * DS20 aws bucket save elog and terminate
    ../aws/bin/cancel-spot.sh sfr-f6cc3e08-39c3-4bc0-80d7-ffb16d9f7798
    * DS23 aws bucket save elog and terminate
    ../aws/bin/cancel-spot.sh sfr-62a98d17-5865-4f65-884d-29439a3a24ec
    * DS13 aws bucket save elog and terminate
    ../aws/bin/cancel-spot.sh sfr-10d57890-9b70-4850-b568-1030f2bf78b3
    * DS1 aws bucket save elog and terminate
    ../aws/bin/cancel-spot.sh sfr-783e2b49-1fd9-48be-a24c-6142730d3333
    [ok,ok,ok,ok,ok,ok,ok,ok,ok,ok,ok,ok,ok,ok,ok,ok,ok,ok,ok,ok,ok,ok,ok,ok,ok]
    ui 13:18 [11] big3store=# Connection to ec2-52-54-210-46.compute-1.amazonaws.com closed by remote host.
    Connection to ec2-52-54-210-46.compute-1.amazonaws.com closed.
    pts/3 13:28 knitta@ip-172-31-32-126[37] s3-ls.sh | grep "2019-07-10" | grep elog | sort
    2019-07-10 13:17:46    1131719 spool/elog-dsu@ip-172-31-81-14.ec2.internal-20190710-131637.7z
    2019-07-10 13:17:51    1070092 spool/elog-dsy@ip-172-31-85-7.ec2.internal-20190710-131643.7z
    2019-07-10 13:17:57    1102331 spool/elog-dsj@ip-172-31-84-145.ec2.internal-20190710-131648.7z
    2019-07-10 13:18:06    1118295 spool/elog-dsi@ip-172-31-82-105.ec2.internal-20190710-131654.7z
    2019-07-10 13:18:10    1083257 spool/elog-dsl@ip-172-31-83-95.ec2.internal-20190710-131700.7z
    2019-07-10 13:18:18    1113979 spool/elog-dsm@ip-172-31-90-129.ec2.internal-20190710-131705.7z
    2019-07-10 13:18:19    1094436 spool/elog-dsx@ip-172-31-81-183.ec2.internal-20190710-131711.7z
    2019-07-10 13:18:23    1078654 spool/elog-dsd@ip-172-31-83-46.ec2.internal-20190710-131716.7z
    2019-07-10 13:18:29    1055231 spool/elog-dsq@ip-172-31-94-19.ec2.internal-20190710-131722.7z
    2019-07-10 13:18:37    1100337 spool/elog-dsc@ip-172-31-92-69.ec2.internal-20190710-131728.7z
    2019-07-10 13:18:44    1063737 spool/elog-dsh@ip-172-31-85-39.ec2.internal-20190710-131733.7z
    2019-07-10 13:18:46    1040210 spool/elog-dsk@ip-172-31-91-90.ec2.internal-20190710-131739.7z
    2019-07-10 13:18:52    1055027 spool/elog-dsv@ip-172-31-92-118.ec2.internal-20190710-131744.7z
    2019-07-10 13:18:58    1034629 spool/elog-dsf@ip-172-31-86-179.ec2.internal-20190710-131750.7z
    2019-07-10 13:19:04    1097412 spool/elog-dsn@ip-172-31-84-252.ec2.internal-20190710-131755.7z
    2019-07-10 13:19:14    1122626 spool/elog-dss@ip-172-31-91-140.ec2.internal-20190710-131801.7z
    2019-07-10 13:19:15    1053314 spool/elog-dst@ip-172-31-81-4.ec2.internal-20190710-131806.7z
    2019-07-10 13:19:21    1115035 spool/elog-dse@ip-172-31-90-96.ec2.internal-20190710-131812.7z
    2019-07-10 13:19:30    1065652 spool/elog-dso@ip-172-31-80-243.ec2.internal-20190710-131818.7z
    2019-07-10 13:19:33    1119599 spool/elog-dsb@ip-172-31-90-178.ec2.internal-20190710-131823.7z
    2019-07-10 13:19:42    1060796 spool/elog-dsa@ip-172-31-81-94.ec2.internal-20190710-131835.7z
    2019-07-10 13:19:43    1158228 spool/elog-dsr@ip-172-31-80-53.ec2.internal-20190710-131829.7z
    2019-07-10 13:19:53    1071211 spool/elog-dsp@ip-172-31-94-209.ec2.internal-20190710-131846.7z
    2019-07-10 13:19:54    1023544 spool/elog-dsw@ip-172-31-94-71.ec2.internal-20190710-131840.7z
    2019-07-10 13:25:27    4479107 spool/elog-dsg@ip-172-31-80-173.ec2.internal-20190710-131851.7z
    2019-07-10 13:25:33      86998 spool/elog-fs@ip-172-31-95-122.ec2.internal-20190710-132527.7z

Following command can teminate them without leaving logs.

    ui 13:16 [10] big3store=# aws terminate quick and no save

====> END OF LINE <====

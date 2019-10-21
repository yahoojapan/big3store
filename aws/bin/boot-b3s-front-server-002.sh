#!/bin/sh
#
#       2nd bootstrap script for invoking big3store front server on AWS
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
. /home/ubuntu/big3store/aws/cf/aws.cf

EXE=`/bin/cat <<EOS
(cd /home/ubuntu/big3store/src;\
 make cookie;\
 make pgsql-cf-fs; make pgsql;\
 make; make hipe;\
 make term; make terminate-fs FS=fs;\
 make start-fs-aws-boot-ds > /dev/null 2>&1;\
 sleep 10; make bootstrap)
EOS`
eval $EXE

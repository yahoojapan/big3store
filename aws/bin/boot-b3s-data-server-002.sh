#!/bin/sh
#
#       2nd bootstrap script for invoking big3store data server on AWS
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
. /home/ubuntu/big3store/aws/cf/aws.cf

PID=`ps ax | grep beam | grep $1 | cut -c-6`
kill -9 $PID

B3SAB=/home/ubuntu/big3store/aws/bin
cp $B3SAB/etc-rc-local-data-server.sh /etc/rc.local

EXE=`/bin/cat <<EOS
(cd /home/ubuntu/big3store/src;\
 make clean;\
 make cookie;\
 make pgsql-cf-ds; make pgsql;\
 make; make hipe;\
 make term;\
 make start-ds-aws DS=$1 FSHN=$2 > /dev/null 2>&1)
EOS`
eval $EXE

IPADDR=`curl -s http://169.254.169.254/latest/meta-data/public-hostname`
AWSCMD="aws --region $aws_sns_region"
SNSMES='big3store data server ($1) has been started on $IPADDR.'
EXE=`/bin/cat <<EOS
$AWSCMD sns publish --topic-arn $aws_sns_arn --message "$SNSMES"
EOS`
#eval $EXE

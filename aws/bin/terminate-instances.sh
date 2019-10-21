#!/bin/sh
#
#       terminate AWS instances
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
ABSDIR=$(dirname $(realpath $0))
. $ABSDIR/../cf/aws.cf
AWSCMD="aws --region $aws_ec2_region"

TNICMD="ec2 terminate-instances --instance-ids $*"
$AWSCMD $TNICMD

$ABSDIR/describe-front-server-instances.sh
$ABSDIR/describe-data-server-instances.sh

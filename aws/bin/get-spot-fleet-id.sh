#!/bin/sh
#
#       get spot fleet request id of AWS EC2
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
ABSDIR=$(dirname $(realpath $0))
. $ABSDIR/../cf/aws.cf
AWSCMD="aws --region $aws_ec2_region --output text"
AWMDII=`curl -s http://169.254.169.254/latest/meta-data/instance-id`

DSQFLD='Tags[?Key==`aws:ec2spot:fleet-request-id`].Value'
DSIQUE="Reservations[*].Instances[*].[$DSQFLD]"
DSICMD="ec2 describe-instances --instance-ids $AWMDII --query $DSIQUE"
$AWSCMD $DSICMD


#!/bin/sh
#
#       terminate self AWS instance
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
ABSDIR=$(dirname $(realpath $0))
. $ABSDIR/../cf/aws.cf
AWSCMD="aws --region $aws_ec2_region --output text"

II=`curl -s http://169.254.169.254/latest/meta-data/instance-id`

DSQFLD='Tags[?Key==`aws:ec2spot:fleet-request-id`].Value'
DSIQUE="Reservations[*].Instances[*].[$DSQFLD]"
DSICMD="ec2 describe-instances --instance-ids $II --query $DSIQUE"
SI=`$AWSCMD $DSICMD`

EXE=`/bin/cat <<EOS
$AWSCMD ec2 cancel-spot-fleet-requests\
 --spot-fleet-request-ids $SI\
 --terminate-instances
EOS`
echo $EXE
eval $EXE

EXE=`/bin/cat <<EOS
$AWSCMD ec2 terminate-instances\
 --instance-ids $II
EOS`
echo $EXE
eval $EXE

#!/bin/sh
#
#       describe big3store data server instances on AWS
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
ABSDIR=$(dirname $(realpath $0))
. $ABSDIR/../cf/aws.cf
AWSCMD="aws --region $aws_ec2_region --output text"

if [ "$1" = "help" ]; then
    echo "usage: describe-data-server-instances.sh [failed|help]" 1>&2
    exit 1
fi

TMPPUB=/tmp/ddsip.txt
SRVFIL=$ABSDIR/../cf/servers.cf
if [ "$1" = "failed" ]; then
    DSQFLD='PublicDnsName'
    DSIQUE="Reservations[*].Instances[*].[$DSQFLD]"
    DSIFIL="Name=tag:$aws_ec2_module_tag_name,Values=$aws_ec2_module_tag_data_server"
    DSICMD="ec2 describe-instances --filters $DSIFIL --query $DSIQUE"
    $AWSCMD $DSICMD > $TMPPUB
    while read line
    do
	if ! grep -sq $line $SRVFIL; then
	    echo 'failed server:' $line
	fi
    done < $TMPPUB
    exit 1
fi

#DSQFLD='InstanceId,PrivateDnsName,PublicDnsName,Tags[?Key==`aws:ec2spot:fleet-request-id`].Value'
DSQFLD='InstanceId,InstanceType,PublicDnsName,State.[Name]'
DSIQUE="Reservations[*].Instances[*].[$DSQFLD]"
DSIFIL="Name=tag:$aws_ec2_module_tag_name,Values=$aws_ec2_module_tag_data_server"
DSICMD="ec2 describe-instances --filters $DSIFIL --query $DSIQUE"
$AWSCMD $DSICMD

#!/bin/sh
#
#       describe big3store front server instances on AWS
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
ABSDIR=$(dirname $(realpath $0))
. $ABSDIR/../cf/aws.cf
AWSCMD="aws --region $aws_ec2_region --output text"

#DSQFLD='InstanceId,PrivateDnsName,PublicDnsName,Tags[?Key==`aws:ec2spot:fleet-request-id`].Value'
DSQFLD='InstanceId,InstanceType,PublicDnsName,State.[Name]'
DSIQUE="Reservations[*].Instances[*].[$DSQFLD]"
DSIFIL="Name=tag:$aws_ec2_module_tag_name,Values=$aws_ec2_module_tag_front_server"
DSICMD="ec2 describe-instances --filters $DSIFIL --query $DSIQUE"
$AWSCMD $DSICMD

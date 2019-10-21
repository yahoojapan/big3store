#!/bin/sh
#
#       describe the AWS images
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
ABSDIR=$(dirname $(realpath $0))
. $ABSDIR/../cf/aws.cf
AWSCMD="aws --region $aws_ec2_region --output text"

EXE=`/bin/cat <<EOS
$AWSCMD ec2 describe-images
  --owners self
  --filters "Name=tag:$aws_ec2_service_tag_name,Values=$aws_ec2_service_tag_value"
            "Name=tag:$aws_ec2_module_tag_name,Values=$aws_ec2_module_tag_front_server"
  --query 'reverse(sort_by(Images,&CreationDate))[*].
           [CreationDate,ImageId,Name,
            BlockDeviceMappings[0].[Ebs.[SnapshotId,VolumeSize]]]'
EOS`
eval $EXE

#!/bin/sh
#
#       describe the AWS snapshots
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
ABSDIR=$(dirname $(realpath $0))
. $ABSDIR/../cf/aws.cf
AWSCMD="aws --region $aws_ec2_region --output text"

EXE=`/bin/cat <<EOS
$AWSCMD ec2 describe-snapshots
  --owner-ids self
  --filters "Name=tag:$aws_ec2_service_tag_name,Values=$aws_ec2_service_tag_value"
            "Name=tag:$aws_ec2_module_tag_name,Values=$aws_ec2_module_tag_front_server"
  --query 'reverse(sort_by(Snapshots,&StartTime))[*].
           [SnapshotId,VolumeSize,StartTime]'
EOS`
eval $EXE

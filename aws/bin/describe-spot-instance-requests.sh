#!/bin/sh
#
#       describes the specified Spot Instance requests.
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
ABSDIR=$(dirname $(realpath $0))
. $ABSDIR/../cf/aws.cf
AWSCMD="aws --region $aws_ec2_region --output text"

EXE=`/bin/cat <<EOS
$AWSCMD ec2 describe-spot-instance-requests
  --query "SpotInstanceRequests[*].[State,InstanceId,SpotInstanceRequestId,CreateTime]"
EOS`
eval $EXE

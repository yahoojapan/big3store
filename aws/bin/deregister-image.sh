#!/bin/sh
#
#       deregisters the specified AMI.
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
ABSDIR=$(dirname $(realpath $0))
. $ABSDIR/../cf/aws.cf
AWSCMD="aws --region $aws_ec2_region --output text"

EXE=`/bin/cat <<EOS
$AWSCMD ec2 deregister-image
  --image-id $1
EOS`
eval $EXE

$ABSDIR/describe-images.sh

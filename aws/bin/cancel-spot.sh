#!/bin/sh
#
#       cancel a spot fleet request of AWS EC2
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
ABSDIR=$(dirname $(realpath $0))
. $ABSDIR/../cf/aws.cf
AWSCMD="aws --region $aws_ec2_region"

if [ $# -ne 1 ]; then
  echo "usage: cancel-spot.sh <spot fleet request id>" 1>&2
  exit 1
fi

AWSSFI=$1
EXE=`/bin/cat <<EOS
$AWSCMD ec2 cancel-spot-fleet-requests\
 --spot-fleet-request-ids $AWSSFI\
 --terminate-instances
EOS`
eval $EXE

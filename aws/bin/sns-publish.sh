#!/bin/sh
#
#       perform AWS SNS publish for notifying a message
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
ABSDIR=$(dirname $(realpath $0))
. $ABSDIR/../cf/aws.cf
AWSCMD="aws --region $aws_sns_region"

EXE=`/bin/cat <<EOS
$AWSCMD sns publish --topic-arn $aws_sns_arn --message "$*"
EOS`
eval $EXE

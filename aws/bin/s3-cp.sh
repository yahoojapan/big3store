#!/bin/sh
#
#       copy a file from AWS S3 bucket
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
ABSDIR=$(dirname $(realpath $0))
. $ABSDIR/../cf/aws.cf
AWSCMD="aws --region $aws_sns_region --output text"

EXE=`/bin/cat <<EOS
$AWSCMD s3 cp s3://$aws_s3_bucket_name/$1 .
EOS`
eval $EXE

#!/bin/sh
#
#       bootstrap script for installing big3store system on AWS
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
. ./aws.cf

git clone https://github.com/epgsql/epgsql.git
(cd epgsql; make)

AWSCMD="aws --region $aws_s3_region"
B3STAR=ossb3s-20191107140414.tbz
$AWSCMD s3 cp s3://$aws_s3_bucket_name/$aws_s3_spool_path/$B3STAR .
tar jxf $B3STAR

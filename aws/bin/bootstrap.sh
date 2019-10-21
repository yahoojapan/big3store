#!/bin/sh
#
#       bootstrap script for invoking big3store system on AWS
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
. ./aws.cf

timedatectl set-timezone $aws_ec2_timezone
apt-get install -y zsh emacs make g++ erlang postgresql postgis postgresql-contrib p7zip-full rlwrap
updatedb
AWSCMD="aws --region $aws_s3_region"
$AWSCMD s3 cp s3://$aws_s3_bucket_name/$aws_s3_bin_path/boot-b3s.sh .
$AWSCMD s3 cp s3://$aws_s3_bucket_name/$aws_s3_bin_path/boot-b3s$1.sh .
chmod a+x *.sh

./boot-b3s$1.sh $2 $3
chown -R ubuntu:ubuntu .

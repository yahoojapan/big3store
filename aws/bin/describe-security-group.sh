#!/bin/sh
#
#       describe the AWS security group assigned for big3store
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
ABSDIR=$(dirname $(realpath $0))
. $ABSDIR/../cf/aws.cf
AWSCMD="aws --region $aws_ec2_region --output text"

EXE=`/bin/cat <<EOS
$AWSCMD ec2 describe-security-groups\
 | sed -ne '/$aws_ec2_security_group_name/,/IPPERMISSIONSEGRESS/p'\
 | head -n -1
EOS`
eval $EXE

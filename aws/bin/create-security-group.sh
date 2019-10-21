#!/bin/sh
#
#       add running host to the AWS security group assigned for big3store
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
ABSDIR=$(dirname $(realpath $0))
. $ABSDIR/../cf/aws.cf
AWSCMD="aws --region $aws_ec2_region"

$AWSCMD ec2 delete-security-group --group-name $aws_ec2_security_group_name

EXE=`/bin/cat <<EOS
$AWSCMD ec2 create-security-group\
 --group-name $aws_ec2_security_group_name\
 --description "automatically created by big3store system"
EOS`
eval $EXE

H=`curl -s http://169.254.169.254/latest/meta-data/public-ipv4`
EXE=`/bin/cat <<EOS
$AWSCMD ec2 authorize-security-group-ingress\
 --group-name $aws_ec2_security_group_name\
 --protocol all --cidr $H/32
EOS`
eval $EXE

EXE=`/bin/cat <<EOS
$AWSCMD ec2 authorize-security-group-ingress\
 --group-name $aws_ec2_security_group_name\
 --protocol all --cidr $aws_ec2_vpc_cidr
EOS`
eval $EXE

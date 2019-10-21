#!/bin/sh
#
#       create front server tags to resources
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
ABSDIR=$(dirname $(realpath $0))
. $ABSDIR/../cf/aws.cf
AWSCMD="aws --region $aws_ec2_region --output text"

if [ -z $1 ]; then
    echo usage: create-tags-front-server.sh \<resource id\>...
    exit 0
fi

EXE=`/bin/cat <<EOS
$AWSCMD ec2 create-tags
 --resources $*
 --tags Key=$aws_ec2_service_tag_name,Value=$aws_ec2_service_tag_value
        Key=$aws_ec2_module_tag_name,Value=$aws_ec2_module_tag_front_server
EOS`
echo $EXE
$EXE

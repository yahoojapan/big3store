#!/bin/sh
#
#       start a big3store front server instance on AWS
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
ABSDIR=$(dirname $(realpath $0))
. $ABSDIR/../cf/aws.cf
AWSCMD="aws --region $aws_ec2_region"

EXE=`/bin/cat <<EOS
$AWSCMD ec2 run-instances\
 --launch-template LaunchTemplateName=$aws_ec2_template_name_front_server
EOS`
eval $EXE

echo
echo for monitoring the progress:
echo tail -f /var/log/cloud-init-output.log
echo

sleep 10
$ABSDIR/describe-front-server-instances.sh

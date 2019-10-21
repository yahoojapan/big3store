#!/bin/sh
#
#       create tags to the latest AMI
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
ABSDIR=$(dirname $(realpath $0))
. $ABSDIR/../cf/aws.cf
AWSCMD="aws --region $aws_ec2_region --output text"

EXE=`/bin/cat <<EOS
$AWSCMD ec2 describe-images
  --image-ids $aws_ec2_ami_id_front_server
  --query 'reverse(sort_by(Images,&CreationDate))[0].
           [BlockDeviceMappings[0].[Ebs.[SnapshotId]]]'
EOS`
SSID=`eval $EXE`

$ABSDIR/create-tags-front-server.sh $aws_ec2_ami_id_front_server
$ABSDIR/create-tags-front-server.sh $SSID

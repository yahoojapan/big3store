#!/bin/sh
#
#       create an Amazon Machine Image (AMI)
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
ABSDIR=$(dirname $(realpath $0))
. $ABSDIR/../cf/aws.cf
AWSCMD="aws --region $aws_ec2_region --output text"

if [ -z $1 ]; then
    echo usage: create_ami.sh \<ami name\>
    exit 0
fi

cp $ABSDIR/etc-rc-local-front-server.sh /etc/rc.local

AWMDII=`curl -s http://169.254.169.254/latest/meta-data/instance-id`

EXE=`/bin/cat <<EOS
$AWSCMD ec2 create-image\
 --instance-id $AWMDII\
 --name $1
EOS`
echo $EXE
AMIID=`eval $EXE`

EXE=`/bin/cat <<EOS
aws_ec2_ami_id_front_server=$AMIID
EOS`
echo $EXE
echo $EXE >> $ABSDIR/../cf/aws.cf

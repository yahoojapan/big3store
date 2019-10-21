#!/bin/sh
#
#       extend the size of an AWS EC2 instance
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
ABSDIR=$(dirname $(realpath $0))
. $ABSDIR/../cf/aws.cf
AWSCMD="aws --region $aws_ec2_region --output text"

if [ $# -ne 1 ]; then
  echo "usage: extend-volume.sh <new size>" 1>&2
  exit 1
fi

AWMDII=`curl -s http://169.254.169.254/latest/meta-data/instance-id`
DSQFLD='BlockDeviceMappings[*].Ebs.VolumeId'
DSIQUE="Reservations[*].Instances[*].[$DSQFLD]"
DSICMD="ec2 describe-instances --instance-ids $AWMDII --query $DSIQUE"
PVOLID=`$AWSCMD $DSICMD`
echo $PVOLID

EMVEXE=`/bin/cat <<EOS
$AWSCMD ec2 modify-volume\
 --volume-id $PVOLID\
 --size $1
EOS`
echo $EMVEXE
$EMVEXE

DVMFIL=`/bin/cat <<EOS
Name=modification-state,\
Values="optimizing","completed"
EOS`

DATTDY=`date +%Y-%m-%d`
DVMQUE=`/bin/cat <<EOS
VolumesModifications[?StartTime>='$DATTDY']\
.{ID:VolumeId,STATE:ModificationState}
EOS`

DVMEXE=`/bin/cat <<EOS
$AWSCMD ec2 describe-volumes-modifications\
 --filters $DVMFIL\
 --query "$DVMQUE"
EOS`
echo $DVMEXE
$DVMEXE

echo do reboot!

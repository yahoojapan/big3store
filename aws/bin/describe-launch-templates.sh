#!/bin/sh
#
#       describe AWS ec2 instance launch templates
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
ABSDIR=$(dirname $(realpath $0))
. $ABSDIR/../cf/aws.cf
AWSCMD="aws --region $aws_ec2_region --output text"

# EXE=`/bin/cat <<EOS
# $AWSCMD ec2 describe-launch-template-versions
#   --launch-template-name $aws_ec2_template_name_front_server
#   --query "LaunchTemplateVersions[*].[LaunchTemplateId]"
# EOS`
# LTID=`eval $EXE`

EXE=`/bin/cat <<EOS
$AWSCMD ec2 describe-launch-template-versions
  --launch-template-name $aws_ec2_template_name_front_server
  --query "LaunchTemplateVersions[*].[CreateTime]"
EOS`
CT=`eval $EXE`

EXE=`/bin/cat <<EOS
$AWSCMD ec2 describe-launch-template-versions
  --launch-template-name $aws_ec2_template_name_front_server
  --query "LaunchTemplateVersions[*].[LaunchTemplateName]"
EOS`
LTNAME=`eval $EXE`

EXE=`/bin/cat <<EOS
$AWSCMD ec2 describe-launch-template-versions
  --launch-template-name $aws_ec2_template_name_front_server
  --query "LaunchTemplateVersions[*].
           [LaunchTemplateData.[ImageId]]"
EOS`
AMIID=`eval $EXE`

echo "$CT	$AMIID	$LTNAME"

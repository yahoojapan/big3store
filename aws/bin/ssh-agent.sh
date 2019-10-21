#!/bin/sh
#
#       prepare to register ssh-agent
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
ABSDIR=$(dirname $(realpath $0))
. $ABSDIR/../cf/aws.cf
. $ABSDIR/../cf/servers.cf

SASFIL=$ABSDIR/../cf/ssh-agent.sh
ssh-agent > $SASFIL

echo perform followings:
echo source $SASFIL
echo ssh-add ~/.ssh/$aws_ec2_key_name

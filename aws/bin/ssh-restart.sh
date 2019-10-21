#!/bin/sh
#
#       restart a data server node of a remote instance using ssh.
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
ABSDIR=$(dirname $(realpath $0))
. $ABSDIR/../cf/aws.cf
. $ABSDIR/../cf/servers.cf
. $ABSDIR/../cf/ssh-agent.sh

EXE=`/bin/cat <<EOS
ssh -i ~/.ssh/$aws_ec2_key_name ubuntu@$1 sudo /home/ubuntu/bbd002.sh
EOS`
echo $EXE
eval $EXE

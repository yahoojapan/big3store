#!/bin/sh
#
#       perform a command on a remote instance using ssh.
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
echo ds_c$1r01
EOS`
HST=`eval $EXE`

EXE=`/bin/cat <<EOS
echo '$'$HST
EOS`
HST=`eval $EXE`

EXE=`/bin/cat <<EOS
echo $HST
EOS`
HST=`eval $EXE`

EXE=`/bin/cat <<EOS
ssh -i ~/.ssh/$aws_ec2_key_name ubuntu@$HST $2 $3
EOS`
echo $EXE
eval $EXE

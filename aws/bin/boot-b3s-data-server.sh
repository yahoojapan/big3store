#!/bin/sh
#
#       bootstrap script for invoking big3store data server on AWS
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
./boot-b3s.sh $1

SCRFIL=./bbd002.sh
EXE=`/bin/cat > $SCRFIL <<EOS
/home/ubuntu/big3store/aws/bin/boot-b3s-data-server-002.sh $1 $2
EOS`
eval $EXE
chmod a+x $SCRFIL

at -f $SCRFIL now + 3min

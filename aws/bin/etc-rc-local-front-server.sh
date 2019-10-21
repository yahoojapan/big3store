#!/bin/sh
#
#       reboot run commands for the front server
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
export HOME=/home/ubuntu

EXE=`/bin/cat <<EOS
(cd /home/ubuntu/big3store/src;\
 make term; make term FS=fs;\
 rm /home/ubuntu/big3store/ebin/b3s.app;\
 make hipe; make start-fs-aws > /dev/null 2>&1;\
 make bootstrap; sleep 10; make bootstrap )
EOS`
eval $EXE

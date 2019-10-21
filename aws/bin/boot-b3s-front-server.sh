#!/bin/sh
#
#       bootstrap script for invoking big3store front server on AWS
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
./boot-b3s.sh fs

at -f ./big3store/aws/bin/boot-b3s-front-server-002.sh now + 3min

#!/bin/sh
#
#       reboot run commands for the data server
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
export HOME=/home/ubuntu
at -f $HOME/bbd002.sh now + 3 min

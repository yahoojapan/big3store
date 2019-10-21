#!/bin/sh
#
#       setup AWS bucket for big3store
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
ABSDIR=$(dirname $(realpath $0))
. $ABSDIR/../cf/aws.cf
AWSCMD="aws --region $aws_s3_region"

YMDHMS=$(date +%Y%m%d%H%M%S)
OSSB3S=ossb3s-$YMDHMS.tbz
AWKCMD=`/bin/cat <<EOS
awk '{gsub("%%OSSB3S%%", "$OSSB3S"); print}' \
$ABSDIR/boot-b3s.sh.skel > $ABSDIR/boot-b3s.sh
EOS`
(cd $ABSDIR/../../src; make clean)
eval $AWKCMD
(cd $ABSDIR/../../..; tar jcf $OSSB3S big3store)

BINDIR=s3://$aws_s3_bucket_name/$aws_s3_bin_path/
SPODIR=s3://$aws_s3_bucket_name/$aws_s3_spool_path/
$AWSCMD s3 cp $ABSDIR/bootstrap.sh $BINDIR
$AWSCMD s3 cp $ABSDIR/boot-b3s.sh $BINDIR
$AWSCMD s3 cp $ABSDIR/boot-b3s-front-server.sh $BINDIR
$AWSCMD s3 cp $ABSDIR/boot-b3s-data-server.sh $BINDIR
$AWSCMD s3 cp $ABSDIR/../cf/aws.cf $BINDIR
$AWSCMD s3 mv $ABSDIR/../../../$OSSB3S $SPODIR
$AWSCMD s3 ls $BINDIR
$AWSCMD s3 ls $SPODIR | grep ossb3s
cat $ABSDIR/boot-b3s.sh

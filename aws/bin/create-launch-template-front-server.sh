#!/bin/sh
#
#       create an AWS launch template for big3store
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
ABSDIR=$(dirname $(realpath $0))
. $ABSDIR/../cf/aws.cf
AWSCMD="aws --region $aws_ec2_region --output text"

DLTOPT=`/bin/cat <<EOS
delete-launch-template\
 --launch-template-name $aws_ec2_template_name_front_server
EOS`
$AWSCMD ec2 $DLTOPT

USRDATFS=`base64 -w0 <<EOS
#!/bin/sh
mv /etc/rc.local /etc/rc.local.save
apt-get update
apt-get upgrade
export HOME=/home/ubuntu
at -f /home/ubuntu/big3store/aws/bin/boot-b3s-front-server-002.sh now + 5 minutes
EOS`

CLTOPTFS=`/bin/cat <<EOS
create-launch-template\
 --launch-template-name $aws_ec2_template_name_front_server\
 --launch-template-data {\
"ImageId":"$aws_ec2_ami_id_front_server",\
"InstanceType":"$aws_ec2_instance_type_front_server",\
"KeyName":"$aws_ec2_key_name",\
"SecurityGroups":["$aws_ec2_security_group_name"],\
"IamInstanceProfile":{"Arn":"$aws_ec2_instance_profile_arn"},\
"TagSpecifications":\
[{"ResourceType":"instance",\
"Tags":\
[{"Key":"$aws_ec2_service_tag_name",\
"Value":"$aws_ec2_service_tag_value"},\
{"Key":"$aws_ec2_module_tag_name",\
"Value":"$aws_ec2_module_tag_front_server"}]},\
{"ResourceType":"volume",\
"Tags":\
[{"Key":"$aws_ec2_service_tag_name",\
"Value":"$aws_ec2_service_tag_value"},\
{"Key":"$aws_ec2_module_tag_name",\
"Value":"$aws_ec2_module_tag_front_server"}]}],\
"UserData":"$USRDATFS"}
EOS`

$AWSCMD ec2 $CLTOPTFS

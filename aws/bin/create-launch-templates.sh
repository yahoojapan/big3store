#!/bin/sh
#
#       create AWS launch templates for front and data servers of big3store
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
ABSDIR=$(dirname $(realpath $0))
. $ABSDIR/../cf/aws.cf
AWSCMD="aws --region $aws_ec2_region"

DLTOPT=`/bin/cat <<EOS
delete-launch-template\
 --launch-template-name $aws_ec2_template_name_front_server
EOS`
$AWSCMD ec2 $DLTOPT

DLTOPT=`/bin/cat <<EOS
delete-launch-template\
 --launch-template-name $aws_ec2_template_name_data_server
EOS`
$AWSCMD ec2 $DLTOPT

USRDATFS=`/bin/base64 -w0 <<EOS
#!/bin/sh
apt-get update
apt-get install -y awscli
export HOME=/home/ubuntu
(cd; \
 aws s3 cp s3://$aws_s3_bucket_name/$aws_s3_bin_path/aws.cf .; \
 aws s3 cp s3://$aws_s3_bucket_name/$aws_s3_bin_path/bootstrap.sh .; \
 chmod u+x bootstrap.sh; \
 ./bootstrap.sh -front-server )
EOS`

USRDATDS=`/bin/base64 -w0 <<EOS
#!/bin/sh
apt-get update
apt-get install -y awscli
export HOME=/home/ubuntu
(cd; \
 aws s3 cp s3://$aws_s3_bucket_name/$aws_s3_bin_path/aws.cf .; \
 aws s3 cp s3://$aws_s3_bucket_name/$aws_s3_bin_path/bootstrap.sh .; \
 chmod u+x bootstrap.sh; \
 ./bootstrap.sh -data-server )
EOS`

CLTOPTFS=`/bin/cat <<EOS
create-launch-template\
 --launch-template-name $aws_ec2_template_name_front_server\
 --launch-template-data {\
"ImageId":"$aws_ec2_ubuntu_ami_id",\
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

CLTOPTDS=`/bin/cat <<EOS
create-launch-template\
 --launch-template-name $aws_ec2_template_name_data_server\
 --launch-template-data {\
"ImageId":"$aws_ec2_ubuntu_ami_id",\
"InstanceType":"$aws_ec2_instance_type_data_server",\
"KeyName":"$aws_ec2_key_name",\
"SecurityGroups":["$aws_ec2_security_group_name"],\
"IamInstanceProfile":{"Arn":"$aws_ec2_instance_profile_arn"},\
"TagSpecifications":\
[{"ResourceType":"instance",\
"Tags":\
[{"Key":"$aws_ec2_service_tag_name",\
"Value":"$aws_ec2_service_tag_value"},\
{"Key":"$aws_ec2_module_tag_name",\
"Value":"$aws_ec2_module_tag_data_server"}]},\
{"ResourceType":"volume",\
"Tags":\
[{"Key":"$aws_ec2_service_tag_name",\
"Value":"$aws_ec2_service_tag_value"},\
{"Key":"$aws_ec2_module_tag_name",\
"Value":"$aws_ec2_module_tag_data_server"}]}],\
"UserData":"$USRDATDS"}
EOS`

$AWSCMD ec2 $CLTOPTFS
$AWSCMD ec2 $CLTOPTDS

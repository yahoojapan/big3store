#!/bin/sh
#
#       start a big3store data server instance on AWS
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
ABSDIR=$(dirname $(realpath $0))
. $ABSDIR/../cf/aws.cf
AWSCMD="aws --region $aws_ec2_region"

if [ $# -ne 2 ]; then
  echo "usage: run-data-server.sh ds01|ds02|... <num of rows>" 1>&2
  exit 1
fi

IPADDR=`curl -s http://169.254.169.254/latest/meta-data/hostname`
USRDAT=`base64 -w0 <<EOS
#!/bin/sh
apt-get update
apt-get install -y awscli
export HOME=/home/ubuntu
(cd; \
 aws s3 cp s3://$aws_s3_bucket_name/$aws_s3_bin_path/aws.cf .; \
 aws s3 cp s3://$aws_s3_bucket_name/$aws_s3_bin_path/bootstrap.sh .; \
 chmod u+x bootstrap.sh; \
 ./bootstrap.sh -data-server $1 $IPADDR)
EOS`

SFRJSN=/tmp/big3store-rsf-config-$1.json
EXE=`/bin/cat > $SFRJSN <<EOS
{
  "SpotPrice": "$aws_ec2_spot_price",
  "TargetCapacity": $2,
  "IamFleetRole": "$aws_ec2_iam_fleet_role",
  "LaunchSpecifications": [
      {
          "ImageId": "$aws_ec2_ubuntu_ami_id",
	  "InstanceType": "$aws_ec2_instance_type_data_server",
	  "KeyName": "$aws_ec2_key_name",
	  "SecurityGroups": [{"GroupId": "$aws_ec2_security_group_id"}],
	  "IamInstanceProfile": {"Arn": "$aws_ec2_instance_profile_arn"},
	  "BlockDeviceMappings" : [
	     {
	           "DeviceName" : "/dev/sda1",
		   "Ebs" : { "VolumeSize" : $aws_ec2_vol_size_data_server }
	     }
	  ],
	  "TagSpecifications": [
	      {
	          "ResourceType": "instance",
	      	  "Tags": [
	              {
		          "Key":"$aws_ec2_service_tag_name",
		      	  "Value":"$aws_ec2_service_tag_value"
		      },
		      {
		          "Key":"$aws_ec2_module_tag_name",
		      	  "Value":"$aws_ec2_module_tag_data_server"
		      }
	      	  ]
	      }
      	  ],
	  "UserData": "$USRDAT"
      }
  ]
}
EOS`
eval $EXE

LOGFIL=/tmp/big3store-rsf-out-$1.log
EXE=`/bin/cat <<EOS
$AWSCMD ec2 request-spot-fleet\
 --spot-fleet-request-config file://$SFRJSN
EOS`
eval $EXE > $LOGFIL 2>&1

SFRIFL=/tmp/big3store-rsf-id-$1.txt
EXE=`/bin/cat <<EOS
grep SpotFleetRequestId $LOGFIL | cut -d'"' -f4
EOS`
eval $EXE > $SFRIFL

cat $SFRIFL

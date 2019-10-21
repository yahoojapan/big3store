#!/bin/sh
#
#       start a big3store front server instance for light tasks on AWS
#
# 	Copyright (C) 2014-2019 UP FAMNIT and Yahoo Japan Corporation
# 	Iztok Savnik <iztok.savnik@famnit.upr.si>
# 	Kiyoshi Nitta <knitta@yahoo-corp.jp>
#
ABSDIR=$(dirname $(realpath $0))
. $ABSDIR/../cf/aws.cf
AWSCMD="aws --region $aws_ec2_region"

USRDAT=`base64 -w0 <<EOS
#!/bin/sh
apt-get update
apt-get install -y awscli
export HOME=/home/ubuntu
(cd; \
 aws s3 cp s3://$aws_s3_bucket_name/$aws_s3_bin_path/aws.cf .; \
 aws s3 cp s3://$aws_s3_bucket_name/$aws_s3_bin_path/bootstrap.sh .; \
 chmod u+x bootstrap.sh; \
 ./bootstrap.sh )
EOS`

SFRJSN=/tmp/big3store-rsf-config-fs.json
EXE=`/bin/cat > $SFRJSN <<EOS
{
  "SpotPrice": "$aws_ec2_spot_price",
  "TargetCapacity": 1,
  "IamFleetRole": "$aws_ec2_iam_fleet_role",
  "LaunchSpecifications": [
      {
          "ImageId": "$aws_ec2_ubuntu_ami_id",
	  "InstanceType": "$aws_ec2_instance_type_front_server_micro",
	  "KeyName": "$aws_ec2_key_name",
	  "SecurityGroups": [{"GroupId": "$aws_ec2_security_group_id"}],
	  "IamInstanceProfile": {"Arn": "$aws_ec2_instance_profile_arn"},
	  "BlockDeviceMappings" : [
	     {
	           "DeviceName" : "/dev/sda1",
		   "Ebs" : { "VolumeSize" : $aws_ec2_vol_size_front_server_micro }
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
		      	  "Value":"$aws_ec2_module_tag_front_server"
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

LOGFIL=/tmp/bigstore-rsf-out-fs.log
EXE=`/bin/cat <<EOS
$AWSCMD ec2 request-spot-fleet\
 --spot-fleet-request-config file://$SFRJSN
EOS`
eval $EXE > $LOGFIL 2>&1

SFRIFL=/tmp/big3store-rsf-id-fs.txt
EXE=`/bin/cat <<EOS
grep SpotFleetRequestId $LOGFIL | cut -d'"' -f4
EOS`
eval $EXE > $SFRIFL

echo
echo for monitoring the progress:
echo tail -f /var/log/cloud-init-output.log
echo

sleep 10
$ABSDIR/describe-front-server-instances.sh
cat $SFRIFL

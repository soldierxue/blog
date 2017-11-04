# blog

## Testing Scenario 
Take AWS Public Data Set https://amazonaws-china.com/public-datasets/nexrad/ as the source, try to copy them to BJS region bucket.

## Step 1 : Generated S3 Bucket Inventory List
The source data bucket is from Region US Standard (N. Virginia), so we launched a bastion Amazon Linux EC2 with proper instance profile 

Configure the default profile, it applies IAM EC2 instance role to source bucket:

```
[ec2-user@ip-172-31-47-193 NEXRAD_Demo]$ aws configure
AWS Access Key ID [None]:
AWS Secret Access Key [None]:
Default region name [us-west-2]: us-east-1
Default output format [None]:
```
Configure the destination profile, it applies specific Access Key & Secret Access Key to Destination bucket (here is cn-north-1)
```
[ec2-user@ip-172-31-47-193 NEXRAD_Demo]$ aws configure --profile bjs
AWS Access Key ID [None]: AKI***************A
AWS Secret Access Key [None]: 7j+R6*****************oDrqU
Default region name [None]: cn-north-1
Default output format [None]:
```

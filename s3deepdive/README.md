# Demos for Blog 

* [Amazon S3 深度实践系列之一：S3 CLI深度解析及性能测试](https://amazonaws-china.com/cn/blogs/china/amazon-s3-depth-of-practice-series-s3-cli-depth-parsing-and-performance-testing/)

## Testing Scenario 
Take AWS Public Data Set https://amazonaws-china.com/public-datasets/nexrad/ as the source, try to copy them to BJS region bucket.

For complete demo please go and ref to [NEXRAD_Demo](https://github.com/soldierxue/blog/NEXRAD_Demo)

## Prerequisite : 
* Python 2.7.x
* Boto3 http://boto3.readthedocs.io/en/latest/guide/quickstart.html
* Disk : 300GB+ gp2
* Memory: 60GB+ (If you run parallel thread in one machine)

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
And add following additional timeout parameters:
```
[default]
region = us-east-1
metadata_service_timeout = 5
metadata_service_num_attempts = 5
[profile bjs]
region = cn-north-1
metadata_service_timeout = 5
metadata_service_num_attempts = 5
```
Execute following similar script, be carefull that, this dataset is very large, it will launch 1400+ parallel aws s3 list-object threads, so please use at least r4.2xlarge (61GB memory) to try this demo or it will throw "cant allocate memeory" error:

```
cd NEXRAD_Demo/inventory
nohup python ../../s3deepdive/s3_inventory.py -b noaa-nexrad-level2 -r us-east-1 -d 3 > noaa-nexrad-level2.log 2>&1 &
```
wait until following s3 thread process count is 0, then you will find this sript deep searched 2 prefix levels and generated total 6563 files that contains s3 objects information in this bucket.
```
[ec2-user@youserver inventory]$ ps -el | grep aws | wc -l
0
[ec2-user@youserver inventory]$ ls noaa-nexrad-level2.*obj* | wc -l
2551
[ec2-user@youserver inventory]$ mkdir 1 2
[ec2-user@youserver inventory]$ find ./ -size -800000c -print0 | xargs -0 -I {} mv {} ./1/
[ec2-user@youserver inventory]$ mv noaa-nexrad-level2.final.*obj* ./2/
```
## Step 2 : Analyze the objects and submit to SQS to prepare for data tansfer tasks

Because the networking from US-EAST-1 to BJS is a bit slow, we assumed it 10KB/s, so we defined following task parameters:
* Max Objects Number per Task : 10
* Max Objects Size Per Task: 20 MB
* Threshold to split is : 5MB
* Chunksize of the Splitted object : 5MB
```

cd NEXRAD_Demo/tasksubmit

nohup python ../../s3deepdive/s3_task_submit.py -d ../inventory/1/ -r us-east-1 > noaa-nexrad-level2.task1.log 2>&1 &
nohup python ../../s3deepdive/s3_task_submit.py -d ../inventory/2/ -r us-east-1 > noaa-nexrad-level2.task2.log 2>&1 &
```
## Step 3 : Read the tasks from SQS and do parallel data transfer

You have many options to parallelly run data transfer task, 

### Option 1: Manully Run multiple threads in one or a few machines


### Option 2: Custom User Data to download the script and run while the ec2 intance is starting, and base on this AMI, launch a spot instance fleet to parallel execution it

### Option 3: User AWS Batch to help you run batch jobs

### Option 4: Create a Container Cluster and run batch jobs by container scheduler

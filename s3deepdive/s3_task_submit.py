#!/bin/python
import argparse
import json
import logging
import os
import time
import datetime
import traceback
import fnmatch
import boto3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def current_milli_time():
    return int(round(time.time() * 1000))

class S3Tasks():
    def __init__(self,directory,region,profile):
        self.folder = directory
        self.file_includes = '*.obj.*'
        self.max_task_objects = 10  # the max total number of objects for one task
        self.max_task_size_mb = 20  # the max total size of objects for one task (MB)
        self.multipart_threshold = 5 # the threshod size(MB) of object need to be splitted
        self.multipart_chunksize = 5 # the chunksize (MB) for each splitted object part
        self.max_queue_msg = 80000 # the max number of message per SQS queue
        self.s3_tasks = []
        self.s3_tasks_split = []
        self.s3_tasks_small_objects = []
        self.test_total_size = 0
        self.test_total_objects = 0
        self.region = region
        self.profile = profile
        self.sqs_endpoint = "https://sqs.cn-north-1.amazonaws.com.cn/124280069046/S3RegionCopyQ"

    def groupTask1(self):
        fscount = 0 # count for files
        objcount = 0 # count for s3 objects
        objsizecount =  0  # count for s3 object size
        task_objects = [] # objects set for one task
        max_size_bytes = self.max_task_size_mb * 1024 * 1024
        task_count = 0
        for root, dirs, files in os.walk(self.folder):
            for file in files:
                # loop all objects and group tasks :
                #   (1) either 100MB  (self.max_task_size_mb)
                #   (2) or 100 objects (self.max_task_objects)
                if fnmatch.fnmatch(file,self.file_includes):
                    objects = self.getObjects(self.folder+file)
                    for s3obj in objects:
                        size = s3obj['Size']
                        if size >= 0 :
                            #### For testing purpose
                            self.test_total_objects = self.test_total_objects + 1
                            self.test_total_size = self.test_total_size + size     
                            ### actual logic                       
                            objcount = objcount +1
                            objsizecount_pre = objsizecount
                            objsizecount_after = objsizecount + size
                            if objsizecount_after > max_size_bytes and objcount == 1:
                                # if one single object exceed the max size
                                task_objects.append(s3obj)
                                task = {'total_size_bytes':objsizecount_after,'total_objects_num':objcount,"objects":task_objects}
                                self.s3_tasks.append(task)
                                task_count = task_count + 1
                                # logger.info("Task# %d, total objects num1 # %d, total objects num 2 # %d",task_count,objcount,len(task_objects))

                                # re-init tasks
                                objcount = 0
                                objsizecount = 0
                                task_objects = []                              
                            elif objcount <= self.max_task_objects and objsizecount_after <= max_size_bytes:
                                # if object added to one task not exceed the max size
                                task_objects.append(s3obj)
                                objsizecount = objsizecount_after
                            elif objcount > self.max_task_objects or objsizecount_after > max_size_bytes:
                                task = {'total_size_bytes':objsizecount_pre,'total_objects_num':objcount-1,"objects":task_objects}
                                self.s3_tasks.append(task)
                                task_count = task_count + 1
                                # logger.info("Task# %d, total objects num1 # %d, total objects num 2 # %d",task_count,objcount-1,len(task_objects))
                                # re-init tasks
                                objcount = 1
                                objsizecount = size
                                task_objects = []
                                task_objects.append(s3obj)
                            else:
                                logger.info("Other condition happened") 
                        else:
                            logger.info("Size < 0 happened")           

                    fscount = fscount + 1
                else:
                    logger.info("File #"+file+" doesn't match our pattern#"+self.file_includes)  
        # if at the end, there are objects left without joining to the any tasks        
        if len(task_objects) > 0  :
            task = {'total_size_bytes':objsizecount,'total_objects_num':objcount,"objects":task_objects}
            self.s3_tasks.append(task)
            task_count = task_count + 1
            logger.info("Task# %d, total objects num1 # %d, total objects num 2 # %d",task_count,objcount,len(task_objects))

        logger.info("Total %d files calculated.",fscount)

    def groupTask2(self):
        # loop for grouped tasks, filter the big size of object and split it into parts tasks
        threshold_bytes = self.multipart_threshold * 1024 * 1024
        chunksize_bytes = self.multipart_chunksize * 1024 * 1024

        for t in self.s3_tasks:
            total_s3obj = t['total_objects_num']
            total_s3obj_size = t['total_size_bytes']
            if total_s3obj_size >= threshold_bytes:
                totalNum = 0
                totalSize = 0
                subObjects = []
                for s3obj in t['objects']:
                    subObj = s3obj
                    size = s3obj['Size'] # bytes
                    if size >= threshold_bytes:
                        # splitted this big size object
                        numberOfParts = size / chunksize_bytes
                        sizeLeft = size % chunksize_bytes
                        if sizeLeft > 0:
                            numberOfParts = numberOfParts + 1
                        subtasks = self.groupIntoRangeParts(s3obj,chunksize_bytes,numberOfParts,size)
                        logger.info("Group big size object # %s , into total parts # %d by chunksize bytes # %d;it's total size is # %d",s3obj['Key'],numberOfParts,chunksize_bytes,size)
                    else:
                        subObjects.append(s3obj)
                        totalNum = totalNum + 1
                        totalSize = totalSize + size
                if len(subObjects) > 0:
                    task = {'total_size_bytes':totalSize,'total_objects_num':totalNum,"objects":subObjects}
                    self.s3_tasks_small_objects.append(task)
            else:
                self.s3_tasks_small_objects.append(t)
                

    def groupIntoRangeParts(self,s3obj,chunksize_bytes,totalParts,totalSizeBytes):
        count = 0
        task_objects = []
        totalSize = 0
        objcount = 0
        max_size_bytes = self.max_task_size_mb * 1024 * 1024
        i = 1
        while i <= totalParts:
            obj = {}
            objsize =  totalSizeBytes%chunksize_bytes if ((i == totalParts) & (chunksize_bytes * totalParts > totalSizeBytes )) else chunksize_bytes
            objsizecount_before = totalSize
            objsizecount_after = totalSize + objsize
            objcount = objcount + 1
            # cal the part range
            range_begin = chunksize_bytes * (i - 1)
            range_end1 = chunksize_bytes * i - 1
            range_end2 = totalSizeBytes - 1
            range_end = range_end1
            if range_end1 > range_end2 : 
                range_end = range_end2

            for k in s3obj.keys():
                obj[k] = s3obj[k]
            obj['range_begin'] = range_begin
            obj['range_end'] = range_end
            obj['partnumber'] = i
            obj['total_parts'] = totalParts

            if objcount <= self.max_task_objects and objsizecount_after <= max_size_bytes:
                task_objects.append(obj)
                totalSize = totalSize + objsize
            elif objcount > self.max_task_objects or objsizecount_after > max_size_bytes:
                task = {'total_size_bytes':objsizecount_before,'total_objects_num':objcount-1,"objects":task_objects}
                self.s3_tasks_split.append(task)
                task_objects = []
                task_objects.append(obj)
                totalSize = objsize
                objcount = 1

            i=i + 1

        objLeft = len(task_objects)
        if objLeft > 0:
            task = {'total_size_bytes':totalSize,'total_objects_num':objcount,"objects":task_objects}
            self.s3_tasks_split.append(task)
  
            
    def debugResult(self):
        total_s3obj = 0
        total_s3obj_size = 0

        for t in self.s3_tasks:
            total_s3obj = total_s3obj + t['total_objects_num']
            total_s3obj_size = total_s3obj_size + t['total_size_bytes']
            # logger.info(json.dumps(t))
            # response = sqs_client.send_message(
            #     QueueUrl=self.sqs_endpoint,
            #     MessageBody= json.dumps(t)
            #     )   
            # logger.info("Msg Id# %s",response['MessageId'])
        
        logger.info("Total s3 objects after 1'st time groups # %d, and total size # %d",total_s3obj,total_s3obj_size)  
        logger.info("Total %d tasks grouped.",len(self.s3_tasks)) 

        total_s3obj = 0
        total_s3obj_size = 0
        for t in self.s3_tasks_small_objects:
            total_s3obj = total_s3obj + t['total_objects_num']
            total_s3obj_size = total_s3obj_size + t['total_size_bytes']              
        logger.info("Total small s3 objects after 2'st time groups # %d, and total size # %d",total_s3obj,total_s3obj_size)
        logger.info("Tasks after second time groups # %d, the objects tasks no need to be splitted",len(self.s3_tasks_small_objects))

        total_s3obj = 0
        total_s3obj_size = 0
        for t in self.s3_tasks_split:
            total_s3obj = total_s3obj + t['total_objects_num']
            total_s3obj_size = total_s3obj_size + t['total_size_bytes']              
        logger.info("Total big size s3 objects after 2'st time groups # %d, and total size # %d",total_s3obj,total_s3obj_size)
        logger.info("Tasks after second time big size object splitting # %d",len(self.s3_tasks_split))
        
        logger.info("Total s3 objects directly calculated from inventory files list # %d, and total size # %d",self.test_total_objects,self.test_total_size) 

    def begin(self):
        # group first time by max objects and max size restriction per task
        self.groupTask1()
        # group sencond time by multiple part upload(big size object splitting)
        self.groupTask2()
        self.debugResult()
        # determine the total task number
        total_small_obj_tasks = len(self.s3_tasks_small_objects)
        total_bigsize_tasks = len(self.s3_tasks_split)
        # send tasks to sqs
        session = boto3.Session(profile_name=self.profile,region_name=self.region)
        sqs_client = session.client('sqs')
        queueNames = ['S3Task_DeadQueue','S3Task_NormalQueue','S3Task_BigSizeQueue']
        visTimeout = '50' # seconds for msg
        deadQRes = sqs_client.create_queue(
            QueueName=queueNames[0],
            Attributes={
                'VisibilityTimeout': visTimeout,
                'MessageRetentionPeriod':'1209600'
            }
        ) 
        logger.info("Dead Q's url# %s",json.dumps(deadQRes))
        deadQUrl = deadQRes['QueueUrl']
        splittedStrs = deadQUrl.split("/")
        size = len(splittedStrs)
        qname = splittedStrs[size - 1]
        awsaccount = splittedStrs[size - 2]
        arn_format = "arn:{0}:sqs:{1}:{2}:{3}"
        awsref = "aws-cn" if self.region.startswith("cn") else "aws"
        deadQArn = arn_format.format(awsref,self.region,awsaccount,qname)
        logger.info("Dead q urn # %s",deadQArn)
        # clear the queue messages
        sqs_client.purge_queue(
            QueueUrl=deadQRes['QueueUrl']
        ) 
        
        # calculate the nubmer of queue needed
        normalQCount = total_small_obj_tasks / self.max_queue_msg
        normalQLeft = total_small_obj_tasks % self.max_queue_msg
        normalQCount = normalQCount if normalQLeft <= 0 else normalQCount + 1
        # create queues
        idxNormal = 0
        normalQUrlArray = []
        while idxNormal < normalQCount:
            idxNormal = idxNormal + 1
            normalQRes = sqs_client.create_queue(
                QueueName=queueNames[1]+str(idxNormal),
                Attributes={
                    'VisibilityTimeout': visTimeout
                    ,'RedrivePolicy':json.dumps({
                        'deadLetterTargetArn':deadQArn,
                        'maxReceiveCount': '10'
                    })
                }
            )
            # logger.info("Normal Q's url# %s",json.dumps(normalQRes)) 
            normalQUrlArray.append(normalQRes['QueueUrl'])
            # clear the queue messages
            sqs_client.purge_queue(
                QueueUrl=normalQRes['QueueUrl']
            )

        # calculate the nubmer of queue needed
        bigsizeQCount = total_bigsize_tasks / self.max_queue_msg
        bigsizeQLeft = total_bigsize_tasks % self.max_queue_msg
        bigsizeQCount = bigsizeQCount if bigsizeQLeft <= 0 else bigsizeQCount + 1
        # create queues
        idxBigsize = 0
        bigsizeQUrlArray = []
        while idxBigsize < bigsizeQCount:
            idxBigsize = idxBigsize + 1
            bigsizeQRes = sqs_client.create_queue(
                QueueName=queueNames[2]+str(idxBigsize),
                Attributes={
                    'VisibilityTimeout': visTimeout
                    ,'RedrivePolicy':json.dumps({
                        'deadLetterTargetArn':deadQArn,
                        'maxReceiveCount': '10'
                    })
                }
            )
            # logger.info("Normal Q's url# %s",bigsizeQRes['QueueUrl'])
            bigsizeQUrlArray.append(bigsizeQRes['QueueUrl'])
            # clear the queue messages
            sqs_client.purge_queue(
                QueueUrl=bigsizeQRes['QueueUrl']
            ) 

        msgcount = 0
        queueIdx = 0
        for t in self.s3_tasks_small_objects:
            logger.info(json.dumps(t))
            response = sqs_client.send_message(
                QueueUrl=normalQUrlArray[queueIdx],
                MessageBody= json.dumps(t)
                )   
            msgcount = msgcount + 1
            if msgcount % self.max_queue_msg == 0:
                queueIdx = queueIdx + 1
        
        logger.info("Total Task Msg# %d for normal objects",msgcount)  

        msgcount = 0
        queueIdx = 0
        for t in self.s3_tasks_split:
            logger.info(json.dumps(t))
            response = sqs_client.send_message(
                QueueUrl=bigsizeQUrlArray[queueIdx],
                MessageBody= json.dumps(t)
                )   
            msgcount = msgcount + 1 
            if msgcount % self.max_queue_msg == 0:
                queueIdx = queueIdx + 1

        logger.info("Total Task Msg# %d for bigsize objects",msgcount)          
              

    def getObjects(self,file):
        if os.path.exists(file):
            with open(file, 'r') as f:
                try:
                    fbuffer = f.read().decode("utf-8")
                    #logger.info("File content#"+json.dumps(fbuffer))
                    jdata = json.loads(fbuffer)
                    objects = []

                    if 'Keys' in jdata:
                        objects = jdata['Keys']
                    if 'Contents' in jdata:
                        objects = jdata['Contents']  

                    if objects and len(objects) > 0:    
                        logger.info("Objects found for file #"+file+","+str(len(objects)))
                    else:
                        objects = []
                        logger.info("No Objects found for file #"+file)        
                    f.close()
                    return objects
                except:
                    logger.info("Something error for cal file #"+file)
                    traceback.print_exc()
                    objects = []
                    return objects
        return []        

def main():
    logger.info("**** Group s3 objects into tasks *****! ")
    parser = argparse.ArgumentParser(description="Execute input file. Supports only python or sh file.")
    parser.add_argument('-d', '--directory', required=True, help="the directory of s3 list objects results")   
    parser.add_argument('-r', '--region', required=False, default='cn-north-1', help="Region. Default 'cn-north-1'")
    parser.add_argument('-p','--profile', required=False, default='default',help="The profile name of aws configure locally, default is 'default'")    
    args = parser.parse_args() 

    mydir = args.directory
    profile = args.profile
    region = args.region

    s3task = S3Tasks(mydir,region,profile)
    start = current_milli_time()
    s3task.begin()
    end = current_milli_time()
    
    logger.info("Total Running Seconds # %d",(end-start)/1000)
    


if __name__ == "__main__":
    main()    

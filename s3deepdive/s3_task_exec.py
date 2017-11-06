#!/bin/python
import argparse
import json
import logging
import time
import datetime
import threading
import boto3
from botocore.config import Config
import traceback

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

isTaskAllSuccessDone = True
def current_milli_time():
    return int(round(time.time() * 1000))

def globalRegionsReplicate(profile,sb,db,s3obj,destProfile):
    logger = logging.getLogger(__name__)
    cfg = Config(connect_timeout=120,read_timeout=600)
    session = boto3.Session(profile_name=profile,cfg)
    s3_client = session.client('s3')
    try:
        okey = s3obj['Key']
        osize = s3obj['Size']
        logger.info("Copy object [%s of size %d ]from source  %s to destination %s",okey,osize,sb,db)
        # for object been splitted, just copy each part into destination bucket
         # first download it , then upload it; if the regions are cross aws china & aws global
        isCrossCNGlobal = False if destProfile == '' or profile == destProfile else True
        isDownloadByRange = False
        rend = 0
        rbegin = 0
        bytesObj = 0
        cachedS3Obj = None
        rangeObjSuffix = ""
        destObjKey = okey
        if 'range_end' in s3obj and 'range_begin' in s3obj:
            rend = s3obj['range_end']
            rbegin = s3obj['range_begin']
            isDownloadByRange = True
            rangeObjSuffix = "."+str(s3obj['partnumber'])
            destObjKey = destObjKey+rangeObjSuffix
            bytesObj = rend - rbegin + 1
           
        if not isCrossCNGlobal:
            s3_client.copy_object(
                ACL='bucket-owner-full-control',
                Bucket=db,
                Key=okey,
                ResponseContentEncoding='utf-8',
                CopySource={'Bucket': sb, 'Key': okey}
            )
            
        elif isDownloadByRange:
            # download the object by range
            res = s3_client.get_object(
                Bucket=sb,
                Key=okey,
                ResponseContentEncoding='utf-8',
                Range="bytes="+str(rbegin)+'-'+str(rend)
            )
            cachedS3Obj = res['Body'].read()
            logger.info("Download parts by range# %s",str(rbegin)+'-'+str(rend))

        else:
            # download the whole object
            logger.info("Download whole object")
            res = s3_client.get_object(
                Bucket=sb,
                Key=okey,
                ResponseContentEncoding='utf-8'
            )            
            cachedS3Obj = res['Body'].read()
            bytesObj = s3obj['Size']

        if cachedS3Obj is not None:
            logger.info("Bytes form len # %d, bytes from Message # %d",len(cachedS3Obj),bytesObj)
            session2 = boto3.Session(profile_name=destProfile,cfg)
            s3_client2 = session2.client('s3')
            s3_client2.put_object(
                ACL='bucket-owner-full-control',
                Body=cachedS3Obj,
                Bucket=db,
                Key=destObjKey,
                ContentLength=bytesObj
            ) 
                
                
    except:
        global isTaskAllSuccessDone
        isTaskAllSuccessDone = False
        traceback.print_exc()    
    

def main():
    logger.info("**** Execution s3 copy actions from tasks queue *****! ")
    parser = argparse.ArgumentParser(description="Execute input file. Supports only python or sh file.")
    parser.add_argument('-q', '--queue', required=True, help="the queue name of s3 tasks") 
    parser.add_argument('-p', '--profile', required=False,default='default', help="the instance profile")
    parser.add_argument('-source_bucket', '--source_bucket', required=True, help="the source bucket")  
    # parser.add_argument('-source_region', '--source_region', required=True, help="the source bucket region")  
    parser.add_argument('-dest_bucket', '--dest_bucket', required=True, help="the destination bucket")
    parser.add_argument('-dest_profile', '--dest_profile', required=False,default='', help="the destination region aws configuration profile, required while copy objects between china & global regions")
    # parser.add_argument('-dest_region', '--dest_region', required=True, help="the destination bucket region")  
                    
    args = parser.parse_args() 

    task_queue = args.queue
    profile = args.profile
    sourceBucket = args.source_bucket
    # sourceRegion = args.source_region
    destBucket = args.dest_bucket
    # destRegion = args.dest_region
    destProfile = args.dest_profile

    start = current_milli_time()
    try:
        session = boto3.Session(profile_name=profile)
        sqs_client = session.resource('sqs')
        # queueUrl = sqs_client.get_queue_url(QueueName=task_queue)
        queue = sqs_client.get_queue_by_name(QueueName=task_queue)
        msgs = queue.receive_messages(VisibilityTimeout=120)
        # msgRes = sqs_client.receive_messages(QueueUrl=queueUrl,VisibilityTimeout=120,MaxNumberOfMessages=1)
        global isTaskAllSuccessDone
        logger.info("Infor Total messages # %d ",len(msgs))
        for message in msgs:
            isTaskAllSuccessDone = True
            body = message.body
            tdata = json.loads(body,'utf-8')
            objects = tdata['objects']
            s3copy_threads = []
            i = 1
            logger.info("Infor Total objects to be copied # %d ",len(objects))
            for s3obj in objects:
                new_thread = threading.Thread(name='s3_cp_thread#'+str(i),
                            target=globalRegionsReplicate,args=(profile,sourceBucket,destBucket,s3obj,destProfile)) 
                s3copy_threads.append({'thread':new_thread})
                new_thread.start()

            isAnyThreadRunning = True
            while isAnyThreadRunning:
                isAnyThreadRunning = False
                for cp_thread in s3copy_threads:
                    t =   cp_thread['thread']   
                    if t.isAlive() :
                        isAnyThreadRunning = True
                        time.sleep(2)
                        break
            
            if isTaskAllSuccessDone :
                message.delete()
                
            
        
    except:
        logger.info("Some errors happened while retriving messages!")
        traceback.print_exc()    
    
    end = current_milli_time()

    logger.info("Total Running Seconds # %d",(end-start)/1000)
    


if __name__ == "__main__":
    main() 

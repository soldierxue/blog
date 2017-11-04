#!/bin/python
import argparse
import json
import logging
import os
import io
import time
import sys
import datetime 
import traceback

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def current_milli_time():
    return int(round(time.time() * 1000))

class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, datetime.date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)

class RecursionPrefixes():
    def __init__(self,b,d,p):
        self.bucket = b
        self.max_wait = 10 # max seconds waiting for one time list-object-v2 operation
        self.max_depth = d # the max depth scaned one by one prefix
        # self.s3_prefixes = []
        self.task_prefixes = [] # task to loop prefixes each level
        self.task_objects = [] # the most depth prefixes to fetch all objects
        self.rtfile_temp = b+".{0}.{1}.obj.{2}"
        self.prefix_para = " --prefix \"{0}\""
        self.profile = p
        self.query = "--query \"{Keys:Contents[].{Key: Key, Size: Size,ETag:ETag},CommonPrefixes:CommonPrefixes[].Prefix}\""
        self.cmd_temp = "nohup aws s3api list-objects-v2 --bucket {0} --delimiter '/' {2} {1} --profile {4} > {3}  &"
        self.cmd_temp_obj = "nohup aws s3api list-objects-v2 --bucket {0} {2} {1} --profile {4} --cli-read-timeout 0 --cli-connect-timeout 0  > {3} &"
    
    def fetchTop(self):
        logger.info("Begin list top level folders and objects!")
        idx = 1
        fileName = self.rtfile_temp.format("0",str(idx),current_milli_time())#bucket is the top level,starting from 0
        cmd = self.cmd_temp.format(self.bucket,self.query,"",fileName,self.profile)
        cmd2 = self.cmd_temp_obj.format(self.bucket,"","",fileName,self.profile)
        if self.max_depth <= 0:
            cmd = cmd2
        logger.info(cmd)
        os.system(cmd) 
        self.task_prefixes.append({'level':0,'file':fileName,'parent':0,'prefix':''})
        return fileName

    def fetch2(self,f,depth,parent):
        if depth > self.max_depth : 
            return

        logger.info("Enter into %s recursive of %s",depth,f)
        tcount = 0
        while(not self.isFileReady(f) and tcount <= self.max_wait):
            time.sleep(2) 
            tcount = tcount + 2
        nextPrex = self.getPrefixes(f)

        if nextPrex and len(nextPrex) > 0:
            logger.info("We have prefixes # %d",len(nextPrex))
            idx = 0
            nextTasks = []
            for p in nextPrex:
                idx = idx + 1
                fidx = parent+"_"+str(idx)
                fileName = self.rtfile_temp.format(str(depth),fidx,current_milli_time())
                para_p_value = p.encode('utf-8')
                para_p = self.prefix_para.format(para_p_value)
                cmd = self.cmd_temp.format(self.bucket,self.query,para_p,fileName,self.profile)
                task = {'level':depth+1,'file':fileName,'parent':fidx,'prefix':para_p_value}
                
                if depth == self.max_depth:
                    self.task_objects.append(p)
                else:
                    os.system(cmd)
                    nextTasks.append(task)
                    self.task_prefixes.append(task)
                    logger.info(cmd)
            
            for t in nextTasks:
                self.fetch2(t['file'],t['level'],t['parent'])    
        else:
            logger.info("No more prefixes")
            return  


    def begin(self):
        file = self.fetchTop()
        # if the max depth equals or greater than 1, recurisively deal with it until the max depth
        if self.max_depth >= 1:
            self.fetch2(file,1,"0")
        fidx = 0
        pauseTimeIdx = 0
        for p in self.task_objects:
            fidx = fidx + 1
            fileName = self.rtfile_temp.format("final",fidx,"")
            para_p_value = p.encode('utf-8')
            para_p = self.prefix_para.format(para_p_value)
            cmd = self.cmd_temp_obj.format(self.bucket,"",para_p,fileName,self.profile)
            logger.info(cmd)
            os.system(cmd)
            if fidx % 100 == 0:
                pauseTimeIdx = pauseTimeIdx + 1
                time.sleep(20 * pauseTimeIdx)
                
            task =  {'level':self.max_depth,'file':fileName,'parent':-1,'prefix':para_p_value}
            self.task_prefixes.append(task)

        #  Tell the summary of asy list object operation for this bucket
        totalFileGenerated = len(self.task_prefixes)
        for t in self.task_prefixes:
            logger.info("List objects into # %s with prefix # %s",t['file'],t['prefix'])
        logger.info("Total # %d results files generated",totalFileGenerated)    

    def isFileReady(self,file):
        if os.path.exists(file):
            with open(file, 'r') as f:
                fline = f.readline().decode("utf-8")
                if fline != "":
                    try:
                        os.rename(file, file)
                        return True
                    except:
                        return False
        return False

    def getPrefixes(self,file):
        if os.path.exists(file):
            with open(file, 'r') as f:
                try:
                    fbuffer = f.read().decode("utf-8")
                    #logger.info("File content#"+json.dumps(fbuffer))
                    jdata = json.loads(fbuffer)
                    prefixes = []
                    if 'CommonPrefixes' in jdata:
                        prefixes = jdata['CommonPrefixes']    
                    f.close()
                    return prefixes
                except:
                    return []
        return []    


def main():
    logger.info("**** Analyize the objects in specific bucket *****! ")
    parser = argparse.ArgumentParser(description="Execute input file. Supports only python or sh file.")
    parser.add_argument('-b', '--bucket', required=True, help="bucket name")                  
    parser.add_argument('-r', '--region', required=False, default='cn-north-1', help="Region. Default 'cn-north-1'")
    parser.add_argument('-d','--depth', required=False, default='2',help="The max prefix depth to be analyzed, default is 1, start index is 0")
    parser.add_argument('-p','--profile', required=False, default='default',help="The profile name of aws configure locally, default is 'default'")
    args = parser.parse_args() 

    region = args.region
    os.environ["AWS_DEFAULT_REGION"] = region

    bucket = args.bucket
    depth = int(args.depth)
    profile = args.profile
    logger.info("Bucket# %s ,region# %s ,with prefixes depth# %d , profile # %s", bucket,region,depth,profile)
    rpobj = RecursionPrefixes(bucket,depth,profile)
    start = current_milli_time()
    rpobj.begin()
    end = current_milli_time()
    logger.info("Total Running Seconds # %d",(end-start)/1000)

if __name__ == "__main__":
    main()    

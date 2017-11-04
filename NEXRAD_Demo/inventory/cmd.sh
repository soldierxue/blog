#!/bin/bash
nohup python ../../s3deepdive/s3_inventory.py -b noaa-nexrad-level2 -r us-east-1 -d 2 > noaa-nexrad-level2.log 2>&1 &
#nohup python ../../s3deepdive/3_inventory.py -b unidata-nexrad-level2-chunks -r us-east-1 -d 1 > unidata-nexrad-level2-chunks.log 2>&1 &

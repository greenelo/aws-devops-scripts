# S3-buckets-remove-all-objects

## Description
This script is to remove all S3 objects, automatically supporting the following regions:
* ap-east-1
* ap-northeast-1
* ap-southeast-1

## Usage
1. update the script with aws access key and secret key (AWS_ACCESS_KEY, AWS_SECRET_KEY)
2. run `pip install -r requirements.txt`
3. run `python3 main.py`
4. input arguments accordingly

## Arguments
1. bucket prefix to be included, e.g. `abc`
2. bucket prefix to be excluded, e.g. `def`
3. dry run mode `y/n`
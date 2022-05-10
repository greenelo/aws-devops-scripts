# ECR-remove-all-repos

## Description
This script is to remove all ECR repositories

## Usage
1. update the script with aws access key and secret key (AWS_ACCESS_KEY, AWS_SECRET_KEY)
2. run `pip install -r requirements.txt`
3. run `python3 main.py`
4. input arguments accordingly

## Arguments
1. region `(default ap-northeast-1)`
2. ecr repo prefix to be included, e.g. `abc-repo`
3. ecr repo prefix to be excluded, e.g. `def-repo`
4. dry run mode `y/n`
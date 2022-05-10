# S3 Remove objects by storage class

## Description
only non-current version will be removed to avoid business impact

## Usage
1. update the script with aws access key and secret key (AWS_ACCESS_KEY, AWS_SECRET_KEY)
2. run `pip install -r requirements.txt`
3. run `python3 main.py`
4. input arguments accordingly

## Arguments
1. region `(default ap-northeast-1)`
2. bucket prefix to be included, e.g. `backup-emp2-clouddev`
3. bucket prefix to be excluded, e.g. `backup-emp2-clouddev-foundation`
4. storage class to be removed `STANDARD|REDUCED_REDUNDANCY|GLACIER|STANDARD_IA|ONEZONE_IA|INTELLIGENT_TIERING|DEEP_ARCHIVE|OUTPOSTS|GLACIER_IR`
5. dry run mode, in dry run mode, only objects will be scanned, no deletion will be performed
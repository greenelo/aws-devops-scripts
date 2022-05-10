import boto3
import threading

AWS_ACCESS_KEY = ""
AWS_SECRET_KEY = ""

sessions = [
    boto3.Session(aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY, region_name='ap-east-1'),
    boto3.Session(aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY, region_name='ap-southeast-1'),
    boto3.Session(aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY, region_name='ap-northeast-1')
]
s3_client_map = {
    "ap-east-1": sessions[0].client("s3"),
    "ap-southeast-1": sessions[1].client("s3"),
    "ap-northeast-1": sessions[2].client("s3")
}
s3_clients = list(s3_client_map.values())


def regional_retryable_s3_client(function):
    result = None
    for s3_client in s3_clients:
        try:
            result = function(s3_client)
            if result is not None:
                return result
        except s3_client.exceptions.from_code('IllegalLocationConstraintException'):
            pass
    if result is None:
        raise Exception("no result")


def regional_s3_client(region: str):
    return s3_client_map.get(region)


def get_bucket_list():
    result = s3_clients[0].list_buckets()
    buckets = []
    if result:
        buckets = result.get("Buckets")
    return buckets


def get_filtered_bucket_name_list(bucket_prefix: str, exclude_bucket_prefix: str):
    all_buckets = get_bucket_list()
    filtered_bucket_names = []
    for bucket in all_buckets:
        bucket_name = bucket.get("Name")
        if (bucket_prefix == '' or bucket_name.startswith(bucket_prefix)) and (
                exclude_bucket_prefix == '' or not bucket_name.startswith(exclude_bucket_prefix)):
            filtered_bucket_names.append(bucket_name)
    return filtered_bucket_names


def get_bucket_location(bucket_name: str):
    result = regional_retryable_s3_client(lambda x: x.get_bucket_location(Bucket=bucket_name))
    return result.get('LocationConstraint')


def get_bucket_acl(bucket_name: str):
    result = regional_retryable_s3_client(lambda x: x.get_bucket_acl(Bucket=bucket_name))
    grants = result.get('Grants')
    return grants


def list_object_versions(bucket_name: str, bucket_region: str, next_token, next_version_id_token):
    s3_client = regional_s3_client(bucket_region)
    if next_token is None or next_version_id_token is None:
        result = s3_client.list_object_versions(Bucket=bucket_name)
    else:
        result = s3_client.list_object_versions(Bucket=bucket_name, KeyMarker=next_token,
                                                VersionIdMarker=next_version_id_token)
    return {
        'objects': result.get("Versions"),
        'next_token': result.get("NextKeyMarker"),
        'next_version_id_token': result.get("NextVersionIdMarker"),
        'size': result.get("MaxKeys")
    }


def delete_object_with_versions(bucket_name: str, bucket_region: str, delete_object_versions):
    s3_client = regional_s3_client(bucket_region)
    result = s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': delete_object_versions, 'Quiet': True})
    return {
        'deleted_objects': result.get("Deleted"),
        'error_objects': result.get("Errors")
    }

def process_for_bucket(bucket_name: str, summary_result, dryrun: bool):
    print(f"checking bucket {bucket_name}")

    bucket_location = get_bucket_location(bucket_name)
    next_token = None
    next_version_id_token = None
    summary_result[bucket_name] = {"success_count": 0, "failure_count": 0, "total_object_count": 0}
    total_object_count = 0

    while True:
        result = list_object_versions(bucket_name, bucket_location, next_token, next_version_id_token)
        next_token = result.get("next_token")
        next_version_id_token = result.get("next_version_id_token")

        objects = result.get("objects")
        if objects is not None:
            total_object_count += len(result.get("objects"))
            delete_object_versions = list(map(lambda x: {"Key": x.get("Key"), "VersionId": x.get("VersionId")}, objects))
            print(f"{total_object_count} objects to be deleted for bucket {bucket_name}")

            if len(delete_object_versions) > 0:
                if not dryrun:
                    delete_result = delete_object_with_versions(bucket_name, bucket_location, delete_object_versions)
                    if delete_result.get("deleted_objects") is not None:
                        summary_result[bucket_name]["success_count"] += len(delete_result.get("deleted_objects"))
                    if delete_result.get("error_objects") is not None:
                        summary_result[bucket_name]["failure_count"] += len(delete_result.get("error_objects"))
                else:
                    print(f"skipping delete... no object will be deleted under dry run mode")

        if next_token is None and next_version_id_token is None:
            break

    summary_result[bucket_name]["total_object_count"] = total_object_count
    print(f"finished examining for bucket {bucket_name}")


def process(bucket_prefix: str, exclude_bucket_prefix: str, dryrun: bool):
    print(f"getting bucket list")
    summary_result = {}
    buckets = get_filtered_bucket_name_list(bucket_prefix, exclude_bucket_prefix)
    process_list = []

    for bucket_name in buckets:
        process_list.append(threading.Thread(target=process_for_bucket, args=(bucket_name, summary_result, dryrun)))

    for p in process_list:
        p.start()

    for p in process_list:
        p.join()

    print(f"=====================================================")
    for bucket_result in summary_result:
        print(f"{bucket_result}: {summary_result.get(bucket_result).get('total_object_count')}")
        print(f"------------------")


bucket_prefix = input("input bucket prefix to be included [leave empty to include all]: ")
exclude_bucket_prefix = input("input bucket prefix to be excluded [leave empty to include all]: ")
dryrun_mode = input("is it dry run mode? [y/n]: ")
dryrun = True
if dryrun_mode == 'n':
    dryrun = False
print(
    f"running for [bucket prefix={bucket_prefix}], [bucket exclude_bucket_prefix={exclude_bucket_prefix}], [dry run={dryrun}]")
confirm = input("pls enter [yes] to confirm: ")
if confirm != 'yes':
    print("exiting script")
    exit(0)
process(bucket_prefix, exclude_bucket_prefix, dryrun)

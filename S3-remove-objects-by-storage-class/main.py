import boto3
import time
import threading

AWS_ACCESS_KEY = ""
AWS_SECRET_KEY = ""

DEFAULT_REGION = "ap-northeast-1"


def get_bucket_list(s3_client):
    result = s3_client.list_buckets()
    buckets = []
    if result:
        buckets = result.get("Buckets")
    return buckets


def get_bucket_region(s3_client, bucket_name: str):
    result = s3_client.get_bucket_location(Bucket=bucket_name)
    return result.get("LocationConstraint")


def get_filtered_bucket_name_list(s3_client, bucket_prefix: str, region: str, exclude_bucket_prefix: str):
    all_buckets = get_bucket_list(s3_client)
    filtered_bucket_names = []
    for bucket in all_buckets:
        bucket_name = bucket.get("Name")
        if (bucket_prefix == '' or bucket_name.startswith(bucket_prefix)) and (exclude_bucket_prefix == '' or not bucket_name.startswith(exclude_bucket_prefix)):
            bucket_region = get_bucket_region(s3_client, bucket_name)
            if bucket_region == region:
                filtered_bucket_names.append(bucket_name)
    return filtered_bucket_names


def list_object_versions(s3_client, bucket_name: str, next_token, next_version_id_token):
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


def delete_object_with_versions(s3_client, bucket_name: str, delete_object_versions):
    result = s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': delete_object_versions, 'Quiet': True})
    return {
        'deleted_objects': result.get("Deleted"),
        'error_objects': result.get("Errors")
    }


def process_for_bucket(s3_client, bucket: str, summary_result, storage_class: str, dryrun: bool):
    print(f"examining bucket {bucket}")
    next_token = None
    next_version_id_token = None
    summary_result[bucket] = {"success_count": 0, "failure_count": 0, "elapsed_time": 0.0, "total_object_count": 0,
                              "total_object_count_to_be_deleted": 0}
    start_time = time.time()
    total_object_count = 0
    total_object_count_to_be_deleted = 0
    skipped_current_version_objects = 0

    while True:
        result = list_object_versions(s3_client, bucket, next_token, next_version_id_token)
        next_token = result.get("next_token")
        next_version_id_token = result.get("next_version_id_token")

        objects = result.get("objects")
        if objects is not None:
            total_object_count += len(result.get("objects"))
            filtered_objects = list(filter(lambda x: x.get("StorageClass") == storage_class and x.get("IsLatest") is False, objects))
            skipped_current_version_objects += len(list(filter(lambda x: x.get("StorageClass") == storage_class and x.get("IsLatest") is True, objects)))

            delete_object_versions = list(
                map(lambda x: {"Key": x.get("Key"), "VersionId": x.get("VersionId")}, filtered_objects))
            total_object_count_to_be_deleted += len(delete_object_versions)
            print(f"{total_object_count_to_be_deleted} objects to be deleted for bucket {bucket}")

            if len(delete_object_versions) > 0:
                if not dryrun:
                    delete_result = delete_object_with_versions(s3_client, bucket, delete_object_versions)
                    if delete_result.get("deleted_objects") is not None:
                        summary_result[bucket]["success_count"] += len(delete_result.get("deleted_objects"))
                    if delete_result.get("error_objects") is not None:
                        summary_result[bucket]["failure_count"] += len(delete_result.get("error_objects"))
                else:
                    print(f"skipping delete... no object will be deleted under dry run mode")

        if next_token is None and next_version_id_token is None:
            break

    end_time = time.time()
    elapsed_time = end_time - start_time
    summary_result[bucket]["total_object_count"] = total_object_count
    summary_result[bucket]["elapsed_time"] = elapsed_time
    summary_result[bucket]["total_object_count_to_be_deleted"] = total_object_count_to_be_deleted
    summary_result[bucket]["skipped_current_version_objects"] = skipped_current_version_objects
    print(f"finished examining for bucket {bucket}")

def process(s3_client, bucket_prefix: str, region: str, storage_class: str, dryrun: bool, exclude_bucket_prefix: str):
    print(f"getting buckets with prefix {bucket_prefix}")

    target_buckets = get_filtered_bucket_name_list(s3_client, bucket_prefix, region, exclude_bucket_prefix)
    summary_result = {}
    process_list = []

    for bucket in target_buckets:
        process_list.append(threading.Thread(target=process_for_bucket, args=(s3_client, bucket, summary_result, storage_class, dryrun)))

    for p in process_list:
        p.start()

    for p in process_list:
        p.join()

    print(f"=====================================================")
    for bucket_result in summary_result:
        print(f"{bucket_result}: ")
        print(
            f"elapsed time: {summary_result.get(bucket_result).get('elapsed_time'):.2f}s"
            f", deleted objects / examined objects: {summary_result.get(bucket_result).get('total_object_count_to_be_deleted')}/{summary_result.get(bucket_result).get('total_object_count')}"
            f", skipped (current version): {summary_result[bucket_result]['skipped_current_version_objects']}")
        print(f"------------------")


region = input("input region [default ap-northeast-1]: ")
bucket_prefix = input("input bucket prefix to be included [leave empty to include all]: ")
exclude_bucket_prefix = input("input bucket prefix to be excluded [leave empty to include all]: ")
storage_class = input(
    "input storage class to be removed [STANDARD|REDUCED_REDUNDANCY|GLACIER|STANDARD_IA|ONEZONE_IA|INTELLIGENT_TIERING|DEEP_ARCHIVE|OUTPOSTS|GLACIER_IR]: ")
dryrun_mode = input("is it dry run mode? [y/n]: ")
dryrun = True
if region == '' or region is None:
    region = DEFAULT_REGION
if dryrun_mode == 'n':
    dryrun = False

print(
    f"running for [region={region}], [bucket prefix={bucket_prefix}], [bucket exclude_bucket_prefix={exclude_bucket_prefix}], [storage class={storage_class}], [dry run={dryrun}]")
confirm = input("pls enter [yes] to confirm: ")
if confirm != 'yes':
    print("exiting script")
    exit(0)

session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY,
                        region_name=region)
client = session.client("s3")

process(client, bucket_prefix, region, storage_class, dryrun, exclude_bucket_prefix)

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


def is_bucket_public_accessible(bucket_name: str):
    grants = get_bucket_acl(bucket_name)
    return len(list(
        filter(lambda grant: grant.get('Grantee').get('URI') == 'http://acs.amazonaws.com/groups/global/AllUsers',
               grants))) > 0


def list_object(bucket_name: str, bucket_region: str, next_token):
    s3_client = regional_s3_client(bucket_region)
    if next_token is None:
        result = s3_client.list_objects_v2(Bucket=bucket_name)
    else:
        result = s3_client.list_objects_v2(Bucket=bucket_name, ContinuationToken=next_token)
    return {
        'objects': result.get("Contents"),
        'next_token': result.get("NextContinuationToken")
    }


def check_object_acl(bucket_name: str, bucket_region: str, object_key: str, public_accessible_object_keys):
    s3_client = regional_s3_client(bucket_region)
    result = s3_client.get_object_acl(Bucket=bucket_name, Key=object_key)
    if len(list(
        filter(lambda grant: grant.get('Grantee').get('URI') == 'http://acs.amazonaws.com/groups/global/AllUsers',
               result.get("Grants")))) > 0:
        public_accessible_object_keys.append(object_key)


def get_public_accessible_objects_in_bucket(bucket_name: str, bucket_region: str):
    next_token = None
    public_accessible_object_keys = []
    total_object_count = 0
    while True:
        object_list_result = list_object(bucket_name, bucket_region, next_token)
        object_list = object_list_result.get("objects")
        print(f"examining {len(object_list)} objects for token {next_token}")
        next_token = object_list_result.get("next_token")
        total_object_count += len(object_list)

        process_list = []

        for object_item in object_list:
            object_key = object_item.get("Key")
            if "product-notification" not in object_key.lower():
                # any_public_accessible_object_exists = check_object_acl(bucket_name, bucket_region, object_key)
                # if any_public_accessible_object_exists:
                #     public_accessible_object_keys.append(object_key)
                print(f"checking object {object_key}")
                process_list.append(threading.Thread(target=check_object_acl, args=(bucket_name, bucket_region, object_key, public_accessible_object_keys)))

        for p in process_list:
            p.start()

        for p in process_list:
            p.join()

        print(f"{total_object_count} objects have been examined, found public objects {public_accessible_object_keys}")

        if next_token is None:
            break

    return public_accessible_object_keys


def check_bucket(bucket_name: str, result):
    print(f"checking bucket {bucket_name}")

    bucket_location = get_bucket_location(bucket_name)
    is_bucket_public = is_bucket_public_accessible(bucket_name)
    public_accessible_object_keys = get_public_accessible_objects_in_bucket(bucket_name, bucket_location)
    result.append({
        'bucket_name': bucket_name,
        'bucket_location': bucket_location,
        'is_bucket_public': is_bucket_public,
        'public_accessible_object_keys': public_accessible_object_keys
    })


def process(bucket_prefix: str, exclude_bucket_prefix: str):
    print(f"getting bucket list")
    summary_result = []
    buckets = get_filtered_bucket_name_list(bucket_prefix, exclude_bucket_prefix)
    process_list = []

    for bucket_name in buckets:
        process_list.append(threading.Thread(target=check_bucket, args=(bucket_name, summary_result)))

    for p in process_list:
        p.start()

    for p in process_list:
        p.join()

    print(f"=====================================================")
    for item in summary_result:
        if item['is_bucket_public'] or len(item['public_accessible_object_keys']) > 0:
            print(f"{item['bucket_name']},{item['bucket_location']},{item['is_bucket_public']}")
            for public_accessible_object in item['public_accessible_object_keys']:
                print(f"{public_accessible_object}")


bucket_prefix = input("input bucket prefix to be included [leave empty to include all]: ")
exclude_bucket_prefix = input("input bucket prefix to be excluded [leave empty to include all]: ")
process(bucket_prefix, exclude_bucket_prefix)

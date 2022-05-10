import boto3
import threading

AWS_ACCESS_KEY = ""
AWS_SECRET_KEY = ""

DEFAULT_REGION = 'ap-northeast-1'


def get_repo_list(ecr_client, next_token):
    if next_token:
        result = ecr_client.describe_repositories(nextToken=next_token)
    else:
        result = ecr_client.describe_repositories()
    if result:
        repositories = result.get("repositories")
        return {
            "repositories": repositories,
            "next_token": result.get("next_token")
        }
    else:
        return None


def get_filtered_repo_list(ecr_client, repo_keyword: str, exclude_repo_keyword: str):
    next_token = None
    filtered_repo_names = []
    while True:
        result = get_repo_list(ecr_client, next_token)
        next_token = result.get("next_token")
        all_repos = result.get("repositories")
        for repo in all_repos:
            repo_name = repo.get("repositoryName")
            if (repo_keyword == '' or repo_keyword in repo_name) and (
                    exclude_repo_keyword == '' or exclude_repo_keyword not in repo_name):
                filtered_repo_names.append(repo_name)
        if next_token is None:
            break
    return filtered_repo_names



def delete_images_in_repo(ecr_client, repo_name: str, summary_result, dryrun: bool):
    next_token = None
    total_image_count = 0
    deleted_image_count = 0
    failure_image_count = 0
    while True:
        if next_token:
            image_result = ecr_client.list_images(repositoryName=repo_name, nextToken=next_token)
        else:
            image_result = ecr_client.list_images(repositoryName=repo_name)
        images = image_result.get("imageIds")
        next_token = image_result.get("nextToken")
        total_image_count += len(images)
        if dryrun:
            print(f"dryrun mode activated, no images will be deleted")
        else:
            delete_result = ecr_client.batch_delete_image(repositoryName=repo_name, imageIds=images)
            deleted_image_count += len(delete_result.get("imageIds"))
            failure_image_count += len(delete_result.get("failures"))
        if not next_token:
            break

    summary_result[repo_name] = {
        "total_image_count": total_image_count,
        "deleted_image_count": deleted_image_count,
        "failure_image_count": failure_image_count
    }


def process(ecr_client, repo_prefix: str, exclude_repo_prefix: str, dryrun: bool):
    print(f"getting repo list")
    summary_result = {}
    repo_names = get_filtered_repo_list(ecr_client, repo_prefix, exclude_repo_prefix)
    process_list = []

    for repo_name in repo_names:
        process_list.append(threading.Thread(target=delete_images_in_repo, args=(ecr_client, repo_name, summary_result, dryrun)))

    for p in process_list:
        p.start()

    for p in process_list:
        p.join()

    print(f"=====================================================")
    for repo_name in summary_result:
        repo_result = summary_result.get(repo_name)
        print(f"{repo_name}: deleted {repo_result.get('deleted_image_count')}/{repo_result.get('total_image_count')}, failed {repo_result.get('failure_image_count')}")
        print(f"------------------")


region = input("input region [default ap-northeast-1]: ")
repo_prefix = input("input ecr repo to be included [leave empty to include all]: ")
exclude_repo_prefix = input("input ecr repo to be excluded [leave empty to include all]: ")
dryrun_mode = input("is it dry run mode? [y/n]: ")
dryrun = True
if region == '' or region is None:
    region = DEFAULT_REGION
if dryrun_mode == 'n':
    dryrun = False
print(
    f"running for [bucket prefix={repo_prefix}], [bucket exclude_bucket_prefix={exclude_repo_prefix}], [dry run={dryrun}]")
confirm = input("pls enter [yes] to confirm: ")
if confirm != 'yes':
    print("exiting script")
    exit(0)

session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY,
                        region_name=region)
client = session.client("ecr")

process(client, repo_prefix, exclude_repo_prefix, dryrun)

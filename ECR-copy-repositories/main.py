import base64
import logging
import boto3
import docker
from natsort import natsorted

AWS_ACCOUNT_ID = ""
AWS_ACCESS_KEY = ""
AWS_SECRET_KEY = ""

FROM_REGION = "ap-east-1"
TO_REGION = "ap-northeast-1"

ECR_URI = "{}.dkr.ecr.{}.amazonaws.com"
IMAGE_TAG_URI = ECR_URI + "/{}:{}"

logging.basicConfig(level=logging.INFO, filename="logfile.log", filemode="a+",
                    format="%(asctime)-15s %(levelname)-8s %(message)s")

docker = docker.from_env()
from_session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY,
                             region_name=FROM_REGION)
from_session_ecr = from_session.client("ecr")
to_session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY,
                           region_name=TO_REGION)
to_session_ecr = to_session.client("ecr")

from_session_ecr_token = from_session_ecr.get_authorization_token()
from_session_ecr_token_parts = base64.b64decode(
    from_session_ecr_token["authorizationData"][0]["authorizationToken"]).decode().split(":")

to_session_ecr_token = to_session_ecr.get_authorization_token()
to_session_ecr_token_parts = base64.b64decode(
    to_session_ecr_token["authorizationData"][0]["authorizationToken"]).decode().split(":")

success_count = 0
failure_count = 0
skipped_count = 0


def log(level: int, message: str):
    logging.log(level, message)
    print(message)


def get_repo_list(ecr: any):
    result = ecr.describe_repositories(maxResults=1000)
    repos = result.get("repositories")
    if repos:
        return list(map(lambda r: r.get("repositoryName"), result.get("repositories")))
    return []


def get_common_repo(list1, list2):
    return list(set(list1) & set(list2))


def get_target_repo():
    from_repos = get_repo_list(from_session_ecr)
    to_repos = get_repo_list(to_session_ecr)
    common_repos = get_common_repo(from_repos, to_repos)
    return common_repos


def get_last_tagged_images(repo_name: str, get_count: int):
    list_image_response = from_session_ecr.describe_images(
        repositoryName=repo_name, filter={"tagStatus": "TAGGED"}, maxResults=1000
    )

    images = list_image_response.get("imageDetails", [])
    get_count = min(get_count, len(images))

    return list(map(lambda image: image.get("imageTags"), natsorted(
        images,
        key=lambda image: image.get("imagePushedAt"),
        reverse=True,
    )[:get_count]))


def pull_and_push_image(repo_name: str, tag: str):
    try:
        from_session_ecr_auth_creds = {'username': from_session_ecr_token_parts[0],
                                       'password': from_session_ecr_token_parts[1]}
        image_from = docker.images.pull(IMAGE_TAG_URI.format(AWS_ACCOUNT_ID, FROM_REGION, repo_name, tag),
                                        auth_config=from_session_ecr_auth_creds)
        log(logging.INFO, "Pulled image: {}".format(image_from))
        image_from.tag(IMAGE_TAG_URI.format(AWS_ACCOUNT_ID, TO_REGION, repo_name, tag))

        to_session_ecr_auth_creds = {'username': to_session_ecr_token_parts[0],
                                     'password': to_session_ecr_token_parts[1]}
        target_image_uri = IMAGE_TAG_URI.format(AWS_ACCOUNT_ID, TO_REGION, repo_name, tag)
        push_details = docker.images.push(target_image_uri, auth_config=to_session_ecr_auth_creds)
        log(logging.INFO, "Pushed image: {}".format(push_details))

        docker.images.remove(image_from.id, force=True)
    except docker.errors.APIError as e:
        log(logging.ERROR, "Error occurred when pull_and_push_image: {}".format(e))
        raise


def process(repo_name: str):
    global success_count, skipped_count
    log(logging.INFO, "===========================")
    log(logging.INFO, "Processing for repo: {}".format(repo_name))
    target_repo_images = to_session_ecr.list_images(repositoryName=repo_name, maxResults=5,
                                                    filter={'tagStatus': 'TAGGED'})
    if target_repo_images.get("imageIds"):
        log(logging.INFO, "Skipping... since target repo already has TAGGED version")
        skipped_count += 1
    else:
        source_repo_images = get_last_tagged_images(repo_name, 5)
        print("Examining for {}".format(source_repo_images))
        flattened_source_repo_images = [item for sublist in source_repo_images for item in sublist]
        pushed_tag_count = 0
        unpushed_tag_count = 0
        for tag in flattened_source_repo_images:
            try:
                pull_and_push_image(repo_name, tag)
                pushed_tag_count += 1
            except Exception:
                unpushed_tag_count += 1

        success_count += 1
        print("Finished (unpushed: {} / pushed: {}) tag for image: {}", unpushed_tag_count, pushed_tag_count, repo_name)


if not docker:
    log(logging.INFO, "Error: no docker found")
target_repos = get_target_repo()
log(logging.INFO, "Target Repository List: {}".format(target_repos))
for repo in target_repos:
    process(repo)

log(logging.INFO, "===========================")
log(logging.INFO, "Summary:")
log(logging.INFO, "Success: {}".format(success_count))
log(logging.INFO, "Failed: {}".format(failure_count))
log(logging.INFO, "Skipped: {}".format(skipped_count))

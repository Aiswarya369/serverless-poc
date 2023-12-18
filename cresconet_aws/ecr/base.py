"""
Utility to build Docker image and push image to ECR required for AWS Batch.
"""
import argparse
import base64
import logging
import os
import sys
from os.path import expanduser
from typing import Tuple

import docker
from boto3 import Session
from docker.errors import BuildError
from docker.models.images import Image

AWS_REGION = os.environ.get("AWS_REGION", "ap-southeast-2")
NEXUS_HOST = os.environ.get("NEXUS_HOST", "ci.dev.rocno.com.au")

# Log Configuration
FORMAT = "%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s"
logging.basicConfig(format=FORMAT, handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("push-to-ecr")
logger.setLevel(logging.INFO)


class EcrImageManager:
    """
    Manages building the Docker image, authenticating the Docker client against AWS ECR, and publishing the Docker image to AWS ECR.
    """

    def __init__(self,
                 operational_environment: str, image_tag: str, module_dir: str,
                 ecr_repo_name: str, aws_profile: str,
                 debug: bool = False):
        self.operational_environment = operational_environment
        self.image_tag = image_tag
        self.module_dir = module_dir
        self.ecr_repo_name = ecr_repo_name
        self.aws_profile = aws_profile
        self.debug = debug

        self.nexus_host = NEXUS_HOST
        self.docker_client = docker.from_env()

        if self.debug:
            logger.setLevel(logging.DEBUG)

    @staticmethod
    def _get_abs_path(relative_path: str) -> str:
        """
        Converts the given 'relative_path' to an absolute path.
        Note: this method is intended to be a 'private' method.

        :param relative_path: the current 'relative' path to be converted to an absolute path.
        :return: the converted absolute path.
        """
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), relative_path)

    def connect_to_ecr(self):
        """
        Uses Boto3 to connect to the ECR client and obtain the URL used to push the Docker image too.
        Authenticates the Docker client, so it can push to ECR.

        :return: ECR registry url, example "https://146276074688.dkr.ecr.ap-southeast-2.amazonaws.com"
        """
        logger.info("Connecting to ECR repo for environment: %s using AWS profile: %s...", self.operational_environment, self.aws_profile)

        boto_session = Session(profile_name=self.aws_profile, region_name=AWS_REGION)
        client = boto_session.client("ecr")

        token = client.get_authorization_token()
        logging.info("Connected to ECR")
        b64token = token["authorizationData"][0]["authorizationToken"].encode("utf-8")
        username, password = base64.b64decode(b64token).decode("utf-8").split(":")
        registry: str = token["authorizationData"][0]["proxyEndpoint"]
        self.docker_client.login(username=username, password=password, registry=registry, reauth=True)

        return registry

    def get_nexus_credentials(self) -> Tuple[str, str]:
        """
        Retrieves the Nexus credentials stored in the '~/.netrc' file.
        These are required to obtain additional packages to be wrapped into the image.

        :return: a string tuple containing the Nexus username and password in the '~/.netrc' file.
        """
        username, password = None, None
        home = expanduser("~")
        with open(os.path.join(home, ".netrc"), encoding="utf-8") as netrc_file:
            # File looks like: `machine ci.dev.rocno.com.au login <username> password <password>`. But could have more than one line.
            for line in netrc_file.readlines():
                line = str(line).strip()
                if self.nexus_host in str(line):
                    content = line.split(" ")
                    username = content[content.index("login") + 1].strip()
                    password = content[content.index("password") + 1].strip()
                    break
            netrc_file.close()

        if None in (username, password):
            raise ValueError(f"Invalid or no username/password found in '~/.netrc' file: {username}/{password}.")

        return username, password

    def build_image(self) -> Image:
        """
        Builds the required image using the 'Dockerfile'.

        :return: the built Docker image.
        """
        logging.info("Building image '%s:%s'...", self.ecr_repo_name, self.image_tag)

        # Get to root director of project.
        path = os.getcwd()
        docker_file = os.path.join(self._get_abs_path("."), "Dockerfile")

        username, password = self.get_nexus_credentials()

        logger.info("Building docker in directory %s...", os.getcwd())
        try:
            image, build_logs = self.docker_client.images.build(
                path=path,
                dockerfile=docker_file,
                rm=True,
                tag=f"{self.ecr_repo_name}:{self.image_tag}",
                buildargs={
                    "ARTIFACTORY_USERNAME": username,
                    "ARTIFACTORY_PASSWORD": password,
                    "MODULE_DIRECTORY": self.module_dir
                })
            for chunk in build_logs:
                if 'stream' in chunk:
                    for line in chunk["stream"].splitlines():
                        logger.debug(line)
        except BuildError as exception:
            for line in exception.build_log:
                if "stream" in line:
                    logger.error(line["stream"].strip())
            raise

        logger.info("Built image successfully.")

        return image

    def tag_and_push_to_ecr(self, image: Image):
        """
        Tags and pushes the given 'image' to the ECR repository.

        :param image: the built Docker image to be pushed to ECR.
        """
        registry = self.connect_to_ecr()
        logging.info("Pushing image to ECR: %s:%s...", self.ecr_repo_name, self.image_tag)
        ecr_repo_name = f"{registry.replace('https://', '')}/{self.ecr_repo_name}"
        image.tag(ecr_repo_name, self.image_tag)
        push_log = self.docker_client.images.push(ecr_repo_name, tag=self.image_tag)
        logging.info(push_log)
        logger.info("Pushed image successfully to ECR.")


def main():
    """
    The main 'controller' function of this module. Used as an entry point in the CLI command.
    Requires five arguments when called via the console:
        - '--stage'
        - '--tag'
        - '--module-dir'.
        - '--ecr-repo-name'
        - '--aws-profile'.
    An additional optional '--debug' argument is also available.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", help="stage help")
    parser.add_argument("--tag", help="tag help")
    parser.add_argument("--module-dir", help="module-dir help")
    parser.add_argument("--ecr-repo-name", help="ecr-repo-name help")
    parser.add_argument("--aws-profile", help="aws-profile help")
    parser.add_argument("--debug", help="debug help", action='store_true')
    args = parser.parse_args()
    logger.info(args)

    ecr_image_manager = EcrImageManager(
        operational_environment=args.stage,
        image_tag=args.tag,
        module_dir=args.module_dir,
        ecr_repo_name=args.ecr_repo_name,
        aws_profile=args.aws_profile,
        debug=args.debug
    )

    built_image = ecr_image_manager.build_image()
    ecr_image_manager.tag_and_push_to_ecr(image=built_image)

from abc import ABC, abstractmethod
from typing import List
import boto3
from botocore import UNSIGNED
from botocore.config import Config
import os
class Downloader(ABC):
    """
    Abstract class for downloading datasets
    """

    def __init__(self, destination: str, download_url: str):
        """
        Constructor for Downloader class
        :param destination: Where files should be downloaded to
        :param download_url: url from which to download the dataset
        """
        self.destination = destination
        self.download_url = download_url


    @abstractmethod
    def download(self, *args, **kwargs):
        """
        Abstract method each inheriting downloader should implement
        :return:
        """
        pass


class S3Downloader(Downloader):
    def __init__(self, destination: str, download_url: str):
        super().__init__(destination, download_url)

    def download(self, prefixes: List[str] = None, *args, **kwargs) -> None:
        """
        Download dataset from S3 bucket
        :param prefixes: list of prefixes to select which subdirectories to download, if None, downloads entire bucket
        :return:
        """
        if prefixes is None:
            self.__download_bucket(bucket_name=self.download_url, prefix="", local_dir=self.destination)

        else:
            for prefix in prefixes:
                self.__download_bucket(bucket_name=self.download_url, prefix=prefix, local_dir=self.destination)

    def __download_file_from_s3(self, bucket_name: str, object_key: str, download_path: str) -> None:
        """
        Downloads a file from AWS S3.

        Parameters:
        bucket_name (str): The name of the S3 bucket.
        object_key (str): The key of the file in the S3 bucket.
        download_path (str): The local path to save the downloaded file.
        """
        s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
        s3.download_file(bucket_name, object_key, download_path)
        print(f"Downloaded {object_key} to {download_path}")

    def __download_bucket(self, bucket_name: str, prefix: str, local_dir: str) -> None:
        """
        Downloads the contents of an S3 bucket.

        Parameters:
        bucket_name (str): The name of the S3 bucket.
        prefix (str): The prefix to filter the objects in the S3 bucket.
        local_dir (str): The local directory to save the downloaded files.
        """
        s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    object_key = obj['Key']
                    if not object_key.endswith("/"):
                        # Determine the local path to save the file
                        local_path = os.path.join(local_dir, object_key)
                        local_dirname = os.path.dirname(local_path)
                        print(f"Downloading {object_key} to {local_dirname}")
                        # Create local directory if it does not exist
                        if not os.path.exists(local_dirname):
                            os.makedirs(local_dirname)
                        # Download the file
                        self.__download_file_from_s3(bucket_name, object_key, local_path)


def get_downloader(downloader_type: str, destination: str, download_url: str) -> Downloader:
    """
    Factory method to get a correct downloader
    :param downloader_type: description of the downloader
    :param destination: where to save the dataset
    :param download_url: from where to download the dataset
    :return:
    """

    legal_downloaders = [
        "s3"
    ]

    if downloader_type not in legal_downloaders:
        raise ValueError(f"{downloader_type} is not a valid downloader")

    if downloader_type == "s3":
        return S3Downloader(destination, download_url)



if __name__ == '__main__':
    downloader = get_downloader("s3", destination=r"C:\Users\edanp\Studies\JobInterviews\BlueWhite\ROSAnnotator\dataset",
                                download_url='racecar-dataset')
    downloader.download()
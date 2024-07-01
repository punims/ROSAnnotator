# Script for downloading all db3 files containing images from the racecar aws dataset
import tempfile
import boto3
import os
import yaml
from botocore import UNSIGNED
from botocore.config import Config
from downloader import get_downloader

def download_yaml_files(bucket_name, prefix, destination) -> list[str]:
    """
    Helper function to download only the yaml files from the dataset
    :param bucket_name: path of the aws bucket
    :param prefix: a prefix of which bucket to download
    :param destination: where to save the files
    :return: list of the paths to the yaml files.
    """

    yaml_paths = []
    # Initialize the S3 client
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

    # List objects in the specified bucket with the given prefix
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    if 'Contents' not in response:
        print(f"No objects found in bucket {bucket_name} with prefix {prefix}")
        return

    for obj in response['Contents']:
        key = obj['Key']
        if key.endswith('.yaml'):
            # Define the destination file path
            final_dest = os.path.join(destination, os.path.dirname(key))
            os.makedirs(final_dest)
            dest_path = os.path.join(final_dest, os.path.basename(key))
            yaml_paths.append(dest_path)
            print(f"Downloading {key} to {dest_path}")

            # Download the file
            s3.download_file(bucket_name, key, dest_path)

    print("Download completed.")
    return yaml_paths

def check_yaml_for_topic(file_path, topic_name) -> bool:
    """
    Check if a racecar YAML file contains a topic with the word topic_name.

    Parameters:
    file_path (str): The path to the YAML file.

    Returns:
    bool: True if a topic with the word 'camera' is found, False otherwise.
    """
    with open(file_path, 'r') as file:
        try:
            data = yaml.safe_load(file)
            for topic in data['rosbag2_bagfile_information']['topics_with_message_count']:
                if topic_name in topic['topic_metadata']['name']:
                    return True
        except yaml.YAMLError as exc:
            print(f"Error reading YAML file {file_path}: {exc}")
            return False
def db_contains_images(yaml_path) -> str:
    """
    Given a path to a yaml file from the race car database, returns prefix if it contains images
    :param yaml_path: path to yaml file
    :return: prefix if it contains images, otherwise None
    """
    if check_yaml_for_topic(yaml_path, topic_name="camera"):
        return os.path.dirname(yaml_path)


def find_files_with_images(bucket_path: str) -> list[str]:
    """
    Downloads meta data files from the racecar bucket, returns list of topics to download
    :param bucket_path: path to aws bucket
    :return: list of topics to download
    """

    PREFIX = 'RACECAR-ROS2/'
    # create tempfolder
    with tempfile.TemporaryDirectory() as tempdir:
    
        # download metadata yaml files
        yaml_paths = download_yaml_files(bucket_path, PREFIX, tempdir)
        prefixes = []
        for yaml_path in yaml_paths:
            prefix = db_contains_images(yaml_path)

            # process prefix into the correct bucket name.
            if prefix:
                index = prefix.find(PREFIX)
                if index != -1:
                    prefix = prefix[index:]
                prefixes.append(prefix)

    print("buckets with images in racecar-dataset are: ", prefixes)
    return prefixes

if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=str, default="racecar-dataset", help="Source for db3 file")
    parser.add_argument("--destination", type=str, help="Destination for extracted images and annotations")
    args = parser.parse_args()

    prefixes = find_files_with_images('racecar-dataset')
    destination = args.destination
    downloader = get_downloader('s3', destination=destination,
                                download_url=args.bucket)
    prefixes = [prefix for prefix in prefixes if "MULTI" in prefix]
    print("Remaining prefixes are", prefixes)
    downloader.download(prefixes=prefixes)

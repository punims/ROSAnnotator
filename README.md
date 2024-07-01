# Annotator 

This package allows downloading, extracting and annotating
different types of datasets. Currently the only dataset fully 
supported is the Race Car dataset that can be found here
https://registry.opendata.aws/racecar-dataset/

## Requirements
Python version used - 3.12.4

Use `pip3 install -r requirements.txt` to install required libraries on your virtualenv

## Downloading Race Car Dataset
Install aws cli tool using the command `pip3 install awscli --upgrade --user`
with your virtual environment activated.

configure your account using `aws configure`

After installing the requirements call the script /downloader/racecar_downloader 
with the following command

`python3 downloader/racecar_downloader --destination <path to save dataset>`

This script will download all buckets with images in the ROS2 format and only those that contain races with
multiple vehicles (buckets with the name MULTI)

This is done by first downloading all yaml files from each sub-bucket, and checking which ones contain topics
related to cameras.

In case you'd like to download the entire bucket, even containing db3 files without images you 
can use the command 

`aws s3 cp --no-sign-request s3://racecar-dataset/ <destination path>  --recursive`

## Extraction and Annotation
Extraction and annotation can be done by running the script

`python3 main.py --source <path to source db3 file> --dest <path to save images and annotations>`


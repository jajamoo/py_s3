import json
from os import environ

import boto3
from botocore.exceptions import ClientError

import logging
from sftp import Sftp

session = boto3.session.Session()
client = session.client(service_name="secretsmanager", region_name=session.region_name)
try:
    secret_values_response = client.get_secret_value(SecretId="dnb-sftp-secrets")
except ClientError as e:
    logging.exception(
        f"Error getting client credentials from Secrets Manager:{e.response}"
    )
    raise
else:
    secret_string = secret_values_response["SecretString"]
    sftp_secrets = json.loads(secret_string)


def lambda_handler(event, context):
    if "Records" in event:
        logging.debug(
            "Handling " + str(len(event["Records"])) + " s3 financial bucket records(s)"
        )
        for record in event["Records"]:
            object_key = record["s3"]["object"]["key"]
            name = record["s3"]["bucket"]["name"]
            process_event(object_key, name)
        return True


def process_event(object_key, bucket_name):
    key = object_key["Records"][0]["s3"]["object"]["key"]
    name = object_key["Records"][0]["s3"]["bucket"]["name"]
    logging.debug(f"Grabbing file (key) {key}... ")
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(environ.get("FINANCE_OUTPUT_BUCKET"))
    environment = environ.get("ENVIRONMENT")
    env_directory = "puts" if environment == "prod" else f"TEST_puts"
    sftp_object = Sftp(
        sftp_secrets["SFTP_SERVER"],
        sftp_secrets["SFTP_USERNAME"],
        sftp_secrets["SFTP_PASSWORD"],
        key,
        name,
        bucket,
        logging,
        env_directory,
    )
    sftp_object.s3_download_process_sftp()

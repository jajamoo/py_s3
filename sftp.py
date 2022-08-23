import io
import os
import zipfile
from os import environ

import boto3
import pysftp

import logging


class Sftp:
    def __init__(
        self,
        sftp_host,
        sftp_username,
        sftp_password,
        object_key,
        bucket_name,
        bucket_object,
        logger,
        env_directory,
    ):
        self.sftp_host = sftp_host
        self.sftp_username = sftp_username
        self.sftp_password = sftp_password
        self.object_key = object_key
        self.bucket_name = bucket_name
        self.bucket_object = bucket_object
        self.file_name = self.object_key.split("/")[-1]
        self.logger = logger
        self.env_directory = env_directory

    def mkdir_p(self, sftp, remote_directory):
        """Change to this directory, recursively making new folders if needed.
        Returns True if any folders were created."""
        if remote_directory == "/":
            # absolute path so change directory to root
            sftp.chdir("/")
            return
        if remote_directory == "":
            # top-level relative directory must exist
            return
        try:
            sftp.chdir(remote_directory)  # sub-directory exists
        except IOError:
            dirname, basename = os.path.split(remote_directory.rstrip("/"))
            self.mkdir_p(sftp, dirname)  # make parent directories
            sftp.mkdir(basename, 777)  # sub-directory missing, so created it
            sftp.chdir(basename)
            return True

    def s3_download_process_sftp(self):
        if ".zip" in self.object_key:
            self.logger.info(
                "Found a zip file in the S3 bucket, extracting files and processing..."
            )
            self.zip_check_and_process()
        else:
            s3 = boto3.resource("s3")
            s3.Bucket(self.bucket_name).download_file(
                f"{self.object_key}", f"/tmp/{self.file_name}"
            )
            self.upload_to_sftp(self.file_name)

    def upload_to_sftp(self, obj_key_or_filename):
        self.logger.info("Uploading file to SFTP server...")
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        try:
            with pysftp.Connection(
                self.sftp_host,
                username=self.sftp_username,
                password=self.sftp_password,
                cnopts=cnopts,
            ) as sftp:
                if ".zip" in self.object_key:
                    self.mkdir_p(
                        sftp,
                        f'{self.env_directory}/{self.file_name.replace(".zip", "")}',
                    )
                else:
                    self.mkdir_p(sftp, f"{self.env_directory}/{obj_key_or_filename}")
                sftp.put(
                    f"/tmp/{obj_key_or_filename}",
                    f"{obj_key_or_filename}",
                    confirm=False,
                )
        except (IOError, OSError) as ex:
            if environ.get("ENVIRONMENT") == "prod":
                raise ex
            else:
                logging.exception('Prod env', ex)

    def zip_check_and_process(self):
        s3 = boto3.resource("s3")
        s3_object = s3.Object(self.bucket_name, self.object_key)
        with io.BytesIO(s3_object.get()["Body"].read()) as tf:
            tf.seek(0)
            with zipfile.ZipFile(tf, mode="r") as zipf:
                zipf.extractall(f"/tmp")
                for subfile in zipf.namelist():
                    self.upload_to_sftp(subfile)

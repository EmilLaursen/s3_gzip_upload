#!/usr/bin/env python3
import shutil
from pathlib import Path

import jmespath
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from boto3.exceptions import S3UploadFailedError

import click

from datetime import datetime
import time

import gzip
import logging

log_folder = Path.home() / '.logs'

log_folder.mkdir(parents=True, exist_ok=True)
log_file = log_folder / 'aws_upload.log'

logging.basicConfig(
    filename=str(log_file),
    filemode='a',
    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
    datefmt='%H:%M:%S',
    level=logging.INFO)


def upload_file(file_path, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_path: File Path object to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_path.name is used
    :return: True if file was uploaded, else False
    """
    # If S3 object_name was not specified, use file_path
    if object_name is None:
        object_name = file_path.name
    # Upload the file
    s3_client = boto3.client('s3')
    try:
        _ = s3_client.upload_file(str(file_path), bucket, object_name)
    except (ClientError, s3_client.exceptions.NoSuchBucket, S3UploadFailedError) as e:
        logging.error(e)
        return False
    return True


def datetime_now_hr_min(): return datetime.now().strftime("%Y-%m-%d_%H:%M")


def get_files_w_ext(ext, files_location):
    files = []
    for file in Path(files_location).iterdir():
        if file.suffix == ext and file.is_file():
            files.append(file)
            logging.info(f'Found file: {file.name}')
    return files


def total_size_mb(files, unit=1024**2): return sum(file.stat().st_size / unit for file in files)


def gzip_files(files, destination):

    dst = Path(destination)

    dst.mkdir(parents=True, exist_ok=True)

    date = str(datetime_now_hr_min())

    zipped_files = []

    for file in files:
        size = file.stat().st_size

        file_dst = dst / f'{file.name}_{date}_.gz'

        start = time.time()

        with file.open(mode='rb') as f_in:
            with gzip.open(file_dst, mode='wb') as f_out:
                # Shutil will chunk the file, and not load everything into memory!
                shutil.copyfileobj(f_in, f_out)

        zipped_files.append(file_dst)

        end = time.time()

        zip_size = file_dst.stat().st_size

        logging.info(f'Zipped file: {file_dst} Elasped time: {end - start:.1f} s. Deflated: {1 - zip_size / size:.2f}')

    return zipped_files


def upload_files(files, bucket):
    upload_sucess = []
    for file in files:
        f_abs_path = file.absolute()
        success = upload_file(f_abs_path, bucket)
        upload_sucess.append(success)
        logging.info(f'S3 upload success: {success} Filename: {f_abs_path}')
    return all(upload_sucess)


def double_check_s3_for_success(bucket, files):

    s3_client = boto3.client('s3')

    jmes_query = '[ResponseMetadata.HTTPStatusCode, ContentLength]'

    double_check_success = []
    for file in files:
        try:
            key = file.name

            response = s3_client.head_object(Bucket=bucket, Key=key)

            extracted = jmespath.search(jmes_query, response)

            r_data = {k: v for k, v in zip(['status_code', 'bytes'], extracted)}

            request_succes = 200

            succes = (request_succes, file.stat().st_size)

            double_check_success.append(tuple(r_data.values()) == succes)

            logging.info(f"S3 head_object call Status: {r_data['status_code']} Bucket: {bucket}, Key: {key}, Bytes: {r_data['bytes']}")

        except ClientError as e:
            logging.error(e)
            double_check_success.append(False)

    return all(double_check_success)


def remove_files(files):
    for file in files:
        file.unlink()
        logging.info(f'Deleted file: {file}')


mb = 1024**2


@click.command()
@click.argument('file_loc')
@click.option('-b', '--bucket', default='dk-new-scrape')
@click.option('-t', '--threshold', default=100)
@click.option('-e', '--ext', default='.jl')
def main(file_loc, bucket, threshold, ext):

    jl_files = get_files_w_ext(ext, file_loc)

    ts = total_size_mb(jl_files)
    logging.info(f'Total files size: {ts:.2f} MiB')

    # Only upload batch of size above threshold.
    if ts <= threshold:
        return

    # Zip all jl_files !
    zip_folder = Path(file_loc) / 'zipped_data'

    zip_files = gzip_files(jl_files, zip_folder)

    remove_files(jl_files)

    success = upload_files(zip_files, bucket)

    if success:
        double_check = double_check_s3_for_success(bucket, zip_files)
        if double_check:
            remove_files(zip_files)


if __name__ == '__main__':
    main()

"""The get test data from s3 procedure."""
import logging
import sys
from io import BytesIO
from zipfile import ZipFile

import pandas
from concurrent.futures import ThreadPoolExecutor
from typing import List
import boto3
from botocore.client import BaseClient
from pydeidentify import DeidentifiedText, Deidentifier

UTF8_ENCODING = "UTF8"
NEW_LINE = "\\n"
PARQUET_SUFFIX = ".parquet"
CSV_SUFFIX = ".csv"
XML_SUFFIX = ".xml"
MAX_WORKERS = 10
AWS_S3_BYTE_RANGE = "bytes=0-10240"
AWS_S3_PATH_SEPARATOR = "/"
AWS_S3_RESPONSE_CONTENTS = "Contents"
AWS_S3_OPERATION_NAME = "list_objects_v2"
AWS_S3_KEY = "Key"
AWS_S3_RESPONSE_BODY = "Body"
AWS_S3_SERVICE = "s3"
AWS_S3_DEIDENTIFIED_PREFIX = "deidentified"
DEIDENTIFIER_INCLUDED_ENTITY_TYPES = {
    "PERSON",
    "ORG",
    "NORP",
    "GPE",
    "LANGUAGE",
    "CARDINAL",
}
DEIDENTIFIER_SPACY_MODEL = "en_core_web_trf"


def s3_directory_lister(
    s3_client: BaseClient, bucket: str, prefix: str
) -> List[str]:
    """Paginate through s3 results and yield all keys."""
    for page in s3_client.get_paginator(AWS_S3_OPERATION_NAME).paginate(
        Bucket=bucket, Prefix=prefix
    ):
        for item in page.get(AWS_S3_RESPONSE_CONTENTS):
            yield item.get(AWS_S3_KEY)


def de_identify_row(row: str) -> DeidentifiedText:
    """Return de-identified row."""
    return Deidentifier(
        included_entity_types=DEIDENTIFIER_INCLUDED_ENTITY_TYPES,
        spacy_model=DEIDENTIFIER_SPACY_MODEL,
    ).deidentify(row)


class DestinationKey(str):
    """AWS S3 destination key."""


class SourceKey(str):
    """AWS S3 source key."""


class Bucket(str):
    """AWS S3 Bucket."""


def de_identify_object(
    s3_client: BaseClient, s3_bucket: Bucket, s3_key: SourceKey
) -> DestinationKey:
    """Return de-identified object key."""
    destination_key = DestinationKey(
        AWS_S3_PATH_SEPARATOR.join([AWS_S3_DEIDENTIFIED_PREFIX, s3_key])
    )
    with BytesIO() as zip_buffer, BytesIO() as out_buffer:
        zip_buffer.write(
            s3_client.get_object(
                Bucket=s3_bucket,
                Key=s3_key,
            ).get(AWS_S3_RESPONSE_BODY).read()
        )
        with ZipFile(zip_buffer, 'r') as zip_object:
            for _name in zip_object.namelist():
                in_buffer = BytesIO()
                with zip_object.open(_name) as myfile:
                    in_buffer.write(myfile.read())

                if _name.endswith(PARQUET_SUFFIX):
                    rows = pandas.read_parquet(in_buffer)

                elif _name.endswith(CSV_SUFFIX):
                    in_buffer.seek(0)
                    rows = pandas.read_csv(in_buffer)

                elif _name.endswith(XML_SUFFIX):
                    in_buffer.seek(0)
                    rows = pandas.read_xml(in_buffer)

        for row in rows.values:
            if row[0]:
                out_buffer.write(
                    de_identify_row(row[0]).text.encode(UTF8_ENCODING)
                )
        out_buffer.seek(0)
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=destination_key,
            Body=out_buffer.getvalue()
        )

    return destination_key


def get_test_data(
    s3_client: BaseClient,
    s3_bucket: str,
    s3_source_prefix: str,
):
    """Use threads to get data and write to s3."""
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for key in s3_directory_lister(s3_client, s3_bucket, s3_source_prefix):
            futures.append(
                executor.submit(
                    de_identify_object,
                    s3_client,
                    s3_bucket,
                    key,
                )
            )
        for future in futures:
            logging.info(future.result())


if __name__ == "__main__":
    get_test_data(
        boto3.client(AWS_S3_SERVICE), sys.argv[1], sys.argv[2]
    )

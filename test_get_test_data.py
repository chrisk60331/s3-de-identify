"""Tests for the get test data from s3 procedure."""
import os
from io import BytesIO
from unittest.mock import patch

import pandas
import boto3
import pytest
from moto import mock_s3
from faker import Faker
from get_test_data import (
    AWS_S3_PATH_SEPARATOR,
    de_identify_row,
    de_identify_object,
    get_test_data,
    s3_directory_lister,
    AWS_S3_RESPONSE_BODY, PARQUET_SUFFIX, CSV_SUFFIX, AWS_S3_DEIDENTIFIED_PREFIX,
)


NEWLINE = "\n"

ENCODING_UTF8 = "UTF8"
TEST = "test"
S3_BUCKET = "foo"
S3_PREFIX = "bar"
DATA_FILE = "data.txt"
S3_KEY = AWS_S3_PATH_SEPARATOR.join([S3_PREFIX, DATA_FILE])
S3_DESTINATION = AWS_S3_PATH_SEPARATOR.join([TEST, S3_KEY])
PII_TEXT = [
    (
        """Tom Smith lives in Tacoma """
        """Washington and is American """
        """speaks English and his """
        """phone number is 206-699-9999."""
    ),
]
DE_IDENTIFIED_TEXT = (
    """PERSON0 lives in GPE0 GPE1 """
    """and is NORP0 speaks LANGUAGE0 """
    """and his phone number is CARDINAL0."""
)
os.environ["MOTO_S3_CUSTOM_ENDPOINTS"] = "http://custom.internal.endpoint"


@pytest.fixture
@mock_s3
def mock_s3_client():
    """Mock an S3 endpoint, add an object, and yield the client."""
    try:
        mock_s3_client = boto3.client(
            "s3", endpoint_url=os.environ["MOTO_S3_CUSTOM_ENDPOINTS"]
        )
        mock_s3_client.create_bucket(Bucket=S3_BUCKET)

        yield mock_s3_client
    finally:
        pass


@mock_s3
def test_de_identified_rows():
    """Test that we can return de-identified data."""
    expected = (
        "PERSON0 PERSON1 PERSON2 PERSON3 PERSON4 PERSON5 PERSON6 PERSON7 "
        "PERSON8 PERSON9"
    )

    fake = Faker("en_US")
    actual = de_identify_row(" ".join([fake.name() for _ in range(10)]))

    assert actual.text == expected


@mock_s3
def test_de_identified_object(mock_s3_client):
    """Test that we can return de-identified data."""
    s3_key = "".join([S3_KEY, CSV_SUFFIX])
    mock_s3_client = list(mock_s3_client)[0]
    mock_s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=NEWLINE.join(["text", *PII_TEXT]).encode(ENCODING_UTF8),
    )

    actual_key = de_identify_object(
        mock_s3_client,
        S3_BUCKET,
        s3_key,
    )

    actual = (
        mock_s3_client.get_object(
            Bucket=S3_BUCKET,
            Key=actual_key,
        )
        .get(AWS_S3_RESPONSE_BODY)
        .read()
        .decode()
    )

    assert actual == DE_IDENTIFIED_TEXT


@mock_s3
def test_de_identified_parquet_object(mock_s3_client):
    """Test that we can return de-identified data."""
    mock_s3_client = list(mock_s3_client)[0]
    input_dataframe = pandas.DataFrame(PII_TEXT, columns=["text"])
    s3_key = "".join([S3_KEY, PARQUET_SUFFIX])

    with BytesIO() as out_buffer:
        input_dataframe.to_parquet(out_buffer, index=False)
        out_buffer.seek(0)
        mock_s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=out_buffer.getvalue()
        )

    actual_key = de_identify_object(mock_s3_client, S3_BUCKET, s3_key)
    actual_text = mock_s3_client.get_object(
        Bucket=S3_BUCKET,
        Key=actual_key,
    ).get(AWS_S3_RESPONSE_BODY).read().decode()

    assert actual_text == DE_IDENTIFIED_TEXT


@mock_s3
def test_s3_directory_lister(mock_s3_client):
    """Test that we can list s3 keys."""
    actual = list(
        s3_directory_lister(list(mock_s3_client)[0], S3_BUCKET, S3_PREFIX)
    )

    assert actual == [S3_KEY]


@mock_s3
def test_get_test_data(mock_s3_client):
    """Test that we can write de-identified data to s3."""
    mock_client = list(mock_s3_client)[0]
    mock_client.put_object(
        Bucket=S3_BUCKET,
        Key=S3_KEY,
        Body="\n".join(["text", *PII_TEXT]).encode(ENCODING_UTF8),
    )

    with patch("get_test_data.s3_directory_lister", return_value=[S3_KEY]):
        get_test_data(mock_client, S3_BUCKET, S3_PREFIX)

    actual = (
        mock_client.get_object(
            Bucket=S3_BUCKET,
            Key=AWS_S3_PATH_SEPARATOR.join([AWS_S3_DEIDENTIFIED_PREFIX, S3_KEY]),
        )
        .get(AWS_S3_RESPONSE_BODY)
        .read()
        .decode()
    )

    assert actual == DE_IDENTIFIED_TEXT

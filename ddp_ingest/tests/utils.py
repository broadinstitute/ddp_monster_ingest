from unittest.mock import Mock
from google.cloud.storage.client import Client
from dagster_utils.resources.google_storage import google_storage_client
from google.cloud.storage.blob import Blob
from google.cloud.storage.bucket import Bucket


def build_mock_storage_client() -> Client:
    # todo refactor me somewhere sane
    c = Mock(spec=google_storage_client)
    c.bucket = Mock(spec=Bucket)

    fake_blob = Mock(spec=Blob)
    fake_blob.name = "fake_blob"
    fake_blob.size = 120
    c.list_blobs = Mock(return_value=[fake_blob])
    return c

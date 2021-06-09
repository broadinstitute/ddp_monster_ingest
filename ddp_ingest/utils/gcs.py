from dataclasses import dataclass
from google.cloud.storage.client import Client
from google.cloud.storage.blob import Blob
from urllib.parse import urlparse


@dataclass
class GsBucketWithPrefix:
    bucket: str
    prefix: str

    def __post_init__(self) -> None:
        assert not self.prefix.startswith("/"), "Prefix must not begin with '/'"


def parse_gs_path(raw_gs_path: str) -> GsBucketWithPrefix:
    url_result = urlparse(raw_gs_path)
    return GsBucketWithPrefix(url_result.netloc, url_result.path[1:])


def path_has_any_data(bucket: str, prefix: str, gcs: Client) -> bool:
    """Checks the given path for any blobs of non-zero size"""
    blobs = [blob for blob in
             gcs.list_blobs(bucket, prefix=prefix)]
    return any([blob.size > 0 for blob in blobs])


def assert_input_path_sane(input_path: str, client: Client) -> None:
    bucket_with_prefix = parse_gs_path(input_path)
    assert path_has_any_data(
        bucket_with_prefix.bucket,
        bucket_with_prefix.prefix,
        client
    ), f"Invalid input path ({input_path}), no data"


def assert_path_exists(bucketWithPrefix: GsBucketWithPrefix, client: Client) -> None:
    blob = Blob(bucketWithPrefix.prefix, bucket=bucketWithPrefix.bucket)
    assert blob.exists(client), f"Path does not exist {bucketWithPrefix.bucket}/{bucketWithPrefix.prefix}"

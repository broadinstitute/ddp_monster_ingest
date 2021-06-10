from unittest.mock import Mock


def build_mock_storage_client() -> Client:
    # todo refactor me somewhere sane
    c = Mock(spec=google_storage_client)
    c.bucket = Mock(spec=google.cloud.storage.bucket.Bucket)

    fake_blob = Mock(spec=google.cloud.storage.blob.Blob)
    fake_blob.name = "fake_blob"
    fake_blob.size = 120
    c.list_blobs = Mock(return_value=[fake_blob])
    return c

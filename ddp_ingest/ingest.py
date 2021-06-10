import json
import io

import data_repo_client
import google.cloud.storage.iam
from dagster import ModeDefinition, pipeline, solid, ResourceDefinition, InputDefinition, Nothing
from dagster.core.execution.context.compute import AbstractComputeExecutionContext
from dagster_utils.contrib.data_repo.typing import JobId
from dagster_utils.resources.google_storage import google_storage_client
from dagster_utils.resources.data_repo.jade_data_repo import jade_data_repo_client
from data_repo_client import JobModel
from google.cloud.storage.client import Client
from unittest.mock import Mock

from ddp_ingest.config import preconfigure_for_mode
from ddp_ingest.resources import input_path, target_dataset, load_tag
from ddp_ingest.utils.gcs import GsBucketWithPrefix, parse_gs_path, assert_input_path_sane, assert_path_exists
from ddp_ingest.tests.utils import build_mock_storage_client


test_mode = ModeDefinition(
    name="test",
    resource_defs={
        "target_dataset": target_dataset,
        "data_repo_client": ResourceDefinition.hardcoded_resource(Mock(spec=data_repo_client.RepositoryApi)),
        "gcs": ResourceDefinition.hardcoded_resource(build_mock_storage_client()),
        "input_path": input_path,
        "load_tag": load_tag
    }
)

dev_mode = ModeDefinition(
    name="dev",
    resource_defs={
        "target_dataset": target_dataset,
        "data_repo_client": preconfigure_for_mode(jade_data_repo_client, "dev"),
        "gcs": google_storage_client,
        "input_path": input_path,
        "load_tag": load_tag
    }
)


@solid(
    required_resource_keys={"gcs"},
    config_schema={
        "scratch_path": str,
    }
)
def clear_scratch_area(context: AbstractComputeExecutionContext) -> int:
    scratch_bucket_with_prefix: GsBucketWithPrefix = parse_gs_path(context.solid_config["scratch_path"])

    blobs = context.resources.gcs.list_blobs(scratch_bucket_with_prefix.bucket,
                                             prefix=f"{scratch_bucket_with_prefix.prefix}/")
    deletions_count = 0
    for blob in blobs:
        blob.delete()
        deletions_count += 1
    context.log.debug(f"clear_scratch_area deleted {deletions_count} blobs under {scratch_bucket_with_prefix.prefix}")
    return deletions_count


@solid(
    required_resource_keys={"gcs", "input_path"},
    config_schema={
        "scratch_path": str,
    },
    input_defs=[InputDefinition("start", Nothing)],
)
def create_scratch_area(context: AbstractComputeExecutionContext) -> GsBucketWithPrefix:
    storage_client: Client = context.resources.gcs
    input_bucket_with_prefix: GsBucketWithPrefix = parse_gs_path(context.resources.input_path)
    scratch_bucket_with_prefix: GsBucketWithPrefix = parse_gs_path(context.solid_config["scratch_path"])

    xfer_requests = []

    for blob in storage_client.list_blobs(
            input_bucket_with_prefix.bucket,
            prefix=f"{input_bucket_with_prefix.prefix}/",
            delimiter="/"
    ):
        # for some reason, we are getting the "root" of the prefix in the included blobs,
        # which is not actually a file that can be uploaded
        # filter it out here until we can figure out what's going on
        if blob.name == f"{input_bucket_with_prefix.prefix}/":
            continue

        payload = {
            "source_path": f"gs://{blob.bucket.name}/{blob.name}",
            "target_path": f"/{blob.name}"
        }
        xfer_requests.append(json.dumps(payload))

    # output in newline separated JSON, aka JSONL
    out = io.StringIO()
    out.write("\n".join(xfer_requests))

    bucket = storage_client.bucket(scratch_bucket_with_prefix.bucket)
    destination_blob_name = f"{scratch_bucket_with_prefix.prefix}/data_transfer_requests.json"
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(out.getvalue())
    return GsBucketWithPrefix(scratch_bucket_with_prefix.bucket, blob.name)


@solid(
    required_resource_keys={"input_path", "data_repo_client", "target_dataset", "load_tag", "gcs"},
)
def load_data_files(context: AbstractComputeExecutionContext, data_transfer_control_file: GsBucketWithPrefix) -> JobId:
    input_path = context.resources.input_path
    storage_client = context.resources.gcs

    assert_input_path_sane(input_path, storage_client)
    assert_path_exists(data_transfer_control_file, storage_client)

    billing_profile_id = context.resources.target_dataset.billing_profile_id
    dataset_id = context.resources.target_dataset.dataset_id

    payload = {
        "profileId": billing_profile_id,
        "loadControlFile": f"gs://{data_transfer_control_file.bucket}/{data_transfer_control_file.prefix}",
        "loadTag": context.resources.load_tag,
        "maxFailedFileLoads": 0
    }
    context.log.info(f'Bulk file ingest payload = {payload}')

    data_repo_client = context.resources.data_repo_client
    job_response: JobModel = data_repo_client.bulk_file_load(
        dataset_id,
        bulk_file_load=payload
    )
    context.log.info(f"bulk file ingest job id = {job_response.id}")
    return JobId(job_response.id)


@solid
def load_metadata_files(job_id: JobId) -> None:
    # TODO
    pass


@pipeline(mode_defs=[dev_mode, test_mode])
def ingest_ddp_data_pipeline() -> None:
    load_metadata_files(
        load_data_files(
            create_scratch_area(clear_scratch_area())
        )
    )

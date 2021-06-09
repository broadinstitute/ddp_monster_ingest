from dagster import InitResourceContext, ModeDefinition, pipeline, solid, resource
from dagster.core.execution.context.compute import AbstractComputeExecutionContext
from dataclasses import dataclass
from google.cloud.storage.client import Client
import json

from dagster_utils.resources.data_repo.jade_data_repo import jade_data_repo_client
from dagster_utils.contrib.data_repo.typing import JobId
from data_repo_client import JobModel

from ddp_ingest.config import preconfigure_for_mode
from ddp_ingest.resources import input_path, target_dataset
from ddp_ingest.utils.gcs import GsBucketWithPrefix, parse_gs_path, assert_input_path_sane, assert_path_exists

dev_mode = ModeDefinition(
    name="dev",
    resource_defs={
        "target_dataset": target_dataset,
        "data_repo_client": preconfigure_for_mode(jade_data_repo_client, "dev"),
        "gcs": None,  # TODO
        "input_path": input_path,
    }
)


@solid(
    required_resource_keys={"gcs", "input_path"},
    config_schema={
        "staging_path": str,
    }
)
def create_staging_area(context: AbstractComputeExecutionContext) -> GsBucketWithPrefix:
    storage_client: Client = context.resources.gcs
    input_bucket_with_prefix: GsBucketWithPrefix = parse_gs_path(context.resources.input_path)
    staging_bucket_with_prefix: GsBucketWithPrefix = parse_gs_path(context.solid_config["staging_path"])

    xfer_requests = []
    for blob in storage_client.list_blobs(
            input_bucket_with_prefix.bucket,
            prefix=input_bucket_with_prefix.prefix
    ):
        payload = {
            "source_path": f"gs://{blob.bucket}{blob.name}",
            "target_path": f"/{blob.name}"
        }
        xfer_requests.append(payload)

    bucket = storage_client.bucket(staging_bucket_with_prefix.bucket)
    destination_blob_name = f"{staging_bucket_with_prefix.prefix}/data_transfer_requests.json"
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(json.dumps(xfer_requests))
    return GsBucketWithPrefix(staging_bucket_with_prefix.bucket, blob.name)


@solid(
    required_resource_keys={"input_path", "data_repo_client", "target_dataset", "load_tag", "gcs"},
)
def load_data_files(context: AbstractComputeExecutionContext, data_transfer_control_file: GsBucketWithPrefix) -> JobId:
    input_path = context.resources.input_path
    data_repo_client = context.resources.data_repo_client
    storage_client = context.resources.gcs

    assert_input_path_sane(input_path, storage_client)
    assert_path_exists(data_transfer_control_file, storage_client)

    billing_profile_id = context.resources.target_dataset.billing_profile_id
    dataset_id = context.resources.target_dataset.dataset_id
    scratch_bucket_name = context.resources.scratch_config.scratch_bucket_name

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
def load_metadata_files() -> None:
    pass


@pipeline
def ingest_ddp_data() -> None:
    load_metadata_files(
        load_data_files(
            create_staging_area()
        )
    )

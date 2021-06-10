from dagster import repository, RepositoryDefinition

from ddp_ingest.ingest import ingest_ddp_data_pipeline


@repository
def repository():
    return [ingest_ddp_data_pipeline]

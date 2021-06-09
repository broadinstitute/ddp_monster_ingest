from dataclasses import dataclass
from dagster import resource, InitResourceContext


@dataclass
class TargetDataset:
    dataset_id: str
    billing_profile_id: str


@resource(
    config_schema={
        "dataset_id": str,
        "billing_profile_id": str
    }
)
def target_dataset(init_context: InitResourceContext) -> TargetDataset:
    return TargetDataset(
        init_context.resource_config["dataset_id"],
        init_context.resource_config["billing_profile_id"]
    )


@resource(
    config_schema={
        "input_path": str
    }
)
def input_path(init_context: InitResourceContext) -> str:
    input_path: str = init_context.resource_config["input_path"]
    return input_path


@resource(
    config_schema={
        "load_tag": str
    }
)
def load_tag(init_context: InitResourceContext) -> str:
    load_tag: str = init_context.resource_config["load_tag"]
    return load_tag

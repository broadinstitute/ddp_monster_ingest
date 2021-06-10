from dagster import execute_pipeline
from dagster.core.definitions.reconstructable import ReconstructablePipeline

from ddp_ingest.ingest import ingest_ddp_data_pipeline


def test_something():
    config = {
        'resources': {
            'input_path': {
                'config': {
                    'input_path': 'gs://fake-input-path'
                }
            },
            'load_tag': {
                'config': {
                    'load_tag': 'test_load_tag'
                }
            },
            'target_dataset': {
                'config': {
                    'billing_profile_id': 'fake_billing_profile_id',
                    'dataset_id': 'fake_dataset_id'
                }
            }
        },
        'solids': {
            'create_scratch_area': {
                'config': {
                    'scratch_path': 'gs://example-bucket'
                }
            },
            'clear_scratch_area': {
                'config': {
                    'scratch_path': 'gs://example-bucket'
                }
            }
        }
    }

    execute_pipeline(
        ingest_ddp_data_pipeline,
        mode="test",
        run_config=config
    )

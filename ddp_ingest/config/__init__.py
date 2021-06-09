import os

from dagster_utils.config.configurators import configurator_aimed_at_directory


preconfigure_for_mode = configurator_aimed_at_directory(os.path.dirname(__file__))

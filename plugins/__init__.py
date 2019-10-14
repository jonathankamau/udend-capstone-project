from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers


class CapstonePlugin(AirflowPlugin):
    name = "capstone_plugin"
    operators = [
        operators.ParquetToRedshiftOperator
    ]
    helpers = [
        helpers.SqlQueries,
        helpers.SaveToParquet
    ]

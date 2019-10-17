from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers


class CapstonePlugin(AirflowPlugin):
    name = "capstone_plugin"
    operators = [
        operators.RenderToS3Operator,
        operators.S3ToRedshiftOperator,
        operators.LoadFactDimOperator,
        operators.DataQualityOperator
    ]
    helpers = [
        helpers.staging_tables,
        helpers.fact_dimension_tables,
        helpers.immigration_table,
        helpers.temperature_table,
        helpers.airport_table,
        helpers.demographics_table,
        helpers.fact_dimension_insert,
        helpers.dim_demographic_table,
        helpers.dim_airport_table,
        helpers.dim_visitor_table,
        helpers.fact_city_data_table,
        helpers.fact_city_table_insert,
        helpers.dim_airport_table_insert,
        helpers.dim_demographic_table_insert,
        helpers.dim_visitor_table_insert
    ]

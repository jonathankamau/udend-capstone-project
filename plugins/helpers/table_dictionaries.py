""" Dictionary file for tables """

staging_tables = {
    'immigration': 'immigration_table',
    'temperature': 'temperature_table',
    'airport': 'airport_table',
    'demographics': 'demographics_table'
}

fact_dimension_tables = {
    'city_data': 'fact_city_data_table',
    'demographic': 'dim_demographic_table',
    'airport': 'dim_airport_table',
    'visitor': 'dim_visitor_table'
}

fact_dimension_insert = {
    'city_data': 'fact_city_table_insert',
    'demographic': 'dim_demographic_table_insert',
    'airport': 'dim_airport_table_insert',
    'visitor': 'dim_visitor_table_insert'
}

class SqlQueries:
    immigration_staging_table_create = (
        """
        CREATE TABLE IF NOT EXISTS immigration_data (
            visitor_id NUMERIC,
            immigration_id NUMERIC,
            year NUMERIC,
            month NUMERIC,
            city NUMERIC,
            country NUMERIC,
            port_of_entry NUMERIC,
            arrival_date NUMERIC,
            address_code VARCHAR,
            departure_date NUMERIC,
            age NUMERIC,
            visa_code NUMERIC,
            gender VARCHAR,
            airline VARCHAR,
            visa_type VARCHAR,
            )
        """
        )

    temperature_staging_table_create = (
        """
        CREATE TABLE IF NOT EXISTS temperature_data (
            date DATE,
            average_temperature NUMERIC,
            city NUMERIC,
            latitude VARCHAR,
            longitude VARCHAR)
        """
        )

    airport_staging_table_create = (
        """
        CREATE TABLE IF NOT EXISTS airport_data (
            airport_code VARCHAR,
            name VARCHAR,
            continent VRCHAR,
            country_code VARCHAR,
            region VARCHAR
        """
        )

    demographics_staging_table_create = (
        """
        CREATE TABLE IF NOT EXISTS demographics_data (
            city VARCHAR,
            state VARCHAR,
            male_population NUMERIC,
            female_population VARCHAR,
            total_population VARCHAR)
        """
        )

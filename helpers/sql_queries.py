class SQLQueries:
    immigration_data_table_create = (
        """
        CREATE TABLE IF NOT EXISTS immigration_data (
            cicid NUMERIC,
            194yr NUMERIC,
            194mon NUMERIC,
            194cit NUMERIC,
            194res NUMERIC,
            194port NUMERIC,
            arrdate NUMERIC,
            i94ddr VARCHAR,
            depdate NUMERIC,
            194bir NUMERIC,
            i94visa NUMERIC,
            gender VARCHAR,
            airline VARCHAR)
        """
        )

    temperature_data_table_create = (
        """
        CREATE TABLE IF NOT EXISTS temperature_data (
            dt DATE,
            AverageTemperature NUMERIC,
            AverageTemperatureUncertainty NUMERIC,
            City NUMERIC,
            Latitude VARCHAR,
            Longitude VARCHAR)
        """
        )

    airport_data_table_create = (
        """
        CREATE TABLE IF NOT EXISTS airport_data (
            type VARCHAR,
            name VARCHAR,
            continent VRCHAR,
            iso_country VARCHAR,
            iso_region VARCHAR
        """
        )

    demographics_data_table_create = (
        """
        CREATE TABLE IF NOT EXISTS demographics_data (
            City VARCHAR,
            State VARCHAR,
            male_population NUMERIC,
            female_poplulation VARCHAR,
            total_polulation VARCHAR)
        """
        )
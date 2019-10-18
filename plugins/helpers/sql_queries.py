immigration_table = (
    """
    CREATE TABLE IF NOT EXISTS public.immigration_table (
        visitor_id NUMERIC,
        immigration_id NUMERIC,
        year NUMERIC,
        month NUMERIC,
        city VARCHAR,
        country NUMERIC,
        port_of_entry NUMERIC,
        arrival_date NUMERIC,
        address_code VARCHAR,
        departure_date NUMERIC,
        age NUMERIC,
        visa_code NUMERIC,
        gender VARCHAR,
        airline VARCHAR,
        visa_type VARCHAR)
    """
    )

temperature_table = (
    """
    CREATE TABLE IF NOT EXISTS public.temperature_table (
        date DATE,
        average_temperature NUMERIC,
        city VARCHAR,
        latitude VARCHAR,
        longitude VARCHAR)
    """
    )

airport_table = (
    """
    CREATE TABLE IF NOT EXISTS public.airport_table (
        airport_code VARCHAR,
        name VARCHAR,
        continent VARCHAR,
        country_code VARCHAR,
        region VARCHAR)
    """
    )

demographics_table = (
    """
    CREATE TABLE IF NOT EXISTS public.demographics_table (
        city VARCHAR,
        state VARCHAR,
        male_population NUMERIC,
        female_population NUMERIC,
        total_population NUMERIC)
    """
    )

fact_city_data_table = ("""
        CREATE TABLE IF NOT EXISTS public.fact_city_data_table (
                city_id varchar(32) NOT NULL,
                city_name varchar(32) NOT NULL,
                country varchar(32) NOT NULL,
                latitude varchar(10) NOT NULL,
                longitude varchar(10) NOT NULL,
                average_temperature numeric NOT NULL,
                date date NOT NULL,
                CONSTRAINT fact_city_pkey PRIMARY KEY (city_id))
    """)

dim_airport_table = ("""
    CREATE TABLE IF NOT EXISTS public.dim_airport_table (
                airport_id varchar(50) NOT NULL,
                airport_code varchar(50) NOT NULL,
                name varchar(500) NOT NULL,
                continent varchar(50) NOT NULL,
                country_code varchar(32) NOT NULL,
                state varchar(32) NOT NULL,
                CONSTRAINT airport_pkey PRIMARY KEY (airport_id))
    """)

dim_demographic_table = ("""
    CREATE TABLE IF NOT EXISTS public.dim_demographic_table (
                demographic_id varchar(100) NOT NULL,
                city varchar(50) NOT NULL,
                state varchar(50) NOT NULL,
                male_population int4 NOT NULL,
                female_population int4 NOT NULL,
                total_population int4 NOT NULL,
                CONSTRAINT demographic_pkey PRIMARY KEY (demographic_id))
    """)

dim_visitor_table = ("""
    CREATE TABLE IF NOT EXISTS public.dim_visitor_table (
                visitor_id varchar(32) NOT NULL,
                year int4 NOT NULL,
                month int4 NOT NULL,
                city varchar(32) NOT NULL,
                gender varchar(32) NOT NULL,
                arrival_date date NOT NULL,
                departure_date date NOT NULL,
                airline varchar(32) NOT NULL,
                CONSTRAINT visitor_pkey PRIMARY KEY (visitor_id))
    """)

fact_city_table_insert = ("""
        INSERT INTO fact_city_data_table (
        city_id,
        city_name,
        country,
        latitude,
        longitude,
        average_temperature,
        date
        )
        SELECT DISTINCT
                md5(imm.city || temp.latitude || temp.longitude) AS city_id,
                imm.city,
                imm.country,
                temp.latitude,
                temp.longitude,
                temp.average_temperature,
                CAST(temp.date AS date) AS date
            FROM immigration_table imm
            LEFT JOIN temperature_table temp
            ON imm.city = UPPER(temp.city)
            WHERE temp.average_temperature IS NOT NULL
    """)

dim_airport_table_insert = (
    """
        INSERT INTO dim_airport_table (
        airport_id,
        airport_code,
        name,
        continent,
        country_code,
        state
        )
        SELECT DISTINCT
                md5(air.airport_code || air.region) AS airport_id,
                air.airport_code,
                air.name,
                air.continent,
                air.country_code,
                air.region
            FROM airport_table air
    """)

dim_demographic_table_insert = (
    """
        INSERT INTO dim_demographic_table (
        demographic_id,
        city,
        state,
        male_population,
        female_population,
        total_population
        )
        SELECT DISTINCT
                md5(dem.city || dem.state) AS demographic_id,
                UPPER(dem.city),
                dem.state,
                dem.male_population,
                dem.female_population,
                dem.total_population
            FROM demographics_table dem
            LEFT JOIN immigration_table imm
            ON imm.city = UPPER(dem.city)
            WHERE dem.male_population IS NOT NULL
            AND dem.female_population IS NOT NULL
            AND dem.total_population IS NOT NULL
    """)

dim_visitor_table_insert = (
    """
        INSERT INTO dim_visitor_table (
        visitor_id,
        year,
        month,
        city,
        gender,
        arrival_date,
        departure_date,
        airline
        )
        SELECT DISTINCT
                md5(imm.arrival_date || imm.departure_date || imm.city) AS visitor_id,
                imm.year,
                imm.month,
                imm.city,
                imm.gender,
                to_date(cast(imm.arrival_date as text), 'YYYY-MM-DD') AS arrival_date,
                to_date(cast(imm.departure_date as text), 'YYYY-MM-DD') AS departure_date,
                imm.airline
            FROM immigration_table imm
            WHERE imm.city IS NOT NULL
            AND imm.month IS NOT NULL
            AND imm.gender IS NOT NULL
            AND imm.arrival_date IS NOT NULL
            AND imm.departure_date IS NOT NULL
            AND imm.airline IS NOT NULL
    """)

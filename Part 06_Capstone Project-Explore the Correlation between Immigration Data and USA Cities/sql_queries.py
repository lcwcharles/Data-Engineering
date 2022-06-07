import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('decp.cfg')
IMMIGRATION_DATA=config.get('S3','IMMIGRATION_DATA')
IMMIGRANT_DATA=config.get('S3','IMMIGRANT_DATA')
AIRPORT_DATA = config.get('S3','AIRPORT_DATA')
TEMPERATURE_DATA = config.get('S3','TEMPERATURE_DATA')
DEMOGRAPHIC_DATA = config.get('S3','DEMOGRAPHIC_DATA')
i94cit_res_data = config.get('S3','i94cit_res_data')
i94port_data = config.get('S3','i94port_data')
i94mode_data = config.get('S3','i94mode_data')
i94addr_data = config.get('S3','i94addr_data')
i94visa_data = config.get('S3','i94visa_data')
REGION=config.get('S3','REGION')
ARN=config.get('IAM_ROLE','ARN')

# DROP TABLES
immigration_events_table_drop = "DROP  TABLE IF EXISTS immigration_events;"
immigration_table_drop = "DROP  TABLE IF EXISTS immigration;"
immigrant_table_drop = "DROP  TABLE IF EXISTS immigrant;"
i94cit_res_table_drop = "DROP  TABLE IF EXISTS i94cit_res;"
i94port_table_drop = "DROP  TABLE IF EXISTS i94port;"
i94mode_table_drop = "DROP  TABLE IF EXISTS i94mode;"
i94addr_table_drop = "DROP  TABLE IF EXISTS i94addr;"
i94visa_table_drop = "DROP  TABLE IF EXISTS i94visa;"
airport_table_drop = "DROP  TABLE IF EXISTS airport;"
temperature_table_drop = "DROP  TABLE IF EXISTS temperature;"
demographic_table_drop = "DROP  TABLE IF EXISTS demographic;"

# CREATE TABLES
immigration_events_table_create= """
                                 CREATE  TABLE IF NOT EXISTS public.immigration_events  (
                                    cicid bigint,
                                    i94yr int,
                                    i94mon int,
                                    i94cit int,
                                    i94res int,
                                    i94port varchar,
                                    arrival_date date,
                                    departure_date date,
                                    i94mode int,
                                    i94addr varchar,
                                    i94visa int,
                                    matflag varchar,
                                    airline varchar,
                                    admnum bigint,
                                    fltno varchar
                                 ); 
                                 """


immigrant_table_create = """
                          CREATE  TABLE IF NOT EXISTS public.immigrant (
                                admnum bigint PRIMARY KEY,
                                i94bir int,
                                gender varchar,
                                biryear int,
                                visatype varchar
                            );   
                             """

i94cit_res_table_create = """
                           CREATE  TABLE IF NOT EXISTS public.i94cit_res (
                                i94cit_res int PRIMARY KEY,
                                origin_travelled_country varchar(256)
                            );  
                             """

i94port_table_create = """
                        CREATE  TABLE IF NOT EXISTS public.i94port (
                            i94port varchar(256) PRIMARY KEY,
                            destination_city varchar(256),
                            region varchar
                        );
                        """

i94mode_table_create = """
                       CREATE  TABLE IF NOT EXISTS public.i94mode (
                            i94mode int4 PRIMARY KEY,
                            transportation varchar(256)
                        );      
                        """

i94addr_table_create = """
                       CREATE  TABLE IF NOT EXISTS public.i94addr (
                            i94addr varchar PRIMARY KEY,
                            "state" varchar(256)
                        );      
                        """

i94visa_table_create = """
                       CREATE  TABLE IF NOT EXISTS public.i94visa (
                            i94visa int NOT NULL,
                            immigration_reason varchar(256),
                            CONSTRAINT i94visa_pkey PRIMARY KEY (i94visa)
                        );      
                        """

airport_table_create = """
                       CREATE  TABLE IF NOT EXISTS public.airport (
                            airport_id INT PRIMARY KEY,
                            ident varchar,
                            "type" varchar,
                            name varchar,
                            elevation_ft decimal(18,1),
                            iso_country varchar,
                            iso_region varchar,
                            municipality varchar,
                            gps_code varchar,
                            local_code varchar,
                            longitude numeric(18,2),
                            latitude numeric(18,2)
                        );      
                             """

temperature_table_create = """
                           CREATE  TABLE IF NOT EXISTS public.temperature (
                                temperature_id INT PRIMARY KEY,
                                Country varchar,
                                City varchar,
                                "month" int,
                                Latitude numeric(18,2),
                                Longitude numeric(18,2),
                                AverageTemperature numeric
                            );  
                             """

demographic_table_create = """
                           CREATE  TABLE IF NOT EXISTS public.demographic (
                                demographic_id INT PRIMARY KEY,
                                city varchar,
                                "state" varchar,
                                median_age numeric(18,1),
                                male_population int,
                                female_population int,
                                total_population int,
                                veterans int,
                                foreign_born int,
                                average_household_size numeric
                            );  
                             """

immigration_table_create = """
                           CREATE  TABLE IF NOT EXISTS public.immigration (
                                cicid bigint PRIMARY KEY,
                                year int,
                                month int,
                                airport_id int REFERENCES airport(airport_id),
                                state_id varchar REFERENCES i94addr(i94addr),
                                city varchar,
                                visa_id int REFERENCES i94visa(i94visa),
                                admnum bigint REFERENCES immigrant(admnum),
                                --temperature_id bigint REFERENCES temperature(temperature_id),
                                --demographic int REFERENCES demographic(demographic_id),
                                longitude numeric(18,2),
                                latitude numeric(18,2)
                            );  
                             """

# STAGING TABLES

stage_immigration_events_copy = ("""COPY immigration_events
                                    FROM '{}/{}'
                                    IAM_ROLE '{}'
                                    FORMAT AS PARQUET; 
                                 """).format(BUCKET_NAME, IMMIGRATION_DATA, ARN)

stage_immigrant_copy = (""" COPY immigrant
                            FROM '{}/{}'
                            IAM_ROLE '{}'
                            FORMAT AS PARQUET; 
                        """).format(BUCKET_NAME, IMMIGRANT_DATA, ARN) 



stage_i94cit_res_copy = ("""COPY immigrant
                            FROM '{}/{}'
                            IAM_ROLE '{}'
                            ignoreheader 1 FORMAT AS CSV; 
                        """).format(BUCKET_NAME, i94cit_res_data, ARN) 

stage_i94port_copy = ("""COPY immigrant
                            FROM '{}/{}'
                            IAM_ROLE '{}'
                            ignoreheader 1 FORMAT AS CSV; 
                        """).format(BUCKET_NAME, i94port_data, ARN) 

stage_i94mode_copy = (""" COPY immigrant
                            FROM '{}/{}'
                            IAM_ROLE '{}'
                            ignoreheader 1 FORMAT AS CSV; 
                        """).format(BUCKET_NAME, i94mode_data, ARN) 

stage_i94addr_copy = ("""COPY immigrant
                            FROM '{}/{}'
                            IAM_ROLE '{}'
                            ignoreheader 1 FORMAT AS CSV; 
                        """).format(BUCKET_NAME, i94addr_data, ARN) 

stage_i94visa_copy = ("""COPY immigrant
                            FROM '{}/{}'
                            IAM_ROLE '{}'
                            ignoreheader 1 FORMAT AS CSV; 
                        """).format(BUCKET_NAME, i94visa_data, ARN) 
stage_airport_copy = ("""COPY immigrant
                            FROM '{}/{}'
                            IAM_ROLE '{}'
                            ignoreheader 1 FORMAT AS CSV; 
                        """).format(BUCKET_NAME, AIRPORT_DATA, ARN) 

stage_temperature_copy = ("""COPY immigrant
                            FROM '{}/{}'
                            IAM_ROLE '{}'
                            ignoreheader 1 FORMAT AS CSV; 
                        """).format(BUCKET_NAME, TEMPERATURE_DATA, ARN) 

stage_demographic_copy = ("""COPY immigrant
                            FROM '{}/{}'
                            IAM_ROLE '{}'
                            ignoreheader 1 FORMAT AS CSV; 
                        """).format(BUCKET_NAME, DEMOGRAPHIC_DATA, ARN) 

# FINAL TABLES

immigration_table_insert = ("""
                        INSERT INTO immigration (cicid, year, month, airport_id, state_id, city, visa_id,
                                                        admnum, longitude, latitude)
                        SELECT im.cicid,
                                im.i94yr AS year,
                                im.i94mon AS month,
                                air.airport_id, 
                                im.i94addr AS state_id,
                                air.municipality AS city,
                                im.i94visa AS visa_id,
                                im.admnum,
                                --tem.temperature_id,
                                --dem.demographic_id,
                                air.longitude,
                                air.latitude
                        FROM immigration_events im
                        JOIN airport as air
                        ON im.i94port = air.local_code
                        --JOIN temperature as tem
                        --ON trunc(air.latitude,0) = trunc(tem.latitude,0) and trunc(air.longitude,0) = trunc(tem.longitude,0) 
                        --and air.municipality = tem.city 
                        --and im.i94mon = tem.month
                        JOIN i94addr as addr
                        ON im.i94addr = addr.i94addr
                        --JOIN demographic dem
                        --ON air.iso_region = addr.i94addr and upper(addr.state) = upper(dem.state) and air.municipality = dem.city
                        WHERE im.i94mode=1 
                        
                        """)

# QUERY LISTS

create_table_queries = [immigration_events_table_create, immigrant_table_create, i94cit_res_table_create, i94port_table_create, 
                        i94mode_table_create, i94addr_table_create, i94visa_table_create, airport_table_create, 
                        temperature_table_create, demographic_table_create,immigration_table_create ]

drop_table_queries = [immigration_events_table_drop, immigration_table_drop, immigrant_table_drop, 
                        i94cit_res_table_drop, i94port_table_drop, i94mode_table_drop, i94addr_table_drop, 
                        i94visa_table_drop, airport_table_drop, temperature_table_drop, demographic_table_drop ]

copy_table_queries = [stage_immigration_events_copy ,stage_immigrant_copy , stage_i94cit_res_copy ,stage_i94port_copy,
                        stage_i94mode_copy ,stage_i94addr_copy ,stage_i94visa_copy ,stage_airport_copy ,
                        stage_temperature_copy ,stage_demographic_copy]

insert_table_queries = [immigration_table_insert ]

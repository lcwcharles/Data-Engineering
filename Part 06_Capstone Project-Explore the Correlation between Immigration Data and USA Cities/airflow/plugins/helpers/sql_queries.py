class SqlQueries:
    immigration_table_insert = ("""
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

CREATE KEYSPACE ineedtoknow WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE ineedtoknow.street_data (
street text,
category text,
import_date timestamp,
latitude double,
longitude double,
description text,
PRIMARY KEY ((street,category), import_date)
)
WITH CLUSTERING ORDER BY (import_date DESC)
AND default_time_to_live = 604800;
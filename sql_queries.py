import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
artist text,
auth text NOT NULL,
firstName text,
gender char(1),
itemInSession int,
lastName text,
length decimal,
level text,
location text,
method text,
page text,
registration numeric,
sessionId int,
song text,
status int,
ts double precision,
userAgent text,
userId int
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
num_songs int, 
artist_id text NOT NULL, 
artist_latitude decimal,
artist_longitude decimal,
artist_location text,
artist_name text, 
song_id text NOT NULL, 
title text NOT NULL,
duration decimal,
year int
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
songplay_id integer IDENTITY(0,1) sortkey distkey, 
start_time timestamp, 
user_id integer not null, 
level varchar(4), 
song_id varchar(25) not null, 
artist_id varchar(25) not null, 
session_id int, 
location text, 
user_agent text
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
user_id  integer not null sortkey distkey,
first_name varchar(25) not null,
last_name varchar(25) not null,
gender char(1) not null, 
level text
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
song_id varchar(25) not null sortkey, 
title text not null, 
artist_id varchar(50) not null, 
year integer not null, 
duration decimal not null
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
artist_id varchar(50) not null sortkey, 
name text not null, 
location text, 
latitude decimal, 
longitude decimal
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time timestamp not null sortkey,
hour integer not null,
day integer not null,
week integer not null,
month integer not null,
year integer not null,
weekday integer not null
);
""")

# STAGING TABLES

staging_events_copy = (""" copy staging_events from '{}'
credentials 'aws_iam_role={}'
format as json 'auto ignorecase' region 'us-west-2';
""").format(config.get('S3','LOG_DATA').strip("'"),config.get('IAM_ROLE','ARN').strip("'"))

staging_songs_copy = (""" copy staging_songs from '{}'
credentials 'aws_iam_role={}'
format as json 'auto' region 'us-west-2';
""").format(config.get('S3','SONG_DATA').strip("'"),config.get('IAM_ROLE','ARN').strip("'"))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second' as start_time,
se.userid,
se.level,
ss.song_id,
ss.artist_id,
se.sessionid,
se.location,
se.useragent
FROM staging_events se
JOIN staging_songs ss
ON se.artist = ss.artist_name AND se.song = ss.title
WHERE se.userId IS NOT NULL AND ss.song_id IS NOT NULL AND ss.artist_id IS NOT NULL AND se.length = ss.duration;
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT se.userid, se.firstname, se.lastname, se.gender, se.level
FROM staging_events se
WHERE se.userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (song_id,title,artist_id,year,duration)
SELECT DISTINCT ss.song_id, ss.title, ss.artist_id, ss.year, ss.duration
FROM staging_songs ss;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT  ss.artist_id, ss.artist_name, ss.artist_location, ss.artist_latitude, ss.artist_longitude 
FROM staging_songs ss;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second' as start_time, 
EXTRACT(HOUR FROM timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second'),
EXTRACT(DAY FROM timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second'),
EXTRACT(WEEK FROM timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second'),
EXTRACT(MONTH FROM timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second'),
EXTRACT(YEAR FROM timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second'),
EXTRACT(DOW FROM timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second')
FROM staging_events se;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
create_staging_table_queries = [staging_events_table_create, staging_songs_table_create]
drop_staging_table_queries = [staging_events_table_drop, staging_songs_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

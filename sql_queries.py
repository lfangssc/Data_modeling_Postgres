
# DROP TABLES

songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists time;"

# CREATE TABLES

songplay_table_create = "create table songplays (songplay_id SERIAL primary key, \
                                                 ts bigint not null, \
                                                 user_id int not null, \
                                                 level varchar(20), \
                                                 song_id varchar(45) not null, \
                                                 artist_id varchar(45) not null, \
                                                 session_id int, \
                                                 location varchar(45), \
                                                 user_agent text);"

user_table_create = "create table users (user_id varchar primary key, \
                                         first_name varchar, \
                                         last_name varchar, \
                                         gender varchar (10), \
                                         level varchar(20));"

song_table_create = "create table songs (song_id varchar primary key, \
                                         title varchar, \
                                         artist_id varchar not null, \
                                         year int, \
                                         duration float);"

artist_table_create = "create table artists (artist_id varchar primary key, \
                                             name varchar, \
                                             location varchar, \
                                             latitude float, \
                                             longitude float);"

time_table_create = "create table time (ts bigint primary key, \
                                        start_time time, \
                                        hour int, \
                                        day int, \
                                        week int, \
                                        month int, \
                                        year int, \
                                        weekday int);"

# INSERT RECORDS

songplay_table_insert = "insert into songplays (ts, user_id, \
                                                level, song_id, \
                                                artist_id, session_id, \
                                                location, user_agent) \
                         values(%s,%s,%s,%s,%s,%s, %s, %s);" 

user_table_insert = "insert into users(user_id, first_name,\
                                       last_name,gender,level) \
                    values (%s,%s,%s,%s,%s) \
                    on conflict (user_id) do update SET level = EXCLUDED.level; "

song_table_insert = "insert into songs(song_id, title, \
                                       artist_id, year, duration) \
                    values (%s, %s, %s, %s, %s) \
                    on conflict (song_id) do nothing; "

artist_table_insert = "insert into artists(artist_id, name, \
                                           location, latitude, longitude) \
                    values (%s, %s, %s, %s, %s) \
                    on conflict (artist_id) do nothing; "



time_table_insert = "insert into time(ts, start_time,\
                                      hour,day,\
                                      week,month,\
                                      year,weekday) \
                    values (%s, %s, %s, %s, %s, %s,%s,%s) \
                    on conflict (ts) do nothing; "

# FIND SONGS

song_select = "SELECT songs.song_id, artists.artist_id FROM songs \
                JOIN artists ON  songs.artist_id=artists.artist_id \
                WHERE songs.title=%s AND artists.name=%s AND songs.duration=%s;"

create_table_queries = [songplay_table_create, 
                        user_table_create, 
                        song_table_create, 
                        artist_table_create, 
                        time_table_create]

drop_table_queries = [songplay_table_drop, 
                      user_table_drop, 
                      song_table_drop, 
                      artist_table_drop, 
                      time_table_drop]
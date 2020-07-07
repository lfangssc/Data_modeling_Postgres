import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import datetime

def process_song_file(cur, filepath):
    """
    This function is about dealing with the song data
    - read the song data from song files into a dataframe
    - Select the song id, title, artist id, year and duration from the song data and insert them into the table songs
    - Select the artist id, artist name, artist location, artist latitude and artist longitude from the song data and insert them into the table artists
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)
 
    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']
                  ].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    
    # insert artist record
    artist_data = df[['artist_id','artist_name',
                      'artist_location','artist_latitude',
                      'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    This function is about dealing with the log data
    - read the log data from the log files into a dataframe and filter by NextSong action
    - read the time column and extract the start time, day, hour, week, month, year and week days
    - Insert the time related info into the table time
    - Select the user id, first name, last name, gender and level from the log dataframe and insert them into the table users
    - get the song ID and artist ID by querying the songs and artists tables to find matches based on song title, artist name, and song duration time.
    - Select the time, user ID, level, song ID, artist ID, session ID, location and user agent and insert them into the table songplays
    """
    # open log file
    df = pd.read_json(filepath,lines = True)

    # filter by NextSong action
    df_nextsong = df.loc[(df['page']=='NextSong')]

    # convert timestamp column to datetime
    t = df_nextsong['ts'].apply(lambda x : 
                                datetime.datetime.fromtimestamp(x/1000.0))
    
    # insert time data records
    
    column_labels = ('ts','start_time','hour',
                     'day','week','month','year','weekday')
    time_df = pd.DataFrame(columns = column_labels)
    time_df['ts']=df_nextsong['ts']
    time_df['start_time']= t.dt.time
    time_df['hour']=t.dt.hour
    time_df['day']=t.dt.day
    time_df['week']=t.dt.week
    time_df['month']=t.dt.month
    time_df['year']=t.dt.year
    time_df['weekday']=t.dt.weekday
    
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
        conn.commit()
    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
        conn.commit()

    # insert songplay records
    for index, row in df_nextsong.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data=(row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location,row.userAgent)

        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This function look into the filepath folder, collect the paths of the files and form a list
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    - Connect to database
    - Look into the data folders to collect the files to process
    - send the song files to the function for processing song files 'process_song_file'
    - send the log files to the function for processing log files 'process_log_file'
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
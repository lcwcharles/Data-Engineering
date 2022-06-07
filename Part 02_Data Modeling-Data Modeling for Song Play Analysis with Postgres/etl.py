import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import json
import time
import numpy as np

"""
Convert the data type <np.*> to <*>, when we extract and transform data from DataFrame, then load them into PostgreSQL. 
"""
from psycopg2.extensions import register_adapter, AsIs

def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)

def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)

def addapt_numpy_float32(numpy_float32):
    return AsIs(numpy_float32)

def addapt_numpy_int32(numpy_int32):
    return AsIs(numpy_int32)

def addapt_numpy_array(numpy_array):
    return AsIs(tuple(numpy_array))

register_adapter(np.float64, addapt_numpy_float64)
register_adapter(np.int64, addapt_numpy_int64)
register_adapter(np.float32, addapt_numpy_float32)
register_adapter(np.int32, addapt_numpy_int32)
register_adapter(np.ndarray, addapt_numpy_array)


def process_song_file(cur, filepath):
    """
    Description: 
        This function can be used to read the file in the filepath (data/song_data)
        to get the songs and artists info and used to populate the songs and artists tables.

    Arguments:
        cur: the cursor object. 
        filepath: song data file path. 

    Returns:
        None
    """
    # open song file
    df_data = pd.DataFrame([json.loads(open(filepath).read())])
    # insert song record
    song_data = df_data.loc[0,['song_id', 'title', 'artist_id', 'year', 'duration']].values.tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df_data.loc[0,['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values.tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Description: 
        This function can be used to read the file in the filepath (data/log_data) 
        to get the user and time info and used to populate the users and time tables.

    Arguments:
        cur: the cursor object. 
        filepath: log data file path. 

    Returns:
        None
    """
    # open log file
    df_log = pd.DataFrame([json.loads((open(filepath).read().split('\n'))[i]) for i in range(len(open(filepath).read().split('\n')))])

    # filter by NextSong action
    df_log = df_log.loc[df_log['page']=='NextSong']

    # convert timestamp column to datetime
    t = pd.DataFrame((df_log['ts']/1000).astype('datetime64[s]'))
    
    # insert time data records
    time_data = (t['ts'], t['ts'].dt.hour, t['ts'].dt.day, t['ts'].dt.week, t['ts'].dt.month, t['ts'].dt.year, t['ts'].dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df_log[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df_log.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (time.ctime(df_log.loc[index,'ts']/1000),str(df_log.loc[index,'userId']),str(df_log.loc[index,'level']),songid,artistid,
                         int(df_log.loc[index,'sessionId']),str(df_log.loc[index,'location']),str(df_log.loc[index,'userAgent']))

        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Description: 
        Get all file paths and call the fuction(process_song_file,process_log_file), 
        then commit to PostgreSQL.

    Arguments:
        cur: the cursor object. 
        conn: the connection of the database.
        filepath: song data file path. 
        func: function to call.

    Returns:
        None
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv


# This should make a connection to a Cassandra instance your local machine 
from cassandra.cluster import Cluster
try: 
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
except Exception as e:
    print(e)

# Create Keyspace
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)
except Exception as e:
    print(e)

# Set KEYSPACE to the keyspace specified above
try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)


## Table 1:  Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4
## Reason: sessionID and itemInSession together are unique, so created a composite primary key with partition key as sessionID and clustering key is itemInSession
query = "DROP TABLE IF EXISTS music_history"
try:
    session.execute(query)
except Exception as e:
    print(e)

query1 = "CREATE TABLE IF NOT EXISTS music_history "
query1 = query1 + "(session_id int, item_in_session int, artist_name text, song_title text, song_length decimal, \
                       PRIMARY KEY (session_id, item_in_session))"
try:
    session.execute(query1)
except Exception as e:
    print(e)


## Table 2: Name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
## Reason: On Data Analysis, we can notice for every userid the sessionid is unique. So created a composite partition key with userid and sessionid. 
#             The query needs sorting by itemInSession, so adding itemInSession as a clustering key
query2 = "DROP TABLE IF EXISTS music_history1"
try:
    session.execute(query2)
except Exception as e:
    print(e)

query3 = "CREATE TABLE IF NOT EXISTS music_history1 "
query3 = query3 + "(user_id int, session_id int, item_in_session int, artist_name text, song_title text, \
                       user_name text,  PRIMARY KEY ((user_id, session_id), item_in_session))"
try:
    session.execute(query3)
except Exception as e:
    print(e)


## Table 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
## Reason: A song can be listened by multiple users and the query needs to be done just on the song_title. 
##              So created a composite primary key with partition key as song_title and clustering key is userid

query4 = "DROP TABLE IF EXISTS music_history2"
try:
    session.execute(query4)
except Exception as e:
    print(e)

query5 = "CREATE TABLE IF NOT EXISTS music_history2 "
query5 = query5 + "(song_title text, user_id int, artist_name text, song_length decimal, session_id int, item_in_session int, \
                       user_name text,  PRIMARY KEY (song_title, user_id))"
try:
    session.execute(query5)
except Exception as e:
    print(e)



# We have provided part of the code to set up the CSV file. Please complete the Apache Cassandra code below#
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        # Insert data into data models
        query = "INSERT INTO music_history (artist_name, song_title, song_length, session_id, item_in_session)"
        query = query + " VALUES (%s, %s, %s, %s, %s)"
        session.execute(query, (line[0], line[9], float(line[5]), int(line[8]), int(line[3])))
        
        query1 = "INSERT INTO music_history1 (artist_name, song_title, session_id, item_in_session, user_name, user_id)"
        query1 = query1 + " VALUES (%s, %s, %s, %s, %s, %s)"
        session.execute(query1, (line[0], line[9], int(line[8]), int(line[3]), line[1] + ' ' + line[4], int(line[10])))
        
        query2 = "INSERT INTO music_history2 (song_title, user_name, user_id)"
        query2 = query2 + " VALUES (%s, %s, %s)"
        session.execute(query2, (line[9], line[1] + ' ' + line[4], int(line[10])))


## Query 1:  Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4

query = "select artist_name, song_title, song_length from music_history WHERE session_id = 338 and item_in_session = 4"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

dataframe = pd.DataFrame(list(rows))
print(dataframe)



## TO-DO: Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
## for userid = 10, sessionid = 182
query = "select artist_name, song_title, item_in_session, user_name from music_history1 WHERE user_id = 10 and  session_id = 182"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
dataframe = pd.DataFrame(list(rows))
print(dataframe)



##  Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

query = "select user_name from music_history2 WHERE song_title = 'All Hands Against His Own'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
dataframe = pd.DataFrame(list(rows))
print(dataframe)


## Drop the tables before closing out the sessions
query = "DROP TABLE IF EXISTS music_history"
try:
    session.execute(query)
except Exception as e:
    print(e)


query2 = "DROP TABLE IF EXISTS music_history1"
try:
    session.execute(query2)
except Exception as e:
    print(e)


query4 = "DROP TABLE IF EXISTS music_history2"
try:
    session.execute(query4)
except Exception as e:
    print(e)



## Close the Session
session.shutdown()
cluster.shutdown()
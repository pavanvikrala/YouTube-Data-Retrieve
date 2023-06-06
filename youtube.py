from googleapiclient.discovery import build
from pymongo import MongoClient
from sqlalchemy import create_engine,text
import mysql.connector
import pandas as pd
import streamlit as st
# Additional libraries
import isodate
import datetime

##fetch information from the YouTube Data API storing the data in a MongoDB database, and creating corresponding tables in a MySQL database.

# Specify API key
key = '****'
youtube = build('youtube', 'v3', developerKey = key)
ID = '****'


def get_channel_info(channel_id=ID, max_results=1):
    channel_info = {
        "Channel_Name": '',
        "Channel_Id": channel_id,
        "Subscription_Count": 0,
        "Channel_Views": 0,
        "Channel_Description": '',
        "Playlist_Id": "",
        "Channel_Status": ""
    }
    request = youtube.channels().list(
        part='snippet, statistics',
        id=channel_id
    )

    response = request.execute()

    if 'items' in response and len(response['items']) > 0:
        channel_info['Channel_Name'] = response['items'][0]['snippet']['title']
        channel_info['Channel_Id'] = ID
        channel_info['Subscription_Count'] = int(response['items'][0]['statistics']['subscriberCount'])
        channel_info['Channel_Views'] = int(response['items'][0]['statistics']['viewCount'])
        channel_info['Channel_Description'] = response['items'][0]['snippet']['description']

        if 'status' in response['items'][0] :
            channel_info['Channel_Status'] = response['items'][0]['status']['privacyStatus']
        else :
            channel_info['Channel_Status'] = None

    playlist_info = get_playlists()
    channel_info['Playlist_Id'] = playlist_info['Playlist_Id']

    return channel_info


def get_playlists(channel_id=ID):
    playlist_info = {
        "Playlist_Id": "",
        "Playlist_Name" :"",
        "Channel_Id" : channel_id
    }

    request1 = youtube.playlists().list(
        part='snippet',
        channelId=channel_id,
        maxResults=1
    )

    response1 = request1.execute()

    if 'items' in response1 and len(response1['items']) > 0:
        playlist_info['Playlist_Id'] = response1['items'][0]['id']
        playlist_info["Playlist_Name"] = response1['items'][0]['snippet']['title']
    else:
        playlist_info['Playlist_Id'] = "No Playlist Found"

    return playlist_info

def search_videos(channel_id = ID, max_results=1) :
    request2 = youtube.search().list(
        part='id',
        q=f'{channel_id}',
        type='video',
        maxResults=max_results
    )
    response2 = request2.execute()

    video_id =''
    for i in response2['items'] :
        video_id = i['id']['videoId']
    return video_id

id_video = search_videos()

def get_video_info(video_id=id_video) :
    request3 = youtube.videos().list(
        part='contentDetails, statistics, snippet',
        id=video_id
    )
    response3 = request3.execute()

    video_info = {
        "Video_Id": video_id,
        "Video_Name": response3['items'][0]['snippet']['title'],
        "Video_Description": response3['items'][0]['snippet']['localized']['description'],
        "Tags": response3['items'][0]['snippet'].get('tags', []),
        "PublishedAt": response3['items'][0]['snippet']['publishedAt'],
        "View_Count": int(response3['items'][0]['statistics']['viewCount']),
        "Like_Count": int(response3['items'][0]['statistics']['likeCount']),
        "Dislike_Count": 0,
        "Favorite_Count": int(response3['items'][0]['statistics']['favoriteCount']),
        "Comment_Count": int(response3['items'][0]['statistics']['commentCount']),
        "Duration": response3['items'][0]['contentDetails']['duration'],
        "Thumbnail": response3['items'][0]['snippet']['thumbnails']['default']['url'],
        "Caption_Status": response3['items'][0]['contentDetails']['caption']
    }
    # Converting the datetime string to a valid MySQL datetime format using strptime() and strftime()
    date_str = response3['items'][0]['snippet']['publishedAt']
    date = datetime.datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%SZ')
    date_mysql = date.strftime('%Y-%m-%d %H:%M:%S')
    video_info['PublishedAt'] = date_mysql

    # Extract seconds from duration field above
    duration_string = response3['items'][0]['contentDetails']['duration']
    duration = isodate.parse_duration(duration_string)
    duration_secs = duration.total_seconds()
    video_info["Duration"] = f"{int(duration_secs)} seconds"

    return video_info

def get_comments(video_id = id_video) :
    request4 = youtube.commentThreads().list(
        part = 'snippet',
        videoId= video_id,
        maxResults = 10,
        textFormat = 'plainText'
    )
    response4 = request4.execute()

    comments = []
    comment_info = {}
    for i in response4.get('items', []) :
        comment = i['snippet']['topLevelComment']['snippet']['textDisplay']
        comments.append(comment)

    if len(comments) > 0 :
        top_comment = response4['items'][0]
        comments_published = top_comment['snippet']['topLevelComment']['snippet']['publishedAt']
        comments_published_date = datetime.datetime.strptime(comments_published, "%Y-%m-%dT%H:%M:%SZ")
        comments_published_date_new = comments_published_date.strftime("%Y-%m-%d %H:%M:%S")


        comment_info = {
            "Comment_Id": top_comment['id'],
            "Comment_Text": top_comment['snippet']['topLevelComment']['snippet']['textDisplay'],
            "Comment_Author": top_comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
            "Comment_PublishedAt": comments_published_date_new
        }

    return comment_info


# Setting up mongodb connection and creating a connection between IDE and mongodb

client = MongoClient('localhost', 27017)
client.drop_database('db_new')
# create a new database called db
db = client['db_new']


# Creating collections in the database
channel_collection = db['channel']
video_collection = db['video']
comment_collection = db['comment']


if len(get_channel_info()) > 0:
    channel_collection.insert_many([get_channel_info()])
if len(get_video_info()) > 0:
    video_collection.insert_many([get_video_info()])
if len(get_comments()) > 0:
    comment_collection.insert_many([get_comments()])

connection = mysql.connector.connect(
host = "***",
user = "***",
password = "***",
database = 'youtube'
)

cha_docs = list(channel_collection.find())
vid_docs = list(video_collection.find())
comm_docs = list(comment_collection.find())

channel_df = pd.DataFrame(cha_docs)
video_df = pd.DataFrame(vid_docs)
comment_df = pd.DataFrame(comm_docs)

channel_df = channel_df.drop('_id', axis=1)
video_df = video_df.drop('_id', axis=1)
comment_df = comment_df.drop('_id', axis=1)

# Let's create tables for the dataframes
cursor = connection.cursor()

# These lines will help if you run the code multiple times
cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

cursor.execute("DROP TABLE IF EXISTS comments")
cursor.execute("DROP TABLE IF EXISTS videos")
cursor.execute("DROP TABLE IF EXISTS playlists")
cursor.execute("DROP TABLE IF EXISTS channels")

cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

# creating channel table
cursor.execute(''' CREATE TABLE channels (
                   channel_id VARCHAR(255) PRIMARY KEY,
                   channel_name VARCHAR(255),
                   subscription_count INT,
                   channel_views INT,
                   channel_description TEXT,
                   channel_status VARCHAR(255)
                   )
''')

# creating playlist table
cursor.execute('''CREATE TABLE playlists (
                    playlist_id VARCHAR(255) PRIMARY KEY,
                    channel_id VARCHAR(255),
                    playlist_name VARCHAR(255),
                    FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
                    )
''')

# Creating video table
cursor.execute('''CREATE TABLE videos(
                    video_id VARCHAR(255) PRIMARY KEY,
                    playlist_id VARCHAR(255),
                    video_name VARCHAR(255),
                    video_description TEXT,
                    published_date DATETIME,
                    view_count INT,
                    like_count INT,
                    dislike_count INT,
                    favorite_count INT,
                    comment_count INT,
                    duration VARCHAR(50),
                    thumbnail VARCHAR(255),
                    caption_status VARCHAR(255),
                    FOREIGN KEY (playlist_id) REFERENCES playlists(playlist_id)
                    )
''')

# Creating the comment table
cursor.execute(''' CREATE TABLE comments (
                    comment_id VARCHAR(255) PRIMARY KEY,
                    video_id VARCHAR(255),
                    comment_text TEXT,
                    comment_author VARCHAR(255),
                    comment_published_date DATETIME,
                    FOREIGN KEY (video_id) REFERENCES videos(video_id)
                    )
''')

# Insert data into the channels table
channel_info = get_channel_info()
if channel_info :
    channel_query = '''
    INSERT INTO channels (
        channel_id ,
        channel_name ,
        subscription_count,
        channel_views ,
        channel_description ,
        channel_status )
    VALUES (%s, %s, %s, %s, %s, %s) '''

    channel_data = (
        channel_info['Channel_Id'],
        channel_info['Channel_Name'],
        channel_info['Subscription_Count'],
        channel_info['Channel_Views'],
        channel_info['Channel_Description'],
        channel_info['Channel_Status']
                    )
    cursor.execute(channel_query, channel_data)

## Insert data into the playlists table

playlist_info = get_playlists()
if playlist_info :
    query = '''
    INSERT INTO playlists (
        playlist_id, 
        channel_id,
        playlist_name
        )
    VALUES (%s, %s, %s) '''
    playlist_data = (
        playlist_info['Playlist_Id'],
        playlist_info['Channel_Id'],
        playlist_info['Playlist_Name']
    )
    cursor.execute(query, playlist_data)

video_info = get_video_info()
if video_info :
    video_query = '''
    INSERT INTO videos (
        video_id,
        playlist_id,
        video_name,
        video_description, 
        published_date,
        like_count,
        dislike_count,
        favorite_count,
        comment_count,
        duration,
        thumbnail,
        caption_status
        )
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
    '''
    video_data = (
        video_info['Video_Id'],
        playlist_info['Playlist_Id'],
        video_info['Video_Name'],
        video_info['Video_Description'],
        video_info['PublishedAt'],
        video_info['Like_Count'],
        video_info['Dislike_Count'],
        video_info['Favorite_Count'],
        video_info['Comment_Count'],
        video_info['Duration'],
        video_info['Thumbnail'],
        video_info['Caption_Status']
        )
    cursor.execute(video_query, video_data)

comment_info = get_comments()
if comment_info :
    comment_query = '''
    INSERT INTO comments (
        comment_id,
        video_id,
        comment_text,
        comment_author,
        comment_published_date
        )
    VALUES ( %s, %s, %s, %s, %s)
    '''
    comment_data = (
        comment_info['Comment_Id'],
        video_info['Video_Id'],
        comment_info['Comment_Text'],
        comment_info['Comment_Author'],
        comment_info['Comment_PublishedAt']
        )
    cursor.execute(comment_query, comment_data)


connection.commit()
cursor.close()
connection.close()

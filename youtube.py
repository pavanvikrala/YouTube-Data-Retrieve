from googleapiclient.discovery import build
from pymongo import MongoClient
import mysql.connector
import pandas as pd
import streamlit as st
# Additional libraries
import isodate
import datetime

# Fetch information from the YouTube Data API storing the data in a MongoDB database
#and creating corresponding tables in a MySQL database.

# Setting up API credentials
key = 'API KEY'

# Creating the YouTube Data APi service
youtube = build('youtube', 'v3', developerKey=key)

def get_channel_info(channel_id):
    """This function takes in channel_id as input and extracts all the required channel information"""
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
        channel_info['Channel_Id'] = channel_id
        channel_info['Subscription_Count'] = int(response['items'][0]['statistics']['subscriberCount'])
        channel_info['Channel_Views'] = int(response['items'][0]['statistics']['viewCount'])
        channel_info['Channel_Description'] = response['items'][0]['snippet']['description']

        if 'status' in response['items'][0]:
            channel_info['Channel_Status'] = response['items'][0]['status']['privacyStatus']
        else:
            channel_info['Channel_Status'] = None

    playlist_info = get_playlists(channel_id)
    channel_info['Playlist_Id'] = playlist_info['Playlist_Id']

    return channel_info


def get_playlists(channel_id):
    """This function takes in channel_id as input and extracts the playlists information like Playlist_id"""
    playlist_info = {
        "Playlist_Id": "",
        "Playlist_Name": "",
        "Channel_Id": channel_id
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

def search_videos(channel_id, max_results=50):
    """This function takes in channel id as input and returns a list of video_ids for that particular channel"""
    video_ids = []
    next_page_token = None

    while True:
        request2 = youtube.search().list(
            part='id',
            channelId=channel_id,
            type='video',
            maxResults=min(max_results, 50),
            pageToken=next_page_token
        )
        response = request2.execute()

        ids = [i['id']['videoId'] for i in response['items']]
        video_ids.extend(ids)

        next_page_token = response.get('nextPageToken')

        if not next_page_token or len(video_ids) >= max_results:
            break


    return video_ids[:max_results]


def get_video_info(video_ids):
    """This function takes in list of video_ids as input
    and returns a list of video information for all the video_ids"""

    video_info_list = []
    for vid in video_ids:
        request3 = youtube.videos().list(
            part='contentDetails, statistics, snippet',
            id=vid
        )
        response3 = request3.execute()

        if 'items' in response3 and len(response3['items']) > 0:
            video_info = {
                "Video_Id": vid,
                "Video_Name": response3['items'][0]['snippet']['title'],
                "Video_Description": response3['items'][0]['snippet']['localized']['description'],
                "Tags": response3['items'][0]['snippet'].get('tags', []),
                "PublishedAt": response3['items'][0]['snippet']['publishedAt'],
                "View_Count": int(response3['items'][0]['statistics'].get('viewCount'), 0),
                "Like_Count": int(response3['items'][0]['statistics'].get('likeCount', 0)),
                "Dislike_Count": int(response3['items'][0]['statistics'].get('dislikeCount', 0)),
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

            # Extracting seconds from duration field above
            duration_string = response3['items'][0]['contentDetails']['duration']
            duration = isodate.parse_duration(duration_string)
            duration_secs = duration.total_seconds()
            video_info["Duration"] = int(duration_secs)

            video_info_list.append(video_info)

    return video_info_list

def get_comments(video_ids):
    """This function will take in list of video ids as input and returns list of comments for all the video_ids"""
    comments = []

    for vid in video_ids:

        request4 = youtube.commentThreads().list(
            part='snippet',
            videoId=vid,
            maxResults=10,
            textFormat='plainText'
        )
        response4 = request4.execute()

        if 'items' in response4 and len(response4['items']) > 0:
            for i in response4['items']:
                video_id = i['snippet']['videoId']
                comment_info = {
                    "Comment_Id": i['id'],
                    "Video_Id": vid,
                    "Comment_Text": i['snippet']['topLevelComment']['snippet']['textDisplay'],
                    "Comment_Author": i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    "Comment_PublishedAt": i['snippet']['topLevelComment']['snippet']['publishedAt']
                }

                comments_published = i['snippet']['topLevelComment']['snippet']['publishedAt']
                comments_published_date = datetime.datetime.strptime(comments_published, "%Y-%m-%dT%H:%M:%SZ")
                comments_published_sql = comments_published_date.strftime("%Y-%m-%d %H:%M:%S")
                comment_info["Comment_PublishedAt"] = comments_published_sql

                comments.append(comment_info)
    return comments

img_url = 'https://res.cloudinary.com/df7cbq3qu/image/upload/v1686548135/youtube_hmjndk.png'
page_config = {"page_title": 'YouTube Project', "page_icon": img_url, "layout": "wide"}
st.set_page_config(**page_config)

st.title(":red[YouTube] :sunglasses: Insights: Unveiling the Hidden Gems of Channel, Video, and Comment Data")

name = st.text_input("Hi👋 What's your name?")

if name:
    st.write(f"Hi :wave: {name}....before we begin, let me tell you that you're gonna discover hidden secrets of YouTube by extracting your preferred channel information:wink:")
    if st.checkbox("Click me for more info") :
        st.write("We're basically extracting YouTube channel hidden information like channel details, videos, comments etc by just three steps.")
        st.info(" Step 1 : Extract all the channel information by taking the channel_id from you.")
        st.info(" Step 2 : Stores the retrieved channel information in MongoDB data lake in the form of collections.")
        st.info(" Step 3 : These collections will be transformed into SQL tables so that we can view them.")
        st.warning("       Finally we can check all the queries listed and view the data retrieved")
        st.info("By the way, you can add data of upto 10 YouTube channels......cool right 😛")

    user_input = st.text_input("Step 1 : Input channel Id and press enter")
    if user_input:
        steps = ['', '🫙 Mongo', '➡️ SQL', '❓🤔Queries']

        choice = st.selectbox("Select an option", steps)
        if choice == '':
            st.warning("Click on actual steps above 😃️")
        if choice == '🫙 Mongo':
            st.image("Image URL")
            st.write("We're gonna store all the extracted channel, video and comment data in MongoDB database")
            if st.button("Click me 😉"):
                with st.spinner("Process initializing ⌛️"):

                    client = MongoClient('localhost', 27017)
                    # create a new database called db
                    db = client['db_new']

                    # Creating collections in the database
                    channel_collection = db['channels']
                    video_collection = db['videos']
                    comment_collection = db['comments']

                    chan_info = get_channel_info(user_input)
                    vid_id = search_videos(user_input)
                    vid_info = get_video_info(vid_id)
                    com_info = get_comments(vid_id)

                    channel_collection.insert_one(chan_info)
                    if len(vid_info) > 0:
                        video_collection.insert_many(vid_info)
                    if len(com_info) > 0:
                        comment_collection.insert_many(com_info)

                    st.balloons()
                    st.success("Successfully retrieved data to MongoDB database 😁", icon="✔️")
                    st.info("Now goto ➡️ SQL step above")

        if choice == '➡️ SQL':
            st.image("Image URL")
            st.write("We're gonna migrate all the collections created from MongoDB data lake to an SQL database as tables ")
            if st.button("Click me to do that 😋"):
                with st.spinner("We're almost there ⏳"):

                    connection = mysql.connector.connect(

                        host="***",
                        user="***",
                        password="***",
                        database='youtube'
                    )
                    client = MongoClient('localhost', 27017)
                    db = client['db_new']

                    channel_collection = db['channels']
                    video_collection = db['videos']
                    comment_collection = db['comments']

                    cha_docs = list(channel_collection.find())
                    vid_docs = list(video_collection.find())
                    comm_docs = list(comment_collection.find())

                    channel_df = pd.DataFrame(cha_docs)
                    video_df = pd.DataFrame(vid_docs)
                    comment_df = pd.DataFrame(comm_docs)

                    channel_df = channel_df.drop('_id', axis=1)
                    video_df = video_df.drop('_id', axis=1)
                    comment_df = comment_df.drop('_id', axis=1)

                    # Let's create tables for the above created dataframes
                    cursor = connection.cursor()

                    # creating channel table
                    cursor.execute(''' CREATE TABLE IF NOT EXISTS channels (
                                           channel_id VARCHAR(255) PRIMARY KEY,
                                           channel_name VARCHAR(255),
                                           subscription_count INT,
                                           channel_views INT,
                                           channel_description TEXT,
                                           channel_status VARCHAR(255)
                                           )
                                    ''')

                    # creating playlist table
                    cursor.execute('''CREATE TABLE IF NOT EXISTS playlists (
                                            playlist_id VARCHAR(255) PRIMARY KEY,
                                            channel_id VARCHAR(255),
                                            playlist_name VARCHAR(255),
                                            FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
                                            )
                        ''')

                    # Creating video table
                    cursor.execute('''CREATE TABLE IF NOT EXISTS videos(
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
                                            duration_secs INT,
                                            thumbnail VARCHAR(255),
                                            caption_status VARCHAR(255),
                                            FOREIGN KEY (playlist_id) REFERENCES playlists(playlist_id)
                                            )
                        ''')

                    # Creating the comment table
                    cursor.execute(''' CREATE TABLE IF NOT EXISTS comments (
                                            comment_id VARCHAR(255) PRIMARY KEY,
                                            video_id VARCHAR(255),
                                            comment_text TEXT,
                                            comment_author VARCHAR(255),
                                            comment_published_date DATETIME,
                                            FOREIGN KEY (video_id) REFERENCES videos(video_id)
                                            )
                        ''')

                    # Insert data into the channels table
                    channel_info = get_channel_info(user_input)
                    if channel_info:
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

                    # Insert data into the playlists table

                    playlist_info = get_playlists(user_input)
                    if playlist_info:
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

                    vid_id = search_videos(user_input)
                    video_info = get_video_info(vid_id)
                    for vid in video_info:

                        if video_info:
                            video_query = '''
                                INSERT INTO videos (
                                    video_id,
                                    playlist_id,
                                    video_name,
                                    video_description,
                                    published_date,
                                    view_count,
                                    like_count,
                                    dislike_count,
                                    favorite_count,
                                    comment_count,
                                    duration_secs,
                                    thumbnail,
                                    caption_status
                                    )
                                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                '''
                            video_data = (
                                vid['Video_Id'],
                                playlist_info['Playlist_Id'],
                                vid['Video_Name'],
                                vid['Video_Description'],
                                vid['PublishedAt'],
                                vid['View_Count'],
                                vid['Like_Count'],
                                vid['Dislike_Count'],
                                vid['Favorite_Count'],
                                vid['Comment_Count'],
                                vid['Duration'],
                                vid['Thumbnail'],
                                vid['Caption_Status']
                            )
                            cursor.execute(video_query, video_data)

                    comment_info = get_comments(vid_id)

                    for com in comment_info:

                        if comment_info:
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
                                com['Comment_Id'],
                                com['Video_Id'],
                                com['Comment_Text'],
                                com['Comment_Author'],
                                com['Comment_PublishedAt']
                            )
                            cursor.execute(comment_query, comment_data)

                    st.snow()
                    st.success("cool🥶...I'm done....You can move on to ❓🤔Queries above and check the questions 😍")
                    connection.commit()
                    cursor.close()
                    connection.close()


        if choice == '❓🤔Queries':
            connection = mysql.connector.connect(
                host="localhost",
                user="root",
                password="Balaji123",
                database='youtube'
            )
            cursor = connection.cursor()

            qsns = ["Choose any question below",
                   "1. What are the names of all the videos and their corresponding channels?",
                   "2. Which channels have the most number of videos, and how many videos do they have?",
                   "3. What are the top 10 most viewed videos and their respective channels?",
                   "4. How many comments were made on each video, and what are their corresponding video names?",
                   "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                   "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                   "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                   "8. What are the names of all the channels that have published videos in the year 2022?",
                   "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                   "10.Which videos have the highest number of comments, and what are their corresponding channel names?" ]

            qsn = st.selectbox("Select a question", qsns)
            if qsn == "1. What are the names of all the videos and their corresponding channels?":
                query1 = '''
                SELECT videos.video_name, channels.channel_name 
                FROM videos
                JOIN playlists ON videos.playlist_id = playlists.playlist_id
                JOIN channels ON playlists.channel_id = channels.channel_id
                '''

                cursor.execute(query1)
                results = cursor.fetchall()

                df = pd.DataFrame(results, columns = ['Video Names', 'Channel Names'])
                st.table(df)

            elif qsn == "2. Which channels have the most number of videos, and how many videos do they have?":
                query2 = '''
                SELECT channel_name, COUNT(videos.video_id) AS video_count
                FROM channels
                JOIN playlists ON channels.channel_id = playlists.channel_id
                JOIN videos ON playlists.playlist_id = videos.playlist_id
                GROUP BY channel_name
                '''
                cursor.execute(query2)
                results = cursor.fetchall()

                df = pd.DataFrame(results, columns= ['Channel name', 'Video Count'])
                st.table(df)

            elif qsn == "3. What are the top 10 most viewed videos and their respective channels?":
                query3 = '''
                SELECT videos.video_name, videos.view_count AS c FROM videos
                ORDER BY c DESC
                LIMIT 10 '''

                cursor.execute(query3)
                results = cursor.fetchall()

                df = pd.DataFrame(results, columns=['Video Name', 'View Count'])
                st.table(df)

            elif qsn == "4. How many comments were made on each video, and what are their corresponding video names?":
                query4 = '''
                SELECT videos.video_name, videos.comment_count AS c FROM videos
                ORDER BY c DESC
                '''
                cursor.execute(query4)
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=['Video Name', 'Num of Comments'])
                st.table(df)

            elif qsn == "5. Which videos have the highest number of likes, and what are their corresponding channel names?":
                query5 = '''
                SELECT channels.channel_name, videos.video_name, videos.like_count
                FROM channels                                                
                JOIN playlists ON channels.channel_id = playlists.channel_id
                JOIN videos ON playlists.playlist_id = videos.playlist_id 
                WHERE (channel_name, like_count) IN (
                SELECT channel_name, MAX(like_count)
                FROM channels
                JOIN playlists ON channels.channel_id = playlists.channel_id
                JOIN videos ON playlists.playlist_id = videos.playlist_id
                GROUP BY channel_name
                )
                ORDER BY like_count DESC                                 
                '''
                cursor.execute(query5)
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=['Channel Name', 'Video Name', 'Highest Likes'])
                st.table(df)

            elif qsn == "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?" :
                query6 = '''
                SELECT video_name, like_count, dislike_count 
                FROM videos
                ORDER BY like_count DESC
                '''
                cursor.execute(query6)
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=['Video Name', 'Likes', 'Dislikes'])
                st.table(df)

            elif qsn == "7. What is the total number of views for each channel, and what are their corresponding channel names?":
                query7 ='''
                SELECT channel_name, channel_views FROM channels
                '''
                cursor.execute(query7)
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=['Channel Name', 'Total Views'])
                st.table(df)

            elif qsn == "8. What are the names of all the channels that have published videos in the year 2022?":
                query8 = '''
                SELECT channel_name, video_name 
                FROM channels 
                JOIN playlists ON channels.channel_id = playlists.channel_id
                JOIN videos ON playlists.playlist_id = videos.playlist_id 
                WHERE EXTRACT(YEAR FROM videos.published_date) = 2022
                '''
                cursor.execute(query8)
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=['Channel Name', 'Video Name'])
                st.table(df)

            elif qsn == "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?" :
                query9 = '''
                SELECT channel_name, avg(videos.duration_secs) 
                FROM channels
                JOIN playlists ON channels.channel_id = playlists.channel_id
                JOIN videos ON playlists.playlist_id = videos.playlist_id
                GROUP BY channel_name
                '''
                cursor.execute(query9)
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=['Channel Name', 'Average Duration'])
                st.table(df)

            elif qsn == "10.Which videos have the highest number of comments, and what are their corresponding channel names?":
                query10 = '''
                SELECT channel_name, video_name, comment_count 
                FROM channels
                JOIN playlists ON channels.channel_id = playlists.channel_id
                JOIN videos ON playlists.playlist_id = videos.playlist_id
                WHERE (channel_name, comment_count) IN (
                SELECT channel_name, MAX(comment_count)
                FROM channels
                JOIN playlists ON channels.channel_id = playlists.channel_id
                JOIN videos ON playlists.playlist_id = videos.playlist_id
                GROUP BY channel_name
                )                                                     
                '''
                cursor.execute(query10)
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=['Channel Name', 'Video Name', 'Comments'])
                st.table(df)

            connection.close()
            cursor.close()

import streamlit as st
from pprint import pprint
from googleapiclient.discovery import build
from pymongo import MongoClient
import pandas as pd
from sqlalchemy import create_engine


# Specify API key
key = 'AIzaSyBSviFQcUwRc83i6H6-Z0VRe5IUe5dUyBA'

def main() :
    st.title("Welcome to YouTube Data harvesting")
    channel_id = st.text_input("Enter channel ID here")

    if st.button("Retrieve and store data in Mongo") :
        if channel_id :

            # This will retrieve data from YouTube API
            youtube = build('youtube', 'v3', developerKey = key)

            # Storing all the info in respective lists
            video_docs=[]
            channel_docs=[]
            comment_docs=[]

            #Make a Request to YouTube API
            request = youtube.channels().list(
                part = 'snippet, statistics',
                id = channel_id
                )
            response = request.execute()
            #pprint(response)

            ## Grabbing channel information
            channel_info = {
                    "Channel_Name": response['items'][0]['snippet']['title'],
                    "Channel_Id": response['items'][0]['id'],
                    "Subscription_Count": int(response['items'][0]['statistics']['subscriberCount']),
                    "Channel_Views": int(response['items'][0]['statistics']['viewCount']),
                    "Channel_Description": response['items'][0]['snippet']['description'],
                    "Playlist_Id": ""
                    }
            ## Grabbing playlist from a channel
            request1 = youtube.playlists().list(
                part='snippet',
                channelId=channel_id,
                maxResults=1
                )
            response1 = request1.execute()

            if 'items' in response1 and len(response1['items']) > 0 :
                channel_info['Playlist_Id'] = response1['items'][0]['id']
            else :
                channel_info['Playlist_Id'] = "No Playlist Found"

            channel_docs.append(channel_info)

            # retrieve videos using pagination
            next_page_token = None

            search = youtube.search().list(
                part = 'id, snippet',
                channelId = 'UCLcZCSrwPqxT2y-EPSTDM1g',
                maxResults=20,
                pageToken=next_page_token,
                type='video'
                )
            search_response = search.execute()

            for i in search_response['items'] :
                video_id = i['id']['videoId']


            request2 = youtube.videos().list(
                    part = 'contentDetails, statistics, snippet',
                    id= video_id
                )
            response2 = request2.execute()

            video_info =  {
                    "Video_Id": video_id,
                    "Video_Name": response2['items'][0]['snippet']['title'],
                    "Video_Description": response2['items'][0]['snippet']['localized']['description'],
                    "Tags": response2['items'][0]['snippet']['tags'],
                    "PublishedAt": response2['items'][0]['snippet']['publishedAt'],
                    "View_Count": response2['items'][0]['statistics']['viewCount'],
                    "Like_Count": response2['items'][0]['statistics']['likeCount'],
                    "Dislike_Count": 0,
                    "Favorite_Count": int(response2['items'][0]['statistics']['favoriteCount']),
                    "Comment_Count": int(response2['items'][0]['statistics']['commentCount']),
                    "Duration": response2['items'][0]['contentDetails']['duration'],
                    "Thumbnail": response2['items'][0]['snippet']['thumbnails']['default']['url'],
                    "Caption_Status": response2['items'][0]['contentDetails']['caption']
                    }

            video_docs.append(video_info)

            response3 = youtube.commentThreads().list(
                    part='snippet',
                    videoId=video_id,
                    maxResults = 1
                ).execute()

            #pprint(response3)

            ## Grabbing comments from a video
            comments = []
            for i in response3['items'] :
                    comment = i['snippet']['topLevelComment']['snippet']['textDisplay']
                    comments.append(comment)

            comment_info =  {
                            "Comment_Id": response3['items'][0]['id'],
                            "Comment_Text": response3['items'][0]['snippet']['topLevelComment']['snippet']['textDisplay'],
                            "Comment_Author": response3['items'][0]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            "Comment_PublishedAt": response3['items'][0]['snippet']['topLevelComment']['snippet']['publishedAt']
                            }

            comment_docs.append(comment_info)

            # Setting up mongodb connection
            # Creating a connection between pycharm and mongodb

            client = MongoClient('localhost', 27017)
            # checking if connection is successfull
            client.test
            # create a new database called db
            db = client['testfile']
            # create collections in database db
            channel_col = db['channel']
            video_col = db['video']
            comment_col = db['comment']

            # Store the above data in MongoDB
            if len(channel_docs) > 0 :
                channel_col.insert_many(channel_docs)
            if len(video_docs) > 0 :
                channel_col.insert_many(video_docs)
            if len(comment_docs) > 0 :
                channel_col.insert_many(comment_docs)

            ## Display a prompt showing --- successfully retrieved data to Mongo
            st.success("Data Successfully retrieved to Mongo 😁")

            import mysql.connector
            # establishing a connection between mysql and pycharm

            bridge = mysql.connector.connect(host='localhost',
                                             user='root',
                                             password='Balaji@123',
                                             database='db',
                                             autocommit=True)

            # Create a database engine
            engine = create_engine(bridge)

            # Now MySQL data Conversion and Queries section
            st.subheader("MySQL Data conversion and Queries")

            if st.button("Tap me to convert the data My SQL") :
                cdocs = list(channel_col.find())
                vdocs = list(video_col.find())
                codocs = list(comment_col.find())

                # Let's convert the above docs to a dataframe
                cdf = pd.DataFrame(cdocs)
                vdf = pd.DataFrame(vdocs)
                codf = pd.DataFrame(codocs)

                # Remove '_id' column from dataframes
                myl = [cdf, vdf,codf]
                for df in myl :
                    if '_id' in df.columns :
                        df = df.drop('_id', axis=1)

                # Now insert the dataframes into mysql tables
                cdf.to_sql('channels', con= engine, if_exists='append', index=False)
                vdf.to_sql('videos', con= engine, if_exists='append', index=False)
                codf.to_sql('comments', con= engine, if_exists='append', index=False)

                st.success("data conversion to MySQL achieved successfully")

                # Query 1 :
                query = '''SELECT video_name, channel_name FROM videos'''
                results = pd.read_sql_query(query, engine)
                st.subheader("Query 1 :")
                st.table(results)

                # Query 2 :
                query = '''SELECT channel_name, COUNT(video_id) AS video_count
                FROM videos GROUP BY channel_name
                ORDER BY video_count DESC'''
                results = pd.read_sql_query(query, engine)
                st.subheader("Query 2 :")
                st.table(results)

                # Query 3 :
                ''' SELECT video_name, channel_name FROM VIDEOSORDER BY view_count DESC LIMIT 10 '''
                results = pd.read_sql_query(query, engine)
                st.subheader("Query 3 :")
                st.table(results)







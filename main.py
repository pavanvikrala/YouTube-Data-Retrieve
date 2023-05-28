# import Google api client to our code and
# build method setups our connection to API

from googleapiclient.discovery import build
from pymongo import MongoClient
import streamlit as st

# Specify API key
key = 'AIzaSyBSviFQcUwRc83i6H6-Z0VRe5IUe5dUyBA'

def main() :
    st.title("YouTube Data Harvesting")
    st.subheader("Let's retrieve instant data from YouTube")
    channel_username = st.text_input("Enter the channel username")

    if st.button("Retrieve and Store Data") :
        if channel_username :
            # Retrieve data from Youtube API
            youtube = build('youtube', 'v3', developerKey = key)

            channel_list = []
            # Make a request to YouTube APi
            request = youtube.channels().list(
            part='snippet, statistics',
            forUsername= channel_username
            )

            response = request.execute()
            if 'items' in response and len(response['items']) > 0:
                channel_info = {
                    "channel_name" : response['items'][0]['snippet']['title'],
                    "channel_id" : response['items'][0]['id'],
                    'subscription_count' : response['items'][0]['statistics']['subscriberCount'],
                    'channel_views' : response['items'][0]['statistics']['viewCount'],
                    'channel_description' : response['items'][0]['snippet']['description'],
                    'playlist_id' : ''
                }



main()




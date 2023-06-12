# YouTube-Data-Retrieval

This project is about how to fetch information from YouTube using API key and also, 
store the retrieved data in a MongoDB data lake creating corresponding tables in a MySQL database. 

The code starts by importing necessary libraries such as googleapiclient, pymongo, mysql.connector, pandas, and streamlit.

Four functions are defined to extract channel, playlist, video and comment information for a user preferred youtube channel.

The main part of the code is a Streamlit application that interacts with the user. 
It asks for the user's name and provides some information about the functionality of the program.

Once the user enters a channel ID, the program allows the user to select different steps: 'Mongo', 'SQL', and 'Queries'. 
Depending on the selected step, the program performs different actions.

In the 'Mongo' step, the program initializes a MongoDB connection, creates collections for channels, videos, and comments, fetches channel, video, and comment data, and inserts the data into the respective collections.

In the 'SQL' step, the program initializes a MySQL connection, creates tables for channels, playlists, videos, and comments, fetches data from MongoDB collections, and inserts the data into the respective tables.

And at last, the user can access to 10 different questions by selecting 'Queries' in the dropdown. These questions are designed in such a way that all the tables are inter-related and required data is extracted using SQL queries.

That's it😍 ....I feel this as one of the challenging projects for any data science aspirant. 

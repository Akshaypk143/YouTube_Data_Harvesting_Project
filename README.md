# YouTube_Data_Harvesting_and_Warehousing_Project
YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit

# Problem Statement:
The problem statement is to create a Streamlit application that allows users to access
and analyze data from multiple YouTube channels. The application should have the
following features:
1. Ability to input a YouTube channel ID and retrieve all the relevant data
(Channel name, subscribers, total video count, playlist ID, video ID, likes,
dislikes, comments of each video) using Google API.
2. Option to store the data in a MongoDB database as a data lake.
3. Ability to collect data for up to 10 different YouTube channels and store them in
the data lake by clicking a button.
4. Option to select a channel name and migrate its data from the data lake to a
SQL database as tables.
5. Ability to search and retrieve data from the SQL database using different
search options, including joining tables to get channel details.


Key Technologies and Skills

Python scripting
Data Collection
API integration
Streamlit
Plotly
Data Management using MongoDB (Atlas) and SQL
Installation

To run this project, you need to install the following packages:

pip install google-api-python-client
pip install pymongo
pip install pandas
pip install psycopg2
pip install streamlit
pip install plotly
Usage

To use this project, follow these steps:

Clone the repository: git clone https://github.com/gopiashokan/Youtube-Harvesting-and-Warehousing.git
Install the required packages: pip install -r requirements.txt
Run the Streamlit app: streamlit run app.py
Access the app in your browser at http://localhost:8501
Features

Retrieve data from the YouTube API, including channel information, playlists, videos, and comments.
Store the retrieved data in a MongoDB database.
Migrate the data to a SQL data warehouse.
Analyze and visualize data using Streamlit and Plotly.
Perform queries on the SQL data warehouse.
Gain insights into channel performance, video metrics, and more.
Retrieving data from the YouTube API

The project utilizes the Google API to retrieve comprehensive data from YouTube channels. The data includes information on channels, playlists, videos, and comments. By interacting with the Google API, we collect the data and merge it into a JSON file.

Storing data in MongoDB

The retrieved data is stored in a MongoDB database based on user authorization. If the data already exists in the database, it can be overwritten with user consent. This storage process ensures efficient data management and preservation, allowing for seamless handling of the collected data.

Migrating data to a SQL data warehouse

The application allows users to migrate data from MongoDB to a SQL data warehouse. Users can choose which channel's data to migrate. To ensure compatibility with a structured format, the data is cleansed using the powerful pandas library. Following data cleaning, the information is segregated into separate tables, including channels, playlists, videos, and comments, utilizing SQL queries.

# Results:
This project aims to develop a user-friendly Streamlit application that utilizes the
Google API to extract information on a YouTube channel, stores it in a MongoDB
database, migrates it to a SQL data warehouse, and enables users to search for
channel details and join tables to view data in the Streamlit app.

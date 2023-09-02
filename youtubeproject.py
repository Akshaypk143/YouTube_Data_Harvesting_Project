# Youtube API libraries
from googleapiclient.discovery import build

# Dashboard libraries
import streamlit as st
import plotly.express as px

# MongoDB library
import pymongo

# SQL libraries
#import mysql.connector as sql
import pymysql

# pandas package
import pandas as pd

# BUILDING CONNECTION WITH YOUTUBE API
api_key = "AIzaSyAP3FvaEN2bu_h3zrOH7S-v5hOXfRkWvMI" #"AIzaSyAP3FvaEN2bu_h3zrOH7S-v5hOXfRkWvMI"
youtube = build('youtube','v3',developerKey=api_key)

# Streamlit app
st.set_page_config(layout='wide')
st.title(':orange[Data Science Capstone Project - ]:black[YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit]')

# MySQL connection setup
mydb = pymysql.connect(
    host='localhost',
    user='root',
    password='Apklpk@123',
    database='youtube_database')
mycursor = mydb.cursor()

# Bridging a connection with MongoDB Atlas and Creating a new database(Youtube)
client = pymongo.MongoClient("localhost:27017")
db = client.Youtube


# FUNCTION TO GET CHANNEL DETAILS
def get_channel_details(channel_id):
    ch_data = []
    response = youtube.channels().list(part='snippet,contentDetails,statistics',
                                       id=channel_id).execute()

    for i in range(len(response['items'])):
        data = dict(Channel_id=channel_id[i],
                    Channel_name=response['items'][i]['snippet']['title'],
                    Playlist_id=response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    Subscribers=response['items'][i]['statistics']['subscriberCount'],
                    Views=response['items'][i]['statistics']['viewCount'],
                    Total_videos=response['items'][i]['statistics']['videoCount'],
                    Description=response['items'][i]['snippet']['description'],
                    Country=response['items'][i]['snippet'].get('country')
                    )
        ch_data.append(data)
    return ch_data


# FUNCTION TO GET VIDEO IDS
def get_channel_videos(channel_id):
    video_ids = []
    # get Uploads playlist id
    res = youtube.channels().list(id=channel_id,
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        res = youtube.playlistItems().list(playlistId=playlist_id,
                                           part='snippet',
                                           maxResults=50,
                                           pageToken=next_page_token).execute()

        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


# FUNCTION TO GET VIDEO DETAILS
def get_video_details(v_ids):
    video_stats = []

    for i in range(0, len(v_ids), 50):
        response = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(v_ids[i:i + 50])).execute()
        for video in response['items']:
            video_details = dict(Channel_name=video['snippet']['channelTitle'],
                                 Channel_id=video['snippet']['channelId'],
                                 Video_id=video['id'],
                                 Title=video['snippet']['title'],
                                 Tags=video['snippet'].get('tags'),
                                 Thumbnail=video['snippet']['thumbnails']['default']['url'],
                                 Description=video['snippet']['description'],
                                 Published_date=video['snippet']['publishedAt'],
                                 Duration=video['contentDetails']['duration'],
                                 Views=video['statistics']['viewCount'],
                                 Likes=video['statistics'].get('likeCount'),
                                 Comments=video['statistics'].get('commentCount'),
                                 Favorite_count=video['statistics']['favoriteCount'],
                                 Definition=video['contentDetails']['definition'],
                                 Caption_status=video['contentDetails']['caption']
                                 )
            video_stats.append(video_details)
    return video_stats

# FUNCTION TO GET COMMENT DETAILS
def get_comments_details(v_id):
    comment_data = []
    try:
        next_page_token = None
        while True:
            response = youtube.commentThreads().list(part="snippet,replies",
                                                     videoId=v_id,
                                                     maxResults=100,
                                                     pageToken=next_page_token).execute()
            for cmt in response['items']:
                data = dict(Comment_id=cmt['id'],
                            Video_id=cmt['snippet']['videoId'],
                            Comment_text=cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author=cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date=cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Like_count=cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            Reply_count=cmt['snippet']['totalReplyCount']
                            )
                comment_data.append(data)
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
    except:
        pass
    return comment_data


# FUNCTION TO GET CHANNEL NAMES FROM MONGODB
def channel_names():
    ch_name = []
    for i in db.channel_details.find():
        ch_name.append(i['Channel_name'])
    return ch_name

tab1,tab2 = st.tabs(["$\huge HARVESTING $", "$\huge WAREHOUSING $"])

with tab1:
    st.markdown("#    ")
    st.write(":green[Enter YouTube Channel_ID below:]")
    ch_id = st.text_input("Hint : Goto youtube channel's home page > Right click > View page source > Find channel_id").split(
        ',')

    if ch_id and st.button("Extract Data"):
        ch_details = get_channel_details(ch_id)
        st.write(f'#### Extracted data from :green["{ch_details[0]["Channel_name"]}"] channel')
        st.table(ch_details)

    if st.button("Store into MongoDB"):
        with st.spinner('Please Wait for it...'):
            ch_details = get_channel_details(ch_id)
            v_ids = get_channel_videos(ch_id)
            vid_details = get_video_details(v_ids)

            def comments():
                com_d = []
                for i in v_ids:
                    com_d += get_comments_details(i)
                return com_d


            comm_details = comments()

            collections1 = db.channel_details
            collections1.insert_many(ch_details)

            collections2 = db.video_details
            collections2.insert_many(vid_details)

            collections3 = db.comments_details
            collections3.insert_many(comm_details)
            st.success("Store into MongoDB successful !!")


with tab2:
    st.markdown("#   ")
    st.markdown("### Select a channel to begin warehousing to SQL")
    ch_names = channel_names()
    user_inp = st.selectbox("Select channel", options=ch_names)


def insert_into_channels():
    collections = db.channel_details
    query = """INSERT INTO channels VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""

    for i in collections.find({"channel_name": user_inp}, {'_id': 0}):
        mycursor.execute(query, tuple(i.values()))
    mydb.commit()


def insert_into_videos():
    collections1 = db.video_details
    query1 = """INSERT INTO videos VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

    for i in collections1.find({"channel_name": user_inp}, {'_id': 0}):
        values = [str(val).replace("'", "''").replace('"', '""') if isinstance(val, str) else val for val in i.values()]
        mycursor.execute(query1, tuple(values))
        mydb.commit()


def insert_into_comments():
    collections1 = db.video_details
    collections2 = db.comments_details
    query2 = """INSERT INTO comments VALUES(%s,%s,%s,%s,%s,%s,%s)"""

    for vid in collections1.find({"channel_name": user_inp}, {'_id': 0}):
        for i in collections2.find({'Video_id': vid['Video_id']}, {'_id': 0}):
            mycursor.execute(query2, tuple(i.values()))
            mydb.commit()


if st.button("Submit"):
    try:
        insert_into_videos()
        insert_into_channels()
        insert_into_comments()
        st.success("Warehousing to MySQL Successful !!")
    except:
        st.error("Channel details already transformed !!")

        st.write("## :orange[Select any below query to get Insights]")
        questions = st.selectbox('Questions',
                                 ['1. What are the names of all the videos and their corresponding channels?',
                                  '2. Which channels have the most number of videos, and how many videos do they have?',
                                  '3. What are the top 10 most viewed videos and their respective channels?',
                                  '4. How many comments were made on each video, and what are their corresponding video names?',
                                  '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                                  '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                                  '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                                  '8. What are the names of all the channels that have published videos in the year 2022?',
                                  '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                  '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])

        if questions == '1. What are the names of all the videos and their corresponding channels?':
            mycursor.execute("""SELECT Video_name AS Video_name, channel_name AS Channel_Name
                                    FROM videos
                                    ORDER BY channel_name""")
            df = pd.DataFrame(mycursor.fetchall(), columns = mycursor.column_names)
            st.write(df)

        elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
            mycursor.execute("""SELECT channel_name AS Channel_Name, total_videos AS Total_Videos
                                    FROM channels
                                    ORDER BY total_videos DESC""")
            df = pd.DataFrame(mycursor.fetchall(), columns =mycursor.column_names)
            st.write(df)
            st.write("### :green[Number of videos in each channel :]")
            # st.bar_chart(df,x= mycursor.column_names[0],y= mycursor.column_names[1])
            fig = px.bar(df,
                         x=mycursor.column_names[0],
                         y=mycursor.column_names[1],
                         orientation='v',
                         color=mycursor.column_names[0]
                         )
            st.plotly_chart(fig, use_container_width=True)

        elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
            mycursor.execute("""SELECT channel_name AS Channel_Name, Video_name AS Video_Title, view_count AS Views 
                                    FROM videos
                                    ORDER BY views DESC
                                    LIMIT 10""")
            df = pd.DataFrame(mycursor.fetchall(), columns =mycursor.column_names)
            st.write(df)
            st.write("### :green[Top 10 most viewed videos :]")
            fig = px.bar(df,
                         x=mycursor.column_names[2],
                         y=mycursor.column_names[1],
                         orientation='h',
                         color=mycursor.column_names[0]
                         )
            st.plotly_chart(fig, use_container_width=True)

        elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
            mycursor.execute("""SELECT a.video_id AS Video_id, Video_name AS Video_Title, b.Total_Comments
                                    FROM videos AS a
                                    LEFT JOIN (SELECT video_id,COUNT(comment_id) AS Total_Comments
                                    FROM comments GROUP BY video_id) AS b
                                    ON a.video_id = b.video_id
                                    ORDER BY b.Total_Comments DESC""")
            df = pd.DataFrame(mycursor.fetchall(), columns =mycursor.column_names)
            st.write(df)

        elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
            mycursor.execute("""SELECT channel_name AS Channel_Name,Video_name AS Title,Like_count AS Like_count 
                                    FROM videos
                                    ORDER BY Like_count DESC
                                    LIMIT 10""")
            df = pd.DataFrame(mycursor.fetchall(), columns =mycursor.column_names)
            st.write(df)
            st.write("### :green[Top 10 most liked videos :]")
            fig = px.bar(df,
                         x=mycursor.column_names[2],
                         y=mycursor.column_names[1],
                         orientation='h',
                         color=mycursor.column_names[0]
                         )
            st.plotly_chart(fig, use_container_width=True)

        elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
            mycursor.execute("""SELECT Video_name AS Title, Like_count AS Like_count
                                    FROM videos
                                    ORDER BY Like_count DESC""")
            df = pd.DataFrame(mycursor.fetchall(), columns =mycursor.column_names)
            st.write(df)

        elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
            mycursor.execute("""SELECT channel_name AS Channel_Name, channel_views AS Views
                                    FROM channels
                                    ORDER BY views DESC""")
            df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
            st.write(df)
            st.write("### :green[Channels vs Views :]")
            fig = px.bar(df,
                         x=mycursor.column_names[0],
                         y=mycursor.column_names[1],
                         orientation='v',
                         color=mycursor.column_names[0]
                         )
            st.plotly_chart(fig, use_container_width=True)

        elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
            mycursor.execute("""SELECT channel_name AS Channel_Name
                                    FROM videos
                                    WHERE published_date LIKE '2022%'
                                    GROUP BY channel_name
                                    ORDER BY channel_name""")
            df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
            st.write(df)

        elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
            mycursor.execute("""SELECT channel_name AS Channel_Name,
                                    AVG(duration)/60 AS "Average_Video_Duration (mins)"
                                    FROM videos
                                    GROUP BY channel_name
                                    ORDER BY AVG(duration)/60 DESC""")
            df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
            st.write(df)
            st.write("### :green[Avg video duration for channels :]")
            fig = px.bar(df,
                         x=mycursor.column_names[0],
                         y=mycursor.column_names[1],
                         orientation='v',
                         color=mycursor.column_names[0]
                         )
            st.plotly_chart(fig, use_container_width=True)

        elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
            mycursor.execute("""SELECT channel_name AS Channel_Name,Video_id AS Video_ID,Comment_count AS Comments
                                    FROM videos
                                    ORDER BY comments DESC
                                    LIMIT 10""")
            df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
            st.write(df)
            st.write("### :green[Videos with most comments :]")
            fig = px.bar(df,
                         x=mycursor.column_names[1],
                         y=mycursor.column_names[2],
                         orientation='v',
                         color=mycursor.column_names[0]
                         )
            st.plotly_chart(fig, use_container_width=True)
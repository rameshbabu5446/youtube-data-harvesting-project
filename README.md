import pymongo
import psycopg2
import pandas as pd
import streamlit as st
from googleapiclient.discovery import build
from psycopg2 import OperationalError


#API KEY
def Api_connect():
     Api_id="AIzaSyBC0dt5OszXqgyDmIo9lgskxl2VvJmd6-s"
     api_service_name="Youtube"
     api_version="v3"

     Youtube=build(api_service_name,api_version,developerKey=Api_id)
     return Youtube

Youtube=Api_connect()

#get channel information
def get_channel_info(channel_id):
        request=Youtube.channels().list(
                        part="snippet,contentDetails,Statistics",
                        id=channel_id             
                 )
        response=request.execute()

        for i in response['items']:
                data=dict(Channel_Name=i["snippet"]["title"],
                        Channel_Id=i["id"],
                        Subscription_Count=i["statistics"]["subscriberCount"],
                        Views=i["statistics"]["viewCount"],
                        Total_Videos=i["statistics"]["videoCount"],
                        Channel_Description=i["snippet"]["description"],
                        Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
        return data


#get video ids
def get_video_ids(channel_id):
    video_Ids=[]
        
    responce=Youtube.channels().list(id=channel_id, 
                                    part='contentDetails').execute()
    playlist_Id=responce['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token=None
        
    while True:
        response1=Youtube.playlistItems().list( 
                                            part='snippet',
                                            playlistId=playlist_Id, 
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
            
        for i in range(len(response1['items'])):
            video_Ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')
            
        if next_page_token is None:
            break
    return video_Ids

#get video information
def get_video_info(video_ids):

    video_data=[]
    for video_id in video_ids:
        request=Youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=video_id)
        response=request.execute()

        for item in response["items"]:
            data=dict(Channel_Name =item['snippet']['channelTitle'],
                        Channel_Id=item['snippet']['channelId'],
                        Video_Id=item['id'],
                        Title=item['snippet']['title'],
                        Tags=item['snippet'].get('tags'),
                        Thumbnail=item['snippet']['thumbnails']['default']['url'],
                        Description=item['snippet']['description'],
                        Published_Date=item['snippet']['publishedAt'],
                        Duration=item['contentDetails']['duration'],
                        Views=item['statistics']['viewCount'],
                        Likes=item['statistics'].get('likeCount'),
                        Comments=item['statistics'].get('commentCount'),
                        Favorite_Count=item['statistics']['favoriteCount'],
                        Definition=item['contentDetails']['definition'],
                        Caption_Status=item['contentDetails']['caption']
            )
            video_data.append(data)
   
    return video_data

#get comment information
def get_comment_info(video_ids):
        comment_data=[]
        try:
                for video_id in video_ids:
                       request = Youtube.commentThreads().list(
                        part="snippet",
                        videoId="4cuQuKoKKNo",
                        maxResults=50
                        )
                response = request.execute()
                              
                for item in response["items"]:
                        data=dict(
                                Comment_Id=item["snippet"]["topLevelComment"]["id"],
                                Video_Id=item["snippet"]["videoId"],
                                Comment_Text =item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                                Comment_Author=item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                Comment_Published=item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])

                        comment_data.append(data)
        except:
                pass
            
        return comment_data

#get playlist ids
def get_playlist_details(channel_id):
    All_data=[]
    next_page_token=None
    next_page=True
    while next_page:

        request=Youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
            )
        response1=request.execute()

        for item in response1['items']:
            data={'PlaylistId':item['id'],
                    'Title':item['snippet']['title'],
                    'ChannelId':item['snippet']['channelId'],
                    'ChannelName':item['snippet']['channelTitle'],
                    'PublishedAt':item['snippet']['publishedAt'],
                    'VideoCount':item['contentDetails']['itemCount']}
            All_data.append(data)
        next_page_token=response1.get('nextPageToken')
        if next_page_token is None:
            next_page=False
    return All_data

#MongoDB Connection check
client=pymongo.MongoClient("mongodb+srv://ramesh:mahendran@cluster0.tkiolrf.mongodb.net/?retryWrites=true&w=majority")
db=client["project-1"]


# upload file  to MongoDB
def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    video_ids=get_video_ids(channel_id)
    video_details=get_video_info(video_ids)
    com_details=get_comment_info(video_ids)
    collection1=db["channel_details"]
    collection1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,"video_information":video_details,
                     "comment_information":com_details})
    return " completed successfully"

#Table creation for channels,playlists, videos, comments
#Table creation for channels
def channels_table():
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="mahendran",
        database="project-1",
        port="5432"
    )
    cursor = mydb.cursor()

    # Deleting channels details
    drop_query = 'DROP TABLE IF EXISTS channels'
    cursor.execute(drop_query)
    mydb.commit()

    # Creating channels table
    create_query = '''
        CREATE TABLE IF NOT EXISTS channels (
            Channel_Name varchar(100),
            Channel_Id varchar(100) PRIMARY KEY,
            Subscription_Count bigint,
            Views bigint,
            Total_Videos int,
            Channel_Description text,
            Playlist_Id varchar(80)
        )
    '''
    cursor.execute(create_query)
    mydb.commit()

    # Converting data into DataFrame
    ch_list = []
    
    db = client["project-1"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data["channel_information"])
    df = pd.DataFrame(ch_list)

    for index, row in df.iterrows():
        insert_query = '''
            INSERT INTO channels (
                Channel_Name,
                Channel_Id,
                Subscription_Count,
                Views,
                Total_Videos,
                Channel_Description,
                Playlist_Id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        '''

        values = (
            row['Channel_Name'],
            row['Channel_Id'],
            row['Subscription_Count'],
            row['Views'],
            row['Total_Videos'],
            row['Channel_Description'],
            row['Playlist_Id']
        )

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except psycopg2.IntegrityError:
            print("Duplicate entry found in channels table")
       
 
# Call the function to create the table and insert data


#Table creation for playlists,
#connecting the playlist
def playlists_table():
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="mahendran",
        database="project-1",
        port="5432"
    )
    cursor = mydb.cursor()

    drop_query = "DROP TABLE IF EXISTS playlists"
    cursor.execute(drop_query)
    mydb.commit()

    create_query = '''
        CREATE TABLE IF NOT EXISTS playlists (
            PlaylistId varchar(100) PRIMARY KEY,
            Title varchar(80),
            ChannelId varchar(100),
            ChannelName varchar(100),
            PublishedAt timestamp,
            VideoCount int
        )
    '''

    cursor.execute(create_query)
    mydb.commit()

    pl_list = []
    
    db = client["project-1"]
    coll1 = db["channel_details"]

    for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1 = pd.DataFrame(pl_list)

    for index, row in df1.iterrows():
        insert_query = '''
            INSERT INTO playlists (
                PlaylistId,
                Title,
                ChannelId,
                ChannelName,
                PublishedAt,
                VideoCount
            )
            VALUES (%s, %s, %s, %s, %s, %s)
        '''

        values = (
            row['PlaylistId'],
            row['Title'],
            row['ChannelId'],
            row['ChannelName'],
            row['PublishedAt'],
            row['VideoCount']
        )

        cursor.execute(insert_query, values)
        mydb.commit()

# Call the function to create the table and insert data
playlists_table()


# Function to create videos table and insert data
def videos_table():
    # Establishing a connection to PostgreSQL database
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="mahendran",
        database="project-1",
        port="5432"
    )
    cursor = mydb.cursor()

    # Dropping the table if it exists
    drop_query = "DROP TABLE IF EXISTS videos"
    cursor.execute(drop_query)
    mydb.commit()

    # Creating the videos table
    create_query = '''
        CREATE TABLE IF NOT EXISTS videos (
            Channel_Name varchar(150),
            Channel_Id varchar(100),
            Video_Id varchar(200) PRIMARY KEY,
            Title varchar(150),
            Tags text,
            Thumbnail varchar(225),
            Description text,
            Published_Date timestamp,
            Duration interval,
            Views bigint,
            Likes bigint,
            Comments int,
            Favorite_Count int,
            Definition varchar(10),
            Caption_Status varchar(50)
        )
    '''
    cursor.execute(create_query)
    mydb.commit()

    # Retrieving data from MongoDB collection
    video_list = []
    db = client["project-1"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(vi_data["video_information"])):
            video_list.append(vi_data["video_information"][i])
    df2 = pd.DataFrame(video_list)

        
    # Inserting data into the videos table
    for index, row in df2.iterrows():
        insert_query = '''
            INSERT INTO videos (
                Channel_Name,
                Channel_Id,
                Video_Id,
                Title,
                Tags,
                Thumbnail,
                Description,
                Published_Date,
                Duration,
                Views,
                Likes,
                Comments,
                Favorite_Count,
                Definition,
                Caption_Status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''

        values = (
            row['Channel_Name'],
            row['Channel_Id'],
            row['Video_Id'],
            row['Title'],
            row['Tags'],
            row['Thumbnail'],
            row['Description'],
            row['Published_Date'],
            row['Duration'],
            row['Views'],
            row['Likes'],
            row['Comments'],
            row['Favorite_Count'],
            row['Definition'],
            row['Caption_Status']
        )

        cursor.execute(insert_query, values)

    # Committing the changes and closing the connection
    mydb.commit()
   

# Call the function to create the table and insert data
videos_table()


def comments_table():
    try:
        mydb = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="mahendran",
            database="project-1",
            port="5432"
        )
        cursor = mydb.cursor()

        drop_query = "DROP TABLE IF EXISTS comments"
        cursor.execute(drop_query)
        mydb.commit()

        create_query = '''
            CREATE TABLE IF NOT EXISTS comments (
                Comment_Id varchar(100) PRIMARY KEY,
                Video_Id varchar(80),
                Comment_Text text,
                Comment_Author varchar(150),
                Comment_Published timestamp
            )
        '''
        cursor.execute(create_query)
        mydb.commit()

        com_list = []
        db = client["project-1"]
        coll1 = db["channel_details"]
        for com_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
            for i in range(len(com_data["comment_information"])):
                com_list.append(com_data["comment_information"][i])
        df3 = pd.DataFrame(com_list)

        for index, row in df3.iterrows():
            insert_query = '''
                INSERT INTO comments (
                    Comment_Id,
                    Video_Id,
                    Comment_Text,
                    Comment_Author,
                    Comment_Published
                )
                VALUES (%s, %s, %s, %s, %s)
            '''
            values = (
                row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_Published']
            )
            try:
                cursor.execute(insert_query, values)
                mydb.commit()
            except psycopg2.IntegrityError:
                print("Duplicate entry found in comments table")

    except psycopg2.Error as e:
        print(f"Error: {e}")
    finally:
        if mydb:
            cursor.close()
            mydb.close()

# Call the function to create the table and insert data
comments_table()



def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()


    return "TABLES CREATED "

def show_channels_table():
    ch_list = []
    db = client["project-1"]
    coll1 = db["channel_details"] 
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df = st.dataframe(ch_list)

    return df

def show_playlists_table():
    db = client["project-1"]
    coll1 =db["channel_details"]
    pl_list = []
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
                pl_list.append(pl_data["playlist_information"][i])
    df1 = st.dataframe(pl_list)
    return df1

def show_videos_table():
    vi_list = []
    db = client["project-1"]
    coll2 = db["channel_details"]
    for vi_data in coll2.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = st.dataframe(vi_list)
    return df2

def show_comments_table():
    com_list = []
    db = client["project-1"]
    coll3 = db["channel_details"]
    for com_data in coll3.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3 = st.dataframe(com_list)
    return df3

# Set the background image path
import streamlit as st

# Relative path to the image file
background_image_path = "C:/Users/one/Desktop/ram-project/pic/background_image.jpg"  # Adjust the path accordingly

# Use HTML and CSS to set the background image
background_style = f"""
    <style>
        body {{
            background-image: url("{background_image_path}");
            background-size: cover;
            font-family: 'Arial', sans-serif;
            color: white;
        }}
        .stApp {{
            color: white;
        }}
        .css-1v3fvcr {{
            color: black;
        }}
    </style>
"""

# Apply the background style
st.markdown(background_style, unsafe_allow_html=True)

# Streamlit app content
st.title("Youtube Data Harvesting 📺")
st.sidebar.title(":red[Youtube Data Harvesting 📺]")

with st.sidebar:
    st.header("SKILL TAKEAWAY")
    st.caption('Python Scripting')
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")

channel_id = st.text_input("Enter the Channel ID")

if st.button("Collect and Store Data"):
    ch_ideas = []
    db = client["project-1"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_ideas.append(ch_data["channel_information"]["Channel_Id"])
    
    if channel_id in ch_ideas:
        st.success("CHANNEL DATA ALREADY EXISTS")
    else:
        insert = channel_details(channel_id)
        st.success(insert)

if st.button("Migrate to SQL"):
    Tables = tables()
    st.success(Tables)

# Creating button for selection
show_table = st.radio("SELECT THE TABLE FOR VIEW", ("CHANNELS", "PLAYLISTS", "VIDEOS", "COMMENTS"))

if show_table == "CHANNELS":
    show_channels_table()

if show_table == "PLAYLISTS":
    show_playlists_table()

if show_table == "VIDEOS":
    show_videos_table()

if show_table == "COMMENTS":
    show_comments_table()


#SQL connection
import psycopg2
from psycopg2 import OperationalError

try:
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="mahendran",
        database="project-1",
        port="5432"
    )
    cursor = mydb.cursor()
    # Continue with database operations using the cursor...
except OperationalError as e:
    print(f"Database connection error: {e}")

question = st.selectbox(
    'Please Select Your Question',
   
    ('1.  All the videos and their corresponding channels',
     '2.  channels have the most number of videos, and how many videos do they have',
     '3. 10 most viewed videos',
     '4. Comments in each video',
     '5. Videos with highest likes',
     '6. likes of all videos',
     '7. views of each channel',
     '8. videos published in the year 2022',
     '9. average duration of all videos in each channel',
     '10. videos with highest number of comments'))

if question == '1. All the videos and their corresponding channels':
    query1 ='''select title as videos,channel_name as channelname from videos '''
    cursor.execute(query1)
    mydb.commit()
    table1 = cursor.fetchall()

    # Displaying the results using Pandas DataFrame and Streamlit
    df1 = pd.DataFrame(table1, columns=["Video Title", "Channel Name"])
    st.write[df1]
elif question == '2. channels have the most number of videos, and how many videos do they have':
    query2 = "select Channel_Name as ChannelName,Total_Videos as NO_Videos from channels order by Total_Videos desc;"
    cursor.execute(query2)
    mydb.commit()
    table2=cursor.fetchall()
    df2=(pd.DataFrame(table2, columns=["Channel Name","No Of Videos"]))
    st.write(df2)

# 3rd question 
mydb = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="mahendran",
    database="project-1",
    port="5432"
)
cursor = mydb.cursor()
# question == '3. 10 most viewed videos':
query3 = '''select Views as views , Channel_name as ChannelName,Title as VideoTitle from videos 
                     order by Views desc limit 10 '''
cursor.execute(query3)
mydb.commit()
table3 = cursor.fetchall()
df3=pd.DataFrame(table3, columns = ["views","channel Name","video title"])
df3

# 4th  question 
mydb = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="mahendran",
    database="project-1",
    port="5432"
)
cursor = mydb.cursor()

# question == '4. Comments in each video':
query4 = "select Comments as No_comments ,Title as VideoTitle from videos where Comments is not null;"
cursor.execute(query4)
mydb.commit()
table4=cursor.fetchall()
df4=pd.DataFrame(table4, columns=["No Of Comments", "Videos Title"])
df4

# 5th  question 
mydb = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="mahendran",
    database="project-1",
    port="5432"
)
cursor = mydb.cursor()


#elif question == '5. Videos with highest likes':
query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos 
                    where Likes is not null order by Likes desc;'''
cursor.execute(query5)
mydb.commit()
table5 = cursor.fetchall()
df5=(pd.DataFrame(table5, columns=["video Title","channel Name","like count"]))
df5

# 6th  question 
mydb = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="mahendran",
    database="project-1",
    port="5432"
)
cursor = mydb.cursor()

#elif question == '6. likes of all videos':
query6 = '''select Likes as likeCount,Title as VideoTitle from videos;'''
cursor.execute(query6)
mydb.commit()
table6 = cursor.fetchall()
df6=(pd.DataFrame(table6, columns=["like count","video title"]))
df6

# 7th  question 
mydb = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="mahendran",
    database="project-1",
    port="5432"
)
cursor = mydb.cursor()

#elif question == '7. views of each channel':
query7 = "select Channel_Name as ChannelName, Views as Channelviews from channels;"
cursor.execute(query7)
mydb.commit()
table7=cursor.fetchall()
df7=(pd.DataFrame(table7, columns=["channel name","total views"]))
df7

# 8th  question 
mydb = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="mahendran",
    database="project-1",
    port="5432"
)
cursor = mydb.cursor()

#elif question == '8. videos published in the year 2022':
query8 = '''select Title as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos 
            where extract(year from Published_Date) = 2022;'''
cursor.execute(query8)
mydb.commit()
table8=cursor.fetchall()
df8=(pd.DataFrame(table8,columns=["Name", "Video Publised On", "ChannelName"]))
df8

# 9th  question 
mydb = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="mahendran",
    database="project-1",
    port="5432"
)
cursor = mydb.cursor()


#elif question == '9. average duration of all videos in each channel':
query9 =  "SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos GROUP BY Channel_Name;"
cursor.execute(query9)
mydb.commit()
table9=cursor.fetchall()
table9 = pd.DataFrame(table9, columns=['ChannelTitle', 'Average Duration'])
Table9=[]
for index, row in table9.iterrows():
    channel_title = row['ChannelTitle']
    average_duration = row['Average Duration']
    average_duration_str = str(average_duration)
    Table9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
print(pd.DataFrame(Table9))

  #10th  question 
mydb = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="mahendran",
    database="project-1",
    port="5432"
)
cursor = mydb.cursor()

#elif question == '10. videos with highest number of comments':
query10 = '''select Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from videos 
                    where Comments is not null order by Comments desc;'''
cursor.execute(query10)
mydb.commit()
table10=cursor.fetchall()
df10=(pd.DataFrame(table10, columns=['Video Title', 'Channel Name', 'NO Of Comments']))
df10

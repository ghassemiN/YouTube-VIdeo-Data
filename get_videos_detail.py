import requests,sys, re
from datetime import datetime
import mysql.connector
from mysql.connector import Error, errorcode
import time
import configparser
import storage

#connection to db and get token
config = configparser.ConfigParser()
config.read('config.ini')
key=config['token']['key']
q=config['keyword']['q']

#Get start and end date 
try:
    connection = storage.connect()
    cursor = connection.cursor()
    #select a row of video's ID table, to get detail of the video.
    get_row="SELECT * FROM youtube_list WHERE is_going=0 and is_done=0 LIMIT 1"
    cursor.execute(get_row)
    records = cursor.fetchall()

    #Check if there is a row to select or not
    if not records:
        print("there is no video ID left in youtube_list to select")
        sys.exit()
        
    #if there is a row, select it's video_id        
    for record in records:
        row_id=record[0]
    connection.commit()

    #update is_going filed after select the video id. it's a flag to show it's taken.
    sql="UPDATE youtube_list SET is_going= 1 WHERE video_id='%s' LIMIT 1" %(row_id)
    cursor.execute(sql)
    connection.commit()

except mysql.connector.Error as error:
    print("Failed to Update a row {}".format(error))
finally:
    if (connection.is_connected()):
        connection.close()

#Define function to insert video's details to db
def insert_list_to_db(videos):
    try:
        connection = storage.connect()
        cursor = connection.cursor()
        count_row=0
        for mydict in videos:
            columns = ', '.join("`" + str(x).replace('/', '_') + "`" for x in mydict.keys())
            values = ', '.join("'" + str(x).replace('/', '_') + "'" for x in mydict.values())
            sql = "INSERT IGNORE INTO %s ( %s ) VALUES ( %s );" % ('videos_details', columns, values)         
            cursor.execute(sql)
            connection.commit()
            if cursor.rowcount == 1:
                count_row = count_row +1

        print(count_row)
        #update is_done filed after insert to table. it's a flag to show it's taken.
        sql="UPDATE youtube_list SET is_done= 1 WHERE video_id='%s' LIMIT 1" %(row_id)
        cursor.execute(sql)
        connection.commit()
        cursor.close()

    except mysql.connector.Error as error:
        print("Failed to insert record into videos_details table {}".format(error))

    finally:
        if (connection.is_connected()):
            connection.close()
            print("MySQL connection is closed")

#get video's details
def get_video_detail():
    url = "https://www.googleapis.com/youtube/v3/videos"
    querystring = {
        "part":"statistics,snippet,contentDetails",
        "key":key,
        "id":row_id,
    }

    headers = {
        'Accept': "application/json",
        'User-Agent': 'PostmanRuntime/7.19.0',
        'Connection': 'keep-alive',
        }

    r = requests.request("GET", url, headers=headers, params=querystring)
    json_data=r.json()
    results =r.json()['items']
    videos=[]

    for result in results:
        #Change the date-time format
        result['snippet']['publishedAt'] = datetime.strptime(result['snippet']['publishedAt'], '%Y-%m-%dT%H:%M:%S.000Z').strftime('%Y-%m-%d %H:%M:%S')

        #Replace Single quote with slash quote, for insert in db with no error.
        result['snippet']['description']= result['snippet']['description'].replace("'", "\'")
        result['snippet']['title']= result['snippet']['title'].replace("'", "\'")

        #Check if the items are empty or not. for insert in db with no error.
        if len(result['statistics']['commentCount'])==0:
            result['statistics']['commentCount']=0
        if len(result['statistics']['likeCount'])==0:
            result['statistics']['likeCount']=0
        if len(result['statistics']['dislikeCount'])==0:
            result['statistics']['dislikeCount']=0
        if len(result['statistics']['favoriteCount'])==0:
            result['statistics']['favoriteCount']=0

        video_data={
            'video_id':result['id'],
            'publishedAt':result['snippet']['publishedAt'],
            'channelId':result['snippet']['channelId'],
            'title': result['snippet']['title'],
            'description': result['snippet']['description'],
            'duration': result['contentDetails']['duration'],
            'viewCount':result['statistics']['viewCount'],
            'likeCount':result['statistics']['likeCount'],
            'dislikeCount':result['statistics']['dislikeCount'],
            'favoriteCount':result['statistics']['favoriteCount'],
            'commentCount':result['statistics']['commentCount'],
            'query_time':datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
        videos.append(video_data)

    #send video's detail to the function for save into db    
    insert_list_to_db(videos)

#Call function to get videos detail
get_video_detail()
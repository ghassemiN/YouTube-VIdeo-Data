import requests,sys, math, re
from datetime import datetime
import mysql.connector
from mysql.connector import Error, errorcode
import time, sys
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
    get_row="SELECT * FROM date_list WHERE is_done=0 LIMIT 1"
    cursor.execute(get_row)
    records = cursor.fetchall()
    for record in records:
        row_id=record[0]
        start_date=record[1]
        end_date=record[2]
    connection.commit()
    sql="UPDATE date_list SET is_done=-1 WHERE is_done=0  LIMIT 1"
    cursor.execute(sql)
    connection.commit()
except mysql.connector.Error as error:
    print("Failed to Update a row {}".format(error))
finally:
    if (connection.is_connected()):
        connection.close()
    
#Define function to insert video's list to db
def insert_list_to_db(videos):
    try:
        connection = storage.connect()
        cursor = connection.cursor()
        count_row=0
        for mydict in videos:
            columns = ', '.join("`" + str(x).replace('/', '_') + "`" for x in mydict.keys())
            values = ', '.join("'" + str(x).replace('/', '_') + "'" for x in mydict.values())
            sql = "INSERT IGNORE INTO %s ( %s ) VALUES ( %s );" % ('youtube_list', columns, values)         
            cursor.execute(sql)
            connection.commit()
            if cursor.rowcount == 1:
                count_row = count_row +1

        print(count_row)
        cursor.close()

    except mysql.connector.Error as error:
        print("Failed to insert record into Youtube_list table {}".format(error))

    finally:
        if (connection.is_connected()):
            connection.close()
            print("MySQL connection is closed")


#get videos list for next pages of the result
def next_page(nextPageToken, url, count_page):
    pagecount=0

    while pagecount<= count_page:
        time.sleep(2)
        next_search_params = {
            "part":"snippet,id",
            "publishedAfter":start_date,
            "publishedBefore":end_date,
            "type":"video",
            "q":"crispr cas9",
            "videoCaption":"closedCaption",
            "maxResults":"50",
            "key":key,
            "pageToken":nextPageToken,
            "order":"date"
        }
        headers = {
            'Accept': "application/json",
        }
        r = requests.request('GET',url, headers=headers, params=next_search_params)
        json_data = r.json()
        nextPageToken = json_data.get("nextPageToken")
        results=r.json()['items']

        videos=[]
        for result in results:
            result['snippet']['publishedAt'] = datetime.strptime(result['snippet']['publishedAt'], '%Y-%m-%dT%H:%M:%S.000Z').strftime('%Y-%m-%d %H:%M:%S')
            video_data={
                'video_id':result['id']['videoId'],
                'video_title': result['snippet']['title'],
                'published_at':result['snippet']['publishedAt'],
                'created_at':datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            videos.append(video_data)
        print(nextPageToken)

        insert_list_to_db(videos)

        if ( nextPageToken == None ):
            break
            
        pagecount += 1

#get video list, include  ID, title and Published date
def get_video_list():
    url = "https://www.googleapis.com/youtube/v3/search"
    querystring = {
        "part":"snippet,id",
        "publishedAfter":start_date,
        "publishedBefore":end_date,
        "type":"video",
        "q":q,
        "videoCaption":"closedCaption",
        "maxResults":"50",
        "key":key,
        "order":"date"
    }

    headers = {
        'Accept': "application/json",
        'User-Agent': 'PostmanRuntime/7.19.0',
        'Connection': 'keep-alive',
        }

    r = requests.request("GET", url, headers=headers, params=querystring)
    json_data=r.json()
    nextPageToken = json_data.get("nextPageToken")
    results =r.json()['items']
    totalResults =r.json()['pageInfo']['totalResults']
    count_page= math.ceil(totalResults/50)
    videos=[]
    print(totalResults)


    for result in results:
        result['snippet']['publishedAt'] = datetime.strptime(result['snippet']['publishedAt'], '%Y-%m-%dT%H:%M:%S.000Z').strftime('%Y-%m-%d %H:%M:%S')
        video_data={
            'video_id':result['id']['videoId'],
            'video_title': result['snippet']['title'],
            'published_at':result['snippet']['publishedAt'],
            'created_at':datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        videos.append(video_data)

    insert_list_to_db(videos)

    if not (nextPageToken is None):
        next_page(nextPageToken, url,count_page)

#Call function to get video list
get_video_list()
import json
import numpy as np
import psycopg2
import pandas as pd
from create_insert_sql import *
import os

path = os.path.abspath(os.path.dirname(__file__))

def readJsonFiles ():

    matchDataJson = [json.loads(line) for line in open(f"{path}/data/match_connect_button_click.json", 'r')]
    playerDataJson = [json.loads(line) for line in open(f"{path}/data/player_match_status_updated.json", 'r')]

    matchDataDF = pd.json_normalize(matchDataJson)
    playerDataDF =  pd.json_normalize(playerDataJson)

    mergedPlayerMatch = pd.merge(playerDataDF, matchDataDF, how="left", on=['user_id', 'match_id'])
    mergedData = mergedPlayerMatch.groupby(['user_id', 'match_id']).apply(lambda x: x.sort_values('event_timestamp_x', ascending = True))

    return mergedData

def process_json_files(cur, conn):

    mergedData = readJsonFiles()

    #PLAYER
    playerDF = mergedData[["user_id","leaver","afk","region"]].drop_duplicates(subset='user_id', keep='last')

    for i, row in playerDF.iterrows():
        cur.execute(player_table_insert, row)
    conn.commit()

    # #MATCH
    matchDF = mergedData[["match_id","match_type", "current_round", "url", "game_y", "state"]].drop_duplicates(subset='match_id', keep='last')

    for i, row in matchDF.iterrows():
        cur.execute(match_table_insert, row)
    conn.commit()

    #ORGANIZER
    organizerDF = mergedData[["entity.id", "entity.type", "organizer_id"]].drop_duplicates(subset='entity.id', keep ='last')

    for i, row in organizerDF.iterrows():
        cur.execute(organizer_table_insert, row)
    conn.commit()

    # PAGES
    pagesDF = mergedData[[ "page.url", "page.title", "page.category", "tracking_session_id"]].drop_duplicates(subset='page.url', keep='last')

    for i, row in pagesDF.iterrows():
        cur.execute(pages_table_insert, row)
    conn.commit()

    #TIME
    timeDF = pd.DataFrame(mergedData["event_timestamp_x"])
    timeDF['datetime']= pd.to_datetime(timeDF['event_timestamp_x']).dt.strftime('%Y-%m-%d %H:%M:%S')
    lastTimeDF= timeDF.drop_duplicates(subset='event_timestamp_x', keep='last')

    for i, row in lastTimeDF.iterrows():
        cur.execute(time_table_insert, row)
    conn.commit()

    #FACT-PLAYERMATCH
    fact = mergedData[["user_id","match_id","entity.id","page.url","event_timestamp_x"]].drop_duplicates(subset=['user_id','match_id'], keep='last')
    for i, row in fact.iterrows():
        if i in fact:
            cur.execute(playermatch_table_insert, fact)
    conn.commit()

    conn.close()
   
def main_etl():

    conn = psycopg2.connect("host=host.docker.internal dbname=faceit user=airflow password=airflow")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    process_json_files(cur,conn)

if __name__ == "__main__":
    main_etl()





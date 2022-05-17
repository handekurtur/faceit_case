import psycopg2

def main ():

    cur,conn = drop_and_create_database()
    drop_tables(cur,conn)
    create_tables(cur,conn)

def drop_and_create_database():

    conn = psycopg2.connect("host=host.docker.internal dbname=airflow user=airflow password=airflow")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    drop_database = "DROP DATABASE IF EXISTS faceit;"
    database_create = "CREATE DATABASE faceit WITH ENCODING 'utf8' TEMPLATE template0;"

    cur.execute(drop_database)
    cur.execute(database_create)

    conn.close()    

    conn = psycopg2.connect("host=host.docker.internal dbname=faceit user=airflow password=airflow")
    cur = conn.cursor()

    return cur, conn

def drop_tables(cur,conn):

    player_table_drop = "DROP TABLE IF EXISTS player;"
    match_table_drop = "DROP TABLE IF EXISTS match;"
    pages_table_drop = "DROP TABLE IF EXISTS pages;"
    entity_table_drop = "DROP TABLE IF EXISTS organizer;"
    datetime_table_drop = "DROP TABLE IF EXISTS timestamp;"
    playermatch_table_create = "DROP TABLE IF EXISTS playermatch;"

    drop_table_queries = [playermatch_table_create ,player_table_drop, match_table_drop, pages_table_drop, entity_table_drop, datetime_table_drop]

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):

    player_table_create = '''CREATE TABLE player(
        user_id varchar PRIMARY KEY,
        leaver boolean, 
        afk boolean,
        region varchar);'''

    match_table_create = '''CREATE TABLE match(
        match_id varchar PRIMARY KEY,
        match_type varchar, 
        current_round varchar,
        url varchar,
        game varchar,
        state varchar);'''
    
    pages_table_create = '''CREATE TABLE pages(
        page_url varchar PRIMARY KEY,
        page_title varchar,
        page_category varchar,
        tracking_session_id varchar);'''

    organizer_table_create = '''CREATE TABLE organizer(
        entity_id varchar PRIMARY KEY,
        entity_type varchar,
        organizer_id varchar);'''

    time_table_create = '''CREATE TABLE timestamp(
        event_timestamp timestamp PRIMARY KEY,
        datetime date);'''
    
    playermatch_table_create =  '''CREATE TABLE playermatch(
        id serial primary key,
        user_id varchar,
        match_id varchar,
        entity_id varchar,
        page_url varchar, 
        event_timestamp timestamp,
        PRIMARY KEY(user_id, match_id));'''

    create_table_queries = [player_table_create, match_table_create, pages_table_create, organizer_table_create, time_table_create, playermatch_table_create]

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    
    conn.close()

player_table_insert = ('''INSERT INTO player (user_id, leaver, afk, region) VALUES (%s,%s,%s,%s) ON CONFLICT (user_id) 
DO UPDATE SET leaver = EXCLUDED.leaver, afk = EXCLUDED.afk, region = EXCLUDED.region;''')

match_table_insert = ('''INSERT INTO match (match_id, match_type, current_round, url, game, state) VALUES (%s,%s,%s,%s,%s,%s) 
ON CONFLICT (match_id) DO UPDATE SET match_type = EXCLUDED.match_type, current_round = EXCLUDED.current_round, url = EXCLUDED.url, game = EXCLUDED.game, state = EXCLUDED.state;''')

pages_table_insert = ('''INSERT INTO pages (page_url, page_title, page_category, tracking_session_id) VALUES (%s,%s,%s,%s) ON CONFLICT (page_url) 
DO UPDATE SET page_title = EXCLUDED.page_title, page_category = EXCLUDED.page_category, tracking_session_id = EXCLUDED.tracking_session_id;''')

time_table_insert = ('''INSERT INTO timestamp (event_timestamp, datetime) VALUES (%s,%s) ON CONFLICT (event_timestamp) DO NOTHING;''')

organizer_table_insert = ('''INSERT INTO organizer (entity_id, entity_type, organizer_id) VALUES (%s,%s,%s) ON CONFLICT (entity_id) 
DO UPDATE SET entity_type = EXCLUDED.entity_type, organizer_id = EXCLUDED.organizer_id;''')

playermatch_table_insert = ('''INSERT INTO playermatch (user_id, match_id, entity_id, page_url, event_timestamp) VALUES (%s,%s,%s,%s,%s)
ON CONFLICT (user_id,match_id) DO UPDATE SET entity_id = EXCLUDED.entity_id, page_url = EXCLUDED.page_url, event_timestamp = EXCLUDED.event_timestamp;''')


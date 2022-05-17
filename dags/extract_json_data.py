import json
import pandas as pd

def readJsonFiles ():
    matchDataJson = [json.loads(line) for line in open('./data/match_connect_button_click.json', 'r')]
    playerDataJson = [json.loads(line) for line in open('./data/player_match_status_updated.json', 'r')]
    matchDataDF = pd.json_normalize(matchDataJson)
    playerDataDF =  pd.json_normalize(playerDataJson)

    mergedMatchAndPlayerDataDF = pd.merge(matchDataDF, playerDataDF, how="left", on=['user_id', 'match_id', 'game'])
    type(mergedMatchAndPlayerDataDF)
    print(mergedMatchAndPlayerDataDF)

readJsonFiles()



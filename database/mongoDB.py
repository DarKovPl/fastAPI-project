import pandas as pd
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
import os
from fastapi import HTTPException
import json

class Database:

    def __init__(self):
        client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)
        try:
            client.server_info()
        except ServerSelectionTimeoutError:
            raise HTTPException(status_code=503, detail="Problem with connection to database.")
        self.nba_players_collection = client["NBAPlayers"]['players']
        self.file_name = "NBAPlayers.csv"
        self.csv_file_path = os.path.join(os.getcwd(), 'files', self.file_name)
        self.players_data = pd.DataFrame([])

    def get_data_from_csv_file(self):
        self.players_data = pd.read_csv(self.csv_file_path, index_col=0)

    def optimise_data_frame(self):
        self.players_data['team_abbreviation'] = self.players_data['team_abbreviation'].astype('category')
        self.players_data['season'] = self.players_data['season'].astype('category')
        self.players_data['age'] = self.players_data['age'].astype('int16')
        self.players_data['player_height'] = self.players_data['player_height'].astype('float16')
        self.players_data['player_weight'] = self.players_data['player_weight'].astype('float16')
        self.players_data['id'] = self.players_data['id'].astype('str')

    def insert_data_to_mongo_database(self):
        data = self.players_data.to_dict(orient='records')
        self.nba_players_collection.insert_many(data)

    def get_data_from_mongo_database(self):
        collection = self.nba_players_collection.find({})
        self.players_data = pd.DataFrame(collection)
        self.players_data.rename(columns={'_id': 'id'}, inplace=True)

        self.optimise_data_frame()
        players = json.loads(self.players_data.to_json(orient="records"))

        return players

    def get_all_players(self):
        return self.get_data_from_mongo_database()


# database_instance = Database()
# database_instance.get_data_from_csv_file()
# database_instance.optimise_data_frame()
# database_instance.insert_data_to_mongo_database()
# database_instance.get_all_players()
#
# Database().get_data_from_mongo_database()
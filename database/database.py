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
        self.current_working_directory = os.getcwd() if os.getcwd().endswith('database') else os.path.join(os.getcwd(), 'database')
        self.csv_file_path = os.path.join(self.current_working_directory, 'files', self.file_name)
        self.players_data = pd.DataFrame([])

    def optimise_data_frame(self):
        self.players_data['team_abbreviation'] = self.players_data['team_abbreviation'].astype('category')
        self.players_data['season'] = self.players_data['season'].astype('category')
        self.players_data['age'] = self.players_data['age'].astype('int16')
        self.players_data['player_height'] = self.players_data['player_height'].astype('float16')
        self.players_data['player_weight'] = self.players_data['player_weight'].astype('float16')

        if '_id' in self.players_data:
            self.players_data.rename(columns={'_id': 'id'}, inplace=True)
            self.players_data['id'] = self.players_data['id'].astype('str')
        else:
            self.players_data.rename(columns={'Unnamed: 0': 'id'}, inplace=True)

    def get_data_from_csv_file(self):
        self.players_data = pd.read_csv(self.csv_file_path)

        self.optimise_data_frame()
        players = json.loads(self.players_data.to_json(orient="records"))
        return players

    def insert_data_to_mongo_database(self):
        data = self.players_data.to_dict(orient='records')
        self.nba_players_collection.insert_many(data)

    def get_data_from_mongo_database(self):
        collection = self.nba_players_collection.find({})
        self.players_data = pd.DataFrame(collection)

        self.optimise_data_frame()
        players = json.loads(self.players_data.to_json(orient="records"))
        return players

    def get_one_player_by_name(self, player_name: str):
        player_data = self.nba_players_collection.find_one({"player_name": player_name})
        self.players_data = pd.DataFrame([player_data])

        self.optimise_data_frame()
        player_data = json.loads(self.players_data.to_json(orient="records"))
        return player_data


# Database().get_one_player_by_name("Travis Knight")

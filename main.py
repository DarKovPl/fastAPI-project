
from pydantic import BaseModel
from fastapi import FastAPI
from database.mongoDB import Database
from typing import List

app = FastAPI()


class NBAPlayers(BaseModel):
    id: str
    player_name: str
    team_abbreviation: str
    age: int
    player_height: float
    player_weight: float
    season: str


@app.get("/players", response_model=List[NBAPlayers], tags=["players"])
def all_players():
    return Database().get_all_players()

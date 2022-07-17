from functools import wraps
from pydantic import BaseModel
from fastapi import FastAPI
from database.database import Database
from typing import List, Optional
import math

app = FastAPI()


class NBAPlayers(BaseModel):
    id: str
    player_name: str
    team_abbreviation: str
    age: int
    player_height: float
    player_weight: float
    season: str


class MetaData(BaseModel):
    total: int
    page_num: int
    on_page: int
    total_pages: int
    pagination: dict


class ResponseModel(BaseModel):
    data: list[NBAPlayers]
    meta: MetaData


@app.get("/players", response_model=ResponseModel, tags=["players"])
async def all_players(page_num: int = 1, page_size: int = 10):
    data = Database().get_data_from_csv_file()
    start = (page_num - 1) * page_size
    end = start + page_size
    total_pages = math.ceil(len(data) / page_size)

    pagination = {}
    if end >= len(data):
        pagination["next"] = "Null"
        if page_num > 1:
            pagination["previous"] = f"/players?page_num={page_num - 1}&page_size={page_size}"
        else:
            pagination["previous"] = "Null"
    else:
        if page_num > 1:
            pagination["previous"] = f"/players?page_num={page_num - 1}&page_size={page_size}"
        else:
            pagination["previous"] = "Null"
        pagination["next"] = f"/players?page_num={page_num + 1}&page_size={page_size}"

    response = ResponseModel(
        data=data[start:end],
        meta={
            "total": len(data),
            "page_num": page_num,
            "on_page": page_size,
            "total_pages": total_pages,
            "pagination": pagination
        }
    )
    return response


@app.get("/player/{player_name}", response_model=list[NBAPlayers], tags=["players"])
async def get_player_by_name(player_name: str):

    return Database().get_one_player_by_name(player_name)

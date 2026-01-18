from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
from database import return_by_id, return_movies, insert_movie as create_movie, remove_by_id, update_record
import psycopg2

app = FastAPI()

@app.get("/")
async def root():
    return {"message":"Movie API working"}

@app.get("/movies")
async def get_movies(limit : int = 5, rating : float = 0.0 ):
    if(limit < 1):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid limit value")
    if(rating < 0 or rating > 10):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid rating value")
    
    data = return_movies(limit, rating)

    return {"movies":data, "count" : len(data)}

@app.get("/movies/{id}")
async def get_by_id(id: int):
    data = return_by_id(id)
    if not data :
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail = "Movie not found")
    return data

class Movie(BaseModel):
    title: str
    release_year: int
    certificate: str | None
    rating: float | None
    meta_score: int | None
    runtime: int | None
    director: str 
    gross: int | None
    overview: str | None
    votes: int | None


@app.post("/movies", status_code=status.HTTP_201_CREATED)
async def insert_movie(movie : Movie):
    try:    
        data_dict = movie.model_dump()
        row_count = create_movie(data_dict)
    except psycopg2.IntegrityError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail = "Movie already exists or constraint violated")
    except psycopg2.OperationalError as e:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail = "Database unavailable")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail= "Internal server error")
    return {"message" : "Movie created"}
    


@app.delete("/movies/{id}")
async def delete_movie(id : int):
    try:    
        rows_count = remove_by_id(id)
    except psycopg2.OperationalError as e:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail = "Database unavailable")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail= "Internal server error")
    if rows_count == 0:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail = "Movie not found" )
    return {"message" : "Movie deleted"}

class MovieUpdate(BaseModel):
    title : str | None = None
    release_year : int | None = None
    certificate : str | None = None
    rating : float | None = None
    meta_score : int | None = None
    runtime : int | None = None
    director : str | None = None
    gross : int | None = None
    overview : str | None = None
    votes : int | None = None

@app.patch("/movies/{id}")
async def update_movie(id : int, movie : MovieUpdate):
    data = movie.model_dump(exclude_unset= True)
    if not data:
        raise HTTPException(status_code = status.HTTP_400_BAD_REQUEST, detail = "No column data provided for update")
    try:
        rows_count = update_record(id,data)
    except psycopg2.IntegrityError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail = "Constraints violated")
    except psycopg2.OperationalError as e:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail = "Database unavailable")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail= "Internal server error")
    if rows_count == 0:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail = "Movie not found" )
    
    return {"message" : "Movie updated", "rows_affected" : rows_count}
import psycopg2 as psy
import psycopg2.pool
from psycopg2.extras import RealDictCursor
import os
import pandas as pd
from dotenv import load_dotenv
from psycopg2.extensions import connection as Connection, cursor as Cursor
from typing import List, Tuple,Dict



load_dotenv()

connection_pool = psy.pool.SimpleConnectionPool(
    1,
    10,
    dbname = os.getenv("DB_NAME"),
    user = os.getenv("DB_USER"),
    password = os.getenv("DB_PASSWORD"),
    host = os.getenv("DB_HOST")
)

def get_connection() -> Connection:
    connection = connection_pool.getconn()
    return connection

def release_connection(connection : Connection):
    connection_pool.putconn(connection)

def return_movies(limit : int , rating : float) -> List[Dict]:


    connection = get_connection()
    try:
        cur = connection.cursor(cursor_factory=RealDictCursor)

        cur.execute("SELECT * FROM movies WHERE rating >= %s LIMIT %s;", (rating, limit))

        movies_data = cur.fetchall()
        cur.close()
        return movies_data
    finally:
        release_connection(connection)
    

def return_by_id(id : int) -> List[Dict]:
    connection = get_connection()
    
    try:
        cur = connection.cursor(cursor_factory=RealDictCursor)    

        cur.execute("SELECT * FROM movies WHERE id = %s;", (id,))

        movie_data = cur.fetchone()

        cur.close()

        return movie_data
    finally:
        release_connection(connection)

def insert_movie(movie : Dict) -> int:
    connection = get_connection()

    try:
        cur = connection.cursor()
    
        cur.execute("INSERT INTO directors(name) VALUES (%s) ON CONFLICT DO NOTHING", (movie["director"],) )

        cur.execute("SELECT id FROM directors WHERE name = %s", (movie["director"],))

        director_id = cur.fetchone()

        movie["director"] = director_id[0]

        INSERT_QUERY = """INSERT INTO movies(title, release_year,certificate,rating,meta_score,runtime,director_id,gross,overview,votes)
            VALUES(%s, %s ,%s ,%s ,%s ,%s ,%s ,%s ,%s, %s)
        """
        cur.execute(
            INSERT_QUERY, 
            (movie['title'], movie['release_year'],movie['certificate'],movie['rating'],movie['meta_score'],movie['runtime'],movie['director'],movie['gross'],movie['overview'],movie['votes'])
        )
        rows_count = cur.rowcount
        cur.close()
        # connection.commit()
        return rows_count
    except Exception as e:
        connection.rollback()
        raise 
    finally:
        release_connection(connection)


def remove_by_id(id: int) -> int:

    connection = get_connection()
    try:
        cur = connection.cursor()

        cur.execute("DELETE FROM movies WHERE id = %s ", (id,))
        count_rows_affected = cur.rowcount
        cur.close()
        return count_rows_affected
    except Exception as e:
        connection.rollback()
        raise
    finally:
        release_connection(connection)

def update_record(id : int , data: Dict) -> int:
    connection = get_connection()

    try:
        cur = connection.cursor()

        if "director" in data.keys():
            cur.execute("SELECT id FROM directors WHERE name = %s", (data["director"],))
            # If director does not exist
            director_id = cur.fetchone()
            if not director_id: 
                cur.execute("INSERT INTO directors(name) VALUES(%s)",(data["director"],))
                cur.execute("SELECT id FROM directors WHERE name = %s", (data["director"],))

                director_id = cur.fetchone()
            data["director_id"] = director_id[0]

        VALID_FIELDS = ["title", "release_year","certificate","rating","meta_score","runtime","director_id","gross","overview","votes"]
        update_query = "UPDATE movies SET "
        data_to_insert = []

        for key in data.keys():
            if key == "director":
                continue
            if key not in VALID_FIELDS:
                continue

            update_query += f"{key} = %s,"
            data_to_insert.append(data[key])

        data_to_insert.append(id)
        update_query = update_query.rstrip(",") + " WHERE id = %s;"
        print(update_query)
        cur.execute(update_query,tuple(data_to_insert))

        count_rows_affected = cur.rowcount
        cur.close()
        return count_rows_affected
    except Exception as e:
        connection.rollback()
        raise
    finally:
        release_connection(connection)
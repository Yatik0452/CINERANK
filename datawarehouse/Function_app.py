import logging
import os
import pyodbc
from azure.storage.blob import BlobServiceClient
import pandas as pd
import azure.functions as func
from chardet.universaldetector import UniversalDetector
import io
import chardet
import ast
import numpy as np
from dotenv import load_dotenv


load_dotenv()
BLOB_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "csv-files"

SQL_SERVER = os.getenv("SERVER")
SQL_DATABASE = os.getenv("DATABASE")
SQL_USERNAME = os.getenv("USERNAME_AZURE")
SQL_PASSWORD = os.getenv("PASSWORD")

# Connect to Azure Blob Storage
blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# Connect to SQL Server
def get_sql_connection():
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USERNAME};"
        f"PWD={SQL_PASSWORD}"
    )
    return conn

# Function to detect file encoding
def detect_file_encoding(blob_data):
    detector = UniversalDetector()
    for line in blob_data:
        detector.feed(line)
        if detector.done:
            break
    detector.close()
    return detector.result["encoding"]

# Function to read CSV file from Blob Storage
def read_csv_from_blob(blob_name):
    blob_client = container_client.get_blob_client(blob_name)
    downloaded_blob = blob_client.download_blob()

    blob_data = downloaded_blob.readall()
    detected_encoding = chardet.detect(blob_data)
    encoding = detected_encoding.get("encoding", "utf-8")

    try:
        csv_string = blob_data.decode(encoding)
    except UnicodeDecodeError as e:
        raise ValueError(f"Failed to decode blob {blob_name} with encoding {encoding}: {str(e)}")

    csv_file = io.StringIO(csv_string)

    try:
        df = pd.read_csv(csv_file)
    except Exception as e:
        raise ValueError(f"Failed to parse CSV {blob_name}: {str(e)}")

    return df

def transform_and_load(df, table_name, primary_key_column=None):
    df = df.fillna('')
    conn = get_sql_connection()
    cursor = conn.cursor()

    # Check for existing primary keys to avoid duplicates (if primary key column is specified)
    if primary_key_column:
        existing_ids_query = f"SELECT {primary_key_column} FROM {table_name}"
        cursor.execute(existing_ids_query)
        existing_ids = {str(row[0]).upper() for row in cursor.fetchall()}  # Normalize to uppercase

        # Normalize incoming primary keys for comparison
        df[primary_key_column] = df[primary_key_column].astype(str).str.upper()

        # Filter out rows with duplicate primary keys
        df = df[~df[primary_key_column].isin(existing_ids)]

    if df.empty:
        logging.info(f"No new data to insert into {table_name}.")
        conn.close()
        return

    for _, row in df.iterrows():
        columns = ', '.join(f"[{col}]" for col in row.index)
        placeholders = ', '.join(['?'] * len(row))
        sql_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        cursor.execute(sql_query, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()

# Azure Function App
app = func.FunctionApp()

@app.route(route="etl_process", auth_level=func.AuthLevel.FUNCTION)
def etl_process(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Starting ETL process.")

    try:
        # --- 1. Load GenreDimension from TMDB genre list ---
        genre_blob_name = "tmdb_genre_list_dataset.csv"
        genre_df = read_csv_from_blob(genre_blob_name)
        genre_df = genre_df.rename(columns={
            "id": "GenreID",
            "name": "GenreName"
        })
        transform_and_load(genre_df, "GenreDimension", primary_key_column="GenreID")

        # --- 2. Load CountryDimension from country_annotation.csv ---
        country_blob_name = "country_annotation.csv"
        country_df = read_csv_from_blob(country_blob_name)
        required_columns = ['code', 'name', 'continent', 'languages']
        missing_columns = [col for col in required_columns if col not in country_df.columns]
        if missing_columns:
            raise ValueError(f"Missing columns in Country Annotation dataset: {', '.join(missing_columns)}")

        country_df = country_df.rename(columns={
            "code": "CountryID",
            "name": "CountryName",
            "continent": "Continent",
            "languages": "LanguagesSpoken"
        })
        country_df["CountryID"] = country_df["CountryID"].apply(lambda x: x.upper())
        # ✅ Drop duplicates by CountryID
        country_df = country_df.drop_duplicates(subset=["CountryID"])
        transform_and_load(country_df[["CountryID", "CountryName", "Continent", "LanguagesSpoken"]],
                           "CountryDimension", primary_key_column="CountryID")

        # --- 3. Load MovieDimension from TMDB ---
        tmdb_blob_name = "tmdb_dataset.csv"
        tmdb_df = read_csv_from_blob(tmdb_blob_name)

        #Function to safely parse genre_ids into a list
        def parse_genre_ids(val):
            if pd.isna(val) or val == '':
                return []  # If empty or NaN, return an empty list
            elif isinstance(val, str):
                val = val.strip()
                if val.startswith("[") and val.endswith("]"):
                    try:
                        return ast.literal_eval(val)  # Safely evaluate the string as a Python list
                    except (ValueError, SyntaxError):
                        return []  # In case of invalid list format, return an empty list
            return []  # Return an empty list for any other type

        # Assuming tmdb_df is already loaded
        tmdb_df = read_csv_from_blob("tmdb_dataset.csv")

        # Apply the genre_id parsing function
        tmdb_df["genre_ids"] = tmdb_df["genre_ids"].apply(parse_genre_ids)

        # Explode the genre_ids column into multiple rows, one for each genre ID
        tmdb_df = tmdb_df.explode("genre_ids")

        # Replace empty strings or invalid genres with NaN in genre_ids column
        tmdb_df["genre_ids"] = tmdb_df["genre_ids"].replace("", np.nan)

        # Convert genre_ids to nullable Int64 (this will automatically handle NaN values)
        tmdb_df["genre_ids"] = tmdb_df["genre_ids"].astype("Int64")

        # # Drop any rows where genre_ids is NaN (invalid genres)
        # tmdb_df = tmdb_df.dropna(subset=["genre_ids"])


        # Extract ReleaseYear from release_date
        tmdb_df['ReleaseYear'] = pd.to_datetime(tmdb_df['release_date'], errors='coerce').dt.year

        movie_df = tmdb_df[["id", "title", "original_title", "original_language"]]
        movie_df = movie_df.rename(columns={
            "id": "MovieID",
            "title": "MovieTitle",
            "original_title": "OriginalTitle",
            "original_language": "OriginalLanguage"
        })
        transform_and_load(movie_df, "MovieDimension", primary_key_column="MovieID")

       # --- 4. Load MovieGenreFact from TMDB and enrich with IMDb region ---
        imdb_blob_name = "imdb_dataset_with_region.csv"
        imdb_df = read_csv_from_blob(imdb_blob_name)

        # Normalize region to lowercase for matching with CountryID
        imdb_df["region"] = imdb_df["region"].str.lower()

        # Load country annotation again to get valid CountryIDs
        valid_country_ids = set(country_df["CountryID"].str.lower())

        # Create a region -> countryid mapping with fallback
        def resolve_country_id(region):
             if pd.isna(region):
                return None
             return region.lower() if region.lower() in valid_country_ids else None

        imdb_df["CountryID"] = imdb_df["region"].apply(resolve_country_id)

        # Select only necessary columns including numVotes (IMDb rating and votes)
        imdb_country_map = imdb_df[["tconst", "CountryID", "averageRating", "numVotes"]].drop_duplicates()

        # Safely parse genre_ids as Python lists
        tmdb_df["genre_ids"] = tmdb_df["genre_ids"].apply(
            lambda x: ast.literal_eval(x) if pd.notnull(x) and isinstance(x, str) else []
        
)

        # Explode genre_ids for fact table
        movie_genre_fact_df = tmdb_df.explode("genre_ids").rename(
            columns={
                "id": "MovieID",
                "genre_ids": "GenreID",
                "popularity": "Popularity",
                "imdb_id": "tconst"
            }
        )
        # Ensure GenreID is treated as an integer
        movie_genre_fact_df["GenreID"] = movie_genre_fact_df["GenreID"].astype("Int64")

        # Join TMDB data with IMDb data on tconst
        movie_genre_fact_df = pd.merge(movie_genre_fact_df, imdb_country_map, how="left", on="tconst")

        # Drop rows without CountryID
        movie_genre_fact_df = movie_genre_fact_df.dropna(subset=["CountryID"])

        # Extract ReleaseYear from release_date if needed
        movie_genre_fact_df["ReleaseYear"] = pd.to_datetime(movie_genre_fact_df["release_date"], errors="coerce").dt.year

        # Rename to match DB schema
        movie_genre_fact_df = movie_genre_fact_df.rename(columns={
            "averageRating": "Rating",
            "numVotes": "NumRatings"
        })

        # Select final columns
        movie_genre_fact_df = movie_genre_fact_df[[
            "MovieID", "GenreID", "Rating", "NumRatings", "ReleaseYear", "Popularity", "CountryID"
        ]]


        # Log the columns to ensure ReleaseYear exists
        logging.info(f"Columns in movie_genre_fact_df: {movie_genre_fact_df.columns}")

        # Load the transformed DataFrame into the MovieGenreFact table
        transform_and_load(movie_genre_fact_df, "MovieGenreFact")


        return func.HttpResponse("ETL process completed successfully.", status_code=200)

    except Exception as e:
        logging.error(f"ETL process failed: {str(e)}")
        return func.HttpResponse(f"ETL process failed: {str(e)}", status_code=500)

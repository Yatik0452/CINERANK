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

    # Read blob data
    blob_data = downloaded_blob.readall()

    # Detect encoding using chardet
    detected_encoding = chardet.detect(blob_data)
    encoding = detected_encoding.get("encoding", "utf-8")  # Default to UTF-8 if detection fails

    # Decode the blob data into a string using the detected encoding
    try:
        csv_string = blob_data.decode(encoding)
    except UnicodeDecodeError as e:
        raise ValueError(f"Failed to decode blob {blob_name} with encoding {encoding}: {str(e)}")

    # Use io.StringIO to convert the string to a file-like object for pandas
    csv_file = io.StringIO(csv_string)

    # Load the CSV into a Pandas DataFrame
    try:
        df = pd.read_csv(csv_file)
    except Exception as e:
        raise ValueError(f"Failed to parse CSV {blob_name}: {str(e)}")
    
    return df

# Insert or update transformed data into SQL
def transform_and_load(df, table_name, primary_key_column=None):
    df = df.fillna('')  # Fill nulls with empty strings
    conn = get_sql_connection()
    cursor = conn.cursor()

    if primary_key_column:
        existing_ids_query = f"SELECT {primary_key_column} FROM {table_name}"
        cursor.execute(existing_ids_query)
        existing_ids = {row[0] for row in cursor.fetchall()}
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
        # Read the TMDB dataset
        tmdb_blob_name = "tmdb_dataset.csv"
        tmdb_df = read_csv_from_blob(tmdb_blob_name)

        # Convert genre_ids from string to list if needed
        if tmdb_df["genre_ids"].dtype == object:
            tmdb_df["genre_ids"] = tmdb_df["genre_ids"].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)

        # Read the Country dataset
        country_blob_name = "country_annotation.csv"
        country_df = read_csv_from_blob(country_blob_name)

        # === CountryDimension ===
        country_df = country_df.rename(columns={
            "code": "CountryID",
            "name": "CountryName",
            "continent": "Continent",
            "languages": "LanguagesSpoken"
        })
        country_df["CountryID"] = country_df["CountryID"].apply(lambda x: x.upper())
        transform_and_load(
            country_df[["CountryID", "CountryName", "Continent", "LanguagesSpoken"]],
            "CountryDimension",
            primary_key_column="CountryID"
        )

        # === MovieDimension ===
        movie_df = tmdb_df[["id", "title", "original_title", "original_language", "release_date"]].rename(columns={
            "id": "MovieID",
            "title": "MovieTitle",
            "original_title": "OriginalTitle",
            "original_language": "OriginalLanguage",
            "release_date": "ReleaseYear"
        })
        transform_and_load(movie_df, "MovieDimension", primary_key_column="MovieID")

        # === GenreDimension ===
        genre_df = tmdb_df.explode("genre_ids")[["genre_ids"]].rename(columns={"genre_ids": "GenreID"})
        genre_df = genre_df.drop_duplicates(subset=["GenreID"])
        transform_and_load(genre_df, "GenreDimension", primary_key_column="GenreID")

        # === MovieGenreFact ===
        movie_genre_fact_df = tmdb_df.explode("genre_ids").rename(columns={
            "id": "MovieID",
            "genre_ids": "GenreID",
            "vote_average": "Rating",
            "vote_count": "NumRatings",
            "release_date": "ReleaseYear",
            "popularity": "Popularity"
        })

        # Add CountryID (sample logic — consider improving this)
        movie_genre_fact_df["CountryID"] = "US"

        # Placeholder for MovieTypeID (you can replace this logic with a real join or mapping)
        movie_genre_fact_df["MovieTypeID"] = 1  # Dummy value for now

        movie_genre_fact_df = movie_genre_fact_df[
            ["MovieID", "GenreID", "MovieTypeID", "Rating", "NumRatings", "ReleaseYear", "Popularity", "CountryID"]
        ]
        transform_and_load(movie_genre_fact_df, "MovieGenreFact")

        return func.HttpResponse("ETL process completed successfully.", status_code=200)

    except Exception as e:
        logging.error(f"ETL process failed: {str(e)}")
        return func.HttpResponse(f"ETL process failed: {str(e)}", status_code=500)


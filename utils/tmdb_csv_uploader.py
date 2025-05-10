# tmdb_csv_uploader.py
from utils.datasetup import *
from utils.dimension_classes import AzureDB
import requests
import pandas as pd

class APICSVUploader:

    @staticmethod
    def retrieve_imdb_ids():
        # Retrieve the IMDb IDs from the file
        blob_name="imdb_dataset.csv"
        database=AzureDB()
        database.access_container("csv-files")
        df = database.access_blob_csv(blob_name=blob_name)

        imdb_ids = df["tconst"].dropna().unique().tolist()  # Drop nulls and keep unique ones
        print("Retrieved IMDb IDs:", imdb_ids)
        return imdb_ids

    @staticmethod
    def get_tmdb_data_by_imdb_id(imdb_id: str):
        url = f"https://api.themoviedb.org/3/find/{imdb_id}?external_source=imdb_id"
        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {tmdb_api_token}"
        }
        response = requests.get(url, headers=headers)
        print(response.text)
        data = response.json()
        return data.get("movie_results", [])
    
    @staticmethod
    def convert_json_to_csv(json_data: list, output_path: str):
        if not json_data:
            print("No data to write.")
            return

        df = pd.DataFrame(json_data)
        df.to_csv(output_path, index=False)
        print(f"CSV saved to {output_path}") 

    @staticmethod
    def generate_tmdb_csv(imdb_ids: list, output_path: str = "./data/tmdb_dataset.csv"):
        combined_data = []
        for imdb_id in imdb_ids:
            print(f"Fetching data for {imdb_id}")
            data = APICSVUploader.get_tmdb_data_by_imdb_id(imdb_id)
            for item in data:
                item['imdb_id'] = imdb_id # Add imdb_id to each record
                combined_data.append(item)
        APICSVUploader.convert_json_to_csv(combined_data, output_path)

    @staticmethod
    def get_tmdb_genre_list():
        url = "https://api.themoviedb.org/3/genre/movie/list?language=en"
        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {tmdb_api_token}"
        }
        response = requests.get(url, headers=headers)
        print(response.text)
        data = response.json()
        return data.get("genres", [])

    @staticmethod
    def generate_tmdb_movie_csv(output_path: str = "./data/tmdb_genre_list_dataset.csv"):
        genres = APICSVUploader.get_tmdb_genre_list()
        APICSVUploader.convert_json_to_csv(genres, output_path)
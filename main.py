import os, uuid
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from dotenv import load_dotenv
from utils.datasetup import *
from utils.dimension_classes import *
from utils.tmdb_csv_uploader import *

class MainETL():
    # List of columns need to be replaced
    def __init__(self) -> None:
        self.drop_columns = []
        self.dimension_tables = []
        
    # def extract(self, csv_file: str):
    #     # Step 1: Extract: use pandas read_csv to open the csv file and extract data
    #     print(f'Step 1: Extracting data from csv file')
    #     self.fact_table = df
    #     print(f'We find {len(self.fact_table.index)} rows and {len(self.fact_table.columns)} columns in csv file: {csv_file}')
    #     print(f'Step 1 finished')
        
    def extract_and_transform(self):
        print("🔍 Starting data extraction and transformation...")

        # blob_name="imdb_dataset.csv"
        # database=AzureDB()
        # database.access_container("csv-files")
        # df = database.access_blob_csv(blob_name=blob_name)

        # Fetch movie type dimension table
        dim_movie_types = MovieTypeDimension()
        self.dimension_tables.append(dim_movie_types)
        print("✅ Loaded MovieType dimension")
        print(dim_movie_types.dimension_table.head())  # Show the first few rows


        # Fetch country data
        database=AzureDB()
        database.access_container("csv-files")
        df = database.access_blob_csv(blob_name="country_annotation.csv")
        print("📥 Retrieved country annotation CSV")

        # Fetch country dimension table
        dim_country = CountryDimension(df)
        self.drop_columns += dim_country.columns
        self.dimension_tables.append(dim_country)
        print("✅ Loaded Country dimension")
        print(dim_country.dimension_table.head())  # Show the first few rows

        # Add derived column: IsEnglishSpeaking
        dim_country.dimension_table['IsEnglishSpeaking'] = dim_country.dimension_table['LanguagesSpoken'].apply(
            lambda x: "English" in x
        )
        print("➕ Added 'IsEnglishSpeaking' column to Country dimension")

        # Fetch IMDb movie data
        df_imdb_movies = database.access_blob_csv(blob_name="imdb_dataset_with_region.csv")
        df = df_imdb_movies
        print("📥 Retrieved IMDb dataset with region")

        # Fetch movie dimension table
        dim_movie = MovieDimension(df)
        self.drop_columns += dim_movie.columns
        self.dimension_tables.append(dim_movie)
        print("✅ Loaded Movie dimension")
        print(dim_movie.dimension_table.head())  # Show the first few rows

        # Fetch TMDb movie data
        df_tmdb_movies = database.access_blob_csv(blob_name="tmdb_dataset.csv")
        df = df_tmdb_movies
        print("📥 Retrieved TMDb dataset")

        # Add PosterString and OriginalLanguage to Movie dimension
        dim_movie.dimension_table['PosterString'] = df['poster_path']
        dim_movie.dimension_table['OriginalLanguage'] = df['original_language']
        print("➕ Added 'PosterString' and 'OriginalLanguage' columns to Movie dimension")

        # Fetch genre data
        df_genres = database.access_blob_csv(blob_name="tmdb_genre_list_dataset.csv")
        df = df_genres
        print("📥 Retrieved genre list CSV")

        # Fetch genre dimension table
        dim_genre = GenreDimension(df)
        self.drop_columns += dim_genre.columns
        self.dimension_tables.append(dim_genre)
        print("✅ Loaded Genre dimension")
        print(dim_genre.dimension_table.head())  # Show the first few rows

        # Generate fact table
        print("⚙️ Generating fact table...")
        self.fact_table = FactTableGenerator(
            df_imdb_movies=df_imdb_movies,
            df_tmdb_movies=df_tmdb_movies,
            df_genres=df_genres,
            df_genre_list=dim_genre.dimension_table,
            df_movie_types=dim_movie_types.dimension_table,
            df_countries=dim_country.dimension_table
        ).generate_fact_table()

        print("🎯 Step 1 and 2 finished: All dimension tables extracted & transformed, fact table generated.")
        
    def load(self):
        # Load all the dimension tables into the database
        for table in self.dimension_tables:
            table.load()

        # Connect to the database and begin a transaction
        with engine.connect() as con:
            trans = con.begin()


            database=AzureDB()
            database.upload_dataframe_sqldatabase(f'MovieGenreFact_dim', self.fact_table)

            # Add foreign key constraints for each dimension table
            # for table in self.dimension_tables:
            #     con.execute(
            #         text(f'ALTER TABLE [dbo].[MovieGenreFact_dim] WITH NOCHECK '
            #             f'ADD CONSTRAINT [FK_{table.__class__.__name__}_dim] FOREIGN KEY ([{table.__class__.__name__}ID]) '
            #             f'REFERENCES [dbo].[{table.__class__.__name__}_dim] ([{table.__class__.__name__}ID]) '
            #             f'ON UPDATE CASCADE ON DELETE CASCADE;')
            #     )
            #     print(f'''
            #         ALTER TABLE [dbo].[MovieGenreFact_dim] WITH NOCHECK
            #         ADD CONSTRAINT [FK_{table.__class__.__name__}_dim] FOREIGN KEY ([{table.__class__.__name__}ID])
            #         REFERENCES [dbo].[{table.__class__.__name__}_dim] ([{table.__class__.__name__}ID])
            #         ON UPDATE CASCADE ON DELETE CASCADE;
            #         ''')

            # MovieTypeID
            con.execute(
                text('ALTER TABLE [dbo].[MovieGenreFact_dim] '
                    'ALTER COLUMN MovieTypeID INT NOT NULL;')
            )
            con.execute(
                text('ALTER TABLE [dbo].[MovieGenreFact_dim] WITH NOCHECK '
                    'ADD CONSTRAINT [FK_MovieType_dim] FOREIGN KEY ([MovieTypeID]) '
                    'REFERENCES [dbo].[MovieType_dim] ([MovieTypeID]) '
                    'ON UPDATE CASCADE ON DELETE CASCADE;')
            )

            # MovieID
            con.execute(
                text('ALTER TABLE [dbo].[MovieGenreFact_dim] WITH NOCHECK '
                    'ADD CONSTRAINT [FK_Movie_dim] FOREIGN KEY ([MovieID]) '
                    'REFERENCES [dbo].[Movie_dim] ([MovieID]) '
                    'ON UPDATE CASCADE ON DELETE CASCADE;')
            )

            # GenreID
            con.execute(
                text('ALTER TABLE [dbo].[MovieGenreFact_dim] WITH NOCHECK '
                    'ADD CONSTRAINT [FK_Genre_dim] FOREIGN KEY ([GenreID]) '
                    'REFERENCES [dbo].[Genre_dim] ([GenreID]) '
                    'ON UPDATE CASCADE ON DELETE CASCADE;')
            )

            # CountryID
            con.execute(
                text('ALTER TABLE [dbo].[MovieGenreFact_dim] '
                    'ALTER COLUMN CountryID varchar(20) NOT NULL;')
            )
            con.execute(
                text('ALTER TABLE [dbo].[MovieGenreFact_dim] WITH NOCHECK '
                    'ADD CONSTRAINT [FK_Country_dim] FOREIGN KEY ([CountryID]) '
                    'REFERENCES [dbo].[Country_dim] ([CountryID]) '
                    'ON UPDATE CASCADE ON DELETE CASCADE;')
            )

            # Commit the transaction
            trans.commit()


        print(f'Step 3 finished: Fact table and dimension tables uploaded successfully with composite key.')
   
    def mainLoop(self):    
        # Step 1
        # self.extract("ETL_Example_Data.csv")
        # Step 2
        self.extract_and_transform()
        # Step 3
        try:
            self.load()
        except:
            self.load()
        
def main():
    print("running main..")

    # Generate TMDb CSV
    # imdb_id_list = APICSVUploader.retrieve_imdb_ids()
    # APICSVUploader.generate_tmdb_csv(imdb_id_list)

    #Generate TMDb Genre List
    # APICSVUploader.generate_tmdb_movie_csv()

    # Upload local csv files to Azure blob storage
    # database = AzureDB()
    # print("running access container..")
    # database.access_container("csv-files")
    # print("running upload blob..")
    # database.upload_blob("tmdb_dataset.csv")
    # database.upload_blob("imdb_dataset.csv") #Replaced with "imdb_dataset_with_region.csv"
    # database.upload_blob("tmdb_genre_list_dataset.csv")
    # database.upload_blob("imdb_dataset_with_region.csv")

    # create an instance of MainETL
    main = MainETL()
    main.mainLoop()


    
if __name__ == '__main__':
    main()
        
    


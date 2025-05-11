from utils.datasetup import *
import pandas as pd
import ast

# blob_name="imdb_dataset.csv"
# database=AzureDB()
# database.access_container("csv-files")
# df = database.access_blob_csv(blob_name=blob_name)

class ModelAbstract():
    def __init__(self,df_dataset):
        self.columns = None
        self.dimension_table = None
        self.df = df_dataset

    def dimension_generator_upgraded(self, df, dimension_name: str, column_mapping: dict, use_existing_pk: bool = False, pk_name: str = None):
        # Select and rename
        dim = df[list(column_mapping.keys())].rename(columns=column_mapping)
        dim = dim.drop_duplicates()

        # Rename or generate PK
        if use_existing_pk:
            if not pk_name:
                raise ValueError("Must provide pk_name when using existing primary key")
            # just making sure the pk_name exists after renaming
            if pk_name not in dim.columns:
                raise ValueError(f"Primary key column '{pk_name}' not found in dimension table after renaming.")
        else:
            if not pk_name:
                pk_name = f"{dimension_name}"
            dim[pk_name] = range(1, len(dim) + 1)

        self.dimension_table = dim
        self.name = dimension_name
        self.columns = list(column_mapping.values())
        self.primary_key = pk_name

        
    def load(self):
        if self.dimension_table is not None:
            # Upload dimension table to data warehouse
            database=AzureDB()
            database.upload_dataframe_sqldatabase(f'{self.name}_dim', self.dimension_table, f'{self.name}ID')
        
            # Saving dimension table as separate file
            # self.dimension_table.to_csv(f'./data/{self.name}_dim.csv')
        else:
            print("Please create a dimension table first using dimension_generator") 
            
class MovieTypeDimension:
    def __init__(self):
        self.dimension_table = pd.DataFrame({
            'MovieTypeName': ['short', 'tvMovie', 'tvEpisode', 'movie', 'tvSeries', 'video', 'tvMiniSeries'],
            'MovieTypeID': [1, 2, 3, 4, 5, 6, 7],
            "TypeDescription": ["Short film", "Television movie", "Television episode", "Feature film", "Television series", "Video", "Television miniseries"]
        })
        self.columns = ['MovieTypeName']
        self.name = "MovieType"

    def load(self):
        if self.dimension_table is not None:
            # Upload dimension table to data warehouse
            blob_name="imdb_dataset.csv"
            database=AzureDB()
            database.upload_dataframe_sqldatabase(f'MovieType_dim', self.dimension_table, 'MovieTypeID')
        
            # Saving dimension table as separate file
            # self.dimension_table.to_csv(f'./data/MovieType_dim.csv')
        else:
            print("Please create a dimension table first using dimension_generator") 
       
        
class CountryDimension(ModelAbstract):
    def __init__(self,df_dataset):
        super().__init__(df_dataset)
        # self.dimension_generator('CountryID', ['CountryName', 'Continent', 'LanguagesSpoken','IsEnglishSpeaking'])
        self.dimension_generator_upgraded(
            dimension_name='Country',
            df=df_dataset,
            column_mapping={
                'code': 'CountryID',
                'name': 'CountryName',
                'continent': 'Continent',
                'languages': 'LanguagesSpoken'
                # 'english': 'IsEnglishSpeaking' - derived column
            },
            use_existing_pk=True,
            pk_name='CountryID'
            )

class MovieDimension(ModelAbstract):
    def __init__(self,df_dataset):
        super().__init__(df_dataset)
        # self.dimension_generator('MovieID', ['MovieTitle', 'OriginalTitle', 'PosterString', 'OriginalLanguage'])
        self.dimension_generator_upgraded(
            dimension_name='Movie',
            df=df_dataset,
            column_mapping={
                'tconst': 'MovieID',
                'primaryTitle': 'MovieTitle',
                'originalTitle': 'OriginalTitle',
            },
            use_existing_pk=True,
            pk_name='MovieID'
            )

class GenreDimension(ModelAbstract):
    def __init__(self,df_dataset):
        super().__init__(df_dataset)
        # self.dimension_generator('GenreID', ['GenreName'])
        self.dimension_generator_upgraded(
            dimension_name='Genre',
            df=df_dataset,
            column_mapping={
                'id': 'GenreID',
                'name': 'GenreName',
            },
            use_existing_pk=True,
            pk_name='GenreID'
            )
        
def clean_genres(val):
    if isinstance(val, list):
        return val
    if pd.isna(val) or val == "[]":
        return []
    if isinstance(val, str):
        try:
            parsed = ast.literal_eval(val)
            return parsed if isinstance(parsed, list) else [parsed]
        except:
            return []
    if isinstance(val, int):
        return [val]
    return []

class FactTableGenerator:
    def __init__(self, df_imdb_movies, df_tmdb_movies, df_genres, df_genre_list, df_movie_types, df_countries):
        self.df_imdb_movies = df_imdb_movies
        self.df_tmdb_movies = df_tmdb_movies
        self.df_genres = df_genres
        self.df_genre_list= df_genre_list
        self.df_movie_types = df_movie_types
        self.df_countries = df_countries

    def generate_fact_table(self):
        print("🔄 Starting fact table generation...")

        # Start with a tmdb movies data
        fact_table = self.df_tmdb_movies.copy()
        print(f"✅ Copied TMDb movie data: {len(fact_table)} rows")

        # Merge IMDb fields (rating and titleType)
        fact_table = pd.merge(
            fact_table,
            self.df_imdb_movies[['tconst', 'averageRating', 'region', 'numVotes', 'startYear','titleType']],
            left_on="imdb_id",
            right_on="tconst",
            how="left"
        )
        print("✅ Merged IMDb data")

        # Clean and explode genres
        fact_table["genre_ids"] = fact_table["genre_ids"].apply(clean_genres)
        fact_table = fact_table.explode("genre_ids").reset_index(drop=True)
        print(f"✅ Exploded genres: {len(fact_table)} rows after exploding")
        # print(fact_table[["imdb_id", "genre_ids"]].head(10))  # change to relevant ID column if needed
        print(fact_table.head())  # Show the first few rows

        # Merge with Genre dimension (corrected: match on GenreID not GenreName)
        fact_table = pd.merge(
            fact_table, 
            self.df_genre_list[['GenreID', 'GenreName']],
            left_on="genre_ids", 
            right_on="GenreID", 
            how="left"
        )
        print("✅ Merged with Genre dimension")

        # Normalize region and code for matching
        fact_table["region"] = fact_table["region"].str.lower()
        self.df_countries["CountryID"] = self.df_countries["CountryID"].str.lower()

        # Merge with Country dimension
        fact_table = pd.merge(
            fact_table,
            self.df_countries[['CountryID']],
            left_on="region",
            right_on="CountryID",
            how="left"
        )
        print("✅ Merged with Country dimension")

        # Fill missing country codes
        fact_table["CountryID"] = fact_table["CountryID"].fillna("us")
        print("ℹ️ Filled missing country codes with 'us'")

        # Merge with MovieType dimension
        fact_table = pd.merge(
            fact_table,
            self.df_movie_types[['MovieTypeName', 'MovieTypeID']],
            left_on="titleType",
            right_on="MovieTypeName",
            how="left"
        )
        print("✅ Merged with MovieType dimension")

        # Select relevant columns
        fact_table = fact_table[['tconst', 'GenreID', 'MovieTypeID', 'CountryID',
                                'averageRating', 'numVotes', 'startYear', 'popularity']]

        # Rename columns
        fact_table.rename(columns={
            'tconst': 'MovieID',
            'averageRating': 'Rating',
            'numVotes': 'NumRatings',
            'startYear': 'ReleaseYear',
            'popularity': 'Popularity',
        }, inplace=True)

        print("✅ Renamed columns")

        # Drop rows with missing values
        before_drop = len(fact_table)
        fact_table = fact_table.dropna()
        after_drop = len(fact_table)
        print(f"🧹 Dropped rows with missing values: {before_drop - after_drop} rows removed")

        print(f"🎉 Fact table generation complete: {len(fact_table)} final rows")
        print(fact_table.head())  # Show the first few rows

        return fact_table


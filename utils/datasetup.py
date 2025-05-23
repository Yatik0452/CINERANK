import os, pyodbc
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
import io
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd
import json

load_dotenv()


# Replace these variables with your actual database credentials
username = os.environ.get('USERNAME_AZURE')
password = os.environ.get('PASSWORD')
server = os.environ.get('SERVER')
database = os.environ.get('DATABASE')
account_storage = os.environ.get('ACCOUNT_STORAGE')
connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
tmdb_api_token = os.getenv('TMDB_API_READ_ACCESS_TOKEN')

# Using pyodbc
engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+18+for+SQL+Server')

class AzureDB():
    def __init__(self, local_path = "./data", account_storage = account_storage):
        self.local_path = local_path
        self.account_url = f"https://{account_storage}.blob.core.windows.net"
        self.default_credential = DefaultAzureCredential()
        self.blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        # self.blob_service_client = BlobServiceClient(self.account_url, credential=self.default_credential)
        
    def access_container(self, container_name): 
        # Use this function to create/access a new container
        try:
            # Creating container if not exist
            self.container_client = self.blob_service_client.create_container(container_name)
            print(f"Creating container {container_name} since not exist in database")
            self.container_name = container_name
    
        except Exception as ex:
            print(f"Acessing container {container_name}")
            # Access the container
            self.container_client = self.blob_service_client.get_container_client(container=container_name)
            self.container_name = container_name
            
    def delete_container(self):
        # Delete a container
        print("Deleting blob container...")
        self.container_client.delete_container()
        print("Done")
        
    def upload_blob(self, blob_name, blob_data = None):
        # Create a file in the local data directory to upload as blob to Azure
        local_file_name = blob_name
        upload_file_path = os.path.join(self.local_path, local_file_name)
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=local_file_name)
        print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)

        if blob_data is not None:
            blob_client.create_blob_from_text(container_name=self.container_name, blob_name=blob_name, text=blob_data)
        else:
            # Upload the created file
            with open(file=upload_file_path, mode="rb") as data:
                blob_client.upload_blob(data, overwrite=True)
                
    def list_blobs(self):
        print("\nListing blobs...")
        # List the blobs in the container
        blob_list = self.container_client.list_blobs()
        for blob in blob_list:
            print("\t" + blob.name)  
            
    def download_blob(self, blob_name):
        # Download the blob to local storage
        download_file_path = os.path.join(self.local_path, blob_name)
        print("\nDownloading blob to \n\t" + download_file_path)
        with open(file=download_file_path, mode="wb") as download_file:
                download_file.write(self.container_client.download_blob(blob_name).readall())
                
    def delete_blob(self, container_name: str, blob_name: str):
        # Deleting a blob
        print("\nDeleting blob " + blob_name)
        blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        blob_client.delete_blob()
        
    def access_blob_csv(self, blob_name):
        # Read the csv blob from Azure
        try:
            print(f"Acessing blob {blob_name}")
            
            # df = pd.read_csv(io.StringIO(self.container_client.download_blob(blob_name).readall().decode('utf-8', errors='replace'))) #swap out chars that can't be decoded with a replacement char
            df = pd.read_csv(io.StringIO(self.container_client.download_blob(blob_name).readall().decode('utf-8', errors='ignore')))  #ignore chars that can't be decoded
            return df      
        except Exception as ex:
            print('Exception:')
            print(ex)
    

    def upload_dataframe_sqldatabase(self, blob_name, blob_data, primary_key_name=None):
        print("\nUploading to Azure SQL server as table:\n\t" + blob_name)
        blob_data.to_sql(blob_name, engine, if_exists='replace', index=False)

        # Check if the primary key column exists in the dataframe
        if primary_key_name and primary_key_name not in blob_data.columns:
            raise ValueError(f"Primary key column '{primary_key_name}' not found in '{blob_name}'")

        with engine.connect() as con:
            trans = con.begin()
            

            # Check for country_dim table to apply varchar
            if blob_name.lower() == "country_dim" or blob_name.lower() == "movie_dim":
                # Apply varchar data type for primary key in country_dim
                con.execute(text(f'ALTER TABLE [dbo].[{blob_name}] ALTER COLUMN {primary_key_name} varchar(20) NOT NULL'))
                # Add the primary key constraint
                con.execute(text(f'ALTER TABLE [dbo].[{blob_name}] ADD CONSTRAINT [PK_{blob_name}] PRIMARY KEY CLUSTERED ([{primary_key_name}] ASC);'))
            elif blob_name == "MovieGenreFact_dim":
                # Make sure the composite key columns are NOT NULL first
                con.execute(text('ALTER TABLE [dbo].[MovieGenreFact_dim] ALTER COLUMN MovieID VARCHAR(20) NOT NULL'))
                con.execute(text('ALTER TABLE [dbo].[MovieGenreFact_dim] ALTER COLUMN GenreID INT NOT NULL'))
                con.execute(
                    text(f'ALTER TABLE [dbo].[{blob_name}]'
                        f'ADD CONSTRAINT PK_MovieGenreFact PRIMARY KEY (MovieID, GenreID);')
                )
            else:
                # Apply int/bigint data type for primary key in other dimension tables
                con.execute(text(f'ALTER TABLE [dbo].[{blob_name}] ALTER COLUMN {primary_key_name} INT NOT NULL'))
                 # Add the primary key constraint
                con.execute(text(f'ALTER TABLE [dbo].[{blob_name}] ADD CONSTRAINT [PK_{blob_name}] PRIMARY KEY CLUSTERED ([{primary_key_name}] ASC);'))

            trans.commit()


                
    def append_dataframe_sqldatabase(self, blob_name, blob_data):
        print("\nAppending to table:\n\t" + blob_name)
        blob_data.to_sql(blob_name, engine, if_exists='append', index=False)
    
    def delete_sqldatabase(self, table_name):
        with engine.connect() as con:
            trans = con.begin()
            con.execute(text(f"DROP TABLE [dbo].[{table_name}]"))
            trans.commit()
            
    def get_sql_table(self, query):        
        # Create connection and fetch data using Pandas        
        df = pd.read_sql_query(query, engine)
        # Convert DataFrame to the specified JSON format
        result = df.to_dict(orient='records')
        return result

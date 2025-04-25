from utils.datasetup import *
import pandas as pd

blob_name="ETL_Example_Data.csv"
database=AzureDB()
database.access_container("example-data")
df = database.access_blob_csv(blob_name=blob_name)

class ModelAbstract():
    def __init__(self):
        self.columns = None
        self.dimension_table = None
        
    def dimension_generator(self, name:str, columns:list):
        dim = df[columns]
        dim = dim.drop_duplicates()
        # Creating primary key for dimension table
        dim[f'{name}_id'] = range(1, len(dim) + 1)

        self.dimension_table = dim
        self.name = name
        self.columns = columns
        
    def load(self):
        if self.dimension_table is not None:
            # Upload dimension table to data warehouse
            database.upload_dataframe_sqldatabase(f'{self.name}_dim', blob_data=self.dimension_table)
        
            # Saving dimension table as separate file
            self.dimension_table.to_csv(f'./data/{self.name}_dim.csv')
        else:
            print("Please create a dimension table first using dimension_generator") 
        
class DimStaff(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator('Staff', ['Natural Key Staff ID', 'Name', 'Contact Phone', 'Home Address', "Email"])
            
class DimDate(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator('Date', ['date'])
        
class DimDepartment(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator('Department', ['Department'])

class DimMaintenanceJob(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator('MaintenanceJob', ['work type'])
        
class DimTravelAllowancePolicy(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator('TravelAllowancePolicy', ['vehicle type', 'travelallowanceRate'])
        

class DimWeatherAllowancePolicy(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator('WeatherAllowancePolicy', ['weather', 'temperature', 'weatehr allowance'])
    
        
        

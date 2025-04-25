import os, uuid
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from dotenv import load_dotenv
from utils.datasetup import *
from utils.dimension_classes import *

class MainETL():
    # List of columns need to be replaced
    def __init__(self) -> None:
        self.drop_columns = []
        self.dimension_tables = []
        
    def extract(self, csv_file="ETL_Example_Data.csv"):
        # Step 1: Extract: use pandas read_csv to open the csv file and extract data
        print(f'Step 1: Extracting data from csv file')
        self.fact_table = df
        print(f'We find {len(self.fact_table.index)} rows and {len(self.fact_table.columns)} columns in csv file: {csv_file}')
        print(f'Step 1 finished')
        
    def transform(self):
        # transform data types
        self.fact_table[['travelallowanceRate']] = self.fact_table[['travelallowanceRate']].astype(float)
        int_cols = ['travel distance', 'weatehr allowance', 'work hours', 'job hourly']
        self.fact_table[int_cols] = self.fact_table[int_cols].astype(int)
        self.fact_table[['weather', 'temperature']] = self.fact_table[['weather', 'temperature']].astype(str)
        
        # fetch staff dimension table
        dim_staff = DimStaff()
        self.drop_columns += dim_staff.columns
        self.dimension_tables.append(dim_staff)
             
        # fetch date dimension table
        dim_date = DimDate()
        self.drop_columns += dim_date.columns
        new_dim =  dim_date.dimension_table[['date']]
        # Convert the date column to datetime format
        new_dim['date'] = pd.to_datetime(new_dim['date'], format='%d/%m/%Y')
        self.fact_table['date'] = pd.to_datetime(self.fact_table['date'], format='%d/%m/%Y')
        # Extract the month number and convert it to month name
        new_dim['date'] = new_dim['date'].dt.month_name()
        self.fact_table['date'] = self.fact_table['date'].dt.month_name()
        new_dim = new_dim.drop_duplicates()
        # Modify primary key for date dimension table
        new_dim[f'Date_id'] = range(1, len(new_dim) + 1)
        dim_date.dimension_table = new_dim
        self.dimension_tables.append(dim_date)
        
        # fetch maintenance job dimension table
        dim_job = DimMaintenanceJob()
        self.drop_columns += dim_job.columns
        self.dimension_tables.append(dim_job)
        
        # fetch department dimension table
        dim_department = DimDepartment()
        self.drop_columns += dim_department.columns
        self.dimension_tables.append(dim_department)
        
        # fetch travel dimension table
        dim_travel_allowance = DimTravelAllowancePolicy()
        self.drop_columns += dim_travel_allowance.columns
        self.dimension_tables.append(dim_travel_allowance)
        
        # fetch weather dimension table
        dim_weather_allowance = DimWeatherAllowancePolicy()
        self.drop_columns += dim_weather_allowance.columns
        
        new_weather = dim_weather_allowance.dimension_table[['weather', 'temperature', 'weatehr allowance']]
        new_weather.loc[new_weather['weather'] == "heavy rain", 'weather'] = "rain"
        self.fact_table.loc[self.fact_table['weather'] == "heavy rain", 'weather'] = "rain"
        
        new_weather = new_weather.drop_duplicates()
        
        # Create new foreign key column
        new_weather['WeatherAllowancePolicy_id'] = range(1, len(new_weather) + 1)
        dim_weather_allowance.dimension_table = new_weather
        self.dimension_tables.append(dim_weather_allowance)
        
        # fetch holiday dimension table
        dim_holiday = ModelAbstract()
        dim_holiday.dimension_generator('Holiday', ['isholiday'])
        self.drop_columns += dim_holiday.columns
        self.dimension_tables.append(dim_holiday)
        
        # Get Travel Allowance amount
        travel_allowance_amount = self.fact_table['travel distance'] * self.fact_table['travelallowanceRate']
        self.fact_table['travel allowance amount'] = travel_allowance_amount
        
        # Get Weather Allowance Amount
        weather_allowance_amount = self.fact_table['weatehr allowance']
        self.fact_table['weather allowance amount'] = weather_allowance_amount 
        
        # Get Hourly Work Payment
        work_payment = self.fact_table['work hours'] * self.fact_table['job hourly']
        self.fact_table['work payment'] = work_payment
        
        # Get Total Payment
        self.fact_table['total pay this job'] = work_payment + travel_allowance_amount + weather_allowance_amount
        
        # Replace columns in fact table with respective foreign keys
        for dim in self.dimension_tables:
            self.fact_table = pd.merge(self.fact_table, dim.dimension_table, on=dim.columns, how='left')
        self.fact_table = self.fact_table.drop(columns=self.drop_columns)
        
        print(f'Step 2 finished')
        
    def load(self):
        for table in self.dimension_tables:
            table.load()
        with engine.connect() as con:
            trans = con.begin()
            self.fact_table['Total_Pay_Fact_id'] = range(1, len(self.fact_table) + 1)
            database.upload_dataframe_sqldatabase(f'Total_Pay_Fact', blob_data=self.fact_table)
            
            # self.fact_table['Total_Pay_Fact_id'] = range(len(self.fact_table) + 2, 2*(len(self.fact_table) + 1))
            # database.append_dataframe_sqldatabase(f'Total_Pay_Fact', blob_data=self.fact_table)
            self.fact_table.to_csv('./data/Total_Pay_Fact.csv')

            for table in self.dimension_tables:
                con.execute(text(f'ALTER TABLE [dbo].[Total_Pay_Fact] WITH NOCHECK ADD CONSTRAINT [FK_{table.name}_dim] FOREIGN KEY ([{table.name}_id]) REFERENCES [dbo].[{table.name}_dim] ([{table.name}_id]) ON UPDATE CASCADE ON DELETE CASCADE;'))
            trans.commit()
            
        print(f'Step 3 finished')
        
    def mainLoop(self):    
        # Step 1
        self.extract()
        # Step 2
        self.transform()
        # Step 3
        try:
            database.delete_sqldatabase('Total_Pay_Fact')
            self.load()
        except:
            self.load()
        
def main():
    # create an instance of MainETL
    main = MainETL()
    main.mainLoop()
    
if __name__ == '__main__':
    main()
        
    


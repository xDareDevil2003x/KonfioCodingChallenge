import PythonScript.src.config as config
from PythonScript.src.SparkFunctions import SparkFunctions

import pandas as pd


class DataWarehousing():
    
    # para el ejercicio se utiliza Iceberg
    
    def CreateConnectionDB():
        DBConn = SparkFunctions.CreateLocalSession()
        return DBConn
    
    
    def CleanEnvironment(DBConn):
        SparkFunctions.IcebergExecuteSql(DBConn, f"DROP TABLE IF EXISTS {config.DB_NAME}.{config.TABLE_NAME}")
        
        
    def TestDataframe():
        data = {'id': ['01coin', '0chain', '0dog', '0-knowledge-network'],
                'symbol': ['zoc', 'zcn', '0dog', '0kn'],
                'name': ['01coin', 'Zus', 'Bitcoin Dogs', '0 Knowledge Network'],
                'date': ['02-10-2024 00:11:52', '02-10-2024 00:45:24', '02-10-2024 04:37:53', '02-10-2024 00:35:44'],
                'price': [0.000181, 0.041630, 0.008505, 0.000450],
                'vsCurrency': ['usd', 'usd', 'usd', 'usd']}
        
        df_data = pd.DataFrame(data)
        
        return df_data
    
    
    def CreateNewTable(DBConn):
        script = f"CREATE TABLE {config.DB_NAME}.{config.TABLE_NAME} (  \n"\
                  "  id STRING,  \n"\
                  "  symbol STRING,  \n"\
                  "  name STRING,  \n"\
                  "  date TIMESTAMP,  \n"\
                  "  price decimal(38,10),  \n"\
                  "  vsCurrency STRING   \n"\
                  ") USING iceberg \n"\
                 f"  LOCATION 'warehouse/iceberg/{config.DB_NAME}/{config.TABLE_NAME}' "
        SparkFunctions.IcebergExecuteSql(DBConn, script)


    def GetDfDataTable(DBConn):
        script = f"SELECT id, symbol, name, date, price, vsCurrency FROM {config.DB_NAME}.{config.TABLE_NAME}"
        data = SparkFunctions.IcebergExecuteGetDataSql(DBConn, script)    
        df_data = data.toPandas()
        #print(df_data.to_string())
        return df_data
    
    
    def GetDfSparkDataTable(DBConn):
        script = f"SELECT id, symbol, name, date, price, vsCurrency FROM {config.DB_NAME}.{config.TABLE_NAME}"
        df_data = SparkFunctions.IcebergExecuteGetDataSql(DBConn, script)
        #print(df_data)
        #print(df_data.toPandas().to_string())
        return df_data
    
    
    def InsertDfInTable(DBConn, df_dataHistBitcoins):
        SparkFunctions.IceberInsertDataTableByRows(DBConn, df_dataHistBitcoins)


    def GetDfSparkCalculatedInsights(DBConn, df_data):
        df_result = SparkFunctions.MovingAverage5days(DBConn, df_data)
        #print(df_result.toPandas().to_string())
        return df_result
    

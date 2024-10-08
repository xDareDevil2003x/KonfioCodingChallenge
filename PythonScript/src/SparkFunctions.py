import PythonScript.src.config as config
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import avg, col

""" DOC: 
    * Iceberg: https://medium.com/expedia-group-tech/a-short-introduction-to-apache-iceberg-d34f628b6799#:~:text=HadoopCatalog%20supports%20tables%20that%20are,to%20the%20latest%20metadata%20file.
        HadoopCatalog supports tables that are stored in HDFS or your local file system
        HiveCatalog uses a Hive Metastore to keep track of your Iceberg table by storing a reference to the latest metadata file
"""

class SparkFunctions():
    
    def CreateLocalSession():
        spark = (SparkSession.builder
                             .master("local[*]")
                             .appName("PySparkEnvironmentLocal")
                             .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1') 
                             .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                             # se crea el catalogo de nombe "local" de tipo hadoop
                             .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
                             .config("spark.sql.catalog.local.type", "hadoop")
                             .config("spark.sql.catalog.local.warehouse", "warehouse/iceberg") # ruta donde estaran almacenadas las tablas (path local)
                             .config("spark.sql.catalog.local.default-namespace", "default")
                             # se configura el catalogo "local" como default
                             .config("spark.sql.defaultCatalog", "local")
                             # logs
                             #.config("spark.eventLog.enabled", "true")
                             #.config("spark.eventLog.dir", "logs/iceberg")
                             .getOrCreate() )
        
        return spark
    
    
    def StopLocalSession(spark):
        spark.stop()
    
    
    def IcebergShowDatabases(spark):
        # para que funcione correctamente la funcion .sql, la version de pyspark "print(pyspark.__version__)" debe ser la misma que la de spark "print(spark.version)"
        spark.sql("SHOW CURRENT DATABASE").show()
        
    
    def IcebergShowNamespace(spark):
        spark.sql("SHOW CURRENT NAMESPACE").show()
        

    def IcebergShowCatalogs(spark):
        spark.sql("SHOW CATALOGS").show()
    
    
    def IcebergExecuteSql(spark, script):
        spark.sql(script)


    def IcebergExecuteGetDataSql(spark, script):
        return spark.sql(script)
    
    
    def IcebergGetTableProperties(spark):
        properties = []
        table_properties = spark.sql(f"DESCRIBE EXTENDED {config.DB_NAME}.{config.TABLE_NAME}").collect()
        for row_property in table_properties:
            properties.append(row_property)
            print(row_property)
            
        return properties
    
    
    def IcebergInsertDataTable(spark, df_data):
        print('> se transforma el pandas dataframe a un spark dataframe')
        df = spark.createDataFrame(df_data)
        
        # ALTERNATIVES
        #df_spark.write.format("iceberg").mode("append").saveAsTable(f"{config.DB_NAME}.{config.TABLE_NAME}")
        #df_spark.write.format("iceberg").mode("append").insertInto(f"{config.DB_NAME}.{config.TABLE_NAME}Test")
        #df_spark.write.format("iceberg").mode("append").save(f"warehouse/iceberg/{config.DB_NAME}/{config.TABLE_NAME}")
        
        df.writeTo(f"{config.DB_NAME}.{config.TABLE_NAME}").append()
        
        
    def IceberInsertDataTableByRows(spark, df_data):
        cont = 1
        
        if len(df_data) > 0:
            insert_list = [f"INSERT INTO {config.DB_NAME}.{config.TABLE_NAME} VALUES \n"]
            
            for row in df_data.itertuples():            
                text = f"('{row.id}', '{row.symbol}', '{row.name}', to_timestamp('{row.date}','dd-MM-yyyy HH:mm:ss'), {row.price}, '{row.vsCurrency}')"
                
                if cont < len(df_data):
                    text = text + ', \n'
                else:
                    text = text + '\n'
                    
                insert_list.append(text)    
                cont = cont + 1
                
            query_insert = " ".join(insert_list)
            
            #print("> Query a ejecutar: \n"+query_insert)
            
            SparkFunctions.IcebergExecuteSql(spark, query_insert)
        else:
            print('> El dataframe que se quiere insertar esta vacio')
            

    def MovingAverage5days(spark, df_data):
        windowSpec = Window.partitionBy("id").orderBy(col("date")).rowsBetween(-4, 0)  # el dia actual se cuenta
        df_result = df_data.withColumn("moving_avg", avg(col("price")).over(windowSpec))
        return df_result

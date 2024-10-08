from datetime import datetime
from PythonScript.src.Bitcoins import Bitcoins
from PythonScript.src.PythonFunctions import PythonFunctions
from PythonScript.src.DataWarehousing import DataWarehousing

def main(dateIni = None, dateEnd = None):
    
    print('\n--> Empieza el proceso ('+datetime.now().strftime('%Y-%m-%d %H:%M:%S')+')') 

    print('\n-> Se validan las fechas del proceo')
    dateIni, dateEnd = PythonFunctions.ValDatesProcess(dateIni, dateEnd)

    print('\n-> Se obtienen las diferentes bitcoins')
    df_listBitcoins = Bitcoins.GetDfList()
    # for testing
    #df_listBitcoins = df_listBitcoins[df_listBitcoins['id'] == 'usd']
    df_listBitcoins = df_listBitcoins[:5] 

    print('\n-> Se obtienen el valor historico por rango de fechas de los bitcoins')
    df_dataHistBitcoins = Bitcoins.GetDfHistoricalValueUSD(df_listBitcoins, dateIni, dateEnd)
    #print(df_dataHistBitcoins.to_string())
    # for testing
    #df_dataHistBitcoins = DataWarehousing.TestDataframe()

    print('\n-> Se crea una conexion a una base de datos')
    DBConn = DataWarehousing.CreateConnectionDB()
    
    print('\n-> Se limpia el ambiente')
    DataWarehousing.CleanEnvironment(DBConn)

    print('\n-> Se crea la tabla de almacenamiento de la data')
    DataWarehousing.CreateNewTable(DBConn)
    
    print('\n-> Se inserta la data en la tabla')
    DataWarehousing.InsertDfInTable(DBConn, df_dataHistBitcoins)
    
    print('\n-> Se obtiene la data previamente insertada')
    df_data = DataWarehousing.GetDfSparkDataTable(DBConn)
    
    print('\n-> Se calcula el moving average')
    df_result = DataWarehousing.GetDfSparkCalculatedInsights(DBConn, df_data)
    
    print('\n-> Se genera el export de la data')
    PythonFunctions.ExportDfSparkDataExcel(df_result)
    
    print('\n--> Termina el proceso ('+datetime.now().strftime('%Y-%m-%d %H:%M:%S')+')') 


# Test 1 dia
#main(dateIni = '02-10-2024', dateEnd = '02-10-2024') # DD-MM-YYYY

# ejecucion completa para el ejercicio: first quarter of 2022 ---> genera error por que la version gratuita no tiene mas de 365 dias de historia
#main(dateIni = '01-01-2022', dateEnd = '31-03-2022') # DD-MM-YYYY

# ejecucion completa del año en curso
main(dateIni = '01-01-2024', dateEnd = '31-03-2024') # DD-MM-YYYY



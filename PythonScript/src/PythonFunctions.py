import PythonScript.src.config as config
from datetime import date, datetime, timezone


class PythonFunctions():
    
    def ValDatesProcess(dateIni, dateEnd):
        
        if dateIni == None or dateEnd == None:
            print('> Alguna fecha se ingreso en nulo, por ende se toma el current date por default (dateIni:'+str(dateIni)+' - dateEnd:'+str(dateEnd)+')')
            dateIni = date.today().strftime('%d-%m-%Y')
            dateEnd = date.today().strftime('%d-%m-%Y')
        else:
            try:
                dateIni = datetime.strptime(dateIni, "%d-%m-%Y").strftime('%d-%m-%Y')
                dateEnd = datetime.strptime(dateEnd, "%d-%m-%Y").strftime('%d-%m-%Y')
            except Exception as e:
                print('> se ingresaron fechas con un formato no valido, formato valido dd-mm-yyyy. Se dejo el current date por default. Error: '+str(e))
                dateIni = date.today().strftime('%d-%m-%Y')
                dateEnd = date.today().strftime('%d-%m-%Y')
                
        if datetime.strptime(dateIni, "%d-%m-%Y") > datetime.strptime(dateEnd, "%d-%m-%Y"):
            print('> se ingreso una fecha de inicio ('+str(dateIni)+') superior a la fecha fin ('+str(dateEnd)+'). Se dejo la fecha de inicio igual a la fecha fin')
            dateIni = dateEnd
            
        print('> El proceso se ejecuta para el periodo del '+str(dateIni)+' al '+str(dateEnd))
        
        return dateIni, dateEnd
    
    
    def TrasnformDateToUnixTime(dateIni, dateEnd):
        timestamp_dateIni = datetime.strptime(str(dateIni)+" 00:00:00", "%d-%m-%Y %H:%M:%S")
        timestamp_dateEnd = datetime.strptime(str(dateEnd)+" 23:59:59", "%d-%m-%Y %H:%M:%S")
    
        unixDateIni = timestamp_dateIni.replace(tzinfo=timezone.utc).timestamp()
        unixDateEnd = timestamp_dateEnd.replace(tzinfo=timezone.utc).timestamp()
        
        return unixDateIni, unixDateEnd
    
    
    def ExportDfSparkDataExcel(df_result):
        df_result = df_result.toPandas()
        df_result.to_excel(config.PATH_EXPORT_EXCEL)
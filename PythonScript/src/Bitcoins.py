import time
import pandas as pd
from datetime import datetime, timezone
from PythonScript.src.CoingeckoAPI import CoingeckoAPI
from PythonScript.src.PythonFunctions import PythonFunctions


class Bitcoins():
    
    def GetDfList():
        error_val, error_msn, response = CoingeckoAPI.GetCoinsList()
        
        if error_val == 'false':
            df_list_coins = pd.DataFrame(response)
            df_list_coins = df_list_coins[['id','symbol','name']]
        else:
            df_list_coins = pd.DataFrame(columns=['id','symbol','name'])
            
        #print(df_list_coins)
        print('> se cargaron '+str(len(df_list_coins))+' coins')
        
        return df_list_coins
    
    
    def GetDfHistoricalValueUSD(df_listBitcoins, dateIni, dateEnd):
        ## IMPORTANTE: se puede usar paralelismo (ThreadPoolExecutor) para traer todos las coins
        results = []
        seconds_sleep = 0.1
        vs_currency = 'usd'
        
        unixDateIni, unixDateEnd = PythonFunctions.TrasnformDateToUnixTime(dateIni, dateEnd)
        print('> se transforma la fecha a formato UNIX, fecha inicio:'+str(dateIni)+' ('+str(unixDateIni)+' UTC) - fecha fin:'+str(dateEnd)+' ('+str(unixDateEnd)+' UTC)')
        
        for coin in df_listBitcoins.itertuples():
            print('> Coin a procesar: '+str(coin.id))
            
            error_val, error_msn, response = CoingeckoAPI.GetCoinsDataHistoricalByIdAndRange(coin.id, vs_currency, unixDateIni, unixDateEnd)
            
            if error_val == 'false':
                if 'prices' in response:
                    for req_row in response['prices']:
                        js = {'id':coin.id,
                              'symbol':coin.symbol,
                              'name':coin.name,
                              'date':datetime.fromtimestamp(int(req_row[0])/1000, timezone.utc).strftime('%d-%m-%Y %H:%M:%S'),
                              'price':req_row[1],
                              'vsCurrency':vs_currency}
                        results.append(js)                    
                else:
                    print('> la response no genero el campo prices el cual es necesario para continuar el proceso')
                    
            #print('> se espera '+str(seconds_sleep)+' segundos para no matar el token')
            time.sleep(seconds_sleep)
                    
        df_dataHistBitcoins = pd.DataFrame(results) if len(results) > 0 else pd.DataFrame(columns = ['id','symbol','name','date','price','vsCurrency'])
        
        return df_dataHistBitcoins

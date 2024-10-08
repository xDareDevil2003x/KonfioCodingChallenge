import requests
import PythonScript.src.config as config

""" DOC: 
    * coingecko: https://docs.coingecko.com/reference/introduction
"""

class CoingeckoAPI():
    
    def PingAPI():
        req = requests.get(config.COINGECKO_URL+'/ping?x_cg_demo_api_key='+config.COINGECKO_KEY)
        print(req.json())


    def GetCryptocurrencyData():
        headers = {'accept': 'application/json',
                   'x_cg_demo_api_key': config.COINGECKO_KEY}
        
        req = requests.get(config.COINGECKO_URL+'/global',
                           headers = headers)
        
        #print(req.json())
        
        error_val = 'false'
        error_msn = ''
        response = None

        if req.status_code != 200:
            error_val = 'true'
            error_msn = req.json()
            print('--> Error al obtener las Crypto currency: '+str(error_msn))
        else:
            response = req.json()
            
        return error_val, error_msn, response
    
    
    def GetCoinsList():
        headers = {'accept': 'application/json',
                   'x_cg_demo_api_key': config.COINGECKO_KEY}
        
        req = requests.get(config.COINGECKO_URL+'/coins/list',
                           headers = headers)
        
        #print(req.json())
        
        error_val = 'false'
        error_msn = ''
        response = None

        if req.status_code != 200:
            error_val = 'true'
            error_msn = req.json()
            print('--> Error al obtener las lista de coins: '+str(error_msn))
        else:
            response = req.json()
            
        return error_val, error_msn, response
    
    
    def GetCoinsDataCurrentlyById(coinId):
        headers = {'accept': 'application/json',
                   'x_cg_demo_api_key': config.COINGECKO_KEY}
        
        req = requests.get(config.COINGECKO_URL+'/coins/'+str(coinId),
                           headers = headers)
        
        #print(req.json())
        
        error_val = 'false'
        error_msn = ''
        response = None

        if req.status_code != 200:
            error_val = 'true'
            error_msn = req.json()
            print('--> Error al obtener las lista de coins: '+str(error_msn))
        else:
            response = req.json()
            
        return error_val, error_msn, response
    
    
    def GetCoinsDataHistoricalByIdAndDate(coinId, dateIn):
        headers = {'accept': 'application/json',
                   'x_cg_demo_api_key': config.COINGECKO_KEY}
        
        # date in format dd-mm-yyyy
        parameters = {'date': dateIn}
        
        req = requests.get(config.COINGECKO_URL+'/coins/'+str(coinId)+'/history',
                           headers = headers,
                           params = parameters)
        
        #print(req.json())
        
        error_val = 'false'
        error_msn = ''
        response = None

        if req.status_code != 200:
            error_val = 'true'
            error_msn = req.json()
            print('--> Error al obtener las lista de coins: '+str(error_msn))
        else:
            response = req.json()
            
        return error_val, error_msn, response
    

    def GetCoinsDataHistoricalByIdAndRange(coinId, vs_currency, unixDateIni, unixDateEnd):
        headers = {'accept': 'application/json',
                   'x_cg_demo_api_key': config.COINGECKO_KEY}
        
        # from and to is a date in UNIX format 
        parameters = {'vs_currency': vs_currency,
                      'from':unixDateIni,
                      'to':unixDateEnd}
        
        req = requests.get(config.COINGECKO_URL+'/coins/'+str(coinId)+'/market_chart/range',
                           headers = headers,
                           params = parameters)
        
        #print(req.json())
        
        error_val = 'false'
        error_msn = ''
        response = None

        if req.status_code != 200:
            error_val = 'true'
            error_msn = req.json()
            print('--> Error al obtener las lista de coins: '+str(error_msn))
        else:
            response = req.json()
            
        return error_val, error_msn, response

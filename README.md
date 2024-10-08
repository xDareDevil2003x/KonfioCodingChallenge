# Coding Challenge - Bitcoin Price
Develop an automated and scalable process to obtain the 5-day moving average of Bitcoin’s price during the first quarter of 2022.

## Configuracion del Environment utilizado
- Python 3.12.7
- Java "23" 2024-09-07
- Java JDK-21
PD: se aconseja realizar esta guia "https://medium.com/analytics-vidhya/installing-and-using-pyspark-on-windows-machine-59c2d64af76e" para instalar pyspark en windows 

## Ejecucion del proceso
1. Como primer paso se debe añadir en el sys.path la ruta donde va a estar la carpeta de ejeccion, esto con el fin de utilizar las librerias mas facilmente. Esto se realiza con el siguiente script:
```
import sys
path_utils = '< Path donde se va a colocar la carpeta de ejecucion C:\\....> '
sys.path.append(path_utils)
print(sys.path)
```

2. Luego se debe configurar los parametros del archivo src/Config.py con las credenciales que utilizara el proceso para su ejecucion.
- COINGECKO_URL = 'Url de la api de Coingecko https://docs.coingecko.com/reference/introduction'
- COINGECKO_KEYLABEL = 'nombre de la key creada en coingecko, se aconseja crear un perfil free para lab'
- COINGECKO_KEY = 'identificador de la key generada en coingecko'
- DB_NAME = 'nombre de la base de datos, para el ejecicio, se utilizo Iceberg como BD'
- TABLE_NAME = 'nombre de la tabla donde se va a almacenar la data generada'
- PATH_EXPORT_EXCEL = 'path donde se almacerara el export de la data local'

3. Por ultimo, cuando ya se tenga todo configurado, se debe ir al archivo main.py en la raiz de la carpeta de ejecucion y ejecutar el proceso. El main toma recibe dos parametros String referentes a la fecha de inicio de ejecucion del programa y a la fecha de finalizacion del mismo, se deben colocar en formato DD-MM-YYYY

*PD: en el mismo codigo se adicionaron comentarios con respecto a la ejecucion del proceso. *
*En el main.py se adicionaron al final llamados al proceso de una fecha determinada para ser utilizados como testing*

## ASSUMPTIONS
- Se asume que el proceso se ejecuta en una maquina Windows 11
- Al ser un ejercicio de Lab, la API de Coingecko utilizada tiene restricciones de la data procesada

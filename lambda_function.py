from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import mysql.connector
from datetime import datetime


def lambda_handler(event, context):
    # conexion al api:
    url = 'https://sandbox-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest'
    parameters = {
        'symbol': 'BTC,ETH'
    }
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': 'b54bcf4d-1bca-4e8e-9a24-22ff2c3d462c',
    }

    session = Session()
    session.headers.update(headers)

    try:
        response = session.get(url, params=parameters)
        data = json.loads(response.text)
        return saveResponse(data)  # llamo a la funcion que guarda los datos en la BD
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        return {
            "statusCode": 500,
            "body": "Error al obtener los registros desde la base de datos ".format(e)
        }


def saveResponse(data):
    db_config = {
        'host': 'ef.clo4rqurd8kf.us-east-2.rds.amazonaws.com',
        'user': 'admin',
        'password': '981129()Sair98',
        'database': 'CoinMarketCap.'
    }
    data_to_insert = []

    # Extraer los campos necesarios y agregarlos a la lista data_to_insert
    for key, value in data["data"].items():
        symbol = value["symbol"]
        price = value["quote"]["USD"]["price"]
        date_added_str = value["date_added"]
        last_updated_str = value["last_updated"]
        symbol_id = value["id"]

        # Convertir las fechas de formato string a objetos datetime
        date_added = datetime.strptime(date_added_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        last_updated = datetime.strptime(
            last_updated_str, "%Y-%m-%dT%H:%M:%S.%fZ")

        data_to_insert.append(
            (symbol, price, date_added, last_updated, symbol_id))

    # Establecer la conexión con la base de datos
    connection = mysql.connector.connect(**db_config)

    try:
        cursor = connection.cursor()

        # Consulta para insertar los datos en la tabla "tu_tabla"
        insert_query = "INSERT INTO updates (symbol, price, date_added, last_updated, id) VALUES (%s, %s, %s, %s, %s)"

        # Ejecutar la inserción de datos en la base de datos
        cursor.executemany(insert_query, data_to_insert)
        connection.commit()
        # print(data)

    except mysql.connector.Error as error:
        return {
            "statusCode": 500,
            "body": "Error al obtener los registros desde la base de datos ".format(error)
        }

    finally:
        # Cerrar el cursor y la conexión
        if connection.is_connected():
            cursor.close()
            connection.close()
            return getDbData(db_config)
            # print("Conexión a la base de datos cerrada")


def getDbData(db_config):
    # Nuevamente se establece la conexión con la base de datos
    connection = mysql.connector.connect(**db_config)

    try:
        cursor = connection.cursor()

        # Consulta SELECT para traer todos los registros de la tabla "tu_tabla"
        select_query = "SELECT * FROM updates"

        # Ejecutar la consulta SELECT
        cursor.execute(select_query)

        # Obtener todos los registros
        records = cursor.fetchall()

        # Convertir los registros a formato JSON
        records_json = []
        for record in records:
            # Convertir cada registro a un diccionario
            record_dict = [
                {
                    "symbol": record[0],
                    "price": record[1],
                    "date_added": record[2].isoformat(),
                    "last_updated": record[3].isoformat(),
                    "id": record[4]
                }]
            records_json.append(record_dict)

       # Cerrar el cursor y la conexión
        cursor.close()
        connection.close()

        # Crear la respuesta HTTP con el resultado JSON
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps(records_json, indent=2)
        }

    except mysql.connector.Error as error:
         # Crear una respuesta de error en caso de que falle la consulta
        response = {
            "statusCode": 500,
            "body": "Error al obtener los registros desde la base de datos ".format(error)
        }
        return response

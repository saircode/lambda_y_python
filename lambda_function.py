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
        print(data)
        saveResponse(data)  # llamo a la funcion que guarda los datos en la BD
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e)


def saveResponse(data):
    db_config = {
        'host': 'ef.clo4rqurd8kf.us-east-2.rds.amazonaws.com',
        'user': 'admin',
        'password': '981129()Sair98',
        'database': 'CoinMarketCap.'
    }
    data_to_insert = []

    connection = mysql.connector.connect(**db_config)

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
        print("Datos insertados correctamente")

    except mysql.connector.Error as error:
        print("Error al insertar datos: {}".format(error))

    finally:
        # Cerrar el cursor y la conexión
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("Conexión a la base de datos cerrada")

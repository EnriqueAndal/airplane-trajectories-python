import sys
import os
import json
import requests
import sqlite3
import time


"""main()
The orchestra of the program. This function executes the complete data ingestion process by:
1. Loading OpenSky API credentials.
2. Authenticating with the API to obtain an access token.
3. Requesting live flight state vectors.
4. Connecting to a SQLite database provided via command-line argument.
5. Creating the required tables if they do not exist.
6. Storing flight data snapshots for planes originating from Mexico.

The program exits early if any critical step fails."""
def main():

    # Obtain API credentials from a local file
    opensky_client_id, opensky_client_secret = obtener_credenciales_de_json()
    if not opensky_client_id or not opensky_client_secret:
        exit()

    # OpenSky authentication
    token = obtener_token(opensky_client_id, opensky_client_secret)
    if not token:
        exit()

    # Calling the main endpoint to retrieve flight status
    todos_los_aviones = llamar_al_endpoint(token, params = None)
    if not todos_los_aviones or "states" not in todos_los_aviones:
        print("No se pudo obtener información de vuelos.")
        exit()

    # Validation of the command-line argument (path to the database)
    if len(sys.argv) != 2:
        print("El formato de tu ruta solo debería ser: python Airplanes_ingest_flight_snapshots.py <path_to_sqlite_db>")
        exit()

    database_exploratoria_path = sys.argv[1]
    print(f"Estas usando la database: {database_exploratoria_path}")

    # Connecting to the SQLite database
    conn_exp = sqlite3.connect(database_exploratoria_path)
    cur = conn_exp.cursor()

    # Creating the protagonist tables
    crear_tabla_principal(cur)

    # Inserting the data obtained in each call to the API to the tables
    llenar_tabla_en_sqlite(todos_los_aviones, conn_exp, cur)

    # Close connection to the database
    conn_exp.close()


"""obtener_credenciales_de_json()
Loads OpenSky API credentials from a local credentials.json file.

The file must be located in the same directory as this script and contain the keys: 'clientId' and 'clientSecret'.

Returns:
    tuple[str | None, str | None]: (client_id, client_secret)"""
def obtener_credenciales_de_json():
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        credentials_path = os.path.join(base_dir, "credentials.json")

        with open(credentials_path) as texto_json_externo:
            texto_json_en_python = json.load(texto_json_externo)

            _opensky_client_id = texto_json_en_python["clientId"]
            _opensky_client_secret = texto_json_en_python["clientSecret"]

        return _opensky_client_id, _opensky_client_secret

    except FileNotFoundError:
        print("El archivo credentials.json no se encuentra en la carpeta del proyecto.")
        return None, None


"""obtener_token(_opensky_client_id, _opensky_client_secret)
Requests an OAuth2 access token from the OpenSky Network API using the Client Credentials flow.

Args:
    _opensky_client_id (str): OpenSky API client ID.
    _opensky_client_secret (str): OpenSky API client secret.

Returns:
    str | None: Access token if successful, otherwise None."""
def obtener_token(_opensky_client_id, _opensky_client_secret):
    try:
        url_token = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }

        data = {
            "grant_type": "client_credentials",
            "client_id": _opensky_client_id,
            "client_secret": _opensky_client_secret
        }

        try:
            response = requests.post(url_token, headers = headers, data = data, timeout = 10)
            response = response.json()

            _token = response["access_token"]
            return _token

        except requests.exceptions.Timeout:
            print("La petición tardó demasiado desde la función obtener_token y fue cancelada.")
            return None

    except requests.exceptions.ConnectionError:
        print("No fue posible conectarse a OpenSky Network por falta de conexión o porque la red está bloqueando el acceso al servidor.")
        return None


""" llamar_al_endpoint(_token, params=None)
Requests the current vector states of all aircraft accessible to the OpenSky API, using an authorization header 
with a previously obtained OAuth2 token.

Args:
    _token (str): Access token obtained from obtener_token().
    params (dict | None): Optional query parameters to filter the request (e.g., geographic bounds). 
                          If None, no filters are applied.

Returns:
    dict | None: JSON response containing the aircraft states, or None if the request fails."""
def llamar_al_endpoint(_token, params = None):
    autorizacion_url = "https://opensky-network.org/api/states/all"

    headers = {
        "Authorization": f"Bearer {_token}"
    }

    try:
        codigo_de_resppuesta = requests.get(autorizacion_url, headers = headers, params = params, timeout = 10)

        aviones_en_json = codigo_de_resppuesta.json()

        return aviones_en_json

    except requests.exceptions.Timeout:
        print("La petición tardó demasiado en la función llamar_al_endpoint y fue cancelada.")
        return None


"""crear_tabla_principal(_cur)
Creates the main database tables required for storing aircraft and snapshot data. If the tables already exist, 
they are left unchanged (no data is deleted or modified).

Args:
    _cur: SQLite cursor used to execute SQL statements.

Returns:
    None"""
def crear_tabla_principal(_cur):
    _cur.executescript(""" 
    CREATE TABLE IF NOT EXISTS Avion_fisico
    (
        id                INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        icao              TEXT UNIQUE,
        call_sign         TEXT,
        pais_origen       TEXT
    );
        
    CREATE TABLE IF NOT EXISTS Snapshots
    (
        id                INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        Avion_fisico_id   INTEGER,
        posicion_temporal INTEGER,
        tiempo_de_captura INTEGER,
        longitud          FLOAT,
        latitud           FLOAT,
        altitud           FLOAT
    );
    """)


"""llenar_tabla_en_sqlite(diccionario, _conn_exp, _cur)
Accesses the "states" vector contained in the dictionary returned by llamar_al_endpoint(_token, params=None) and 
iterates plane by plane, filtering only those whose country of origin is Mexico.

For each valid plane, it inserts its physical information into the Avion_fisico table (avoiding duplicates using 
INSERT OR IGNORE) and stores a temporal snapshot of its position in the Snapshots table.
Finally, all changes are committed to the SQLite database.

Args:
    diccionario (dict): Dictionary returned by llamar_al_endpoint(_token, params=None), must contain a "states" key.
    _conn_exp: SQLite connection object created in main().
    _cur: SQLite cursor used to execute SQL statements.

Returns:
    None"""
def llenar_tabla_en_sqlite(diccionario, _conn_exp, _cur):
    if not diccionario["states"]:
        print("Hay un error con el formato del archivo JSON que ha provocado que no podamos llenar la tabla.")
        return None

    else:
        tiempo_de_captura = int(time.time())

        for avion in diccionario["states"]:
            if avion[2] != "Mexico":
                continue

            icao = avion[0]
            call_sign = avion[1]
            pais_origen = avion[2]
            posicion_temporal = avion[3]
            longitud = avion[5]
            latitud = avion[6]
            altitud = avion[7]

            _cur.execute("""INSERT OR IGNORE INTO Avion_fisico (icao, call_sign, pais_origen)
                            VALUES (?, ?, ?)""",
                           (icao, call_sign, pais_origen))

            _cur.execute("""SELECT id FROM Avion_fisico WHERE icao = ? """, (icao,))

            Avion_fisico_id = _cur.fetchone()[0]

            _cur.execute("""INSERT INTO Snapshots (Avion_fisico_id, posicion_temporal, tiempo_de_captura, longitud, latitud, altitud)
                            VALUES (?, ?, ?, ?, ?, ?)""",
                            (Avion_fisico_id, posicion_temporal, tiempo_de_captura, longitud, latitud, altitud))

        _conn_exp.commit()
        print("Las tablas ha sido llenada con éxito")

# aThis line allows code to run only when the script is executed, not when it's imported.
if __name__ == "__main__":
    main()
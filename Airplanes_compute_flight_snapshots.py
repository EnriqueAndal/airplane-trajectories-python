import sys
from math import radians, sin, cos, acos
import sqlite3

"""main()
This function orchestrates the post-processing stage of the OpenSky flight pipeline by:
1. Validating the command-line argument that specifies the path to the SQLite database.
2. Establishing a connection to the provided SQLite database.
3. Computing the great-circle distance (in kilometers) between the first and last observed positions of each valid 
    aircraft trajectory stored in the database.
4. Persisting the computed distances back into the corresponding table.
5. Closing the database connection gracefully.

The program exits early if the database path is not provided or if no valid trajectories are found to process."""
def main():

    # Validation of the command-line argument (path to the database)
    if len(sys.argv) != 2:
        print("El formato de tu ruta solo debería ser: python Airplanes_compute_flight_snapshots.py <path_to_sqlite_db>")
        exit()

    database_retrieval_path = sys.argv[1]
    print(f"Estas usando la database: {database_retrieval_path}")

    # Connecting to the SQLite database
    conn_retr = sqlite3.connect(database_retrieval_path)
    cur_retr = conn_retr.cursor()

    # Calculate the kilometric distance between the first and last received position of the aircraft
    calcular_distancias_trayectorias(conn_retr, cur_retr)

    # Close connection to the database
    conn_retr.close()

"""calcular_distancias_trayectorias(_conn_retr, _cur_retr)
Computes the great-circle distance (in kilometers) between the initial and final geographic positions of each aircraft 
trajectory stored in the SQLite table `Trayectorias_validas`.

The function retrieves, via an SQL subquery, the starting and ending latitude and longitude for each aircraft 
(`Avion_fisico_id`). 
For each trajectory, it calculates the distance using the spherical law of cosines and updates the corresponding record 
in the database by filling the column `distancia_inicio_a_fin_km`.

Args:
    _conn_retr: SQLite connection object created in main().
    _cur_retr: Cursor used to execute SQL queries.

Returns:
    None."""
def calcular_distancias_trayectorias(_conn_retr, _cur_retr):
    # SQL subquery to retrieve initial and final coordinates for each valid trajectory
    sql_subquery = """SELECT Avion_fisico_id, lat_inicio, lon_inicio, lat_fin, lon_fin
                      FROM Trayectorias_validas"""

    # Execute the query and fetch all results to the table
    _cur_retr.execute(sql_subquery)
    lista_por_avion = _cur_retr.fetchall()

    # If the table is empty, there are no valid trajectories to process
    if not lista_por_avion:
        print("La database esta vacía, lo que implica que no hubo trayectorias válidas para procesar.")
        return None

    # Iterate over each aircraft trajectory
    for avion in lista_por_avion:
        Avion_id_en_python = avion[0]
        lat_inicio = radians(avion[1])
        lon_inicio = radians(avion[2])
        lat_fin = radians(avion[3])
        lon_fin = radians(avion[4])

        # Compute the great-circle distance using the spherical law of cosines
        distancia_km = round(6371 * acos(sin(lat_inicio) * sin(lat_fin) + cos(lat_inicio) * cos(lat_fin) * cos(lon_inicio - lon_fin)), 2)

        # Update the distance column for the current aircraft trajectory
        _cur_retr.execute("""UPDATE Trayectorias_validas
                             SET distancia_inicio_a_fin_km = ?
                              WHERE Avion_fisico_id = ?""", (distancia_km, Avion_id_en_python))

    # Persist changes to the database
    _conn_retr.commit()
    print("Has agregado la columna con nuevos datos exitosamente")

# This line allows code to run only when the script is executed, not when it's imported.
if __name__ == "__main__":
    main()
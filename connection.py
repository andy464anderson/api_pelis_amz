import psycopg2

#creamos la funcion para conectarnos a la base de datos

def conectar():
    #creamos la conexion a la base de datos
    conn = psycopg2.connect(
        host="database-1.chc8yiqnazpv.eu-north-1.rds.amazonaws.com",
        database="usuarios_3bbx",
        user="postgres",
        password="0quGIz37OzqNznc5nSTCvrWijLFoueZu",
        sslmode="require"
    )
    #devolvemos la conexion
    return conn
# creamos una api utilizando fastAPI
# utilizamos el spark session para leer el archivo csv

# Importamos las librerias necesarias
from datetime import date
from connection import conectar
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from fastapi.responses import FileResponse
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, when
# from pyspark.sql.types import *
from fastapi.responses import JSONResponse
# from pyspark.sql.types import *
# from pyspark.sql.functions import *
from typing import List
import pandas as pd
import ast
import json
# import imdb


# creamos la clase Pelicula
class Pelicula(BaseModel):
    adult: bool
    genres: str
    id: int
    imdb_id: str
    title: str
    overview: str
    release_date: str
    runtime: float
    cast: str
    crew: str
    keywords: str
    poster: str


# # creamos la sesión de Spark
# spark = SparkSession.builder.appName("Api_De_Peliculas").getOrCreate()

# leemos el archivo csv
df = pd.read_parquet("peliculas.parquet")

# convertimos el dataframe de pandas a una lista de diccionarios de la clase Pelicula
lista = [Pelicula(**row) for row in df.to_dict("records")]



# creamos la api
app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost:8080",
    "http://localhost:3000"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# creamos la ruta para acceder a la api
@app.get("/")
async def read_main():
    return JSONResponse(content={"message": "Bienvenido a la api de peliculas"})


@app.get("/peliculas")
async def get_peliculas():
    # las peliculas se devuelven en formato json
    return lista


# creamos la ruta para acceder a una pelicula en concreto
@app.get("/peliculas/{id}")
async def get_pelicula(id: int):
    dfIdPelicula = df.loc[lambda df: df['id'] == id]
    # convertimos el dataframe a una lista de diccionarios
    if dfIdPelicula.empty:
        return JSONResponse(content={"message": "No existe la pelicula"})
    # devolvemos la pelicula en formato json
    return [Pelicula(**row) for row in dfIdPelicula.to_dict("records")]



# creamos la ruta para acceder a las peliculas de un genero en concreto
@app.get("/peliculas/genero/{genero}")
async def get_peliculas_genero(genero: str):
    # filtramos el dataframe de pandas por el genero de la pelicula pero el genero es una lista
    # por lo que tenemos que filtrar por cada elemento de la lista y covertir en mayusculas
    df2 = df[df['genres'].apply(
        lambda x: genero.upper() in x.upper())]
    # convertimos el dataframe a una lista de diccionarios
    lista = [Pelicula(**row) for row in df2.to_dict("records")]
    # devolvemos las peliculas en formato json
    return lista


# creamos la ruta para acceder a las peliculas de un año en concreto
@app.get("/peliculas/fecha/{fecha}")
async def get_peliculas_fecha(fecha: date):
    # filtramos el dataframe por el año de la pelicula
    df2 = df.loc(df.release_date == fecha)
    # devolvemos las peliculas en formato json
    return [Pelicula(**row) for row in df2.to_dict("records")]

# -------------------------------------------------------------------------------
# creamos al api para los usuarios

# creamos la clase Usuario


class Usuario(BaseModel):
    id: int
    correo: str
    clave: str
    rol: str
    nombre_usuario: str
    nombre_completo: str


@app.get("/usuarios")
async def get_usuarios():
    # creamos la conexion a la base de datos
    conn = conectar()
    # creamos el cursor
    cursor = conn.cursor()
    # ejecutamos la consulta
    cursor.execute("SELECT * FROM usuario")
    # obtenemos los resultados
    usuarios = cursor.fetchall()
    # cerramos la conexion
    conn.close()
    cursor.close()
    # convertimos los resultados en una lista de diccionarios
    lista_usuarios = []
    for usuario in usuarios:
        lista_usuarios.append({
            "id": usuario[0],
            "correo": usuario[1],
            "clave": usuario[2],
            "rol": usuario[3],
            "nombre_usuario": usuario[4],
            "nombre_completo": usuario[5]
        })

    # devolvemos los usuarios en formato json
    return lista_usuarios


@app.get("/usuario/{id}")
async def get_usuario(id):
    conn = conectar()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM usuario WHERE id = %s", (id,))
    usuario = cursor.fetchone()
    conn.close()
    cursor.close()
    lista_usuarios = {
        "id": usuario[0],
        "correo": usuario[1],
        "clave": usuario[2],
        "rol": usuario[3],
        "nombre_usuario": usuario[4],
        "nombre_completo": usuario[5]
    }

    return lista_usuarios


@app.get("/perfil/{nombre_usuario}")
async def get_usuario(nombre_usuario):
    conn = conectar()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM usuario WHERE nombre_usuario = %s", (nombre_usuario,))
    usuario = cursor.fetchone()
    conn.close()
    cursor.close()
    usuarioPerfil = {
        "id": usuario[0],
        "correo": usuario[1],
        "clave": usuario[2],
        "rol": usuario[3],
        "nombre_usuario": usuario[4],
        "nombre_completo": usuario[5]
    }

    return usuarioPerfil


@app.get("/usuario/correo/{correo}/{clave}")
async def get_usuario(correo: str, clave: str):
    conn = conectar()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM usuario WHERE correo = %s and clave = %s", (correo, clave))
    usuario = cursor.fetchone()

    if cursor.rowcount == 0:
        return {"error": "Usuario no encontrado"}

    conn.close()
    cursor.close()
    lista_usuarios = {
        "id": usuario[0],
        "correo": usuario[1],
        "clave": usuario[2],
        "rol": usuario[3],
        "nombre_usuario": usuario[4],
        "nombre_completo": usuario[5]
    }

    return lista_usuarios


@app.post("/usuario")
async def post_usuario(usuario: Usuario):
    conn = conectar()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO usuario (correo, nombre_completo, nombre_usuario, clave, rol) VALUES (%s, %s, %s, %s, %s)",
                   (usuario.correo, usuario.nombre_completo, usuario.nombre_usuario, usuario.clave, usuario.rol))
    conn.commit()
    conn.close()
    cursor.close()
    return usuario


@app.put("/usuario/{id}")
async def put_usuario(id: int, usuario: Usuario):
    conn = conectar()
    cursor = conn.cursor()
    cursor.execute("UPDATE usuario SET correo = %s, nombre_completo = %s, nombre_usuario = %s, clave = %s, rol = %s WHERE id = %s",
                   (usuario.correo, usuario.nombre_completo, usuario.nombre_usuario, usuario.clave, usuario.rol, id))
    conn.commit()
    conn.close()
    cursor.close()
    return usuario


@app.delete("/usuario/{id}")
async def delete_usuario(id: int):
    conn = conectar()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM usuario WHERE id = %s", (id,))
    conn.commit()
    conn.close()
    cursor.close()
    return {"message": "Usuario eliminado"}


@app.delete("/listasUsuario/{id}")
async def delete_listas_usuario(id: int):
    conn = conectar()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM lista WHERE usuario_id = %s", (id,))
    conn.commit()
    conn.close()
    cursor.close()
    return {"message": "Listas eliminadas"}


@app.delete("/reviewsUsuario/{id}")
async def delete_reviews_usuario(id: int):
    conn = conectar()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM review WHERE id_usuario = %s", (id,))
    conn.commit()
    conn.close()
    cursor.close()
    return {"message": "Reviews eliminadas"}


@app.delete("/seguidoresUsuario/{id}")
async def delete_seguidores_usuario(id: int):
    conn = conectar()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM seguidor WHERE id_usuario_seguidor = %s", (id,))
    conn.commit()
    conn.close()
    cursor.close()
    return {"message": "Seguidores eliminados"}


@app.delete("/seguidosUsuario/{id}")
async def delete_seguidos_usuario(id: int):
    conn = conectar()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM seguidor WHERE id_usuario_seguido = %s", (id,))
    conn.commit()
    conn.close()
    cursor.close()
    return {"message": "Seguidos eliminados"}


# --------------------------------------------------------------------------------
# creamos la api para los seguidores

# creamos la clase Seguidor
class Seguidor(BaseModel):
    id_seguidor: int
    id_usuario_seguidor: int
    id_usuario_seguido: int


class newSeguidor(BaseModel):
    id_usuario_seguidor: int
    id_usuario_seguido: int

# creamos la api para los seguidores


@app.get("/seguidores")
async def get_seguidores():
    conn = conectar()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM seguidor")
    seguidores = cursor.fetchall()
    conn.close()
    cursor.close()
    lista_seguidores = []
    for seguidor in seguidores:
        lista_seguidores.append({
            "id_seguidor": seguidor[0],
            "id_usuario_seguidor": seguidor[1],
            "id_usuario_seguido": seguidor[2]
        })

    return lista_seguidores


# traemos los seguidores de un usuario
@app.get("/seguidores/{id}")
async def get_seguidor(id):
    conn = conectar()
    cursor = conn.cursor()
    cursor.execute('SELECT s.id_seguidor, s.id_usuario_seguidor, s.id_usuario_seguido, u.rol, u.nombre_usuario, u.nombre_completo FROM seguidor s join usuario u on s.id_usuario_seguidor = u.id WHERE id_usuario_seguido = %s', (id,))
    seguidores = cursor.fetchall()
    conn.close()
    cursor.close()
    lista_seguidores = []
    for seguidor in seguidores:
        lista_seguidores.append({
            "id_seguidor": seguidor[0],
            "id_usuario_seguidor": seguidor[1],
            "id_usuario_seguido": seguidor[2],
            "rol": seguidor[3],
            "nombre_usuario": seguidor[4],
            "nombre_completo": seguidor[5]
        })

    return lista_seguidores

# traemos los seguidos de un usuario


@app.get("/seguidos/{id}")
async def get_seguidor(id):
    conn = conectar()
    cursor = conn.cursor()
    cursor.execute('SELECT s.id_seguidor, s.id_usuario_seguidor, s.id_usuario_seguido, u.rol, u.nombre_usuario, u.nombre_completo FROM seguidor s join usuario u on s.id_usuario_seguido = u.id WHERE id_usuario_seguidor = %s', (id,))
    seguidores = cursor.fetchall()
    conn.close()
    cursor.close()
    lista_seguidores = []
    for seguidor in seguidores:
        lista_seguidores.append({
            "id_seguidor": seguidor[0],
            "id_usuario_seguidor": seguidor[1],
            "id_usuario_seguido": seguidor[2],
            "rol": seguidor[3],
            "nombre_usuario": seguidor[4],
            "nombre_completo": seguidor[5]
        })

    return lista_seguidores

# insertamos un seguidor


@app.post("/seguidor")
async def post_seguidor(seguidor: newSeguidor):
    try:
        conn = conectar()
        cursor = conn.cursor()
        cursor.execute("INSERT INTO seguidor (id_usuario_seguidor, id_usuario_seguido) VALUES (%s, %s)",
                       (seguidor.id_usuario_seguidor, seguidor.id_usuario_seguido))
        conn.commit()
        print("Datos del seguidor insertados correctamente")
    except Exception as e:
        print("Error al insertar datos del seguidor:", e)
        conn.rollback()
    finally:
        conn.close()
        cursor.close()
    return seguidor


# eliminamos un seguidor
@app.delete("/seguidor/{id_usuario_seguidor}/{id_usuario_seguido}")
async def delete_seguidor(id_usuario_seguidor: int, id_usuario_seguido: int):
    conn = conectar()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM seguidor WHERE id_usuario_seguidor = %s and id_usuario_seguido = %s",
                   (id_usuario_seguidor, id_usuario_seguido))
    conn.commit()
    conn.close()
    cursor.close()
    return {"message": "Seguidor eliminado"}




@app.get("/cercanas/{id}")
async def get_cercanas(id):
    conn = conectar()
    cursor = conn.cursor()
    cursor.execute('SELECT DISTINCT u.id, u.nombre_usuario, u.nombre_completo FROM usuario u JOIN seguidor s1 ON u.id = s1.id_usuario_seguido JOIN seguidor s2 ON s1.id_usuario_seguidor = s2.id_usuario_seguido WHERE s2.id_usuario_seguidor = %s AND u.id NOT IN (SELECT id_usuario_seguido FROM seguidor WHERE id_usuario_seguidor = %s) LIMIT 6', (id,id,))
    seguidores = cursor.fetchall()
    conn.close()
    cursor.close()
    lista_seguidores = []
    for seguidor in seguidores:
        lista_seguidores.append({
            "id_usuario": seguidor[0],
            "nombre_usuario": seguidor[1],
            "nombre_completo": seguidor[2],
        })

    return lista_seguidores

@app.get("/actividad/{id}")
async def get_actividad(id):
    conn = conectar()
    cursor = conn.cursor()
    cursor.execute("SELECT r.id_pelicula, r.valoracion, u.nombre_usuario FROM review r INNER JOIN seguidor s ON r.id_usuario = s.id_usuario_seguido INNER JOIN usuario u ON u.id = s.id_usuario_seguido WHERE s.id_usuario_seguidor = %s AND r.fecha >= CURRENT_DATE - INTERVAL '7 days' ORDER BY r.fecha DESC;", (id,))
    seguidores = cursor.fetchall()
    conn.close()
    cursor.close()
    lista_actividad = []
    for seguidor in seguidores:
        lista_actividad.append({
            "id_pelicula": seguidor[0],
            "valoracion": seguidor[1],
            "nombre_usuario": seguidor[2],
        })

    return lista_actividad

@app.get("/populares")
async def get_populares():
    conn = conectar()
    cursor = conn.cursor()
    cursor.execute("SELECT seguidores.id_usuario, seguidores.nombre_usuario, seguidores.nombre_completo, seguidores.num_seguidores, r.id AS id_review, r.contenido, r.valoracion, r.fecha, r.id_pelicula FROM (SELECT u.id as id_usuario, u.nombre_usuario as nombre_usuario, u.nombre_completo as nombre_completo, COUNT(s.id_usuario_seguidor) AS num_seguidores FROM usuario u LEFT JOIN seguidor s ON u.id = s.id_usuario_seguido GROUP BY u.id, u.nombre_usuario, u.nombre_completo ORDER BY num_seguidores DESC LIMIT 5) AS seguidores LEFT JOIN review r ON seguidores.id_usuario = r.id_usuario WHERE r.id IN (SELECT id FROM review WHERE id_usuario = seguidores.id_usuario and contenido != '' ORDER BY fecha desc LIMIT 4) ORDER BY seguidores.id_usuario, seguidores.num_seguidores asc;")
    seguidores = cursor.fetchall()
    conn.close()
    cursor.close()
    lista_seguidores = []
    for seguidor in seguidores:
        lista_seguidores.append({
            "id_usuario": seguidor[0],
            "nombre_usuario": seguidor[1],
            "nombre_completo": seguidor[2],
            "num_seguidores": seguidor[3],
            "id_review": seguidor[4],
            "review_contenido": seguidor[5],
            "review_valoracion": seguidor[6],
            "review_fecha": seguidor[7],
            "id_pelicula": seguidor[8],
        })

    return lista_seguidores



@app.get("/toplikes")
async def get_toplikes():
    conn = conectar()
    cursor = conn.cursor()
    cursor.execute("SELECT pelicula_id, COUNT(*) AS total_likes FROM (SELECT UNNEST(peliculas) AS pelicula_id FROM lista WHERE lower(tipo) = 'likes') AS subquery GROUP BY pelicula_id ORDER BY total_likes DESC limit 5;")
    pelis = cursor.fetchall()
    conn.close()
    cursor.close()
    lista_pelis = []
    for peli in pelis:
        lista_pelis.append({
            "pelicula_id": peli[0],
            "total_likes": peli[1],
        })

    return lista_pelis
@app.get("/topvaloracion")
async def get_topvaloracion():
    conn = conectar()
    cursor = conn.cursor()
    cursor.execute("SELECT id_pelicula, AVG(valoracion) as valoracion_media FROM review where valoracion != -1 group by id_pelicula order by valoracion_media desc limit 5;")
    pelis = cursor.fetchall()
    conn.close()
    cursor.close()
    lista_pelis = []
    for peli in pelis:
        lista_pelis.append({
            "pelicula_id": peli[0],
            "valoracion_media": peli[1],
        })

    return lista_pelis

# --------------------------------------------------------------------------------
# creamos la api para los reviews

# creamos la clase Review

class Review(BaseModel):
    id: int
    id_usuario: int
    id_pelicula: int
    contenido: str
    valoracion: int
    fecha: date


class NewReview(BaseModel):
    id_usuario: int
    id_pelicula: int
    contenido: str
    valoracion: int
    fecha: date

# traemos todos los reviews


@app.get("/reviews")
async def get_reviews():
    conn = conectar()
    cursor = conn.cursor()
    cursor.execute("SELECT r.id, r.id_usuario, r.id_pelicula, r.contenido, r.valoracion, r.fecha, u.nombre_usuario FROM review r join usuario u on r.id_usuario = u.id where contenido != '' order by r.fecha desc, r.id desc limit 5")
    reviews = cursor.fetchall()
    conn.close()
    cursor.close()
    lista_reviews = []
    for review in reviews:
        lista_reviews.append({
            "id": review[0],
            "id_usuario": review[1],
            "id_pelicula": review[2],
            "contenido": review[3],
            "valoracion": review[4],
            "fecha": review[5],
            "nombre_usuario": review[6],
        })

    return lista_reviews

# traemos los reviews de una pelicula


@app.get("/reviews/{id}")
async def get_review(id):
    conn = conectar()
    cursor = conn.cursor()
    cursor.execute("SELECT r.id, r.id_usuario, r.id_pelicula, r.contenido, r.valoracion, r.fecha, u.rol, u.nombre_usuario, u.nombre_completo FROM review r join usuario u on r.id_usuario = u.id WHERE r.id_pelicula = %s order by r.fecha desc, r.id desc", (id,))
    reviews = cursor.fetchall()
    conn.close()
    cursor.close()
    lista_reviews = []
    for review in reviews:
        lista_reviews.append({
            "id": review[0],
            "id_usuario": review[1],
            "id_pelicula": review[2],
            "contenido": review[3],
            "valoracion": review[4],
            "fecha": review[5],
            "rol": review[6],
            "nombre_usuario": review[7],
            "nombre_completo": review[8]
        })

    return lista_reviews

# traemos los reviews de un usuario


@app.get("/reviews/usuario/{id}")
async def get_review(id):
    conn = conectar()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM review WHERE id_usuario = %s", (id,))
    reviews = cursor.fetchall()
    conn.close()
    cursor.close()
    lista_reviews = []
    for review in reviews:
        lista_reviews.append({
            "id": review[0],
            "id_usuario": review[1],
            "id_pelicula": review[2],
            "contenido": review[3],
            "valoracion": review[4],
            "fecha": review[5]
        })

    return lista_reviews


# creamos un review
@app.post("/review")
async def post_review(review: NewReview):
    try:
        conn = conectar()
        cursor = conn.cursor()
        cursor.execute("INSERT INTO review (id_usuario, id_pelicula, contenido, valoracion, fecha) VALUES (%s, %s, %s, %s, %s)",
                       (review.id_usuario, review.id_pelicula, review.contenido, review.valoracion, review.fecha))
        conn.commit()
        print("Datos de la review insertados correctamente")
    except Exception as e:
        print("Error al insertar datos de la review:", e)
        conn.rollback()
    finally:
        conn.close()
        cursor.close()
    return review


# actualizamos un review
@app.put("/review/{id}")
async def put_review(id: int, review: Review):
    conn = conectar()
    cursor = conn.cursor()
    cursor.execute("UPDATE review SET id_usuario = %s, id_pelicula = %s, contenido = %s, valoracion = %s, fecha = %s WHERE id = %s",
                   (review.id_usuario, review.id_pelicula, review.contenido, review.valoracion, review.fecha, id))
    conn.commit()
    conn.close()
    cursor.close()
    return review

# eliminamos un review


@app.delete("/review/{id}")
async def delete_review(id: int):
    conn = conectar()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM review WHERE id = %s", (id,))
    conn.commit()
    conn.close()
    cursor.close()
    return {"message": "Review eliminado"}

# --------------------------------------------------------------------------------
# creamos la api para los listas

# creamos la clase Lista


class Lista(BaseModel):
    id: int
    nombre_lista: str
    tipo: str
    usuario_id: int
    publica: bool
    peliculas: List[int]


class updateLista(BaseModel):
    id: int
    peliculas: List[int]


class editarLista(BaseModel):
    nombre_lista: str
    publica: bool


# traemos todas las listas
@app.get("/listas")
async def get_listas():
    conn = conectar()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM lista")
    listas = cursor.fetchall()
    conn.close()
    cursor.close()
    lista_listas = []
    for lista in listas:
        lista_listas.append({
            "id": lista[0],
            "nombre_lista": lista[1],
            "tipo": lista[2],
            "usuario_id": lista[3],
            "publica": lista[4],
            "peliculas": lista[5]
        })

    return lista_listas

# traemos los listas de un usuario


@app.get("/listas/{id}")
async def get_lista(id):
    conn = conectar()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM lista WHERE usuario_id = %s", (id,))
    listas = cursor.fetchall()
    conn.close()
    cursor.close()
    lista_listas = []
    for lista in listas:
        lista_listas.append({
            "id": lista[0],
            "nombre_lista": lista[1],
            "tipo": lista[2],
            "usuario_id": lista[3],
            "publica": lista[4],
            "peliculas": lista[5]
        })

    return lista_listas

# creamos un lista


@app.post("/lista")
async def post_lista(lista: Lista):
    conn = conectar()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO lista (nombre_lista, tipo, usuario_id, publica, peliculas) VALUES (%s, %s, %s, %s, %s)",
                   (lista.nombre_lista, lista.tipo, lista.usuario_id, lista.publica, lista.peliculas))
    conn.commit()
    conn.close()
    cursor.close()
    return lista

# actualizamos una lista


@app.put("/lista/{id}")
async def put_lista(id: int, lista: editarLista):
    conn = conectar()
    cursor = conn.cursor()
    cursor.execute("UPDATE lista SET nombre_lista = %s, publica = %s WHERE id = %s",
                   (lista.nombre_lista, lista.publica, id))
    conn.commit()
    conn.close()
    cursor.close()
    return lista


# actualizamos las peliculas de una lista
@app.put("/peliculasLista/{id}")
async def put_lista(lista: updateLista):
    conn = conectar()
    cursor = conn.cursor()
    cursor.execute("UPDATE lista SET peliculas = %s WHERE id = %s",
                   (lista.peliculas, lista.id))
    conn.commit()
    conn.close()
    cursor.close()
    return lista

# eliminamos una lista


@app.delete("/lista/{id}")
async def delete_lista(id: int):
    conn = conectar()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM lista WHERE id = %s", (id,))
    conn.commit()
    conn.close()
    cursor.close()
    return {"message": "Lista eliminada"}

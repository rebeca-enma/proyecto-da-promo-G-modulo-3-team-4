
# Importar librería para la conexión con MySQL
# -----------------------------------------------------------------------
import mysql.connector
from mysql.connector import errorcode


# Importar librerías para manipulación y análisis de datos
# -----------------------------------------------------------------------
import pandas as pd
import numpy as np


def creacion_bbdd (usuario, contrasenya):
    """Esta funcion crea la bbdd raw_data en mysql

    Args:
    - usuario: usuario para la conexion al servidor
    - contraseña: contraseña para la conexión al servidor

    Returns:
    No devuelve ningún valor
    """
    
    cnx = mysql.connector.connect(user=usuario, password=contrasenya,
                                host='127.0.0.1')


    mycursor = cnx.cursor()
    query = "CREATE SCHEMA RAW_DATA"

    try: 
        mycursor.execute(query) 
    
        print("BBDD creada")

    except mysql.connector.Error as err:
        print(err)
        print("Error Code:", err.errno)
        print("SQLSTATE", err.sqlstate)
        print("Message", err.msg)


def creacion_tablas (usuario, contrasenya, bbdd):
    """Esta funcion crea las tablas empleado, empleado_empresa y registros en mysql

    Args:
    - usuario: usuario para la conexion al servidor
    - contraseña: contraseña para la conexión al servidor
    - bbdd: nombre de la bbdd donde queremos crear las tablas

    Returns:
    No devuelve ningún valor
    """
    
    cnx = mysql.connector.connect(user=usuario, password=contrasenya,
                                host='127.0.0.1', database= bbdd)


    # tabla empleado: campos relacionados con el empleados que no varian en los distintos registros
    mycursor = cnx.cursor()
    query = "CREATE TABLE `empleado` (`EMPLOYEE_NUMBER` int NOT NULL,`AGE` int DEFAULT NULL,`EDUCATION` int DEFAULT NULL,`EDUCATION_FIELD` varchar(100) DEFAULT NULL,`GENDER` varchar(50) DEFAULT NULL,`MARITAL_STATUS` varchar(50) DEFAULT NULL,`WORK_LIFE_BALANCE` int DEFAULT NULL,`NUM_COMPANIES_WORKED` int DEFAULT NULL,`RELATIONSHIP_SATISFACTION` int DEFAULT NULL,`TOTAL_WORKING_YEARS` int DEFAULT NULL,PRIMARY KEY (`EMPLOYEE_NUMBER`))"
    
    try: 
        mycursor.execute(query)
    
        print("Tabla empleado creada")

    except mysql.connector.Error as err:
        print(err)
        print("Error Code:", err.errno)
        print("SQLSTATE", err.sqlstate)
        print("Message", err.msg)

    # tabla empleado_empresa: campos que relacionan al empleado con la empresa que no varian en los distintos registros
    mycursor = cnx.cursor()
    query = "CREATE TABLE `empleado_empresa` (`EMPLOYEE_NUMBER` int NOT NULL,`DEPARTMENT` varchar(100) DEFAULT NULL,`BUSINESS_TRAVEL` varchar(100) DEFAULT NULL,`JOB_INVOLVEMENT` int DEFAULT NULL,`JOB_ROLE` varchar(100) DEFAULT NULL,`JOB_SATISFACTION` int DEFAULT NULL,`ATTRITION` tinyint(1) DEFAULT NULL,`YEARS_AT_COMPANY` int DEFAULT NULL,`MONTHLY_RATE` int DEFAULT NULL,`PERCENT_SALARY_HIKE` int DEFAULT NULL,`STOCK_OPTION_LEVEL` int DEFAULT NULL,`TRAINING_TIMES_LAST_YEAR` int DEFAULT NULL,`YEARS_SINCE_LAST_PROMOTION` int DEFAULT NULL,`YEARS_WITH_CURRENT_MANAGER` int DEFAULT NULL,`DAILY_RATE` int DEFAULT NULL,`PERFORMANCE_RATING` int DEFAULT NULL,PRIMARY KEY (`EMPLOYEE_NUMBER`),CONSTRAINT `empleado_empresa_ibfk_1` FOREIGN KEY (`EMPLOYEE_NUMBER`) REFERENCES `empleado` (`EMPLOYEE_NUMBER`))"
    
    try: 
        mycursor.execute(query)
    
        print("Tabla empleado_empresa creada")

    except mysql.connector.Error as err:
        print(err)
        print("Error Code:", err.errno)
        print("SQLSTATE", err.sqlstate)
        print("Message", err.msg)

    # tabla registro: campos que varian en los distintos registros que hay para cada employee number
    mycursor = cnx.cursor()
    query = "CREATE TABLE `registro` (`ID_REGISTRO` int NOT NULL AUTO_INCREMENT,`EMPLOYEE_NUMBER` int NOT NULL,`DISTANCE_FROM_HOME` int DEFAULT NULL,`REMOTE_WORK` tinyint(1) DEFAULT NULL,`MONTHLY_INCOME` int DEFAULT NULL,`HOURLY_RATE` int DEFAULT NULL,`OVER_TIME` tinyint(1) DEFAULT NULL,`ENVIRONMENT_SATISFACTION` int DEFAULT NULL,PRIMARY KEY (`ID_REGISTRO`),KEY `EMPLOYEE_NUMBER` (`EMPLOYEE_NUMBER`),CONSTRAINT `registro_ibfk_1` FOREIGN KEY (`EMPLOYEE_NUMBER`) REFERENCES `empleado` (`EMPLOYEE_NUMBER`))"
    
    try: 
        mycursor.execute(query)
    
        print("Tabla registro creada")

    except mysql.connector.Error as err:
        print(err)
        print("Error Code:", err.errno)
        print("SQLSTATE", err.sqlstate)
        print("Message", err.msg)

   
    cnx.close()

##  QUERYS DE INSERCION DE DATOS ##

query_insertar_empleado = "INSERT INTO empleado (EMPLOYEE_NUMBER, AGE, EDUCATION, EDUCATION_FIELD, GENDER, MARITAL_STATUS, WORK_LIFE_BALANCE, NUM_COMPANIES_WORKED, RELATIONSHIP_SATISFACTION, TOTAL_WORKING_YEARS) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

query_insertar_empleado_empresa = "INSERT INTO empleado_empresa (EMPLOYEE_NUMBER, DEPARTMENT, BUSINESS_TRAVEL, JOB_INVOLVEMENT, JOB_ROLE, JOB_SATISFACTION, ATTRITION, YEARS_AT_COMPANY, MONTHLY_RATE, PERCENT_SALARY_HIKE, STOCK_OPTION_LEVEL, TRAINING_TIMES_LAST_YEAR, YEARS_SINCE_LAST_PROMOTION, YEARS_WITH_CURRENT_MANAGER, DAILY_RATE, PERFORMANCE_RATING) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

query_insertar_registro = "INSERT INTO `registro` (EMPLOYEE_NUMBER, DISTANCE_FROM_HOME, REMOTE_WORK, MONTHLY_INCOME, HOURLY_RATE, OVER_TIME, ENVIRONMENT_SATISFACTION) VALUES (%s, %s, %s, %s, %s, %s, %s)"


def insertar_datos(query, contraseña, nombre_bbdd, lista_tuplas):
    """
    Inserta datos en una base de datos utilizando una consulta y una lista de tuplas como valores.

    Args:
    - query (str): Consulta SQL con placeholders para la inserción de datos.
    - contraseña (str): Contraseña para la conexión a la base de datos.
    - nombre_bbdd (str): Nombre de la base de datos a la que se conectará.
    - lista_tuplas (list): Lista que contiene las tuplas con los datos a insertar.

    Returns:
    No devuelve ningún valor, pero inserta los datos en la base de datos.

    """
    cnx = mysql.connector.connect(
        user="root", 
        password=contraseña, 
        host="127.0.0.1", database=nombre_bbdd
    )

    mycursor = cnx.cursor()

    try:
        mycursor.executemany(query, lista_tuplas)
        cnx.commit()
        print(mycursor.rowcount, "registro/s insertado/s.")
        cnx.close()

    except mysql.connector.Error as err:
        print(err)
        print("Error Code:", err.errno)
        print("SQLSTATE", err.sqlstate)
        print("Message", err.msg)
        cnx.close()


def convertir_float(lista_tuplas):
    """
    Esta funcion convierte los elementos de una lista de tuplas a float cuando sea posible

    Args:
    - lista_tuplas: una lista que contiene tuplas 

    Returns:
    Devuelve una nueva lista con las mismas tuplas, pero con los elementos convertidos a float, cuando sea posible
    """
    datos_tabla_caract_def = []
    
    for tupla in lista_tuplas:
        lista_intermedia = []
        for elemento in tupla:
            try:
                lista_intermedia.append(float(elemento))
            except:
                lista_intermedia.append(elemento)
            
        datos_tabla_caract_def.append(tuple(lista_intermedia))
    
    return datos_tabla_caract_def

def convertir_int(lista_tuplas):
    """
    Esta funcion convierte los elementos de una lista de tuplas a int cuando sea posible

    Args:
    - lista_tuplas: una lista que contiene tuplas 

    Returns:
    Devuelve una nueva lista con las mismas tuplas, pero con los elementos convertidos a int, cuando sea posible
    """
    datos_tabla_caract_def = []
    
    for tupla in lista_tuplas:
        lista_intermedia = []
        for elemento in tupla:
            try:
                lista_intermedia.append(int(elemento))
            except:
                lista_intermedia.append(elemento)
            
        datos_tabla_caract_def.append(tuple(lista_intermedia))
    
    return datos_tabla_caract_def


def cambio_unknown (lista_tuplas):
    """
    Esta funcion convierte los Unknown en 0, solo cuando este en la posicion 0 de la tupla,
    es decir, para la columna EMPLOYEE NUMBER   

    Args:
    - lista_tuplas: una lista que contiene tuplas 

    Returns:
    Devuelve una nueva lista con las tuplas modificadas
    """
    lista_para_tupla = []
    for tupla in lista_tuplas:
        lista = list(tupla)
        if lista[0] == "Unknown":
            lista[0] = 0
            lista_para_tupla.append(tuple(lista))
        else:
            lista_para_tupla.append(tuple(lista))
        
    return lista_para_tupla
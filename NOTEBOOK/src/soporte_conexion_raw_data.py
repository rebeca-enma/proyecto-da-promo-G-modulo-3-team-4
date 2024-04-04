
# Importar librería para la conexión con MySQL
# -----------------------------------------------------------------------
import mysql.connector
from mysql.connector import errorcode


# Importar librerías para manipulación y análisis de datos
# -----------------------------------------------------------------------
import pandas as pd
import numpy as np


def creacion_bbdd (usuario, contrasenya):
    
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


query_insertar_empleado = "INSERT INTO empleado (EMPLOYEE_NUMBER, AGE, EDUCATION, EDUCATION_FIELD, GENDER, MARITAL_STATUS, WORK_LIFE_BALANCE, NUM_COMPANIES_WORKED, RELATIONSHIP_SATISFACTION, TOTAL_WORKING_YEARS) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

query_insertar_empleado_empresa = "INSERT INTO empleado_empresa (EMPLOYEE_NUMBER, DEPARTMENT, BUSINESS_TRAVEL, JOB_INVOLVEMENT, JOB_ROLE, JOB_SATISFACTION, ATTRITION, YEARS_AT_COMPANY, MONTHLY_RATE, PERCENT_SALARY_HIKE, STOCK_OPTION_LEVEL, TRAINING_TIMES_LAST_YEAR, YEARS_SINCE_LAST_PROMOTION, YEARS_WITH_CURRENT_MANAGER, DAILY_RATE, PERFORMANCE_RATING) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

query_insertar_registro = "CREATE TABLE `registro` (ID_REGISTRO, EMPLOYEE_NUMBER, DISTANCE_FROM_HOME, REMOTE_WORK, MONTHLY_INCOME, HOURLY_RATE, OVER_TIME, ENVIRONMENT_SATISFACTION) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"


def insertar_datos(query, contraseña, nombre_bbdd, lista_tuplas):
    """
    Inserta datos en una base de datos utilizando una consulta y una lista de tuplas como valores.

    Args:
    - query (str): Consulta SQL con placeholders para la inserción de datos.
    - contraseña (str): Contraseña para la conexión a la base de datos.
    - nombre_bbdd (str): Nombre de la base de datos a la que se conectará.
    - lista_tuplas (list): Lista que contiene las tuplas con los datos a insertar.

    Returns:
    - None: No devuelve ningún valor, pero inserta los datos en la base de datos.

    This function connects to a MySQL database using the given credentials, executes the query with the provided list of tuples, and commits the changes to the database. In case of an error, it prints the error details.
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
#%%
import pandas as pd
import numpy as np 
from word2number import w2n
# Imputación de nulos usando métodos avanzados estadísticos
# -----------------------------------------------------------------------
from sklearn.impute import SimpleImputer
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer
from sklearn.impute import KNNImputer

# Librerías de visualización
# -----------------------------------------------------------------------
import seaborn as sns
import matplotlib.pyplot as plt
from IPython.display import display


# Configuración
pd.set_option('display.max_columns', None) # para poder visualizar todas las columnas de los DataFrames

##FUNCIONES EXPLORACION##

def exploracion_general (dataframe):
    """Esta función proporciona una descripción personalizada de un DataFrame,
    incluyendo estadísticas descriptivas y tipos de datos de cada columna.
    
    Argumentos:
    df : DataFrame de Pandas
        El DataFrame para el cual se generará la descripción
        
    La funcion no tiene return pero devuelve varios prints con
    la informacion que necesitamos:
    - describe separados por col numericas y categoricas
    - dtypes por columna
    - shape
    - info
    - total de nulos
    - total de duplicados)"""
       
    print(f"------EXPLORACION DATAFRAME ABC CORPORATION------")
    print("-------Descripción numéricas:---------")
    print(dataframe.describe())
    print("-------Descripción categoricas:---------")
    print(dataframe.describe(include="O"))
    print("------Tipos:---------")
    print(dataframe.dtypes)
    print("------Forma del DataFrame:------")
    print(dataframe.shape)
    print("------Información:---------")
    print(dataframe.info())
    print("------Nulos:---------")
    print(dataframe.isnull().sum())
    print("------Duplicados:---------")
    print(dataframe.duplicated().sum())
    
def exploracion_columna (dataframe):

    for columna in list(dataframe.columns):

        print(f" \n----------- ESTAMOS ANALIZANDO LA COLUMNA: '{columna.upper()}' -----------\n")
        print(f"* Nº de datos: {len(dataframe[columna].to_list())}")
        print(f"* Frecuencia de valores en la columna: \n {dataframe[columna].value_counts()}")
        print(f"* Datos unicos en la columna {len(dataframe[columna].unique())}")
        print(f"* Los valores son de tipo: {type(columna)}")
        print(f"La suma de datos nulos {dataframe[columna].isnull().sum()}")
        print(dataframe[columna].unique()) 

##FUNCIONES LIMPIEZA##

# Eliminacion de las columnas, tras la exploracion de los datos
def eliminar_columnas (dataframe, lista):
    dataframe.drop(lista, axis=1, inplace=True)

# Renombrar columnas
def renombrar_columnas (dataframe):
    dataframe.columns = [col.upper() for col in dataframe.columns]

# Reemplazamos los números escritos en letras en ingles por integers
def transformar_age(dataframe,columna): 
    for i, age in enumerate (dataframe[columna]): 
        try:
            age = int(age)
            dataframe.loc[i, columna] = age 
        except: 
            numero_entero = w2n.word_to_num(age)  
            dataframe.loc[i, columna] = numero_entero

#Reemplazamos guiones por espacios
def transformar_bus_travel (dataframe, columna):
    dataframe[columna] = dataframe[columna].str.replace("_", " ").replace("-", " ")

#Reemplazamos , por . y los strings de "nan" por np.nan
def cambiar_float(dataframe,columna): 
     for i, element in enumerate (dataframe[columna]): 
        if element == "nan$" or element == "nan" or element == "NaN":
            element = np.nan
            dataframe.loc[i, columna] = element
        elif type(element) == str:
            try:
                element = element.replace(",", ".").replace("$","")
                element = float(element)
                dataframe.loc[i, columna] = element
            except: 
                print(f"Error daily {i, element}")
        else:
            continue

#Reemplazamos True, Yes por 1 y False, No por 0, ademas pasan de der STR a INT
def transformar_remotework(dataframe, columna):
    diccionario_mapeo = {"True": 0, "Yes": 1, "False": 0, "1": 1, "0": 0, "No":0}
    try:
        dataframe[columna] = dataframe[columna].map(diccionario_mapeo)

    except:
        print(f"Error al transformar la columna {columna}")

# Cambiamos los valores del DF de STR a INT
def cambiar_int(dataframe,columna): 
    for i, element in enumerate (dataframe[columna]): 
        if type(element) == str:
            try:
                element = element.split(",")[0]
                int(element)
                dataframe.loc[i, columna] = element
            except: 
                print(f"Error daily {i, element}")
        else:
            continue

# Cambiamos a minusculas y quitamos espacios
def transformar_department (dataframe, columna):
    dataframe[columna] = dataframe[columna].str.lower().str.strip()

#Cambiamos 0 a male y 1 a female
def transformar_gender (dataframe, columna):
    mapeo = {0 : "male", 1 : "female"}
    dataframe[columna]= dataframe[columna].map(mapeo)

#Reemplazamos 'Not Available' por NaN y el punto y decimales y convertir a entero
def transformar_hourlyrate(dataframe, columna):
    for i, valor in enumerate(dataframe[columna]):
        if isinstance(valor, str):
            try:
                # Reemplazar 'Not Available' por NaN
                if valor.strip() == 'Not Available':
                    dataframe.loc[i, columna] = np.nan
                else:
                    # Eliminar el punto y decimales y convertir a entero
                    valor = float(valor.replace('.', '').split('.')[0])
                    dataframe.loc[i, columna] = valor
            except ValueError:
                print(f"Error de datos en fila {i}: {valor}")
                dataframe.loc[i, columna] = np.nan
        else:
            continue

# Correccion ortografica y conversion a minusculas
def transformar_maritalstatus(dataframe, columna):
    for i, valor in enumerate(dataframe[columna]):
        if isinstance(valor, str):
            # Convertir a minúsculas
            valor = valor.lower()
            # Corregir el error ortográfico usando replace
            valor = valor.replace('marreid', 'married')
            dataframe.loc[i, columna] = valor
        else:
            continue
# Convertimos a tipo float64
def transformar_float(dataframe, columna):
    dataframe[columna] = dataframe[columna].astype('float64')  # 'Int64' es una forma segura de convertir a enteros con manejo de valores nulos

# Devuelve el primer dígito del número entero (eliminando las unidades)
def cambiar_numero(columna):
    if isinstance(columna, int):
        return int(str(columna)[0])   
    else:
        return columna

# Funcion general de llamada a todas las anteriores
def limpieza_df(dataframe, lista):

    eliminar_columnas(dataframe, lista)
    renombrar_columnas(dataframe)
    
    for columna in dataframe.columns:
       
        if columna == "AGE":
            transformar_age(dataframe, columna)

        elif columna == "REMOTEWORK" or columna == "OVERTIME" or columna == "ATTRITION":
            transformar_remotework(dataframe, columna)

        elif columna == "BUSINESSTRAVEL":
            transformar_bus_travel(dataframe, columna) 

        elif columna == "DAILYRATE" or columna == "EMPLOYEENUMBER" or columna == "MONTHLYINCOME"  or columna == "WORKLIFEBALANCE"  or columna == "TOTALWORKINGYEARS" or columna == "PERFORMANCERATING":
            cambiar_float(dataframe, columna)

        elif columna == "DEPARTMENT" or columna == "EDUCATIONFIELD" or columna == "JOBROLE":
            transformar_department(dataframe, columna)

        elif columna == "GENDER":
            transformar_gender(dataframe, columna)

        elif columna == "YEARSWITHCURRMANAGER" or columna == "YEARSSINCELASTPROMOTION" or columna ==  "YEARSATCOMPANY" or columna ==  "TRAININGTIMESLASTYEAR":
            cambiar_int(dataframe, columna)
            
        elif columna == "HOURLYRATE":
            transformar_hourlyrate(dataframe, columna)
        
        elif columna == "MARITALSTATUS":
            transformar_maritalstatus(dataframe,columna)
        
        elif columna == "DISTANCEFROMHOME":
            dataframe[columna] = dataframe[columna].apply(lambda dato : abs(dato))

        elif columna == "ENVIRONMENTSATISFACTION":
            dataframe[columna] = dataframe[columna].apply(cambiar_numero)

        else:
            continue
    
    lista_float =   ['AGE', 'DAILYRATE', 'EMPLOYEENUMBER', 'HOURLYRATE',
       'MONTHLYINCOME','PERFORMANCERATING', 'TOTALWORKINGYEARS', 'WORKLIFEBALANCE']
    for columna in lista_float: 
        transformar_float(dataframe, columna)

    return dataframe

##GESTION NULOS##

def exploracion_nulos (dataframe):

    # sacamos una lista de las variables categoricas que tienen nulos
    nulos_esta_cat = dataframe[dataframe.columns[dataframe.isnull().any()]].select_dtypes(include = "O").columns
    print("Las columnas categóricas que tienen nulos son : \n ")
    print(nulos_esta_cat)
    print("........................")

    # % de nulos que tenemos en cada una de las columnas anteriores
    print("El porcentaje de nulos de cada una de las anteriores es: \n")
    print(dataframe[nulos_esta_cat].isnull().sum() / dataframe.shape[0])
    print("........................")

    # % de nulos por categoria de cada columna
    for col in nulos_esta_cat:
        print(f"La distribución de las categorías para la columna {col.upper()}")
        display(dataframe[col].value_counts() / dataframe.shape[0])
        print("........................")

    # sacamos una lista de las variables numericas que tienen nulos
    nulos_esta_num = dataframe[dataframe.columns[dataframe.isnull().any()]].select_dtypes(include = np.number).columns
    print("Las columnas numéricas que tienen nulos son : \n ")
    print(nulos_esta_num)
    print("........................")

    # nulos que tenemos en cada una de las columnas numericas
    print("El porcentaje de nulos de cada una de las anteriores es: \n")
    print(dataframe[nulos_esta_num].isnull().sum() / dataframe.shape[0])


# Sustituimos los nulos con una nueva categoria "Unknown" porque en ninguna columna la moda destaca.

def gestion_nulos (dataframe, lista_unknown, lista_imputacion, lista_moda):

    for columna in lista_unknown:
        dataframe[columna] = dataframe[columna].fillna("Unknown")
        print("Comprobacion de la ausencia de los nulos")
        print(dataframe[lista_unknown].isnull().sum()) #comprobamos que no tienen nulos despues de reemplazar por Unknown

    for columna in lista_moda:
        dataframe[columna] = dataframe[columna].fillna(dataframe[columna].mode()[0])
        print("Comprobacion de la ausencia de los nulos")
        print(dataframe[lista_moda].isnull().sum()) #comprobamos que no tienen nulos despues de reemplazar por la moda

    clase_imputer = IterativeImputer(max_iter = 20, random_state = 42)
    imputer_knn_ejercicios = KNNImputer(n_neighbors = 5)

    for columna in lista_imputacion:

        # creamos columnas imputadas con iterative
        transformamos= clase_imputer.fit_transform(dataframe[lista_imputacion])

        # creamos una nueva lista con el nombre que pondremos a las columnas que salen de "IterativeImputer"
        lista_iterative = []
        for col in lista_imputacion:
            col = col + "_iterative" # añade "_iterative" al nombre original
            lista_iterative.append(col)

        # Añadimos al df copia las columnas nuevas de IterativeImputer
        dataframe[lista_iterative] = transformamos
        print("Comprobacion de la ausencia de los nulos")
        print(dataframe[lista_iterative].isnull().sum()) #comprobamos que no tienen nulos despues de la imputacion

        # creamos columnas imputadas con knn

        imputer_knn_imputado_ejer = imputer_knn_ejercicios.fit_transform(dataframe[lista_imputacion])

        # creamos una nueva lista con el nombre que pondremos a las columnas que salen de "KNNImputer"
        lista_knn = []
        for col in lista_imputacion:
            col = col + "_knn" # añade "_knn" al nombre original
            lista_knn.append(col)   

        # Añadimos al df las columnas nuevas de KNNImputer
        dataframe[lista_knn] = imputer_knn_imputado_ejer
        print("Comprobacion de la ausencia de los nulos")
        print(dataframe[lista_knn].isnull().sum())

        # creamos una nueva lista con las columnas originales que hemos imputado y las columnas generadas por IterativeImputer y KNNImputer
        lista_describe = []
        for i in range(len(lista_imputacion)):
            lista_describe.append(lista_imputacion[i])
            lista_describe.append(lista_iterative[i])
            lista_describe.append(lista_knn[i])

        #para comparar y decidir con cual nos quedamos mostramos un describe 
        print("Resultado de las imputaciones:")
        print(dataframe.describe()[lista_describe])

    return dataframe

def renombrar_imputacion (dataframe):

    nombre_antiguo = []
    for columna in dataframe.columns:
        if "_iterative" in columna or "_knn" in columna:
            nombre_antiguo.append(columna)
        else:
            continue

    nombre_nuevo = []
    for columna in nombre_antiguo:
        n = columna.split("_")[0]
        nombre_nuevo.append(n)
# Key valor actual, Value nuevo nombre (nombre que tenian antes para que cuadre con la documentacion)
    diccionario_nombres = dict(zip(nombre_antiguo, nombre_nuevo))

    dataframe.rename(columns = diccionario_nombres, inplace = True)

    return dataframe

##GESTION DUPLICADOS##

def exploracion_duplicados (dataframe):
    print("Estos son los duplicados generales del DataFrame: \n")
    print(dataframe.duplicated().sum())

    print("Estos son los duplicados por columna: \n")
    for columna in dataframe.columns:
        print(f"--------- COLUMNA {columna} ---------\n")
        print(dataframe[columna].duplicated().sum())


def eliminar_duplicados (dataframe):
    dataframe.drop_duplicates(keep = "first", inplace = True)
    print("Comprobacion de la ausencia de duplicados")
    print(dataframe.duplicated().sum())
    return dataframe
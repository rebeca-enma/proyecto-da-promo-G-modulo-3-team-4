#%%
import pandas as pd
from src import soporte_eda as eda
from src import soporte_conexion_raw_data as conexion

# Apertura y lectura del documento csv
df_data = pd.read_csv("../DATA/DATA.csv", index_col= 0)

#%%
# Funciones exploracion
eda.exploracion_general(df_data)
eda.exploracion_columna(df_data)

#%%
#Funciones limpieza
lista_eliminar = ["SameAsMonthlyIncome", "Salary", "NUMBERCHILDREN","RoleDepartament", "employeecount", "Over18", "DateBirth", "YearsInCurrentRole","StandardHours"]
df_limpio = eda.limpieza_df(df_data, lista_eliminar)

#%%
#Funcion exploracion nulos
eda.exploracion_nulos(df_limpio)

#%%
#Segun la informacion que nos de la exploracion, decidiremos como gestionar los nulos de cada columna. Creamos listas segun las funciones que se van a utilizar
lista_desconocido = ["BUSINESSTRAVEL", "DEPARTMENT", "EDUCATIONFIELD", "MARITALSTATUS", "EMPLOYEENUMBER"]
lista_imputar = ["DAILYRATE", "PERFORMANCERATING", "TOTALWORKINGYEARS"]   
lista_moda = ["WORKLIFEBALANCE"]

#Funciones gestion nulos
df_gestion_nulos = eda.gestion_nulos(df_limpio, lista_desconocido, lista_imputar, lista_moda)

#%%
#De la funcion de imputacion hemos duplicado columnas unas usando IterativeImputer y otras KNN Imputer. En esta lista se añaden las columnas que no queramos conservar
borrar_columnas = ['DAILYRATE', 'DAILYRATE_knn', 'PERFORMANCERATING', 'PERFORMANCERATING_knn', 'TOTALWORKINGYEARS', 'TOTALWORKINGYEARS_knn']

eda.eliminar_columnas(df_gestion_nulos, borrar_columnas)
df_sin_nulos = eda.renombrar_imputacion(df_gestion_nulos)

#%%
#Funcion exploracion duplicados
eda.exploracion_duplicados(df_sin_nulos)

#Funcion gestion duplicados
df_sin_duplicados = eda.eliminar_duplicados(df_sin_nulos)

#%%
#Creacion de bbdd y tablas
conexion.creacion_bbdd("root", "AlumnaAdalab")
conexion.creacion_tablas("root", "AlumnaAdalab", "raw_data")
# %%

datos_tabla_empleado = list(set(zip(df_sin_duplicados["EMPLOYEENUMBER"].values, df_sin_duplicados["AGE"].values, df_sin_duplicados["EDUCATION"].values, df_sin_duplicados["EDUCATIONFIELD"].values, df_sin_duplicados["GENDER"].values, df_sin_duplicados["MARITALSTATUS"].values, df_sin_duplicados["WORKLIFEBALANCE"].values, df_sin_duplicados["NUMCOMPANIESWORKED"].values, df_sin_duplicados["RELATIONSHIPSATISFACTION"].values, df_sin_duplicados["TOTALWORKINGYEARS"].values)))
datos_tabla_empleado_empresa = list(set(zip(df_sin_duplicados["EMPLOYEENUMBER"].values, df_sin_duplicados["DEPARTMENT"].values, df_sin_duplicados["BUSINESSTRAVEL"].values, df_sin_duplicados["JOBINVOLVEMENT"].values, df_sin_duplicados["JOBROLE"].values, df_sin_duplicados["JOBSATISFACTION"].values, df_sin_duplicados["ATTRITION"].values, df_sin_duplicados["YEARSATCOMPANY"].values, df_sin_duplicados["MONTHLYRATE"].values, df_sin_duplicados["PERCENTSALARYHIKE"].values, df_sin_duplicados["STOCKOPTIONLEVEL"].values, df_sin_duplicados["TRAININGTIMESLASTYEAR"].values, df_sin_duplicados["YEARSSINCELASTPROMOTION"].values, df_sin_duplicados["YEARSWITHCURRMANAGER"].values, df_sin_duplicados["DAILYRATE"].values, df_sin_duplicados["PERFORMANCERATING"].values)))
datos_tabla_registro = list(set(zip(df_sin_duplicados["EMPLOYEENUMBER"].values, df_sin_duplicados["DISTANCEFROMHOME"].values, df_sin_duplicados["REMOTEWORK"].values, df_sin_duplicados["MONTHLYINCOME"].values, df_sin_duplicados["HOURLYRATE"].values, df_sin_duplicados["OVERTIME"].values, df_sin_duplicados["ENVIRONMENTSATISFACTION"].values)))
#%%
### Da error por ser int64 y float 64 en vez de int y float, hemos intentado solucionarlo
### con la funcion de conversion que hay en la leccion pero no funciona
#datos_clientes_def = bss.convertir_int(datos_tabla_clientes)
#datos_productos_def = bss.convertir_float(datos_tabla_productos)
#%%
conexion.insertar_datos(conexion.query_insertar_empleado, "AlumnaAdalab", "Empresa_f", datos_tabla_empleado)
conexion.insertar_datos(conexion.query_insertar_empleado_empresa, "AlumnaAdalab", "Empresa_f", datos_tabla_empleado_empresa)
conexion.insertar_datos(conexion.query_insertar_registro, "AlumnaAdalab", "Empresa_f", datos_tabla_registro)
#%%
df_sin_duplicados
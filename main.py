#%%
import pandas as pd
from src import soporte_eda as eda
from src import soporte_conexion_raw_data as conexion
 
# Apertura y lectura del documento csv
df_data = pd.read_csv("DATA/DATA.csv", index_col= 0)

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
df_empleado_unico = df_sin_duplicados.drop_duplicates(subset='EMPLOYEENUMBER')
# %%

datos_tabla_empleado = list(set(zip(df_empleado_unico["EMPLOYEENUMBER"].values, df_empleado_unico["AGE"].values, df_empleado_unico["EDUCATION"].values, df_empleado_unico["EDUCATIONFIELD"].values, df_empleado_unico["GENDER"].values, df_empleado_unico["MARITALSTATUS"].values, df_empleado_unico["WORKLIFEBALANCE"].values, df_empleado_unico["NUMCOMPANIESWORKED"].values, df_empleado_unico["RELATIONSHIPSATISFACTION"].values, df_empleado_unico["TOTALWORKINGYEARS"].values)))
datos_tabla_empleado_empresa = list(set(zip(df_empleado_unico["EMPLOYEENUMBER"].values, df_empleado_unico["DEPARTMENT"].values, df_empleado_unico["BUSINESSTRAVEL"].values, df_empleado_unico["JOBINVOLVEMENT"].values, df_empleado_unico["JOBROLE"].values, df_empleado_unico["JOBSATISFACTION"].values, df_empleado_unico["ATTRITION"].values, df_empleado_unico["YEARSATCOMPANY"].values, df_empleado_unico["MONTHLYRATE"].values, df_empleado_unico["PERCENTSALARYHIKE"].values, df_empleado_unico["STOCKOPTIONLEVEL"].values, df_empleado_unico["TRAININGTIMESLASTYEAR"].values, df_empleado_unico["YEARSSINCELASTPROMOTION"].values, df_empleado_unico["YEARSWITHCURRMANAGER"].values, df_empleado_unico["DAILYRATE"].values, df_empleado_unico["PERFORMANCERATING"].values)))
datos_tabla_registro = list(set(zip(df_sin_duplicados["EMPLOYEENUMBER"].values, df_sin_duplicados["DISTANCEFROMHOME"].values, df_sin_duplicados["REMOTEWORK"].values, df_sin_duplicados["MONTHLYINCOME"].values, df_sin_duplicados["HOURLYRATE"].values, df_sin_duplicados["OVERTIME"].values, df_sin_duplicados["ENVIRONMENTSATISFACTION"].values)))
#%%
datos_tabla_empleado1 = conexion.convertir_float(datos_tabla_empleado)
datos_tabla_empleado_empresa1 = conexion.convertir_float(datos_tabla_empleado_empresa)
datos_tabla_registro1 = conexion.convertir_float(datos_tabla_registro)

datos_tabla_empleado2 = conexion.convertir_int(datos_tabla_empleado1)
datos_tabla_empleado_empresa2 = conexion.convertir_int(datos_tabla_empleado_empresa1)
datos_tabla_registro2 = conexion.convertir_int(datos_tabla_registro1)

#%%

datos_tabla_empleado3 = conexion.cambio_unknown(datos_tabla_empleado2)
datos_tabla_empleado_empresa3 = conexion.cambio_unknown(datos_tabla_empleado_empresa2)
datos_tabla_registro3 = conexion.cambio_unknown(datos_tabla_registro2)

#%%
conexion.insertar_datos(conexion.query_insertar_empleado, "AlumnaAdalab", "raw_data", datos_tabla_empleado3)
#%%
conexion.insertar_datos(conexion.query_insertar_empleado_empresa, "AlumnaAdalab", "raw_data", datos_tabla_empleado_empresa3)
#%%
conexion.insertar_datos(conexion.query_insertar_registro, "AlumnaAdalab", "raw_data", datos_tabla_registro3)






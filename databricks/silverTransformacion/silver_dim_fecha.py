import pandas as pd

df=pd.read_parquet("bronze_archivosParquet/ventas_2024_limpio.parquet")

#borrar duplicados
dim_Fecha=df[["Fecha"]].drop_duplicates().reset_index(drop=True)

#crear columnas desde ID HASTA NOMBRES DEL MES
dim_Fecha["ID_Fecha"]=df.index+1
dim_Fecha["Año"]=dim_Fecha["Fecha"].dt.year
dim_Fecha["Mes"]=dim_Fecha["Fecha"].dt.month
dim_Fecha["dia"]=dim_Fecha["Fecha"].dt.day
dim_Fecha["mes_nombre"]=dim_Fecha["Fecha"].dt.month_name()

#ordenar mis columnas de fecha
dim_Fecha=dim_Fecha[["ID_Fecha","Fecha","Año","Mes","dia","mes_nombre"]]


#guardamos mi parquet 
dim_Fecha.to_parquet("silver_archivosParquet/dim_fecha.parquet",index=False)
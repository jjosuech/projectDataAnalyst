import pandas as pd

df=pd.read_parquet("bronze_archivosParquet/ventas_2024_limpio.parquet")

#elimino duplicados
df=df[["Producto"]].drop_duplicates().reset_index(drop=True)

#agrego id_producto
df["ID_Producto"]=df.index+1

#ordeno mis columas
df=df[["ID_Producto","Producto"]]

#guardo el parquet del dim para producto
df.to_parquet("silver_archivosParquet/dim_producto.parquet",index=False)
df.to_excel("silver_archivosParquet/dim_producto.xlsx",index=False)
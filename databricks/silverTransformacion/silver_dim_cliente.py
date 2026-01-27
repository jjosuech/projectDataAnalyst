import pandas as pd

df=pd.read_parquet("bronze_archivosParquet/ventas_2024_limpio.parquet")

#primero elimino duplicados
dim_cliente=(df[["Cliente"]].drop_duplicates().reset_index(drop=True))

#segundo pus creo los id clientes para el parquet de clientes
dim_cliente["ID_Cliente"]=dim_cliente.index+1

#reordeno las columnas
dim_cliente=dim_cliente[["ID_Cliente","Cliente"]]

#guardo el parquet del dim para cliente
dim_cliente.to_parquet("silver_archivosParquet/dim_cliente.parquet",index=False)
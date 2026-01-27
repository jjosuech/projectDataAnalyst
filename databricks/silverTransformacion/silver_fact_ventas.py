import pandas as pd

df=pd.read_parquet("bronze_archivosParquet/ventas_2024_limpio.parquet")

dim_fecha = pd.read_parquet("silver_archivosParquet/dim_fecha.parquet")
dim_producto = pd.read_parquet("silver_archivosParquet/dim_producto.parquet")
dim_cliente = pd.read_parquet("silver_archivosParquet/dim_cliente.parquet")

#hacemos la normalizacion para mayusculas y que coincidan las columnas para el join
df.columns=df.columns.str.upper()
dim_fecha.columns=dim_fecha.columns.str.upper()
dim_producto.columns=dim_producto.columns.str.upper()
dim_cliente.columns=dim_cliente.columns.str.upper()

#el join pero usando merge y left como join isquierda
fact= df.merge(
    dim_cliente,
    on="CLIENTE",
    how="left"
)

fact= fact.merge(
    dim_producto,
    on="PRODUCTO",
    how="left"
)

fact=fact.merge(
    dim_fecha,
    on="FECHA",
    how="left"
)

#ordenando para guardar
fact_ventas=fact[[
    "ID_CLIENTE",
    "ID_PRODUCTO",
    "ID_FECHA",
    "CANTIDAD",
    "PRECIO",
    "TOTAL_VENTA"
]]

fact_ventas["ID_VENTA"]=fact_ventas.index+1

fact_ventas=fact_ventas[[
    "ID_VENTA",
    "ID_CLIENTE",
    "ID_PRODUCTO",
    "ID_FECHA",
    "CANTIDAD",
    "PRECIO",
    "TOTAL_VENTA"
]]

fact_ventas.to_parquet("silver_archivosParquet/fact_ventas.parquet", index=False)

fact_ventas.to_excel("silver_archivosParquet/fact_ventas.xlsx", index=False)
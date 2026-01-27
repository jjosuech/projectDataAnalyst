import pandas as pd

#ruta = "archivosParquet"

#os.makedirs(ruta, exist_ok=True)

df=pd.read_excel("raw_archivosExcel/ventas_2024.xlsx")
#df.info()
#print(df.head())

df["Precio"]=pd.to_numeric(df["Precio"],errors="coerce")
df["Cantidad"]=pd.to_numeric(df["Cantidad"],errors="coerce")

df=df.dropna(subset=["Precio","Cantidad","Cliente"])
df["Cantidad"]=df["Cantidad"].astype(int)

df=df[df["Cantidad"]>0]
df=df[df["Precio"]>0]

df["Cliente"]=df["Cliente"].str.strip().str.upper()
df["Producto"]=df["Producto"].str.strip().str.upper()

df["TOTAL_VENTA"]=df["Cantidad"]*df["Precio"]

df.info()
df.describe()

#Visualizo con filtros el archivo de excel y armo un parquet si esta bien para el big data en nube
df.to_excel("bronze_archivosParquet/ventas_2024_limpio.xlsx",index=False)
df.to_parquet("bronze_archivosParquet/ventas_2024_limpio.parquet",index=False)
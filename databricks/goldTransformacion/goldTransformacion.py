import pandas as pd

fact_ventas = pd.read_parquet("silver_archivosParquet/fact_ventas.parquet")

fact_ventas.info()

gold_ventas_cliente = (
    fact_ventas
    .groupby("ID_CLIENTE")
    .agg(
        TOTAL_VENTAS=("TOTAL_VENTA","sum"),
        CANTIDAD_VENTAS=("ID_VENTA","count")
    ).reset_index()
)

gold_ventas_producto=(
    fact_ventas
    .groupby("ID_PRODUCTO")
    .agg(
        INGRESOS=("TOTAL_VENTA","sum"),
        UNIDADES_VENDIDAS=("CANTIDAD","sum")
    )
)

gold_ventas_fecha=(
    fact_ventas
    .groupby("ID_FECHA")
    .agg(
        VENTAS_FECHA=("TOTAL_VENTA","sum")
    )
)


gold_ventas_cliente.to_parquet("gold_archivosParquet/gold_ventas_cliente.parquet", index=False)
gold_ventas_producto.to_parquet("gold_archivosParquet/gold_ventas_producto.parquet", index=True)
gold_ventas_fecha.to_parquet("gold_archivosParquet/gold_ventas_fecha.parquet", index=True)
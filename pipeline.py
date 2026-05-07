from prefect import flow

# 1. Import semua fungsi flow dimensi
from dim_kurir_etl import etl_dim_kurir_flow
from dim_pelanggan_etl import etl_dim_pelanggan_flow
from dim_produk_etl import etl_dim_produk_flow
from dim_toko_etl import flow_etl_dim_toko
from dim_waktu_etl import flow_etl_dim_waktu

# 2. Import semua fungsi flow fact
from fact_sales_etl import etl_fact_sales_flow
from fact_delivery_etl import etl_fact_delivery_flow
from fact_target_sales_etl import etl_fact_target_sales_flow # Ini yang tertinggal sebelumnya

@flow(name="Main ETL Anggang", description="Orchestrator untuk seluruh pipeline DW")
def main_dw_flow():
    # FASE 1: Eksekusi Dimensi Terlebih Dahulu
    etl_dim_kurir_flow()
    etl_dim_pelanggan_flow()
    etl_dim_produk_flow()
    flow_etl_dim_toko()
    flow_etl_dim_waktu()

    # FASE 2: Eksekusi Fact Setelah Semua Dimensi Selesai
    etl_fact_sales_flow()
    etl_fact_delivery_flow()
    etl_fact_target_sales_flow()

if __name__ == "__main__":
    main_dw_flow()
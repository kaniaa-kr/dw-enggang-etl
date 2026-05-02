from prefect import flow

# 1. Import semua fungsi flow dimensi
from dim_kurir_etl import run_kurir_etl
from dim_pelanggan_etl import run_pelanggan_etl
from dim_produk_etl import run_produk_etl
from dim_toko_etl import run_toko_etl
from dim_waktu_etl import run_waktu_etl

# 2. Import semua fungsi flow fact
from fact_sales_etl import run_sales_etl
from fact_delivery_etl import run_delivery_etl
from fact_target_sales_etl import run_target_sales_etl # Ini yang tertinggal sebelumnya

@flow(name="Main ETL Anggang", description="Orchestrator untuk seluruh pipeline DW")
def main_dw_flow():
    # FASE 1: Eksekusi Dimensi Terlebih Dahulu
    run_kurir_etl()
    run_pelanggan_etl()
    run_produk_etl()
    run_toko_etl()
    run_waktu_etl()

    # FASE 2: Eksekusi Fact Setelah Semua Dimensi Selesai
    run_sales_etl()
    run_delivery_etl()
    run_target_sales_etl()

if __name__ == "__main__":
    main_dw_flow()
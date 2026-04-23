# File: pipeline.py
from prefect import flow, task

# 1. Definisikan Task dengan decorator @task
@task(name="Ambil Data Mentah", retries=2, retry_delay_seconds=5)
def extract_data():
    print("Mengambil data dari sumber...")
    return [1, 2, 3, 4, 5]

@task(name="Transformasi Data")
def transform_data(data):
    print("Memproses data...")
    return [x * 10 for x in data]

@task(name="Simpan Data")
def load_data(data):
    print(f"Menyimpan data ke database: {data}")

# 2. Definisikan Flow dengan decorator @flow
@flow(name="Pipeline ETL Sederhana", description="Contoh flow untuk materi kuliah SI")
def etl_flow():
    # Rangkai task di dalam flow
    data_mentah = extract_data()
    data_bersih = transform_data(data_mentah)
    load_data(data_bersih)

# 3. Jalankan flow
if __name__ == "__main__":
    etl_flow()

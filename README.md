# Data Warehouse ETL Pipeline (Tugas Data Warehouse)

Repository ini berisi kumpulan *script* ETL (Extract, Transform, Load) menggunakan **Prefect**, **Pandas**, dan **PostgreSQL** untuk studi kasus PT Enggang Ritel Nusantara.

---

## 1. Persiapan Awal (Virtual Environment)
Sangat disarankan memakai Virtual Environment agar *package* project ini tidak tercampur dengan Python global di PC Anda.

**Cara Memasang Environment:**
1. Di terminal Linux / MacOS: `python3 -m venv .venv`
2. Di terminal Windows: `python -m venv .venv`

**Cara Mengaktifkan Environment:**
- **Linux/MacOS / Git Bash:** `source .venv/bin/activate`
- **Windows (CMD):** `.venv\Scripts\activate`
- **Windows (PowerShell):** `.\.venv\Scripts\Activate.ps1` *(JIka error saat menjalankan, gunakan perintah `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process` terlebih dahulu)*

Jika aktif, terminal akan memunculkan tulisan `(.venv)`.

---

## 2. Instalasi Dependensi
Setelah environment aktif, instal seluruh spesifikasi paket (termasuk modul untuk keamanan password):
```bash
pip install pandas psycopg2-binary prefect requests python-dotenv
```

---

## 3. Konfigurasi Kredensial Database (SANGAT PENTING!) 🔒
Agar password *PostgreSQL* Anda tidak dapat dilihat oleh orang lain saat repository di-push ke GitHub, kita akan menyembunyikannya menggunakan dotenv.

**Langkah-langkah:**
1. Buat suatu file baru di dalam folder ini dan beri nama tepat: `.env`
2. Buka file `.env` tersebut, dan *copy-paste* teks di bawah ini (isikan sesuai data database Anda sendiri):
```ini
DB_HOST=localhost
DB_NAME=dwh_enggang_khatulistiwa
OLTP_DB_NAME=oltp_enggang_khatulistiwa
DB_USER=postgres
DB_PASSWORD=masukkan_password_postgres_kamu_di_sini
DB_PORT=5432
```
3. Pastikan juga kamu memiliki file bernama `.gitignore` yang memuat teks `.env` di dalamnya. Hal ini akan memproteksi file `.env` agar tidak ikut terkirim ketika kita mengetik `git add` dan `git push`.

*(Note: File `.py` di dalam repository ini sudah disesuaikan agar bisa membaca nilai dari file `.env`.)*

---

## 4. Menjalankan Pipeline ETL 🚀
Untuk melakukan proses Extract, Transform, dan Load (secara manual tanpa jadwal), jalankan perintah:
```bash
python pipeline.py
```
*(Script akan menjalankan seluruh dimensi secara berurutan, lalu dilanjutkan dengan tabel-tabel fakta).*

---

## 5. Penjadwalan (Scheduling) Menggunakan Prefect
Sesuai arahan tugas untuk men-deploy pipeline dan menjadwalkan agar berjalan otomatis setiap hari pada pukul **23:59**.

**Langkah-langkah di terminal:**
1. **Buat Work Pool:**
   ```bash
   prefect work-pool create "pool-kampus" --type process
   ```
2. **Deploy Pipeline ke Work Pool:** (beserta argumen `--cron` jadwal 23:59)
   ```bash
   prefect deploy pipeline.py:main_dw_flow -n "Deploy-ETL-Harian" -p "pool-kampus" --cron "59 23 * * *"
   ```
3. **Lihat Dashboard UI Prefect (Untuk Screenshot Tugas):**
   ```bash
   prefect server start
   ```
   Lalu buka web Anda di `http://127.0.0.1:4200/`. Pergi ke tab **Deployments** dan Anda akan melihat bahwa `Deploy-ETL-Harian` sudah siap dan terjadwal pukul `23:59`. Anda dapat me-*screenshot* halaman ini!
4. **Nyalakan Worker (Agar jadwal tersebut benar-benar tereksekusi):**
   Buka 1 tab terminal baru, aktifkan environment `source .venv/bin/activate`, dan jalankan:
   ```bash
   prefect worker start --pool "pool-kampus"
   ```
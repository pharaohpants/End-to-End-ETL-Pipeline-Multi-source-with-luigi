# ETL Pipeline - Perusahaan XYZ

End-to-end ETL (Extract, Transform, Load) data pipeline menggunakan **Luigi** untuk Perusahaan XYZ. Pipeline ini mengumpulkan data dari 3 sumber berbeda, membersihkannya, dan memuatnya ke Data Warehouse PostgreSQL.

---

## 📊 Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                              │
├──────────────────┬───────────────────┬───────────────────────────┤
│  PostgreSQL DB   │  CSV File         │  Web Scraping             │
│  Amazon Sales    │  Electronics      │  Detik.com Articles       │
│  (Tim Sales)     │  Products         │  (Tim Data Scientist)     │
│                  │  (Tim Product)    │                           │
└────────┬─────────┴─────────┬─────────┴──────────────┬────────────┘
         │                   │                        │
         ▼                   ▼                        ▼
┌──────────────────────────────────────────────────────────────────┐
│                      EXTRACT (Luigi Tasks)                       │
│  ExtractSalesData    ExtractProductsData    ExtractDetikArticles │
│  [INCREMENTAL]       [STATIC]               [STATIC]            │
└────────┬─────────────────────┬──────────────────────┬────────────┘
         │                     │                      │
         ▼                     ▼                      ▼
┌──────────────────────────────────────────────────────────────────┐
│                     TRANSFORM (Luigi Tasks)                      │
│  TransformSalesData  TransformProductsData  TransformReviewsData │
│  [INCREMENTAL]       [STATIC]               [STATIC]            │
└────────┬─────────────────────┬──────────────────────┬────────────┘
         │                     │                      │
         ▼                     ▼                      ▼
┌──────────────────────────────────────────────────────────────────┐
│                 LOAD to Data Warehouse (UPSERT)                  │
│  ┌────────────┐  ┌──────────────┐  ┌──────────────────────────┐ │
│  │ fact_sales │  │ dim_products │  │   nlp_training_data      │ │
│  │ dim_date   │  │              │  │                          │ │
│  └────────────┘  └──────────────┘  └──────────────────────────┘ │
│                    PostgreSQL (Docker)                            │
└──────────────────────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
ETL Optional Project/
├── main.py                  # Entry point - menjalankan seluruh ETL pipeline
├── Task/
│   ├── extract.py           # Extract: PostgreSQL, CSV, Web Scraping
│   ├── transform.py         # Transform: Cleansing & validasi data
│   └── load.py              # Load: UPSERT ke Data Warehouse
├── Scripts/
│   ├── run_etl.sh           # Shell script runner (Linux/WSL)
│   ├── setup_crontab.sh     # Setup cron scheduling
│   ├── monitor.sh           # Monitoring pipeline
│   └── cleanup.sh           # Cleanup logs & temp files
├── Data Source/
│   └── ElectronicsProductsPricingData.csv  # Source data produk (static)
├── docker-compose.yml       # Docker setup untuk Data Warehouse
├── dockerfile               # PostgreSQL 15 Alpine image
├── init.sql                 # SQL inisialisasi DW
├── requirement.txt          # Python dependencies
├── run_etl.bat              # Windows batch runner
├── setup_scheduler.ps1      # Windows Task Scheduler setup
├── ETL_Pipeline_Design.md   # Dokumentasi design pipeline
├── sales data.ipynb         # Jupyter Notebook analisis
├── study case.txt           # Deskripsi study case
├── .env.example             # Template environment variables
├── .gitignore               # Git ignore rules
└── README.md                # Dokumentasi ini
```

---

## 🚀 Getting Started

### Prerequisites

- **Python** 3.10+
- **Docker** & Docker Compose
- **PostgreSQL** (source database untuk sales data)
- **WSL/Linux** (untuk cron scheduling, opsional)

### 1. Clone Repository

```bash
git clone <repository-url>
cd "ETL Optional Project"
```

### 2. Setup Virtual Environment

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/WSL
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirement.txt
```

### 4. Setup Environment Variables

```bash
cp .env.example .env
```

Edit `.env` dan isi kredensial database:

```dotenv
DATABASE_URL=postgresql://postgres:your_password@localhost:5432/etl_db
WAREHOUSE_URL=postgresql://dw_user:dw_password@localhost:5433/xyz_warehouse
LOG_LEVEL=INFO
```

### 5. Start Data Warehouse (Docker)

```bash
docker-compose up -d
```

Verifikasi container berjalan:

```bash
docker ps
# xyz_data_warehouse should be running on port 5433
```

### 6. Pastikan Source Database Tersedia

Source database PostgreSQL (port `5432`) harus sudah berisi tabel `amazon_sales_data`.

---

## ▶️ Menjalankan Pipeline

### Full Pipeline

```bash
python main.py
```

### Per Phase

```bash
python main.py extract      # Hanya extract
python main.py transform    # Hanya transform
python main.py load         # Hanya load
```

### Utilitas

```bash
python main.py clean        # Hapus semua output files (force re-run)
python main.py help         # Tampilkan bantuan
```

### Via Shell Script (Linux/WSL)

```bash
chmod +x Scripts/run_etl.sh
./Scripts/run_etl.sh
```

### Via Batch File (Windows)

```cmd
run_etl.bat
```

---

## 📅 Scheduling (Cron)

### Setup Cron Job (Linux/WSL)

```bash
chmod +x Scripts/setup_crontab.sh
./Scripts/setup_crontab.sh
```

Default schedule: **setiap 5 menit** (dapat diubah di `setup_crontab.sh`).

### Untuk Production (Daily)

Edit `Scripts/setup_crontab.sh`, uncomment baris:

```bash
# Daily at 02:00 AM
0 2 * * * /path/to/run_etl.sh >> /path/to/logs/cron_$(date +%Y%m%d).log 2>&1
```

### Windows Task Scheduler

```powershell
.\setup_scheduler.ps1
```

---

## 🔄 Strategy: Incremental vs Static

Pipeline menggunakan dua strategi berbeda berdasarkan sifat data:

| Data Source | Strategy | Behavior |
|---|---|---|
| **Sales Data** | INCREMENTAL | `complete()` menggunakan `_is_file_fresh()` → selalu rerun, UPSERT ke DW |
| **Products Data** | STATIC | `complete()` cek flag file → run sekali, skip pada run berikutnya |
| **NLP Data** | STATIC | `complete()` cek flag file → run sekali, skip pada run berikutnya |
| **dim_date** | STATIC | `complete()` cek DB count → run sekali |

### Bagaimana INCREMENTAL Bekerja

```
Pipeline Run #1 (t=0)
  flag tidak ada / file lama → complete()=False → run task
  → UPSERT ke DW → flag ditulis → complete()=True ✅

Pipeline Run #2 (t < 5 menit)
  flag masih fresh (< 300s) → complete()=True → SKIP ⏭️

Pipeline Run #3 (t > 5 menit)
  flag sudah tua (> 300s) → complete()=False → rerun task ✅
```

---

## 🏗️ Data Warehouse Schema

```
┌──────────────┐     ┌──────────────────┐
│   dim_date   │     │   dim_products   │
├──────────────┤     ├──────────────────┤
│ date_id (PK) │     │ product_id (PK)  │
│ full_date    │     │ product_name     │
│ year         │     │ brand            │
│ month        │     │ manufacturer     │
│ day          │     │ main_category    │
│ quarter      │     │ prices_min       │
│ day_of_week  │     │ prices_max       │
│ day_name     │     │ prices_average   │
│ month_name   │     │ prices_currency  │
│ is_weekend   │     │ availability     │
└──────┬───────┘     └──────────────────┘
       │
       │ FK
       ▼
┌─────────────────────┐     ┌───────────────────────┐
│    fact_sales       │     │  nlp_training_data    │
├─────────────────────┤     ├───────────────────────┤
│ sale_id (PK)        │     │ text_id (PK)          │
│ product_name        │     │ article_id            │
│ main_category       │     │ judul                 │
│ sub_category        │     │ deskripsi             │
│ date_id (FK)        │     │ category              │
│ discount_price      │     │ url (UNIQUE)          │
│ actual_price        │     │ tanggal_artikel       │
│ discount_percentage │     │ scraped_date          │
│ ratings             │     └───────────────────────┘
│ no_of_ratings       │
│ image_url           │
│ product_link        │
│ UNIQUE(product_name,│
│   product_link)     │
└─────────────────────┘
```

**Catatan:** `fact_sales` dan `dim_products` **tidak memiliki relasi FK** karena domain berbeda:
- Sales data berisi **semua kategori** produk Amazon (fashion, electronics, dll)
- Products data berisi **khusus produk elektronik** saja

---

## 🧪 Testing Scenario

### 1. Insert Data Baru di Source

```sql
-- Connect ke source DB (port 5432)
INSERT INTO amazon_sales_data 
  ("name", main_category, sub_category, image, link, ratings, no_of_ratings, discount_price, actual_price)
VALUES 
  ('Testing Product', 'Testing Category', 'Testing Sub Category', 
   'https://example.com/image.png', 'https://example.com/', 5, 30, 450, 1000);
```

### 2. Jalankan Pipeline

```bash
python main.py
```

### 3. Verifikasi di Data Warehouse

```sql
-- Connect ke DW (port 5433)
SELECT * FROM fact_sales 
WHERE product_name = 'Testing Product';
```

Data testing berhasil jika record **muncul di Data Warehouse** setelah pipeline dijalankan.

---

## 📝 Logs

Pipeline log disimpan di folder `logs/`:

```
logs/
├── etl_pipeline_YYYYMMDD_HHMMSS.log   # Pipeline execution log
├── warehouse_tables_created.flag        # DW tables creation flag
├── dim_date_loaded.flag                 # Date dimension flag
├── dim_products_loaded.flag             # Products dimension flag
├── fact_sales_loaded.flag               # Sales fact flag (INCREMENTAL)
├── nlp_training_data_loaded.flag        # NLP data flag
└── flags/                               # Task-level flags
    ├── .extract_products.done
    ├── .extract_detik.done
    ├── .transform_products.done
    └── .transform_reviews.done
```

---

## 🛠️ Tech Stack

| Component | Technology |
|---|---|
| **Orchestrator** | Luigi |
| **Language** | Python 3.10+ |
| **Source DB** | PostgreSQL 15 |
| **Data Warehouse** | PostgreSQL 15 (Docker) |
| **Containerization** | Docker & Docker Compose |
| **Scheduling** | Crontab (Linux/WSL) / Task Scheduler (Windows) |
| **Web Scraping** | BeautifulSoup4 + Requests |
| **Data Processing** | Pandas |
| **ORM** | SQLAlchemy |

---

## 👤 Author

Data Engineer Team - Perusahaan XYZ

---

## 📄 License

This project is for educational purposes (Pacmann - Intro to Data Engineering).

# ETL Pipeline Multi Source

End-to-end ETL (Extract, Transform, Load) data pipeline menggunakan **Luigi** untuk Perusahaan XYZ. Pipeline ini mengumpulkan data dari 3 sumber berbeda, membersihkannya, dan memuatnya ke Data Warehouse PostgreSQL.

# Problem Statement
Perusahaan XYZ merupakan perusahaan rintisan yang belum memiliki infrastruktur data yang terintegrasi dan terstandarisasi. Data tersebar di berbagai tim dengan format, struktur, dan kualitas yang berbeda-beda.

Beberapa permasalahan utama yang diidentifikasi:

1. Tidak adanya database terpusat untuk menyimpan data yang telah diproses.
2. Kualitas data yang rendah (missing values, inkonsistensi format).
3. Struktur data yang tidak seragam antar sumber.
4. Kebutuhan data baru untuk keperluan riset NLP yang belum tersedia.
5. Belum adanya automated ETL pipeline untuk memastikan data terproses secara konsisten.

Kondisi ini menghambat proses analisis, pelaporan, dan pengembangan model data science.

# Requirement Gathering & Solution
Data source :
1. Sales Data : https://hub.docker.com/r/shandytp/amazon-sales-data-docker-db 
2. Product Data : https://drive.google.com/file/d/1J0Mv0TVPWv2L-So0g59GUiQJBhExPYl6/view?usp=sharing
3. Web scraping : disini saya melakukan scraping artikel berita detik.com dengan tag AI 

---

## 💡 Solusi yang Di-propose

Untuk mengatasi permasalahan data Perusahaan XYZ, solusi yang dibangun adalah **Automated ETL Pipeline** dengan pendekatan berikut:

| Masalah | Solusi |
|---|---|
| Tidak ada database terpusat | Membangun **Data Warehouse PostgreSQL** (Docker) sebagai single source of truth |
| Kualitas data rendah | Tahap **Transform** melakukan cleansing: handle missing values, konversi tipe data, validasi range (ratings 0-5), hapus duplikat |
| Struktur data tidak seragam | **Standarisasi schema** di Data Warehouse: `dim_date`, `dim_products`, `fact_sales`, `nlp_training_data` |
| Kebutuhan data NLP | **Web scraping** artikel Detik.com (tag AI) sebagai training data untuk tim Data Scientist |
| Belum ada automated pipeline | **Luigi** sebagai workflow orchestrator: dependency management, auto-skip task yang sudah selesai, scheduling via cron/Task Scheduler |

**Strategi Pipeline:**
- **INCREMENTAL** untuk Sales Data → selalu re-extract & UPSERT setiap run, sehingga data selalu up-to-date
- **STATIC** untuk Products & NLP → run sekali saja, skip pada run berikutnya (hemat resource)
- **UPSERT** (INSERT ON CONFLICT UPDATE) → tidak ada duplikasi di Data Warehouse

---
# Rancangan Desain ETL Pipeline 
<img width="2487" height="954" alt="image" src="https://github.com/user-attachments/assets/f3f2f11d-a194-4304-b819-fd7410c8b644" />

## ⚙️ Cara Kerja ETL Pipeline

### Alur Keseluruhan

Pipeline bekerja dalam **3 fase utama** yang diorkestrasi oleh **Luigi**:

```
EXTRACT → TRANSFORM → LOAD
```

Setiap fase terdiri dari Luigi Tasks yang memiliki **dependency** satu sama lain. Luigi secara otomatis menentukan urutan eksekusi dan meng-skip task yang sudah selesai berdasarkan method `complete()`.

### Fase 1: EXTRACT (Mengambil Data dari 3 Sumber)

| Task | Sumber | Metode | Output | Strategi |
|---|---|---|---|---|
| `ExtractSalesData` | PostgreSQL (port 5432) | `pd.read_sql()` via SQLAlchemy | `Data Source/raw_SalesData.csv` | **INCREMENTAL** — `_is_file_fresh()` < 30s |
| `ExtractProductsData` | CSV lokal | `pd.read_csv()` | `Data Source/raw/products_raw.csv` | **STATIC** — cek flag file |
| `ExtractDetikArticles` | Detik.com (tag AI) | BeautifulSoup scraping | `Data Source/raw/reviews_raw.csv` | **STATIC** — cek flag file |

### Fase 2: TRANSFORM (Membersihkan & Validasi Data)

| Task | Proses Cleaning | Output |
|---|---|---|
| `TransformSalesData` | Hapus simbol ₹, konversi harga ke float, hitung discount_percentage, validasi ratings (0-5), hapus duplikat berdasarkan (name, link) | `Data Source/cleaned/sales_cleaned.csv` |
| `TransformProductsData` | Handle missing values (brand, manufacturer), normalisasi harga min/max/average, standarisasi currency, hapus duplikat | `Data Source/cleaned/products_cleaned.csv` |
| `TransformReviewsData` | Cleaning teks (strip whitespace, normalize spasi), hapus duplikat URL & judul, filter artikel kosong, reset article_id | `Data Source/cleaned/reviews_cleaned.csv` |

### Fase 3: LOAD (Muat ke Data Warehouse via UPSERT)

| Task | Target Table | Strategi | Keterangan |
|---|---|---|---|
| `CreateWarehouseTables` | Semua tabel | STATIC | DDL: CREATE TABLE jika belum ada |
| `LoadDimDate` | `dim_date` | STATIC | Generate 2 tahun tanggal otomatis |
| `LoadDimProducts` | `dim_products` | STATIC | Append dari cleaned products |
| `LoadFactSales` | `fact_sales` | **INCREMENTAL** | UPSERT: `ON CONFLICT (product_name, product_link) DO UPDATE` |
| `LoadNLPTrainingData` | `nlp_training_data` | STATIC | Append dari cleaned articles |

### Strategi Incremental vs Static

```
INCREMENTAL (Sales Data):
  Run #1 → flag belum ada / file lama → complete()=False → JALANKAN → tulis flag
  Run #2 (< 5 menit) → flag masih fresh → complete()=True → SKIP ⏭️
  Run #3 (> 5 menit) → flag sudah expired → complete()=False → JALANKAN ulang ✅

STATIC (Products & NLP Data):
  Run #1 → flag belum ada → complete()=False → JALANKAN → tulis flag
  Run #2 → flag sudah ada → complete()=True → SKIP selamanya ⏭️
  (Kecuali flag dihapus manual via: python main.py clean)
```

### Cara Menjalankan Pipeline

**1. Setup awal (sekali saja):**
```bash
# Buat virtual environment & install dependencies
python -m venv venv
venv\Scripts\activate          # Windows
pip install -r requirement.txt

# Setup environment variables
cp .env.example .env           # Edit .env sesuai kredensial

# Jalankan Data Warehouse container
docker-compose up -d
```

**2. Menjalankan pipeline:**
```bash
python main.py                 # Full pipeline (Extract + Transform + Load)
python main.py extract         # Hanya extract
python main.py transform       # Hanya transform
python main.py load            # Hanya load
python main.py clean           # Hapus semua flag & output → force re-run
python main.py help            # Tampilkan bantuan
```

**3. Scheduling otomatis:**
```bash
# Linux/WSL - Cron setiap 5 menit
chmod +x Scripts/setup_crontab.sh && ./Scripts/setup_crontab.sh

# Windows - Task Scheduler
.\setup_scheduler.ps1
```

---

## 🛠️ Stack, Tools & Library

| Kategori | Tool/Library | Fungsi |
|---|---|---|
| **Orchestrator** | Luigi | Workflow orchestrator: dependency management antar task, auto-skip task complete, retry |
| **Bahasa** | Python 3.10+ | Bahasa utama seluruh pipeline |
| **Data Processing** | Pandas | Membaca CSV, cleaning, transformasi DataFrame, kalkulasi kolom baru |
| **Database ORM** | SQLAlchemy | Koneksi ke PostgreSQL (source & DW), eksekusi query SQL, UPSERT operations |
| **DB Driver** | psycopg2 | PostgreSQL adapter untuk Python (digunakan oleh SQLAlchemy) |
| **Web Scraping** | BeautifulSoup4 | Parsing HTML halaman Detik.com, ekstraksi judul, deskripsi, URL artikel |
| **HTTP Client** | Requests | Mengirim HTTP GET request ke Detik.com dengan retry logic |
| **Environment** | python-dotenv | Membaca file `.env` untuk konfigurasi (DATABASE_URL, WAREHOUSE_URL) |
| **Database** | PostgreSQL 15 (Alpine) | Source DB (port 5432) & Data Warehouse (port 5433) |
| **Container** | Docker & Docker Compose | Menjalankan Data Warehouse PostgreSQL dalam container terisolasi |
| **Scheduling** | Crontab / Task Scheduler | Menjalankan pipeline secara berkala (default: setiap 5 menit) |
| **Logging** | Python logging | Log ke file (`logs/etl_pipeline_*.log`) dan console |

---

## 📝 Snippet Code dari Masing-Masing Task

### 📥 EXTRACT Tasks

**ExtractSalesData** — Mengambil data penjualan dari PostgreSQL (INCREMENTAL):

```python
# Task/extract.py

class ExtractSalesData(luigi.Task):
    """
    INCREMENTAL: complete() checks if output is "fresh" (< 30 seconds old)
    → Before run():  file is old/missing → complete()=False → Luigi runs task
    → After run():   file just created   → complete()=True  → Luigi marks done
    → Next pipeline: file is old again   → reruns automatically
    """
    def output(self):
        return luigi.LocalTarget('Data Source/raw_SalesData.csv')
    
    def complete(self):
        return _is_file_fresh(self.output().path, max_age_seconds=30)
    
    def run(self):
        query_extract = "SELECT * FROM amazon_sales_data"
        sales_data = pd.read_sql(sql=query_extract, con=source_engine)
        
        os.makedirs('Data Source', exist_ok=True)
        sales_data.to_csv(self.output().path, index=False)
        
        print(f"[SUCCESS] Sales data extracted: {len(sales_data)} rows")
```

**ExtractProductsData** — Membaca data produk dari CSV (STATIC):

```python
# Task/extract.py

class ExtractProductsData(luigi.Task):
    """STATIC: Run once, skip if output + flag exist"""
    source_file = luigi.Parameter(default='Data Source/ElectronicsProductsPricingData.csv')
    
    def output(self):
        return luigi.LocalTarget('Data Source/raw/products_raw.csv')
    
    def complete(self):
        if self.output().exists() and os.path.exists('logs/flags/.extract_products.done'):
            return True
        return False
    
    def run(self):
        products_data = pd.read_csv(self.source_file)
        
        os.makedirs('Data Source/raw', exist_ok=True)
        products_data.to_csv(self.output().path, index=False)
        
        # Tulis flag → next run akan SKIP
        os.makedirs('logs/flags', exist_ok=True)
        with open('logs/flags/.extract_products.done', 'w') as f:
            f.write(f"Extracted at: {datetime.now()}\nTotal rows: {len(products_data)}\n")
        
        print(f"[SUCCESS] Products extracted: {len(products_data)} rows")
```

**ExtractDetikArticles** — Web scraping artikel Detik.com (STATIC):

```python
# Task/extract.py

class ExtractDetikArticles(luigi.Task):
    """STATIC: Run once, skip if output + flag exist"""
    base_url = luigi.Parameter(default='https://www.detik.com/tag/ai/?sortby=time&page=1')
    max_articles_per_page = luigi.IntParameter(default=100)
    
    def output(self):
        return luigi.LocalTarget('Data Source/raw/reviews_raw.csv')
    
    def complete(self):
        if self.output().exists() and os.path.exists('logs/flags/.extract_detik.done'):
            return True
        return False
    
    def _scrape_single_page(self, url, max_retries=3):
        """Scrape articles dari satu halaman dengan retry logic"""
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(url, headers=headers, timeout=15)
                soup = BeautifulSoup(response.text, 'html.parser')
                articles = soup.find_all('article', limit=self.max_articles_per_page)
                
                articles_data = []
                for article in articles:
                    title_elem = article.find('h2', class_='title')
                    judul = title_elem.get_text(strip=True) if title_elem else None
                    desc_elem = article.find('p')
                    deskripsi = desc_elem.get_text(strip=True) if desc_elem else None
                    link_elem = article.find('a', href=True)
                    article_url = link_elem['href'] if link_elem else None
                    
                    if judul and article_url:
                        articles_data.append({
                            'judul': judul, 'deskripsi': deskripsi,
                            'url': article_url, 'category': category,
                            'tanggal_artikel': tanggal_artikel,
                            'scraped_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        })
                
                # Pagination: cari link halaman berikutnya
                return pd.DataFrame(articles_data), next_page_url
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
                time.sleep(attempt * 5)  # Exponential backoff
        return pd.DataFrame(), None
    
    def run(self):
        all_articles = []
        current_url = self.base_url
        
        while current_url:
            df, next_page_url = self._scrape_single_page(current_url)
            if not df.empty:
                all_articles.append(df)
            current_url = next_page_url
            time.sleep(2)  # Rate limiting
        
        final_df = pd.concat(all_articles, ignore_index=True)
        final_df.insert(0, 'article_id', range(1, len(final_df) + 1))
        final_df.to_csv(self.output().path, index=False, encoding='utf-8-sig')
        
        print(f"[SUCCESS] Scraped {len(final_df)} articles")
```

---

### 🔄 TRANSFORM Tasks

**TransformSalesData** — Cleaning data penjualan (INCREMENTAL):

```python
# Task/transform.py

class TransformSalesData(luigi.Task):
    """INCREMENTAL: complete() checks if output is "fresh" (< 30 seconds old)"""
    
    def requires(self):
        from Task.extract import ExtractSalesData
        return ExtractSalesData()
    
    def output(self):
        return luigi.LocalTarget('Data Source/cleaned/sales_cleaned.csv')
    
    def complete(self):
        return _is_file_fresh(self.output().path, max_age_seconds=30)
    
    def run(self):
        df = pd.read_csv(self.requires().output().path)
        
        # [1] Drop Unnamed columns
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        
        # [2] Cleansing 'name' → fillna, strip
        df['name'] = df['name'].fillna('Unknown Product').str.strip()
        
        # [3] Categories → fillna
        df['main_category'] = df['main_category'].fillna('Uncategorized')
        df['sub_category'] = df['sub_category'].fillna('General')
        
        # [4] Ratings → numeric, clip 0-5
        df['ratings'] = pd.to_numeric(df['ratings'], errors='coerce').fillna(0.0)
        df.loc[df['ratings'] > 5, 'ratings'] = 5.0
        df.loc[df['ratings'] < 0, 'ratings'] = 0.0
        
        # [5] no_of_ratings → numeric, int
        df['no_of_ratings'] = pd.to_numeric(df['no_of_ratings'], errors='coerce').fillna(0).astype(int)
        
        # [6] Prices → hapus simbol ₹ dan koma, konversi ke float
        def clean_price(price_str):
            if pd.isna(price_str):
                return None
            price_str = str(price_str).replace('₹', '').replace(',', '').strip()
            try:
                return float(price_str)
            except ValueError:
                return None
        
        df['discount_price'] = df['discount_price'].apply(clean_price)
        df['actual_price'] = df['actual_price'].apply(clean_price)
        df['discount_price'] = df['discount_price'].fillna(df['actual_price'])
        df['actual_price'] = df['actual_price'].fillna(df['discount_price'])
        
        # Hitung discount_percentage
        df['discount_percentage'] = 0.0
        mask = (df['actual_price'] > 0) & (df['discount_price'] < df['actual_price'])
        df.loc[mask, 'discount_percentage'] = (
            (df.loc[mask, 'actual_price'] - df.loc[mask, 'discount_price']) / 
            df.loc[mask, 'actual_price'] * 100
        ).round(2)
        
        # [7] URLs → fillna empty string
        df['image'] = df['image'].fillna('')
        df['link'] = df['link'].fillna('')
        
        # [8] Hapus duplikat berdasarkan (name, link)
        df = df.drop_duplicates(subset=['name', 'link'], keep='first')
        
        # [9] Drop rows tanpa name
        df = df.dropna(subset=['name'])
        
        df.to_csv(self.output().path, index=False)
        print(f"[SUCCESS] Sales Data Cleaned! Final shape: {df.shape}")
```

**TransformProductsData** — Normalisasi data produk (STATIC):

```python
# Task/transform.py

class TransformProductsData(luigi.Task):
    """STATIC: run once, cek flag file"""
    
    def requires(self):
        from Task.extract import ExtractProductsData
        return ExtractProductsData()
    
    def output(self):
        return luigi.LocalTarget('Data Source/cleaned/products_cleaned.csv')
    
    def complete(self):
        if self.output().exists() and os.path.exists('logs/flags/.transform_products.done'):
            return True
        return False
    
    def run(self):
        df = pd.read_csv(self.requires().output().path)
        
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        df['name'] = df['name'].fillna('Unknown Product').str.strip()
        df['brand'] = df['brand'].fillna('Unknown')
        df['manufacturer'] = df['manufacturer'].fillna(df['brand'])
        
        # Standarisasi harga: numeric, handle min > max
        df['prices.amountMax'] = pd.to_numeric(df['prices.amountMax'], errors='coerce')
        df['prices.amountMin'] = pd.to_numeric(df['prices.amountMin'], errors='coerce')
        df['prices.amountMax'] = df['prices.amountMax'].fillna(df['prices.amountMin']).fillna(0.0)
        df['prices.amountMin'] = df['prices.amountMin'].fillna(df['prices.amountMax']).fillna(0.0)
        
        # Swap jika max < min
        mask = df['prices.amountMax'] < df['prices.amountMin']
        df.loc[mask, ['prices.amountMax', 'prices.amountMin']] = (
            df.loc[mask, ['prices.amountMin', 'prices.amountMax']].values
        )
        
        # Hitung average
        df['prices.average'] = (df['prices.amountMax'] + df['prices.amountMin']) / 2
        df['prices.currency'] = df['prices.currency'].fillna('USD').str.upper().str.strip()
        df['prices.availability'] = df['prices.availability'].fillna('Unknown')
        df['prices.condition'] = df['prices.condition'].fillna('New').str.title()
        
        # Hapus duplikat
        df = df.drop_duplicates(subset=['name', 'brand'], keep='first')
        
        df.to_csv(self.output().path, index=False)
        with open('logs/flags/.transform_products.done', 'w') as f:
            f.write(f"Transformed at: {datetime.now()}\n")
        
        print(f"[SUCCESS] Products Cleaned! Shape: {df.shape}")
```

**TransformReviewsData** — Cleaning data artikel NLP (STATIC):

```python
# Task/transform.py

class TransformReviewsData(luigi.Task):
    """STATIC: run once, cek flag file"""
    
    def requires(self):
        from Task.extract import ExtractDetikArticles
        return ExtractDetikArticles()
    
    def output(self):
        return luigi.LocalTarget('Data Source/cleaned/reviews_cleaned.csv')
    
    def complete(self):
        if self.output().exists() and os.path.exists('logs/flags/.transform_reviews.done'):
            return True
        return False
    
    def run(self):
        df = pd.read_csv(self.requires().output().path)
        
        df = df.dropna(how='all')
        df['judul'] = df['judul'].fillna('').str.strip().str.replace(r'\s+', ' ', regex=True)
        df['deskripsi'] = df['deskripsi'].fillna('').str.strip().str.replace(r'\s+', ' ', regex=True)
        df['tanggal_artikel'] = df['tanggal_artikel'].fillna('').str.strip()
        df['scraped_date'] = pd.to_datetime(df['scraped_date'], errors='coerce').fillna(datetime.now())
        df['url'] = df['url'].fillna('').str.strip()
        df['category'] = df['category'].fillna('Uncategorized').str.strip()
        
        # Hapus duplikat URL & judul
        df = df.drop_duplicates(subset=['url'], keep='first')
        df = df.drop_duplicates(subset=['judul'], keep='first')
        
        # Filter artikel kosong & reset ID
        df = df[df['judul'].str.len() > 0].reset_index(drop=True)
        df['article_id'] = range(1, len(df) + 1)
        
        final_columns = ['article_id', 'judul', 'deskripsi', 'category', 'url', 'tanggal_artikel', 'scraped_date']
        df = df[final_columns]
        
        df.to_csv(self.output().path, index=False, encoding='utf-8-sig')
        with open('logs/flags/.transform_reviews.done', 'w') as f:
            f.write(f"Transformed at: {datetime.now()}\n")
        
        print(f"[SUCCESS] NLP Data Cleaned! Shape: {df.shape}")
```

---

### 📤 LOAD Tasks

**CreateWarehouseTables** — DDL: Membuat tabel Data Warehouse:

```python
# Task/load.py

class CreateWarehouseTables(luigi.Task):
    """Create Data Warehouse tables with proper schema"""
    
    def complete(self):
        # Cek flag file DAN keberadaan tabel di database
        if not self.output().exists():
            return False
        inspector = inspect(get_warehouse_engine())
        tables = inspector.get_table_names()
        required = ['dim_date', 'dim_products', 'fact_sales', 'nlp_training_data']
        return all(t in tables for t in required)
    
    def run(self):
        engine = get_warehouse_engine()
        ddl_statements = [
            "DROP TABLE IF EXISTS fact_sales, dim_products, dim_date, nlp_training_data CASCADE;",
            """CREATE TABLE dim_date (
                date_id SERIAL PRIMARY KEY, full_date DATE UNIQUE NOT NULL,
                year INTEGER, month INTEGER, day INTEGER, quarter INTEGER,
                day_of_week INTEGER, day_name VARCHAR(10), month_name VARCHAR(10),
                is_weekend BOOLEAN, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);""",
            """CREATE TABLE dim_products (
                product_id SERIAL PRIMARY KEY, product_name VARCHAR(500) NOT NULL,
                brand VARCHAR(200), manufacturer VARCHAR(200), main_category TEXT,
                sub_category VARCHAR(200), prices_min DECIMAL(15,2), prices_max DECIMAL(15,2),
                prices_average DECIMAL(15,2), prices_currency VARCHAR(10),
                availability VARCHAR(100), condition VARCHAR(100));""",
            """CREATE TABLE fact_sales (
                sale_id SERIAL PRIMARY KEY, product_name TEXT NOT NULL,
                main_category VARCHAR(200), sub_category VARCHAR(200),
                date_id INTEGER REFERENCES dim_date(date_id),
                discount_price DECIMAL(15,2), actual_price DECIMAL(15,2),
                discount_percentage DECIMAL(5,2), ratings DECIMAL(3,2),
                no_of_ratings INTEGER, image_url TEXT, product_link TEXT,
                CONSTRAINT uq_fact_sales_product UNIQUE (product_name, product_link));""",
            """CREATE TABLE nlp_training_data (
                text_id SERIAL PRIMARY KEY, article_id INTEGER NOT NULL,
                judul TEXT NOT NULL, deskripsi TEXT, category VARCHAR(100),
                url TEXT UNIQUE, tanggal_artikel VARCHAR(100), scraped_date TIMESTAMP);"""
        ]
        with engine.connect() as conn:
            for ddl in ddl_statements:
                conn.execute(text(ddl))
                conn.commit()
```

**LoadFactSales** — UPSERT data sales ke fact_sales (INCREMENTAL):

```python
# Task/load.py

class LoadFactSales(luigi.Task):
    """INCREMENTAL: UPSERT mode — ON CONFLICT DO UPDATE"""
    
    def requires(self):
        from Task.transform import TransformSalesData
        return {'sales': TransformSalesData(), 'dates': LoadDimDate()}
    
    def complete(self):
        # INCREMENTAL: flag harus fresh (< 300 detik / 5 menit)
        return _is_file_fresh(self.output().path, max_age_seconds=300)
    
    def run(self):
        sales_df = pd.read_csv(self.requires()['sales'].output().path)
        engine = get_warehouse_engine()
        
        # Assign random date_id (FK ke dim_date)
        dates_df = pd.read_sql('SELECT date_id FROM dim_date', engine)
        sales_df['date_id'] = [random.choice(dates_df['date_id'].tolist()) for _ in range(len(sales_df))]
        
        before_count = pd.read_sql("SELECT COUNT(*) as cnt FROM fact_sales", engine)['cnt'].iloc[0]
        
        # UPSERT per-row dengan chunking (5000 rows/chunk)
        upsert_sql = """
            INSERT INTO fact_sales 
                (product_name, main_category, sub_category, date_id,
                 discount_price, actual_price, discount_percentage,
                 ratings, no_of_ratings, image_url, product_link, updated_at)
            VALUES (:product_name, :main_category, :sub_category, :date_id,
                    :discount_price, :actual_price, :discount_percentage,
                    :ratings, :no_of_ratings, :image_url, :product_link, CURRENT_TIMESTAMP)
            ON CONFLICT (product_name, product_link)
            DO UPDATE SET
                discount_price = EXCLUDED.discount_price,
                actual_price = EXCLUDED.actual_price,
                ratings = EXCLUDED.ratings,
                no_of_ratings = EXCLUDED.no_of_ratings,
                updated_at = CURRENT_TIMESTAMP
        """
        
        chunk_size = 5000
        for i in range(0, len(fact_sales), chunk_size):
            chunk = fact_sales.iloc[i:i + chunk_size]
            with engine.connect() as conn:
                for _, row in chunk.iterrows():
                    conn.execute(text(upsert_sql), row.to_dict())
                conn.commit()
        
        after_count = pd.read_sql("SELECT COUNT(*) as cnt FROM fact_sales", engine)['cnt'].iloc[0]
        print(f"   Before: {before_count} | After: {after_count} | New: {after_count - before_count}")
```

**LoadDimProducts** — Load ke dim_products (STATIC):

```python
# Task/load.py

class LoadDimProducts(luigi.Task):
    """STATIC: Run once, cek flag + DB count"""
    
    def requires(self):
        from Task.transform import TransformProductsData
        return {'tables': CreateWarehouseTables(), 'data': TransformProductsData()}
    
    def complete(self):
        if not self.output().exists():
            return False
        result = pd.read_sql("SELECT COUNT(*) as cnt FROM dim_products", get_warehouse_engine())
        return result['cnt'].iloc[0] > 0
    
    def run(self):
        products_df = pd.read_csv(self.requires()['data'].output().path)
        
        dim_products = pd.DataFrame({
            'product_name': products_df['name'],
            'brand': products_df['brand'],
            'manufacturer': products_df['manufacturer'],
            'main_category': products_df['categories'],
            'sub_category': products_df['primaryCategories'],
            'prices_min': products_df['prices.amountMin'],
            'prices_max': products_df['prices.amountMax'],
            'prices_average': products_df['prices.average'],
            'prices_currency': products_df['prices.currency'],
            'availability': products_df['prices.availability'],
            'condition': products_df['prices.condition']
        })
        
        dim_products.to_sql('dim_products', get_warehouse_engine(), if_exists='append', index=False)
        print(f"[SUCCESS] dim_products: {len(dim_products)} rows loaded")
```

**LoadNLPTrainingData** — Load artikel NLP (STATIC):

```python
# Task/load.py

class LoadNLPTrainingData(luigi.Task):
    """STATIC: Run once, cek flag + DB count"""
    
    def requires(self):
        from Task.transform import TransformReviewsData
        return {'tables': CreateWarehouseTables(), 'data': TransformReviewsData()}
    
    def complete(self):
        if not self.output().exists():
            return False
        result = pd.read_sql("SELECT COUNT(*) as cnt FROM nlp_training_data", get_warehouse_engine())
        return result['cnt'].iloc[0] > 0
    
    def run(self):
        reviews_df = pd.read_csv(self.requires()['data'].output().path)
        
        nlp_data = pd.DataFrame({
            'article_id': reviews_df['article_id'],
            'judul': reviews_df['judul'],
            'deskripsi': reviews_df['deskripsi'],
            'category': reviews_df['category'],
            'url': reviews_df['url'],
            'tanggal_artikel': reviews_df['tanggal_artikel'],
            'scraped_date': pd.to_datetime(reviews_df['scraped_date'])
        })
        
        nlp_data = nlp_data[nlp_data['judul'].notna() & (nlp_data['judul'] != '')]
        nlp_data.to_sql('nlp_training_data', get_warehouse_engine(), if_exists='append', index=False)
        print(f"[SUCCESS] nlp_training_data: {len(nlp_data)} articles loaded")
```

---

## 📊 Hasil Output dari Masing-Masing Scenario

### Scenario 1: First Run (Pipeline Pertama Kali, Belum Ada Data)

```
> python main.py

============================================================
    FULL ETL PIPELINE STARTED
============================================================
  Strategy   : INCREMENTAL (Sales) + STATIC (Products, NLP)
============================================================

--- PHASE 1: EXTRACT ---
============================================================
[EXTRACT] Sales Data from PostgreSQL (INCREMENTAL)
[SUCCESS] Sales data extracted: 1,465 rows
[SAVED]   Data Source/raw_SalesData.csv
============================================================
[EXTRACT] Electronics Products Data (STATIC - ONE TIME)
[SUCCESS] Products extracted: 8,956 rows
[NOTE]    Will NOT re-extract on next runs (STATIC)
============================================================
[EXTRACT] Detik.com Articles (STATIC - ONE TIME)
   Scraped 10 articles from page 1
   Scraped 10 articles from page 2
   ...
[SUCCESS] Scraped 150 articles
[NOTE]    Will NOT scrape again on next runs (STATIC)
============================================================

--- PHASE 2: TRANSFORM ---
============================================================
[TRANSFORM] Sales Data Cleansing (INCREMENTAL)
   Original shape: (1465, 10)
   [1/9] Dropping Unnamed columns...
   [2/9] Cleansing 'name' column...
   [3/9] Cleansing category columns...
   [4/9] Cleansing 'ratings' column...
   [5/9] Cleansing 'no_of_ratings' column...
   [6/9] Cleansing price columns...
   [7/9] Cleansing URL columns...
   [8/9] Removing duplicates... Removed 115 duplicates
   [9/9] Removing rows with critical missing data... Removed 0 rows
[SUCCESS] Sales Data Cleaned! Final shape: (1350, 10)
============================================================
[TRANSFORM] Products Data Cleansing (STATIC - ONE TIME)
   Original shape: (8956, 25)
   Removed 1,135 duplicates
[SUCCESS] Products Cleaned! Shape: (7821, 25)
============================================================
[TRANSFORM] NLP Training Data Cleansing (STATIC - ONE TIME)
   Original shape: (150, 6)
   Removed 3 URL duplicates
   Removed 2 title duplicates
[SUCCESS] NLP Data Cleaned! Shape: (145, 7)
============================================================

--- PHASE 3: LOAD ---
============================================================
[LOAD] Creating Data Warehouse Tables
   ✅ dim_date: [date_id, full_date, year, month, day, ...]
   ✅ dim_products: [product_id, product_name, brand, ...]
   ✅ fact_sales: [sale_id, product_name, main_category, ...]
   ✅ nlp_training_data: [text_id, article_id, judul, ...]
[SUCCESS] All tables created!
============================================================
[LOAD] Date Dimension (STATIC)
   Loaded 731 dates
[SUCCESS] dim_date ready!
============================================================
[LOAD] Products Dimension (STATIC - ONE TIME)
   Loaded 7,821 products
[SUCCESS] dim_products loaded!
============================================================
[LOAD] Sales Fact Table (INCREMENTAL - UPSERT)
   Source records  : 1,350
   Existing in DW  : 0
   Processed: 1,350/1,350
   ┌────────────────────────────────────┐
   │  UPSERT Summary                    │
   ├────────────────────────────────────┤
   │  Before :          0 records       │
   │  After  :      1,350 records       │
   │  New    :      1,350 records       │
   └────────────────────────────────────┘
[SUCCESS] fact_sales loaded!
============================================================
[LOAD] NLP Training Data (STATIC - ONE TIME)
   Loaded 145 articles
[SUCCESS] nlp_training_data loaded!
============================================================

    ETL PIPELINE COMPLETED SUCCESSFULLY!
  Duration   : 85.23s
============================================================
```

**Keterangan:** Semua task dijalankan karena belum ada flag file. Jumlah row berkurang dari Extract ke Transform karena proses cleaning dan deduplikasi.

---

### Scenario 2: Immediate Re-run (< 30 detik setelah run pertama)

```
> python main.py

============================================================
    FULL ETL PIPELINE STARTED
============================================================

--- PHASE 1: EXTRACT ---
[SKIP] Products already extracted (STATIC)
[SKIP] Detik articles already scraped (STATIC)
(Sales → _is_file_fresh=True within 30s → SKIP)

--- PHASE 2: TRANSFORM ---
[SKIP] Products already transformed (STATIC)
[SKIP] NLP data already transformed (STATIC)
(Sales → _is_file_fresh=True within 30s → SKIP)

--- PHASE 3: LOAD ---
(dim_date → DB count > 0 → SKIP)
(dim_products → flag exists + DB count > 0 → SKIP)
(fact_sales → _is_file_fresh within 300s → SKIP)
(nlp → flag exists + DB count > 0 → SKIP)

    ETL PIPELINE COMPLETED SUCCESSFULLY!
  Duration   : 2.15s
============================================================
```

**Keterangan:** SEMUA task di-SKIP. Sales data flag masih fresh (< 30 detik). Products & NLP sudah ada flag (STATIC). Pipeline selesai sangat cepat (~2 detik).

---

### Scenario 3: Re-run Setelah > 5 Menit (Incremental Trigger untuk Sales)

```
> python main.py

--- PHASE 1: EXTRACT ---
============================================================
[EXTRACT] Sales Data from PostgreSQL (INCREMENTAL)
[SUCCESS] Sales data extracted: 1,465 rows                         ← ✅ RUN (flag expired)
============================================================
[SKIP] Products already extracted (STATIC)                          ← ⏭️ SKIP
[SKIP] Detik articles already scraped (STATIC)                      ← ⏭️ SKIP

--- PHASE 2: TRANSFORM ---
============================================================
[TRANSFORM] Sales Data Cleansing (INCREMENTAL)                      ← ✅ RUN
   Removed 115 duplicates
[SUCCESS] Sales Data Cleaned! Final shape: (1350, 10)
============================================================
[SKIP] Products already transformed (STATIC)                        ← ⏭️ SKIP
[SKIP] NLP data already transformed (STATIC)                        ← ⏭️ SKIP

--- PHASE 3: LOAD ---
============================================================
[LOAD] Sales Fact Table (INCREMENTAL - UPSERT)                      ← ✅ RUN
   Source records  : 1,350
   Existing in DW  : 1,350
   ┌────────────────────────────────────┐
   │  UPSERT Summary                    │
   │  Before :      1,350 records       │
   │  After  :      1,350 records       │
   │  New    :          0 records       │  ← Tidak ada data baru, hanya UPDATE
   └────────────────────────────────────┘
[SUCCESS] fact_sales loaded!
============================================================

    ETL PIPELINE COMPLETED SUCCESSFULLY!
  Duration   : 42.10s
============================================================
```

**Keterangan:** Hanya Sales pipeline yang re-run (flag sudah expired > 30 detik/5 menit). Products & NLP tetap SKIP (STATIC). UPSERT mendeteksi tidak ada data baru, sehingga hanya melakukan UPDATE pada record yang sudah ada.

---

### Scenario 4: Setelah Insert Data Baru di Source Database

**Step 1:** Insert data testing di source PostgreSQL:
```sql
-- Connect ke source DB (port 5432)
INSERT INTO amazon_sales_data 
  ("name", main_category, sub_category, image, link, ratings, no_of_ratings, discount_price, actual_price)
VALUES 
  ('Testing Product', 'Testing Category', 'Testing Sub Category', 
   'https://example.com/image.png', 'https://example.com/', 5, 30, 450, 1000);
```

**Step 2:** Jalankan pipeline:
```
> python main.py

--- PHASE 1: EXTRACT ---
[EXTRACT] Sales Data from PostgreSQL (INCREMENTAL)
[SUCCESS] Sales data extracted: 1,466 rows                          ← 1 row baru!

--- PHASE 2: TRANSFORM ---
[TRANSFORM] Sales Data Cleansing (INCREMENTAL)
[SUCCESS] Sales Data Cleaned! Final shape: (1351, 10)

--- PHASE 3: LOAD ---
[LOAD] Sales Fact Table (INCREMENTAL - UPSERT)
   Source records  : 1,351
   Existing in DW  : 1,350
   ┌────────────────────────────────────┐
   │  UPSERT Summary                    │
   │  Before :      1,350 records       │
   │  After  :      1,351 records       │
   │  New    :          1 records       │  ← 1 record baru berhasil INSERT
   └────────────────────────────────────┘
[SUCCESS] fact_sales loaded!
============================================================
```

**Step 3:** Verifikasi di Data Warehouse:
```sql
-- Connect ke DW (port 5433)
SELECT product_name, discount_price, actual_price, discount_percentage, ratings
FROM fact_sales WHERE product_name = 'Testing Product';
```

| product_name | discount_price | actual_price | discount_percentage | ratings |
|---|---|---|---|---|
| Testing Product | 450.00 | 1000.00 | 55.00 | 5.00 |

---

### Scenario 5: Setelah `python main.py clean` (Force Re-run Semua)

```
> python main.py clean

Cleaning output files...
  Removed: Data Source/raw_SalesData.csv
  Removed: Data Source/raw/products_raw.csv
  Removed: Data Source/raw/reviews_raw.csv
  Removed: Data Source/cleaned/sales_cleaned.csv
  Removed: Data Source/cleaned/products_cleaned.csv
  Removed: Data Source/cleaned/reviews_cleaned.csv
  Removed: logs/flags/.extract_products.done
  Removed: logs/flags/.extract_detik.done
  Removed: logs/flags/.transform_products.done
  Removed: logs/flags/.transform_reviews.done
  Removed: logs/warehouse_tables_created.flag
  Removed: logs/dim_date_loaded.flag
  Removed: logs/dim_products_loaded.flag
  Removed: logs/fact_sales_loaded.flag
  Removed: logs/nlp_training_data_loaded.flag

  Cleaned 15 files. All tasks will re-run on next execution.
```

**Keterangan:** Semua file output dan flag dihapus. Pipeline berikutnya (`python main.py`) akan berjalan dari awal sama seperti Scenario 1.

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

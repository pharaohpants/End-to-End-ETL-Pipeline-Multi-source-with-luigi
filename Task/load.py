"""
Load Module - Data Warehouse Loading
=====================================

Schema Design:
- dim_date          : Date dimension (shared, auto-generated 2 years)
- dim_products      : Electronics product catalog (from products.csv)
- fact_sales        : Amazon sales transactions (STANDALONE)
- nlp_training_data : Detik.com articles for NLP training

Strategy:
- INCREMENTAL: Sales pipeline → _is_file_fresh() → always UPSERT on next run
- STATIC: Products, NLP, dim_date → run once with flag/DB check
"""

import pandas as pd
import luigi
from sqlalchemy import create_engine, text, inspect
import os
import time
from dotenv import load_dotenv
from datetime import datetime, timedelta
import random

# Load environment variables
load_dotenv()


def get_warehouse_engine():
    """Create connection to Data Warehouse PostgreSQL"""
    connection_string = os.getenv('WAREHOUSE_URL', 'postgresql://dw_user:dw_password@localhost:5433/xyz_warehouse')
    engine = create_engine(connection_string)
    return engine


def _is_file_fresh(filepath, max_age_seconds=300):
    """
    Check if file was created/modified within the last N seconds.
    
    For INCREMENTAL tasks:
    - Before run() → file is old/missing → False → Luigi runs task
    - After run()  → file just written   → True  → Luigi marks done
    - Next pipeline run → file is old    → False → Luigi runs again
    
    Default 300s (5 min) to accommodate long-running UPSERT operations.
    """
    if not os.path.exists(filepath):
        return False
    
    file_mtime = os.path.getmtime(filepath)
    age = time.time() - file_mtime
    return age <= max_age_seconds


# ============================================================
# CREATE TABLES (run once)
# ============================================================
class CreateWarehouseTables(luigi.Task):
    """Create Data Warehouse tables with proper schema"""
    
    def output(self):
        return luigi.LocalTarget('logs/warehouse_tables_created.flag')
    
    def complete(self):
        if not self.output().exists():
            return False
        try:
            engine = get_warehouse_engine()
            inspector = inspect(engine)
            tables = inspector.get_table_names()
            required = ['dim_date', 'dim_products', 'fact_sales', 'nlp_training_data']
            return all(t in tables for t in required)
        except Exception:
            return False
    
    def run(self):
        print("=" * 60)
        print("[LOAD] Creating Data Warehouse Tables")
        print("=" * 60)
        
        engine = get_warehouse_engine()
        
        ddl_statements = [
            """
            DROP TABLE IF EXISTS fact_sales CASCADE;
            DROP TABLE IF EXISTS dim_products CASCADE;
            DROP TABLE IF EXISTS dim_date CASCADE;
            DROP TABLE IF EXISTS nlp_training_data CASCADE;
            """,
            """
            CREATE TABLE dim_date (
                date_id SERIAL PRIMARY KEY,
                full_date DATE UNIQUE NOT NULL,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                day INTEGER NOT NULL,
                quarter INTEGER NOT NULL,
                day_of_week INTEGER NOT NULL,
                day_name VARCHAR(10) NOT NULL,
                month_name VARCHAR(10) NOT NULL,
                is_weekend BOOLEAN NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX idx_dim_date_full_date ON dim_date(full_date);
            """,
            """
            CREATE TABLE dim_products (
                product_id SERIAL PRIMARY KEY,
                product_name VARCHAR(500) NOT NULL,
                brand VARCHAR(200),
                manufacturer VARCHAR(200),
                main_category TEXT,
                sub_category VARCHAR(200),
                prices_min DECIMAL(15,2),
                prices_max DECIMAL(15,2),
                prices_average DECIMAL(15,2),
                prices_currency VARCHAR(10),
                availability VARCHAR(100),
                condition VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE fact_sales (
                sale_id SERIAL PRIMARY KEY,
                product_name TEXT NOT NULL,
                main_category VARCHAR(200),
                sub_category VARCHAR(200),
                date_id INTEGER REFERENCES dim_date(date_id),
                discount_price DECIMAL(15,2),
                actual_price DECIMAL(15,2),
                discount_percentage DECIMAL(5,2),
                ratings DECIMAL(3,2),
                no_of_ratings INTEGER,
                image_url TEXT,
                product_link TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT uq_fact_sales_product UNIQUE (product_name, product_link)
            );
            CREATE INDEX idx_fact_sales_date ON fact_sales(date_id);
            CREATE INDEX idx_fact_sales_category ON fact_sales(main_category);
            """,
            """
            CREATE TABLE nlp_training_data (
                text_id SERIAL PRIMARY KEY,
                article_id INTEGER NOT NULL,
                judul TEXT NOT NULL,
                deskripsi TEXT,
                category VARCHAR(100),
                url TEXT UNIQUE,
                tanggal_artikel VARCHAR(100),
                scraped_date TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        ]
        
        with engine.connect() as conn:
            for ddl in ddl_statements:
                conn.execute(text(ddl))
                conn.commit()
        
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        for table in ['dim_date', 'dim_products', 'fact_sales', 'nlp_training_data']:
            cols = [c['name'] for c in inspector.get_columns(table)]
            print(f"   ✅ {table}: {cols}")
        
        os.makedirs('logs', exist_ok=True)
        with self.output().open('w') as f:
            f.write(f'Tables created at {datetime.now()}')
        
        print(f"\n[SUCCESS] All tables created!")
        print("=" * 60 + "\n")


# ============================================================
# DIM_DATE = STATIC (run once)
# ============================================================
class LoadDimDate(luigi.Task):
    """Load Date Dimension - 2 years of dates (STATIC)"""
    
    def requires(self):
        return CreateWarehouseTables()
    
    def output(self):
        return luigi.LocalTarget('logs/dim_date_loaded.flag')
    
    def complete(self):
        if not self.output().exists():
            return False
        try:
            engine = get_warehouse_engine()
            result = pd.read_sql("SELECT COUNT(*) as cnt FROM dim_date", engine)
            return result['cnt'].iloc[0] > 0
        except Exception:
            return False
    
    def run(self):
        print("=" * 60)
        print("[LOAD] Date Dimension (STATIC)")
        print("=" * 60)
        
        engine = get_warehouse_engine()
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=730)
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        
        date_dim = pd.DataFrame({
            'full_date': date_range,
            'year': date_range.year,
            'month': date_range.month,
            'day': date_range.day,
            'quarter': date_range.quarter,
            'day_of_week': date_range.dayofweek,
            'day_name': date_range.day_name(),
            'month_name': date_range.month_name(),
            'is_weekend': date_range.dayofweek.isin([5, 6])
        })
        
        date_dim.to_sql('dim_date', engine, if_exists='append', index=False)
        
        print(f"   Loaded {len(date_dim)} dates")
        
        os.makedirs('logs', exist_ok=True)
        with self.output().open('w') as f:
            f.write(f'dim_date: {len(date_dim)} records at {datetime.now()}')
        
        print(f"[SUCCESS] dim_date ready!")
        print("=" * 60 + "\n")


# ============================================================
# DIM_PRODUCTS = STATIC (run once)
# ============================================================
class LoadDimProducts(luigi.Task):
    """Load Products Dimension (STATIC - ONE TIME)"""
    
    def requires(self):
        from Task.transform import TransformProductsData
        return {
            'tables': CreateWarehouseTables(),
            'data': TransformProductsData()
        }
    
    def output(self):
        return luigi.LocalTarget('logs/dim_products_loaded.flag')
    
    def complete(self):
        if not self.output().exists():
            return False
        try:
            engine = get_warehouse_engine()
            result = pd.read_sql("SELECT COUNT(*) as cnt FROM dim_products", engine)
            return result['cnt'].iloc[0] > 0
        except Exception:
            return False
    
    def run(self):
        print("=" * 60)
        print("[LOAD] Products Dimension (STATIC - ONE TIME)")
        print("=" * 60)
        
        engine = get_warehouse_engine()
        products_df = pd.read_csv(self.requires()['data'].output().path)
        
        dim_products = pd.DataFrame({
            'product_name': products_df['name'].fillna('Unknown'),
            'brand': products_df['brand'].fillna('Unknown'),
            'manufacturer': products_df['manufacturer'].fillna('Unknown'),
            'main_category': products_df['categories'].fillna('Uncategorized'),
            'sub_category': products_df['primaryCategories'].fillna('Uncategorized'),
            'prices_min': products_df['prices.amountMin'].fillna(0),
            'prices_max': products_df['prices.amountMax'].fillna(0),
            'prices_average': products_df['prices.average'].fillna(0),
            'prices_currency': products_df['prices.currency'].fillna('USD'),
            'availability': products_df['prices.availability'].fillna('Unknown'),
            'condition': products_df['prices.condition'].fillna('Unknown')
        })
        
        dim_products.to_sql('dim_products', engine, if_exists='append', index=False)
        
        print(f"   Loaded {len(dim_products)} products")
        
        os.makedirs('logs', exist_ok=True)
        with self.output().open('w') as f:
            f.write(f'dim_products: {len(dim_products)} records at {datetime.now()}')
        
        print(f"[SUCCESS] dim_products loaded!")
        print("=" * 60 + "\n")


# ============================================================
# FACT_SALES = INCREMENTAL (always rerun with UPSERT)
# ============================================================
class LoadFactSales(luigi.Task):
    """
    Load Sales Fact Table - UPSERT Mode
    
    INCREMENTAL: complete() uses _is_file_fresh()
    → Before run(): flag is old/missing → complete()=False → Luigi runs UPSERT
    → After run():  flag just written   → complete()=True  → Luigi marks done
    → Next pipeline run: flag is old    → complete()=False → UPSERT again
    
    max_age_seconds=300 (5 min) to accommodate long UPSERT duration
    """
    
    def requires(self):
        from Task.transform import TransformSalesData
        return {
            'sales': TransformSalesData(),
            'dates': LoadDimDate()
        }
    
    def output(self):
        return luigi.LocalTarget('logs/fact_sales_loaded.flag')
    
    def complete(self):
        # INCREMENTAL: hanya "complete" jika flag baru saja ditulis (< 5 menit)
        # → UPSERT 95k records butuh ~60-90 detik
        # → Flag ditulis di AKHIR run()
        # → Luigi cek complete() SEGERA setelah run() → flag masih fresh → True
        # → Next pipeline run → flag sudah tua (> 5 menit) → False → rerun
        return _is_file_fresh(self.output().path, max_age_seconds=300)
    
    def run(self):
        print("=" * 60)
        print("[LOAD] Sales Fact Table (INCREMENTAL - UPSERT)")
        print(f"[TIME] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
        sales_df = pd.read_csv(self.requires()['sales'].output().path)
        engine = get_warehouse_engine()
        
        # Get date_ids for FK
        dates_df = pd.read_sql('SELECT date_id FROM dim_date', engine)
        date_ids = dates_df['date_id'].tolist()
        
        before_count = pd.read_sql("SELECT COUNT(*) as cnt FROM fact_sales", engine)['cnt'].iloc[0]
        
        print(f"   Source records  : {len(sales_df)}")
        print(f"   Existing in DW  : {before_count}")
        
        # Assign random date_id
        random.seed(42)
        sales_df['date_id'] = [random.choice(date_ids) for _ in range(len(sales_df))]
        
        # Prepare DataFrame
        fact_sales = pd.DataFrame({
            'product_name': sales_df['name'].fillna('Unknown'),
            'main_category': sales_df['main_category'].fillna('Uncategorized'),
            'sub_category': sales_df['sub_category'].fillna('General'),
            'date_id': sales_df['date_id'],
            'discount_price': pd.to_numeric(sales_df['discount_price'], errors='coerce').fillna(0),
            'actual_price': pd.to_numeric(sales_df['actual_price'], errors='coerce').fillna(0),
            'discount_percentage': pd.to_numeric(sales_df['discount_percentage'], errors='coerce').fillna(0),
            'ratings': pd.to_numeric(sales_df['ratings'], errors='coerce').fillna(0),
            'no_of_ratings': pd.to_numeric(sales_df['no_of_ratings'], errors='coerce').fillna(0).astype(int),
            'image_url': sales_df['image'].fillna(''),
            'product_link': sales_df['link'].fillna('')
        })
        
        # Cap values to fit schema constraints
        fact_sales['discount_price'] = fact_sales['discount_price'].clip(0, 9999999999999.99)
        fact_sales['actual_price'] = fact_sales['actual_price'].clip(0, 9999999999999.99)
        fact_sales['discount_percentage'] = fact_sales['discount_percentage'].clip(0, 999.99)
        fact_sales['ratings'] = fact_sales['ratings'].clip(0, 5.00)
        
        # ============================
        # ALWAYS UPSERT
        # ============================
        upsert_sql = """
            INSERT INTO fact_sales 
                (product_name, main_category, sub_category, date_id,
                 discount_price, actual_price, discount_percentage,
                 ratings, no_of_ratings, image_url, product_link, updated_at)
            VALUES 
                (:product_name, :main_category, :sub_category, :date_id,
                 :discount_price, :actual_price, :discount_percentage,
                 :ratings, :no_of_ratings, :image_url, :product_link, CURRENT_TIMESTAMP)
            ON CONFLICT (product_name, product_link)
            DO UPDATE SET
                main_category = EXCLUDED.main_category,
                sub_category = EXCLUDED.sub_category,
                discount_price = EXCLUDED.discount_price,
                actual_price = EXCLUDED.actual_price,
                discount_percentage = EXCLUDED.discount_percentage,
                ratings = EXCLUDED.ratings,
                no_of_ratings = EXCLUDED.no_of_ratings,
                image_url = EXCLUDED.image_url,
                updated_at = CURRENT_TIMESTAMP
        """
        
        chunk_size = 5000
        total_processed = 0
        
        for i in range(0, len(fact_sales), chunk_size):
            chunk = fact_sales.iloc[i:i + chunk_size]
            
            with engine.connect() as conn:
                for _, row in chunk.iterrows():
                    conn.execute(
                        text(upsert_sql),
                        {
                            'product_name': row['product_name'],
                            'main_category': row['main_category'],
                            'sub_category': row['sub_category'],
                            'date_id': int(row['date_id']),
                            'discount_price': float(row['discount_price']),
                            'actual_price': float(row['actual_price']),
                            'discount_percentage': float(row['discount_percentage']),
                            'ratings': float(row['ratings']),
                            'no_of_ratings': int(row['no_of_ratings']),
                            'image_url': row['image_url'],
                            'product_link': row['product_link']
                        }
                    )
                conn.commit()
            
            total_processed = min(i + chunk_size, len(fact_sales))
            print(f"   Processed: {total_processed:,}/{len(fact_sales):,}")
        
        # Final count
        after_count = pd.read_sql("SELECT COUNT(*) as cnt FROM fact_sales", engine)['cnt'].iloc[0]
        new_records = after_count - before_count
        
        print(f"\n   ┌────────────────────────────────────┐")
        print(f"   │  UPSERT Summary                    │")
        print(f"   ├────────────────────────────────────┤")
        print(f"   │  Before : {before_count:>10,} records     │")
        print(f"   │  After  : {after_count:>10,} records     │")
        print(f"   │  New    : {new_records:>10,} records     │")
        print(f"   └────────────────────────────────────┘")
        
        # Write flag LAST — so _is_file_fresh() returns True when Luigi checks
        os.makedirs('logs', exist_ok=True)
        with self.output().open('w') as f:
            f.write(f'fact_sales: {after_count} (new: {new_records}) at {datetime.now()}')
        
        print(f"\n[SUCCESS] fact_sales loaded!")
        print("=" * 60 + "\n")


# ============================================================
# NLP = STATIC (run once)
# ============================================================
class LoadNLPTrainingData(luigi.Task):
    """Load NLP Training Data (STATIC - ONE TIME)"""
    
    def requires(self):
        from Task.transform import TransformReviewsData
        return {
            'tables': CreateWarehouseTables(),
            'data': TransformReviewsData()
        }
    
    def output(self):
        return luigi.LocalTarget('logs/nlp_training_data_loaded.flag')
    
    def complete(self):
        if not self.output().exists():
            return False
        try:
            engine = get_warehouse_engine()
            result = pd.read_sql("SELECT COUNT(*) as cnt FROM nlp_training_data", engine)
            return result['cnt'].iloc[0] > 0
        except Exception:
            return False
    
    def run(self):
        print("=" * 60)
        print("[LOAD] NLP Training Data (STATIC - ONE TIME)")
        print("=" * 60)
        
        engine = get_warehouse_engine()
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
        nlp_data.to_sql('nlp_training_data', engine, if_exists='append', index=False)
        
        print(f"   Loaded {len(nlp_data)} articles")
        
        os.makedirs('logs', exist_ok=True)
        with self.output().open('w') as f:
            f.write(f'nlp: {len(nlp_data)} records at {datetime.now()}')
        
        print(f"[SUCCESS] nlp_training_data loaded!")
        print("=" * 60 + "\n")


class LoadAllData(luigi.WrapperTask):
    def requires(self):
        return [
            LoadFactSales(),
            LoadDimProducts(),
            LoadNLPTrainingData()
        ]


if __name__ == '__main__':
    luigi.build([LoadAllData()], local_scheduler=True)
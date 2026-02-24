import pandas as pd
import luigi
import os
import time
from datetime import datetime


def _is_file_fresh(filepath, max_age_seconds=30):
    """
    Check if file was created/modified within the last N seconds.
    """
    if not os.path.exists(filepath):
        return False
    
    file_mtime = os.path.getmtime(filepath)
    age = time.time() - file_mtime
    return age <= max_age_seconds


# ============================================================
# SALES = INCREMENTAL (always rerun)
# ============================================================
class TransformSalesData(luigi.Task):
    """
    Transform dan cleansing Amazon Sales Data
    
    INCREMENTAL: complete() checks if output is "fresh" (< 30 seconds old)
    """
    
    def requires(self):
        from Task.extract import ExtractSalesData
        return ExtractSalesData()
    
    def output(self):
        return luigi.LocalTarget('Data Source/cleaned/sales_cleaned.csv')
    
    def complete(self):
        return _is_file_fresh(self.output().path, max_age_seconds=30)
    
    def run(self):
        print("=" * 60)
        print("[TRANSFORM] Sales Data Cleansing (INCREMENTAL)")
        print(f"[TIME]     {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
        # Load data
        df = pd.read_csv(self.requires().output().path)
        print(f"   Original shape: {df.shape}")
        
        # 1. Handle Unnamed columns
        print("\n   [1/9] Dropping Unnamed columns...")
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        
        # 2. Column: name
        print("   [2/9] Cleansing 'name' column...")
        df['name'] = df['name'].fillna('Unknown Product').str.strip()
        
        # 3. Categories
        print("   [3/9] Cleansing category columns...")
        df['main_category'] = df['main_category'].fillna('Uncategorized')
        df['sub_category'] = df['sub_category'].fillna('General')
        
        # 4. Ratings
        print("   [4/9] Cleansing 'ratings' column...")
        df['ratings'] = pd.to_numeric(df['ratings'], errors='coerce').fillna(0.0)
        df.loc[df['ratings'] > 5, 'ratings'] = 5.0
        df.loc[df['ratings'] < 0, 'ratings'] = 0.0
        
        # 5. Number of ratings
        print("   [5/9] Cleansing 'no_of_ratings' column...")
        df['no_of_ratings'] = pd.to_numeric(df['no_of_ratings'], errors='coerce').fillna(0).astype(int)
        
        # 6. Prices
        print("   [6/9] Cleansing price columns...")
        def clean_price(price_str):
            if pd.isna(price_str):
                return None
            price_str = str(price_str).replace('\u20b9', '').replace(',', '').strip()
            try:
                return float(price_str)
            except ValueError:
                return None
        
        df['discount_price'] = df['discount_price'].apply(clean_price)
        df['actual_price'] = df['actual_price'].apply(clean_price)
        df['discount_price'] = df['discount_price'].fillna(df['actual_price'])
        df['actual_price'] = df['actual_price'].fillna(df['discount_price'])
        
        # Calculate discount percentage
        df['discount_percentage'] = 0.0
        mask = (df['actual_price'] > 0) & (df['discount_price'] < df['actual_price'])
        df.loc[mask, 'discount_percentage'] = (
            (df.loc[mask, 'actual_price'] - df.loc[mask, 'discount_price']) / 
            df.loc[mask, 'actual_price'] * 100
        ).round(2)
        
        # 7. URLs
        print("   [7/9] Cleansing URL columns...")
        df['image'] = df['image'].fillna('')
        df['link'] = df['link'].fillna('')
        
        # 8. Duplicates
        print("   [8/9] Removing duplicates...")
        original_len = len(df)
        df = df.drop_duplicates(subset=['name', 'link'], keep='first')
        print(f"         Removed {original_len - len(df)} duplicates")
        
        # 9. Missing data
        print("   [9/9] Removing rows with critical missing data...")
        original_len = len(df)
        df = df.dropna(subset=['name'])
        print(f"         Removed {original_len - len(df)} rows")
        
        # Final columns
        final_columns = [
            'name', 'main_category', 'sub_category', 'image', 'link',
            'ratings', 'no_of_ratings', 'discount_price', 'actual_price', 
            'discount_percentage'
        ]
        df = df[final_columns]
        
        # Save
        os.makedirs('Data Source/cleaned', exist_ok=True)
        df.to_csv(self.output().path, index=False)
        
        print(f"\n[SUCCESS] Sales Data Cleaned!")
        print(f"   Final shape: {df.shape}")
        print(f"[SAVED]   {self.output().path}")
        print("=" * 60 + "\n")


# ============================================================
# PRODUCTS = STATIC (run once)
# ============================================================
class TransformProductsData(luigi.Task):
    """Transform products data (STATIC - ONE TIME)"""
    
    def requires(self):
        from Task.extract import ExtractProductsData
        return ExtractProductsData()
    
    def output(self):
        return luigi.LocalTarget('Data Source/cleaned/products_cleaned.csv')
    
    def complete(self):
        if self.output().exists() and os.path.exists('logs/flags/.transform_products.done'):
            print("[SKIP] Products already transformed (STATIC)")
            return True
        return False
    
    def run(self):
        print("=" * 60)
        print("[TRANSFORM] Products Data Cleansing (STATIC - ONE TIME)")
        print("=" * 60)
        
        df = pd.read_csv(self.requires().output().path)
        print(f"   Original shape: {df.shape}")
        
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        df['name'] = df['name'].fillna('Unknown Product').str.strip()
        df['brand'] = df['brand'].fillna('Unknown')
        df['manufacturer'] = df['manufacturer'].fillna(df['brand'])
        df['prices.amountMax'] = pd.to_numeric(df['prices.amountMax'], errors='coerce')
        df['prices.amountMin'] = pd.to_numeric(df['prices.amountMin'], errors='coerce')
        df['prices.amountMax'] = df['prices.amountMax'].fillna(df['prices.amountMin'])
        df['prices.amountMin'] = df['prices.amountMin'].fillna(df['prices.amountMax'])
        df['prices.amountMax'] = df['prices.amountMax'].fillna(0.0)
        df['prices.amountMin'] = df['prices.amountMin'].fillna(0.0)
        mask = df['prices.amountMax'] < df['prices.amountMin']
        df.loc[mask, ['prices.amountMax', 'prices.amountMin']] = (
            df.loc[mask, ['prices.amountMin', 'prices.amountMax']].values
        )
        df['prices.average'] = (df['prices.amountMax'] + df['prices.amountMin']) / 2
        df['prices.currency'] = df['prices.currency'].fillna('USD').str.upper().str.strip()
        df['prices.availability'] = df['prices.availability'].fillna('Unknown')
        df['prices.condition'] = df['prices.condition'].fillna('New').str.title()
        df['prices.isSale'] = df['prices.isSale'].fillna(False)
        df['categories'] = df['categories'].fillna('Uncategorized')
        df['primaryCategories'] = df['primaryCategories'].fillna(df['categories'])
        
        original_len = len(df)
        df = df.drop_duplicates(subset=['name', 'brand'], keep='first')
        print(f"   Removed {original_len - len(df)} duplicates")
        
        df = df.dropna(how='all')
        
        os.makedirs('Data Source/cleaned', exist_ok=True)
        df.to_csv(self.output().path, index=False)
        
        os.makedirs('logs/flags', exist_ok=True)
        with open('logs/flags/.transform_products.done', 'w') as f:
            f.write(f"Transformed at: {datetime.now()}\n")
        
        print(f"\n[SUCCESS] Products Cleaned! Shape: {df.shape}")
        print("=" * 60 + "\n")


# ============================================================
# NLP / REVIEWS = STATIC (run once)
# ============================================================
class TransformReviewsData(luigi.Task):
    """Transform NLP data (STATIC - ONE TIME)"""
    
    def requires(self):
        from Task.extract import ExtractDetikArticles
        return ExtractDetikArticles()
    
    def output(self):
        return luigi.LocalTarget('Data Source/cleaned/reviews_cleaned.csv')
    
    def complete(self):
        if self.output().exists() and os.path.exists('logs/flags/.transform_reviews.done'):
            print("[SKIP] NLP data already transformed (STATIC)")
            return True
        return False
    
    def run(self):
        print("=" * 60)
        print("[TRANSFORM] NLP Training Data Cleansing (STATIC - ONE TIME)")
        print("=" * 60)
        
        df = pd.read_csv(self.requires().output().path)
        print(f"   Original shape: {df.shape}")
        
        df = df.dropna(how='all')
        df['judul'] = df['judul'].fillna('').str.strip().str.replace(r'\s+', ' ', regex=True)
        df['deskripsi'] = df['deskripsi'].fillna('').str.strip().str.replace(r'\s+', ' ', regex=True)
        df['tanggal_artikel'] = df['tanggal_artikel'].fillna('').str.strip()
        df['scraped_date'] = pd.to_datetime(df['scraped_date'], errors='coerce').fillna(datetime.now())
        df['url'] = df['url'].fillna('').str.strip()
        df['category'] = df['category'].fillna('Uncategorized').str.strip()
        
        original_len = len(df)
        df = df.drop_duplicates(subset=['url'], keep='first')
        print(f"   Removed {original_len - len(df)} URL duplicates")
        
        current_len = len(df)
        df = df.drop_duplicates(subset=['judul'], keep='first')
        print(f"   Removed {current_len - len(df)} title duplicates")
        
        df = df[df['judul'].str.len() > 0]
        df = df.reset_index(drop=True)
        
        if 'article_id' in df.columns:
            df['article_id'] = range(1, len(df) + 1)
        else:
            df.insert(0, 'article_id', range(1, len(df) + 1))
        
        final_columns = ['article_id', 'judul', 'deskripsi', 'category', 'url', 'tanggal_artikel', 'scraped_date']
        df = df[final_columns]
        
        os.makedirs('Data Source/cleaned', exist_ok=True)
        df.to_csv(self.output().path, index=False, encoding='utf-8-sig')
        
        os.makedirs('logs/flags', exist_ok=True)
        with open('logs/flags/.transform_reviews.done', 'w') as f:
            f.write(f"Transformed at: {datetime.now()}\n")
        
        print(f"\n[SUCCESS] NLP Data Cleaned! Shape: {df.shape}")
        print("=" * 60 + "\n")


class TransformAllData(luigi.WrapperTask):
    def requires(self):
        return [
            TransformSalesData(),
            TransformProductsData(),
            TransformReviewsData()
        ]


if __name__ == '__main__':
    luigi.build([TransformAllData()], local_scheduler=True)
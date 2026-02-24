import pandas as pd
import luigi
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time

# Load environment variables
load_dotenv()

def db_source_postgres_engine():
    """Create PostgreSQL engine connection"""
    connection_string = os.getenv('DATABASE_URL')
    engine = create_engine(connection_string)
    return engine

# Global engine instance
source_engine = db_source_postgres_engine()


def _is_file_fresh(filepath, max_age_seconds=30):
    """
    Check if file was created/modified within the last N seconds.
    Used for INCREMENTAL tasks: 
    - Before run() → file is old → complete() returns False → Luigi runs the task
    - After run()  → file is fresh → complete() returns True → Luigi marks as done
    """
    if not os.path.exists(filepath):
        return False
    
    file_mtime = os.path.getmtime(filepath)
    age = time.time() - file_mtime
    return age <= max_age_seconds


# ============================================================
# SALES = INCREMENTAL (always rerun)
# ============================================================
class ExtractSalesData(luigi.Task):
    """
    Extract sales data from PostgreSQL database
    
    INCREMENTAL: complete() checks if output is "fresh" (< 30 seconds old)
    → Before run():  file is old/missing → complete()=False → Luigi runs task
    → After run():  file just created   → complete()=True  → Luigi marks done
    → Next pipeline run: file is old again → reruns automatically
    """
    
    def output(self):
        return luigi.LocalTarget('Data Source/raw_SalesData.csv')
    
    def complete(self):
        # INCREMENTAL: hanya "complete" jika file baru saja dibuat (< 30 detik)
        # → Sebelum run():  file lama → False → Luigi jalankan task
        # → Setelah run():  file baru → True  → Luigi anggap selesai
        # → Run berikutnya: file lama lagi → False → jalankan ulang
        return _is_file_fresh(self.output().path, max_age_seconds=30)
    
    def run(self):
        print("=" * 60)
        print("[EXTRACT] Sales Data from PostgreSQL (INCREMENTAL)")
        print(f"[TIME]    {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
        query_extract = "SELECT * FROM amazon_sales_data"
        sales_data = pd.read_sql(sql=query_extract, con=source_engine)
        
        os.makedirs('Data Source', exist_ok=True)
        sales_data.to_csv(self.output().path, index=False)
        
        print(f"[SUCCESS] Sales data extracted: {len(sales_data)} rows")
        print(f"[SAVED]   {self.output().path}")
        print("=" * 60 + "\n")


# ============================================================
# PRODUCTS = STATIC (run once)
# ============================================================
class ExtractProductsData(luigi.Task):
    """
    Extract electronics products data from CSV file
    
    STATIC: Run once, skip if output + flag exist
    """
    source_file = luigi.Parameter(default='Data Source/ElectronicsProductsPricingData.csv')
    
    def output(self):
        return luigi.LocalTarget('Data Source/raw/products_raw.csv')
    
    def complete(self):
        if self.output().exists() and os.path.exists('logs/flags/.extract_products.done'):
            print("[SKIP] Products already extracted (STATIC)")
            return True
        return False
    
    def run(self):
        print("=" * 60)
        print("[EXTRACT] Electronics Products Data (STATIC - ONE TIME)")
        print("=" * 60)
        
        if not os.path.exists(self.source_file):
            raise FileNotFoundError(f"Source file not found: {self.source_file}")
        
        products_data = pd.read_csv(self.source_file)
        
        os.makedirs('Data Source/raw', exist_ok=True)
        products_data.to_csv(self.output().path, index=False)
        
        os.makedirs('logs/flags', exist_ok=True)
        with open('logs/flags/.extract_products.done', 'w') as f:
            f.write(f"Extracted at: {datetime.now()}\n")
            f.write(f"Total rows: {len(products_data)}\n")
        
        print(f"[SUCCESS] Products extracted: {len(products_data)} rows")
        print(f"[NOTE]    Will NOT re-extract on next runs (STATIC)")
        print("=" * 60 + "\n")


# ============================================================
# NLP / DETIK ARTICLES = STATIC (run once)
# ============================================================
class ExtractDetikArticles(luigi.Task):
    """
    Extract articles from Detik.com for NLP training
    
    STATIC: Run once, skip if output + flag exist
    """
    base_url = luigi.Parameter(default='https://www.detik.com/tag/ai/?sortby=time&page=1')
    max_articles_per_page = luigi.IntParameter(default=100)
    
    def output(self):
        return luigi.LocalTarget('Data Source/raw/reviews_raw.csv')
    
    def complete(self):
        if self.output().exists() and os.path.exists('logs/flags/.extract_detik.done'):
            print("[SKIP] Detik articles already scraped (STATIC)")
            return True
        return False
    
    def _scrape_single_page(self, url, max_retries=3):
        """Scrape articles from a single page with retry logic"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(url, headers=headers, timeout=15)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')
                
                articles = soup.find_all('article', limit=self.max_articles_per_page)
                articles_data = []
                scraped_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                for idx, article in enumerate(articles, 1):
                    try:
                        title_elem = article.find('h2', class_='title')
                        judul = title_elem.get_text(strip=True) if title_elem else None
                        
                        desc_elem = article.find('p')
                        deskripsi = desc_elem.get_text(strip=True) if desc_elem else None
                        
                        date_elem = article.find('span', class_='date')
                        tanggal_artikel = None
                        category = None
                        
                        if date_elem:
                            category_elem = date_elem.find('span', class_='category')
                            category = category_elem.get_text(strip=True) if category_elem else None
                            date_text = date_elem.get_text(strip=True)
                            if category:
                                date_text = date_text.replace(category, '').strip()
                            tanggal_artikel = date_text
                        
                        link_elem = article.find('a', href=True)
                        article_url = link_elem['href'] if link_elem else None
                        
                        if judul and article_url:
                            articles_data.append({
                                'judul': judul,
                                'deskripsi': deskripsi,
                                'tanggal_artikel': tanggal_artikel,
                                'url': article_url,
                                'category': category,
                                'scraped_date': scraped_date
                            })
                    except Exception:
                        continue
                
                df = pd.DataFrame(articles_data)
                
                next_page_url = None
                pagination_links = soup.find_all('a', class_='last', href=True)
                for link in pagination_links:
                    img = link.find('img', alt='Kanan')
                    if img:
                        next_page_url = link['href']
                        if not next_page_url.startswith('http'):
                            next_page_url = 'https://www.detik.com' + next_page_url
                        break
                
                return df, next_page_url
                
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
                if attempt < max_retries:
                    time.sleep(attempt * 5)
                else:
                    return pd.DataFrame(), 'TIMEOUT'
            except Exception:
                return pd.DataFrame(), None
    
    def run(self):
        all_articles = []
        current_url = self.base_url
        page_num = 1
        timeout_count = 0
        max_consecutive_timeouts = 3
        
        print("=" * 60)
        print("[EXTRACT] Detik.com Articles (STATIC - ONE TIME)")
        print("=" * 60)
        
        while current_url:
            print(f"\n--- PAGE {page_num} ---")
            df, next_page_url = self._scrape_single_page(current_url)
            
            if next_page_url == 'TIMEOUT':
                timeout_count += 1
                if timeout_count >= max_consecutive_timeouts:
                    break
                from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
                parsed = urlparse(current_url)
                params = parse_qs(parsed.query)
                current_page = int(params.get('page', ['1'])[0])
                params['page'] = [str(current_page + 1)]
                new_query = urlencode({k: v[0] for k, v in params.items()})
                current_url = urlunparse(parsed._replace(query=new_query))
                page_num += 1
                time.sleep(5)
                continue
            
            timeout_count = 0
            
            if not df.empty:
                all_articles.append(df)
                print(f"   Scraped {len(df)} articles from page {page_num}")
            else:
                break
            
            if not next_page_url:
                break
            
            current_url = next_page_url
            page_num += 1
            time.sleep(2)
        
        if all_articles:
            final_df = pd.concat(all_articles, ignore_index=True)
            final_df.insert(0, 'article_id', range(1, len(final_df) + 1))
            
            os.makedirs('Data Source/raw', exist_ok=True)
            final_df.to_csv(self.output().path, index=False, encoding='utf-8-sig')
            
            os.makedirs('logs/flags', exist_ok=True)
            with open('logs/flags/.extract_detik.done', 'w') as f:
                f.write(f"Scraped at: {datetime.now()}\n")
                f.write(f"Total articles: {len(final_df)}\n")
            
            print(f"\n[SUCCESS] Scraped {len(final_df)} articles")
            print(f"[NOTE]    Will NOT scrape again on next runs (STATIC)")
        else:
            os.makedirs('Data Source/raw', exist_ok=True)
            pd.DataFrame().to_csv(self.output().path, index=False)
        
        print("=" * 60 + "\n")


class ExtractAllData(luigi.WrapperTask):
    def requires(self):
        return [
            ExtractSalesData(),
            ExtractProductsData(),
            ExtractDetikArticles()
        ]


if __name__ == '__main__':
    luigi.build([ExtractAllData()], local_scheduler=True)
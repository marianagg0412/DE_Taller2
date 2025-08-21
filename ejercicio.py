from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager
from pymongo import MongoClient
from datetime import datetime
import time
import logging
import os
from dotenv import load_dotenv

class MercadoLibreScraper:
    def __init__(self, mongo_uri=None, database_name=None, collection_name="mercadolibre"):
        load_dotenv()
        if mongo_uri is None:
            mongo_uri = os.getenv("MONGO_URI")
        if database_name is None:
            database_name = os.getenv("MONGO_DBNAME", "mydatabase")

        self.client = MongoClient(mongo_uri)
        self.db = self.client[database_name]
        self.collection = self.db[collection_name]

        self.driver = None
        self.wait = None
        self._setup_driver()

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def _setup_driver(self):
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service)
        self.wait = WebDriverWait(self.driver, 10)
    
    def search_products(self, keyword):
        self.driver.get('https://www.mercadolibre.com.co/')

        search_box = self.wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="cb1-edit"]')))
        search_box.clear()
        search_box.send_keys(keyword)

        search_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/header/div/div[2]/form/button')))
        search_button.click()

        self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'li.ui-search-layout__item')))

    def scrape_current_page(self):
        html = self.driver.page_source
        soup = BeautifulSoup(html, 'html.parser')

        items = soup.select('li.ui-search-layout__item')
        products = []

        for item in items:
            # Selecciona el enlace del título con clase poly-component__title
            link = item.select_one('a.poly-component__title')
            if link:
                titulo = link.text.strip()
                href = link.get('href')
                
                price_element = item.select_one('.poly-component__price .andes-money-amount__fraction')
                precio = price_element.text.strip() if price_element else None

                review_element = item.select_one('.poly-content__column .poly-component__reviews .poly-reviews__rating')
                calificacion = review_element.text.strip() if review_element else None

                product_data = {
                    "titulo": titulo,
                    "url": href,
                    "precio": precio,
                    "calificacion": calificacion,
                    "fecha_extraccion": datetime.now()
                }
                products.append(product_data)
        
        return products

    def navigate_to_next_page(self):
        try:
            # Scroll to about 85% down the page (where pagination usually is)
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.85);")
            time.sleep(3) 
            
            next_button = None
            
            for i in range(3):
                try:
                    next_button = self.driver.find_element(By.XPATH, '//a[@title="Siguiente"]')
                    if next_button.is_enabled() and next_button.is_displayed():
                        break
                except:
                    # Try scrolling a bit more if not found
                    self.driver.execute_script("window.scrollBy(0, 150);")
                    time.sleep(1)
                    continue
            
            if not next_button:
                self.logger.info("Next button not found after multiple attempts")
                return False
            
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
            time.sleep(2)
            
            self.driver.execute_script("arguments[0].click();", next_button)
            self.logger.info("Successfully clicked next button")
            
            time.sleep(4)
            
            self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'li.ui-search-layout__item')))
            
            time.sleep(2)
            
            return True
        except Exception as e:
            self.logger.info(f"No more pages available: {e}")
            return False
        
    def scrape_multiple_pages(self, keyword, max_pages=5):
        all_products = []

        self.search_products(keyword)

        for page in range (1, max_pages + 1):
            self.logger.info(f"Scraping page {page} for keyword '{keyword}'")

            products = self.scrape_current_page()
            
            for product in products:
                product['page'] = page
                product['keyword'] = keyword
            
            all_products.extend(products)
            self.logger.info(f"Found {len(products)} products on page {page}")

            if page < max_pages:
                self.logger.info(f"Attempting to navigate to page {page + 1}")
                if not self.navigate_to_next_page():
                    self.logger.info(f"No more pages available. Stopped at page {page}")
                    break
            
        return all_products

    def scrape_multiple_keywords(self, keywords, max_pages=5):
        all_products = []

        for keyword in keywords:
            self.logger.info(f"Starting search for keyword: {keyword}")

            products = self.scrape_multiple_pages(keyword, max_pages)
            
            all_products.extend(products)
            self.logger.info(f"Total products found for keyword '{keyword}': {len(products)}")

            time.sleep(3)
        
        return all_products
            
    def save_to_mongo(self, products):
        if products:
            result = self.collection.insert_many(products)
            self.logger.info(f"Inserted {len(result.inserted_ids)} products into MongoDB.")
        else:
            self.logger.warning("No products to save.")
    
    def clear_database(self):
        result = self.collection.delete_many({})
        return result.deleted_count

    def analyze_data(self):
        print("=== MERCADOLIBRE DATA ANALYSIS ===")

        #Query 1: Basics
        total_products = self.collection.count_documents({})
        print(f"Total products scraped: {total_products}")

        # Query 2: Products by keyword
        pipeline = [
            {"$group": {"_id": "$keyword", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        print("\nProducts by keyword:")
        for result in self.collection.aggregate(pipeline):
            print(f"  {result['_id']}: {result['count']}")
        
        # Query 3: Price analysis by keyword
        pipeline = [
            {"$match": {"precio": {"$ne": None}}},
            {"$addFields": {
                "precio_numerico": {
                    "$toDouble": {
                        "$replaceAll": {
                            "input": {"$replaceAll": {"input": "$precio", "find": ".", "replacement": ""}},
                            "find": ",",
                            "replacement": "."
                        }
                    }
                }
            }},
            {"$group": {
                "_id": "$keyword",
                "precio_promedio": {"$avg": "$precio_numerico"},
                "precio_minimo": {"$min": "$precio_numerico"},
                "precio_maximo": {"$max": "$precio_numerico"}
            }},
            {"$sort": {"precio_promedio": -1}}
        ]
        
        print("\nPrice analysis by keyword:")
        for result in self.collection.aggregate(pipeline):
            print(f"  {result['_id']}:")
            print(f"    Average: ${result['precio_promedio']:,.0f}")
            print(f"    Range: ${result['precio_minimo']:,.0f} - ${result['precio_maximo']:,.0f}")
        
        # Query 4: Find products with highest ratings
        rated_products = list(self.collection.find(
            {"calificacion": {"$ne": None}},
            {"titulo": 1, "calificacion": 1, "keyword": 1, "precio": 1}
        ).limit(5))
        
        print(f"\nProducts with ratings (sample of {len(rated_products)}):")
        for product in rated_products:
            print(f"  {product['keyword']}: {product['titulo'][:40]}... (Rating: {product['calificacion']})")
        

    def close(self):
        if self.driver:
            self.driver.quit()
        if self.client:
            self.client.close()
    
if __name__ == "__main__":
    scraper = MercadoLibreScraper()
    try:
        keywords = ['computador', 'celular', 'televisor']
        products = scraper.scrape_multiple_keywords(keywords, max_pages=2)
        scraper.save_to_mongo(products)
        scraper.analyze_data()

        # scraper.clear_database()
    except Exception as e:
        scraper.logger.error(f"An error occurred: {e}")
    finally:
        scraper.close()
                               
                
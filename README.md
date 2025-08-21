# MercadoLibre Scraper

This project contains a Python class and script to scrape product data from MercadoLibre Colombia using Selenium, BeautifulSoup, and MongoDB.

## Features
- Searches MercadoLibre for one or more keywords (e.g., "computador", "celular")
- Navigates through multiple pages of search results for each keyword
- Extracts product details such as title, price, URL, and more
- Optionally enters each product page for more detailed information
- Stores all results in a MongoDB collection
- Includes analysis functions to answer questions about the scraped data (e.g., average price, product counts by keyword)

## Main Components
- **MercadoLibreScraper class**: Handles browser automation, data extraction, pagination, and MongoDB storage
- **scrape_multiple_pages**: Scrapes multiple pages for a single keyword
- **scrape_multiple_keywords**: Scrapes multiple keywords, reusing the pagination logic
- **analyze_data**: Runs aggregation queries on the stored data to answer business questions
- **clear_database**: Deletes all documents in the MongoDB collection (for fresh runs)

## Usage
1. **Install dependencies**:
   - Python 3.8+
   - `pip install selenium beautifulsoup4 pymongo webdriver-manager`
   - Chrome browser installed
   - MongoDB running locally (see connection string in code)

2. **Run the script**:
   ```bash
   python3 ejercicio.py
   ```
   The script will scrape MercadoLibre for the configured keywords and store the results in MongoDB.

3. **Analyze data**:
   The script includes methods to analyze the data and print summaries (e.g., products per keyword, average price).

## Configuration
- Edit the `keywords` list in the `__main__` section to change search terms.
- Adjust `max_pages` to control how many result pages to scrape per keyword.
- MongoDB connection details can be changed in the class constructor.

## Notes
- The scraper uses Selenium with ChromeDriver (automatically managed by webdriver-manager).
- The script is designed for educational/demo purposes. For production use, add more error handling and respect MercadoLibre's robots.txt and terms of service.

## Example Output
```
Products by keyword:
  computador: 157
  celular: 165
  televisor: 142

Average price by keyword:
  computador: $1,500,000
  celular: $900,000
  televisor: $2,000,000
```

---

**Author:** Mariana González G
**Date:** August 2025

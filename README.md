```markdown
# shopify_scraper (example Scrapy project)

Quick start:

1. Create project folder and files as provided.
2. Install dependencies:
   pip install -r requirements.txt

3. If using Playwright (JS sites):
   pip install scrapy-playwright playwright
   playwright install

4. Run spiders:

- Shopify JSON-backed spider:
  scrapy crawl shopify_products -a shop=jewelry-demo.myshopify.com -o products.json

- Generic HTML spider:
  scrapy crawl generic_product -a start_url="https://example.com/category/page1" -o products.json

- Run a single spider file without project settings:
  scrapy runspider shopify_scraper/spiders/shopify_products.py -a shop=yourshop.myshopify.com -o products.json

Notes & Best Practices:
- Always obey robots.txt unless you have explicit permission.
- Respect rate limits and use DOWNLOAD_DELAY / AUTOTHROTTLE.
- For JavaScript heavy sites, use scrapy-playwright or a headless browser.
- For large-scale scraping, use a proxy pool and rotate IPs.
- Avoid storing credentials or scraping behind login without permission.
- Test and tune CSS/XPath selectors for each site.
```
import os
import sys


# Minimal + practical settings for scraping responsibly
BOT_NAME = "Hurtfelt"

SPIDER_MODULES = ["shopify_scraper.spiders"]
NEWSPIDER_MODULE = "shopify_scraper.spiders"

# Obey robots.txt by default — change only if you have permission to crawl
ROBOTSTXT_OBEY = True

# Concurrency and throttling (politeness)
CONCURRENT_REQUESTS = 8
DOWNLOAD_DELAY = 1.0         # seconds between requests to same domain
CONCURRENT_REQUESTS_PER_DOMAIN = 4
CONCURRENT_REQUESTS_PER_IP = 2
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1.0
AUTOTHROTTLE_MAX_DELAY = 10.0
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0

# Retries and timeouts
RETRY_ENABLED = True
RETRY_TIMES = 3
DOWNLOAD_TIMEOUT = 30

# Caching (development)
HTTPCACHE_ENABLED = True
HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
HTTPCACHE_EXPIRATION_SECS = 0

# Middlewares
DOWNLOADER_MIDDLEWARES = {
    'shopify_scraper.middlewares.RotateUserAgentMiddleware': 400,
    # 'shopify_scraper.middlewares.ProxyMiddleware': 410,  # enable if using proxies
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': 550,
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
}

# Pipelines
ITEM_PIPELINES = {
    'shopify_scraper.pipelines.JsonWriterPipeline': 300,
    'scrapy.pipelines.images.ImagesPipeline': 400,  # optional if you want images
}

# Images pipeline settings (optional)
IMAGES_STORE = 'images'   # local folder; ensure writable
IMAGES_EXPIRES = 90

# Output file default (used by pipeline)
OUTPUT_JSON = 'products.json'

# Logging
LOG_LEVEL = 'INFO'

# Playwright (optional, for JS-heavy sites)
# To use, install scrapy-playwright and enable these settings
# DOWNLOAD_HANDLERS = {"http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler", "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler"}
# TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
# PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 30000


if "SCRAPY_CLUSTER" in os.environ or "SHUB" in os.environ:
    from scrapy.utils import ossignal
    ossignal.install_shutdown_handlers = lambda *a, **kw: None
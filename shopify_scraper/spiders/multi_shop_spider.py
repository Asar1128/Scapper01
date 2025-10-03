import scrapy
import json
import os
import re
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse
from scrapy import signals

def _safe_shop_filename(shop: str) -> str:
    """Make a filesystem-safe filename from a shop domain."""
    safe = re.sub(r'[^A-Za-z0-9._-]+', '_', shop)
    return f"products_{safe}.jsonl"

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

class MultiShopSpider(scrapy.Spider):
    name = "multi_shop"
    custom_settings = {
        "CONCURRENT_REQUESTS": 4,
        "DOWNLOAD_DELAY": 1.0,
        "RETRY_TIMES": 3,
        "RETRY_HTTP_CODES": [429, 500, 502, 503, 504],
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1,
        "AUTOTHROTTLE_MAX_DELAY": 10,
    }

    def __init__(self, shops_file=None, shops=None, collection=None, tag=None, product_type=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Build shops from arg, environment, or settings
        self.shops = []
        if shops:
            self.shops.extend([s.strip() for s in shops.split(',') if s.strip()])
        if not self.shops:
            env_shops = os.environ.get('SHOPS')
            if env_shops:
                self.shops.extend([s.strip() for s in env_shops.split(',') if s.strip()])
        if not self.shops and hasattr(self, 'settings'):
            settings_shops = self.settings.getlist('SHOPS') or self.settings.get('SHOPS')
            if settings_shops:
                if isinstance(settings_shops, str):
                    self.shops.extend([s.strip() for s in settings_shops.split(',') if s.strip()])
                elif isinstance(settings_shops, (list, tuple)):
                    self.shops.extend([str(s).strip() for s in settings_shops if str(s).strip()])
        
        if not self.shops:
            raise ValueError("Provide shops=<comma,separated,list> or set SHOPS env/setting")

        # Filters (may be None)
        self.collection = collection.strip() if collection else None
        self.filter_tag = tag.strip().lower() if tag else None
        self.filter_product_type = product_type.strip().lower() if product_type else None
        
        self.shop_stats = {}
        self.shop_currency = {}
        self._header_written = set()
        self._header_yielded = set()
        self.issues_file = None
        self.seen_ids = {}
        self.pagination_mode = {}
        self.max_pages_per_shop = 500  # Increased limit for very large catalogs
        self.consecutive_empty_pages = {}  # Track empty pages to detect end
        self.shop_pagination_strategies = {}  # Track which strategy works for each shop

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def start_requests(self):
        for shop in self.shops:
            shop = shop.replace('https://', '').replace('http://', '').strip().rstrip('/')
            self.shop_stats.setdefault(shop, {'items': 0, 'saved': 0, 'failed': 0, 'pages_crawled': 0})
            self.seen_ids[shop] = set()
            self.pagination_mode[shop] = "page"  # Start with page-based
            self.consecutive_empty_pages[shop] = 0
            self.shop_pagination_strategies[shop] = "standard"  # standard, offset, or collection_pagination

            # Prefetch currency
            yield scrapy.Request(
                f"https://{shop}/collections/all",
                callback=self.parse_currency_page,
                meta={'shop': shop},
                dont_filter=True,
                priority=10,
            )

            # Start with standard pagination
            yield self._build_initial_request(shop)

    def _build_initial_request(self, shop, strategy="standard", page=1, offset=0):
        """Build initial request based on strategy"""
        if self.collection:
            if strategy == "standard":
                url = f"https://{shop}/collections/{self.collection}/products.json?limit=250&page={page}"
            elif strategy == "offset":
                url = f"https://{shop}/collections/{self.collection}/products.json?limit=250&offset={offset}"
            else:  # collection_pagination
                url = f"https://{shop}/collections/{self.collection}?page={page}&view=json"
        else:
            if strategy == "standard":
                url = f"https://{shop}/products.json?limit=250&page={page}"
            elif strategy == "offset":
                url = f"https://{shop}/products.json?limit=250&offset={offset}"
            else:  # Try alternate products endpoint
                url = f"https://{shop}/products.json?page={page}&limit=250"
        
        return scrapy.Request(
            url,
            callback=self.parse_products_json,
            meta={
                'shop': shop, 
                'page': page,
                'offset': offset,
                'strategy': strategy,
                'handle_httpstatus_list': [200, 401, 403, 404, 406, 429, 500]
            },
            dont_filter=True,
        )

    def write_shop_item(self, item_dict: dict, shop: str):
        try:
            filename = _safe_shop_filename(shop)
            if shop not in self._header_written:
                code = self.shop_currency.get(shop)
                header = {
                    "type": "currency_info",
                    "shop": shop,
                    "currency": code,
                    "detected_at": _now_iso(),
                }
                with open(filename, "a", encoding="utf-8") as f:
                    f.write(json.dumps(header, ensure_ascii=False))
                    f.write("\n")
                self._header_written.add(shop)

            with open(filename, "a", encoding="utf-8") as f:
                f.write(json.dumps(item_dict, ensure_ascii=False))
                f.write("\n")
            self.shop_stats[shop]['items'] += 1
            self.shop_stats[shop]['saved'] += 1
        except Exception as e:
            self.shop_stats[shop]['failed'] += 1
            self.logger.error("Failed to write data for site=%s error=%s", shop, e)

    def parse_products_json(self, response):
        shop = response.meta.get('shop')
        page = response.meta.get('page', 1)
        offset = response.meta.get('offset', 0)
        strategy = response.meta.get('strategy', 'standard')
        
        self.shop_stats.setdefault(shop, {'items': 0, 'saved': 0, 'failed': 0, 'pages_crawled': 0})
        
        # Safety check
        if page > self.max_pages_per_shop:
            self.logger.warning(f"Reached max pages for {shop} at page {page}")
            return
            
        self.shop_stats[shop]['pages_crawled'] += 1

        # Handle non-200 responses
        if response.status != 200:
            self.logger.warning(f"Got status {response.status} for {response.url}")
            
            # Try different strategy if current one fails
            if response.status in [404, 406]:
                self.logger.info(f"Trying alternative pagination strategy for {shop}")
                yield from self._try_alternative_strategy(shop, strategy, page, offset)
                return
            return

        try:
            # Try to parse as JSON first
            data = json.loads(response.text)
            products = data.get('products') or []
            
            # If no products in JSON, check if it's HTML (some shops return HTML for products.json)
            if not products and '<html' in response.text.lower():
                self.logger.info(f"Shop {shop} returned HTML instead of JSON, trying alternative strategy")
                yield from self._try_alternative_strategy(shop, strategy, page, offset)
                return
                
        except json.JSONDecodeError:
            # If JSON parsing fails, it might be HTML
            self.logger.info(f"JSON parse failed for {shop}, trying alternative strategy")
            yield from self._try_alternative_strategy(shop, strategy, page, offset)
            return
        except Exception as e:
            self.logger.error("Parse failed for %s: %s", response.url, e)
            yield self._build_next_request(shop, strategy, page, offset, response.url)
            return

        # Track empty pages
        if not products:
            self.consecutive_empty_pages[shop] += 1
            self.logger.info(f"Empty page {page} for {shop}. Consecutive empty: {self.consecutive_empty_pages[shop]}")
            
            # Stop after 3 consecutive empty pages
            if self.consecutive_empty_pages[shop] >= 3:
                self.logger.info(f"Stopping {shop} after 3 consecutive empty pages")
                return
        else:
            self.consecutive_empty_pages[shop] = 0

        new_ids = set()
        for prod in products:
            product_id = prod.get('id')
            if product_id and product_id not in self.seen_ids[shop]:
                new_ids.add(product_id)
                variants = prod.get('variants', []) or []
                name = prod.get('title')
                price = None
                if variants:
                    first_variant = variants[0] if isinstance(variants[0], dict) else None
                    price = first_variant.get('price') if first_variant else None

                images = prod.get('images', []) or []
                image_url = None
                if images:
                    first_img = images[0] if isinstance(images[0], dict) else None
                    image_url = first_img.get('src') if first_img else images[0] if isinstance(images[0], str) else None

                handle = prod.get('handle')
                product_url = f"https://{shop}/products/{handle}" if handle else None

                item_dict = {
                    'product_id': product_id,
                    'name': name,
                    'price': price,
                    'image_url': image_url,
                    'url': product_url,
                    'scraped_at': _now_iso(),
                    'page': page,
                    'strategy': strategy,
                }
                self.write_shop_item(item_dict, shop)
                yield {**item_dict}

        # Update seen IDs
        self.seen_ids[shop].update(new_ids)
        
        self.logger.info(f"Shop {shop}: Page {page}, Strategy {strategy}, found {len(products)} products, {len(new_ids)} new")

        # Continue to next page
        yield self._build_next_request(shop, strategy, page, offset, response.url, len(products))

    def _try_alternative_strategy(self, shop, current_strategy, page, offset):
        """Try alternative pagination strategies when current one fails"""
        strategies = ['standard', 'offset', 'alternate']
        
        # Remove current strategy
        if current_strategy in strategies:
            strategies.remove(current_strategy)
        
        if strategies:
            next_strategy = strategies[0]
            self.logger.info(f"Switching {shop} from {current_strategy} to {next_strategy} strategy")
            yield self._build_initial_request(shop, next_strategy, 1, 0)
        else:
            self.logger.warning(f"All pagination strategies failed for {shop}")

    def _build_next_request(self, shop, strategy, current_page, current_offset, current_url, products_count=0):
        """Build the next pagination request based on strategy"""
        if strategy == "standard":
            next_page = current_page + 1
            return self._build_initial_request(shop, strategy, next_page, 0)
        
        elif strategy == "offset":
            next_offset = current_offset + 250
            # If we got fewer than 250 products, we might be at the end
            if products_count < 250:
                self.logger.info(f"Offset strategy: Got only {products_count} products, likely at end for {shop}")
                return
            return self._build_initial_request(shop, strategy, 1, next_offset)
        
        else:  # alternate strategy
            next_page = current_page + 1
            return self._build_initial_request(shop, strategy, next_page, 0)

    def _extract_currency_from_page_source_json(self, response):
        text = response.text or ""
        patterns = [
            r"\"currency\"\s*:\s*\"([A-Z]{3})\"",
            r"\bcurrency\s*:\s*\"([A-Z]{3})\"",
            r"\b\"currency_code\"\s*:\s*\"([A-Z]{3})\"",
            r"\b\"shop_currency\"\s*:\s*\"([A-Z]{3})\"",
        ]
        snippet = text[:200000]
        for pat in patterns:
            m = re.search(pat, snippet)
            if m:
                return m.group(1)
        return None

    def parse_currency_page(self, response):
        shop = response.meta.get('shop')
        code = self._extract_currency_from_page_source_json(response)
        if code:
            self.shop_currency[shop] = code

    def spider_closed(self, spider):
        summary_path = "crawl_summary.txt"
        with open(summary_path, "w", encoding="utf-8") as f:
            f.write("crawl summary per shop\n")
            for shop, s in sorted(self.shop_stats.items()):
                line = f"{shop}: items={s.get('items',0)}, saved={s.get('saved',0)}, failed={s.get('failed',0)}, pages_crawled={s.get('pages_crawled',0)}\n"
                f.write(line)
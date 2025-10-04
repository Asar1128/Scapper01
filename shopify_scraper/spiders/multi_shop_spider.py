import scrapy
import json
import os
import re
from datetime import datetime, timezone

def _safe_shop_filename(shop: str) -> str:
    """Make a filesystem-safe filename from a shop domain."""
    safe = re.sub(r'[^A-Za-z0-9._-]+', '_', shop)
    return f"products_{safe}.jsonl"

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
        
        # Build shops from args, env, or settings
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

        # Optional filters
        self.collection = collection.strip() if collection else None
        self.filter_tag = tag.strip().lower() if tag else None
        self.filter_product_type = product_type.strip().lower() if product_type else None
        
        self.shop_stats = {}
        self.seen_ids = {}
        self.pagination_mode = {}
        self.max_pages_per_shop = 500
        self.consecutive_empty_pages = {}
        self.shop_pagination_strategies = {}

    def start_requests(self):
        for shop in self.shops:
            shop = shop.replace('https://', '').replace('http://', '').strip().rstrip('/')
            self.shop_stats.setdefault(shop, {'items': 0, 'saved': 0, 'failed': 0, 'pages_crawled': 0})
            self.seen_ids[shop] = set()
            self.pagination_mode[shop] = "page"
            self.consecutive_empty_pages[shop] = 0
            self.shop_pagination_strategies[shop] = "standard"
            yield self._build_initial_request(shop)

    def _build_initial_request(self, shop, strategy="standard", page=1, offset=0):
        """Build initial request based on strategy."""
        if self.collection:
            if strategy == "standard":
                url = f"https://{shop}/collections/{self.collection}/products.json?limit=250&page={page}"
            elif strategy == "offset":
                url = f"https://{shop}/collections/{self.collection}/products.json?limit=250&offset={offset}"
            else:
                url = f"https://{shop}/collections/{self.collection}?page={page}&view=json"
        else:
            if strategy == "standard":
                url = f"https://{shop}/products.json?limit=250&page={page}"
            elif strategy == "offset":
                url = f"https://{shop}/products.json?limit=250&offset={offset}"
            else:
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

    def parse_products_json(self, response):
        shop = response.meta.get('shop')
        page = response.meta.get('page', 1)
        offset = response.meta.get('offset', 0)
        strategy = response.meta.get('strategy', 'standard')
        
        self.shop_stats.setdefault(shop, {'items': 0, 'saved': 0, 'failed': 0, 'pages_crawled': 0})
        
        if page > self.max_pages_per_shop:
            self.logger.warning(f"Reached max pages for {shop} at page {page}")
            return
            
        self.shop_stats[shop]['pages_crawled'] += 1

        if response.status != 200:
            self.logger.warning(f"Got status {response.status} for {response.url}")
            if response.status in [404, 406]:
                yield from self._try_alternative_strategy(shop, strategy, page, offset)
                return
            return

        try:
            data = json.loads(response.text)
            products = data.get('products') or []
            if not products and '<html' in response.text.lower():
                yield from self._try_alternative_strategy(shop, strategy, page, offset)
                return
        except json.JSONDecodeError:
            yield from self._try_alternative_strategy(shop, strategy, page, offset)
            return
        except Exception as e:
            self.logger.error("Parse failed for %s: %s", response.url, e)
            yield self._build_next_request(shop, strategy, page, offset, response.url)
            return

        if not products:
            self.consecutive_empty_pages[shop] += 1
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
                
                isFullyOutOfStock = False
                isVariantOutOfStock = False
                if variants:
                    all_unavailable = all(v.get('available') is False for v in variants)
                    any_unavailable = any(v.get('available') is False for v in variants)
                    isFullyOutOfStock = all_unavailable
                    isVariantOutOfStock = any_unavailable

                yield {
                    'shop': shop,
                    'product_id': product_id,
                    'name': name,
                    'price': price,
                    'image_url': image_url,
                    'url': product_url,
                    'isFullyOutOfStock': isFullyOutOfStock,
                    'isVariantOutOfStock': isVariantOutOfStock,
                }

        self.seen_ids[shop].update(new_ids)
        yield self._build_next_request(shop, strategy, page, offset, response.url, len(products))

    def _try_alternative_strategy(self, shop, current_strategy, page, offset):
        strategies = ['standard', 'offset', 'alternate']
        if current_strategy in strategies:
            strategies.remove(current_strategy)
        if strategies:
            next_strategy = strategies[0]
            self.logger.info(f"Switching {shop} from {current_strategy} to {next_strategy}")
            yield self._build_initial_request(shop, next_strategy, 1, 0)
        else:
            self.logger.warning(f"All pagination strategies failed for {shop}")

    def _build_next_request(self, shop, strategy, current_page, current_offset, current_url, products_count=0):
        if strategy == "standard":
            return self._build_initial_request(shop, strategy, current_page + 1, 0)
        elif strategy == "offset":
            if products_count < 250:
                return
            return self._build_initial_request(shop, strategy, 1, current_offset + 250)
        else:
            return self._build_initial_request(shop, strategy, current_page + 1, 0)

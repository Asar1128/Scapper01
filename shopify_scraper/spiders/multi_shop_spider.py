import scrapy
import json
import os
import re
from datetime import datetime, timezone
from urllib.parse import urljoin
from scrapy import signals
from ..items import ProductItem


# Returns individual shop filenames( Eg data of the indiividual file will be stored in products_<shop>.jsonl)
def _safe_shop_filename(shop: str) -> str:
    """Make a filesystem-safe filename from a shop domain."""
    safe = re.sub(r'[^A-Za-z0-9._-]+', '_', shop)
    return f"products_{safe}.jsonl"

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

class MultiShopSpider(scrapy.Spider):
    name = "multi_shop"
    custom_settings = {
        "CONCURRENT_REQUESTS": 8,
        "DOWNLOAD_DELAY": 0.5,
    }

    def __init__(self, shops_file=None, shops=None, collection=None, tag=None, product_type=None, *args, **kwargs):
        """
        Accept optional filters:
          - collection: Shopify collection handle (requests /collections/<handle>/products.json)
          - tag: product tag to filter by (case-insensitive)
          - product_type: product_type to filter by (case-insensitive)

        Usage:
          scrapy crawl multi_shop -a shops_file=shops.txt -a collection=sneakers
        """
        super().__init__(*args, **kwargs)
        # Build shops from arg, environment, or settings (no local text files)
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
        self.issues_file = None

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def start_requests(self):
        for shop in self.shops:
            shop = shop.replace('https://', '').replace('http://', '').strip().rstrip('/')
            self.shop_stats.setdefault(shop, {'items': 0, 'saved': 0, 'failed': 0})
            # Prefetch currency from /collections/all so we can write header first
            yield scrapy.Request(
                f"https://{shop}/collections/all",
                callback=self.parse_currency_page,
                meta={'shop': shop},
                dont_filter=True,
                priority=10,
            )
            # Choose endpoint: collection products JSON if collection filter provided, otherwise site-wide products.json
            if self.collection:
                url = f"https://{shop}/collections/{self.collection}/products.json?limit=250"
            else:
                url = f"https://{shop}/products.json?limit=250"
            yield scrapy.Request(
                url,
                callback=self.parse_products_json,
                meta={
                    'shop': shop,
                },
                dont_filter=True,
            )

    # ---- Issue recording helper ----
    def record_shop_issue(self, shop: str, issue: str, url: str = None):
        """Log issue details; avoid filesystem writes for Zyte compatibility."""
        self.logger.warning("Issue | shop=%s | url=%s | %s", shop, url or '-', issue)

    # ---- Writing helpers ----

    def write_shop_item(self, item_dict: dict, shop: str):
        try:
            filename = _safe_shop_filename(shop)
            # Ensure currency header is written once at the top of the file
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
            self.shop_stats.setdefault(shop, {'items': 0, 'saved': 0, 'failed': 0})
            self.shop_stats[shop]['items'] += 1
            self.shop_stats[shop]['saved'] += 1
            ident = item_dict.get('url') or item_dict.get('product_id') or item_dict.get('title') or "<unknown>"
            self.logger.info("Data retrieved and saved: site=%s item=%s", shop, ident)
        except Exception as e:
            self.shop_stats.setdefault(shop, {'items': 0, 'saved': 0, 'failed': 0})
            self.shop_stats[shop]['failed'] += 1
            self.logger.error("Failed to write data for site=%s error=%s", shop, e)
            self.record_shop_issue(shop, f"Write error: {e}")

    # ---- Parsing logic ----
    def parse_products_json(self, response):
        shop = response.meta.get('shop')
        self.shop_stats.setdefault(shop, {'items': 0, 'saved': 0, 'failed': 0})
        try:
            data = json.loads(response.text)
        except Exception as e:
            self.shop_stats[shop]['failed'] += 1
            self.logger.error("JSON parse failed for %s: %s", response.url, e)
            return

        products = data.get('products') or []
        if not products:
            self.logger.info("No products returned from JSON for site: %s", shop)

        for prod in products:

                # Compute availability flags based on variants
                variants = prod.get('variants', []) or []
                availability_flags = []
                for v in variants:
                    if isinstance(v, dict):
                        if 'available' in v and v.get('available') is not None:
                            availability_flags.append(bool(v.get('available')))
                        elif 'inventory_quantity' in v and v.get('inventory_quantity') is not None:
                            try:
                                availability_flags.append(int(v.get('inventory_quantity', 0)) > 0)
                            except Exception:
                                availability_flags.append(True)
                        else:
                            availability_flags.append(True)
                is_variant_out_of_stock = (len(availability_flags) > 0 and any(not a for a in availability_flags))
                is_fully_out_of_stock = (len(availability_flags) > 0 and all(not a for a in availability_flags))

                # Minimal output fields
                name = prod.get('title')
                product_id = prod.get('id')
                price = None
                if variants:
                    first_variant = variants[0] if isinstance(variants[0], dict) else None
                    price = first_variant.get('price') if first_variant else None

                images = prod.get('images', []) or []
                first_image_src = None
                for img in images:
                    if isinstance(img, dict):
                        if img.get('src'):
                            first_image_src = img.get('src')
                            break
                    elif isinstance(img, str) and img:
                        first_image_src = img
                        break
                image_url = urljoin(f"https://{shop}", first_image_src) if first_image_src else None

                item_dict = {
                    'product_id': product_id,
                    'name': name,
                    'price': price,
                    'image_url': image_url,
                    'isFullyOutOfStock': is_fully_out_of_stock,
                    'isVariantOutOfStock': is_variant_out_of_stock,
                }

                self.write_shop_item(item_dict, shop)

        # pagination (since_id)
        if products:
            last_id = products[-1].get('id')
            if last_id:
                next_url = f"https://{shop}/products.json?limit=250&since_id={last_id}"
                yield scrapy.Request(
                    next_url,
                    callback=self.parse_products_json,
                    meta={'shop': shop},
                    dont_filter=True,
                )

    # ---- Currency extraction from page source JSON ----
    def _extract_currency_from_page_source_json(self, response):
        # Only look for explicit JSON-like currency keys in the source
        text = response.text or ""
        # Common JSON patterns
        patterns = [
            r"\"currency\"\s*:\s*\"([A-Z]{3})\"",           # {"currency":"PKR"}
            r"\bcurrency\s*:\s*\"([A-Z]{3})\"",                # currency:"PKR"
            r"\b\"currency_code\"\s*:\s*\"([A-Z]{3})\"",   # {"currency_code":"PKR"}
            r"\b\"shop_currency\"\s*:\s*\"([A-Z]{3})\"",    # {"shop_currency":"PKR"}
        ]
        # Scan a reasonable prefix of the document for speed
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
        # Write header line immediately if not yet written
        if shop not in self._header_written:
            filename = _safe_shop_filename(shop)
            header = {
                "type": "currency_info",
                "shop": shop,
                "currency": self.shop_currency.get(shop),
                "detected_at": _now_iso(),
            }
            try:
                with open(filename, "a", encoding="utf-8") as f:
                    f.write(json.dumps(header, ensure_ascii=False))
                    f.write("\n")
                self._header_written.add(shop)
            except Exception as e:
                self.logger.error("Failed writing currency header for %s: %s", shop, e)
    def spider_closed(self, spider):
        summary_path = "crawl_summary.txt"
        try:
            with open(summary_path, "w", encoding="utf-8") as f:
                f.write("crawl summary per shop\n")
                for shop, s in sorted(self.shop_stats.items()):
                    line = f"{shop}: items={s.get('items',0)}, saved={s.get('saved',0)}, failed={s.get('failed',0)}\n"
                    f.write(line)
                    if s.get('saved', 0):
                        self.logger.info("Summary: Data retrieved from site %s: saved=%s items", shop, s.get('saved', 0))
                    if s.get('failed', 0):
                        self.logger.warning("Summary: Data NOT retrieved for site %s: failures=%s", shop, s.get('failed', 0))
            self.logger.info("Crawl summary written to %s", summary_path)
        except Exception as e:
            self.logger.error("Failed to write crawl summary: %s", e)   
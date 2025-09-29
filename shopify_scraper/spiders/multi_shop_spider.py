import scrapy
import json
import os
import re
from datetime import datetime, timezone
from urllib.parse import urljoin
from scrapy import signals
from ..items import ProductItem

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

        # Stats per shop
        self.shop_stats = {}
        # Cache detected currency per shop and track header writing
        self.shop_currency = {}  # { shop: (code or None, source or None) }
        self._header_written = set()

        # No filesystem-based issue/protected files in Zyte mode
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
            # Prefetch currency from /collections/all (common on Shopify) so we can write header first
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
                    'attempt': 1,
                    'handle_httpstatus_list': [401, 403, 404],
                    'filter_tag': self.filter_tag,
                    'filter_product_type': self.filter_product_type,
                    'collection': self.collection,
                },
                errback=self.errback_handler,
                dont_filter=True,
            )

    # ---- Issue recording helper ----
    def record_shop_issue(self, shop: str, issue: str, url: str = None):
        """Log issue details; avoid filesystem writes for Zyte compatibility."""
        self.logger.warning("Issue | shop=%s | url=%s | %s", shop, url or '-', issue)

    # ---- Errback & writing helpers ----
    def errback_handler(self, failure):
        req = failure.request if failure else None
        shop = req.meta.get('shop') if req and req.meta else None
        msg = failure.getErrorMessage() if hasattr(failure, 'getErrorMessage') else str(failure)
        if shop:
            self.shop_stats.setdefault(shop, {'items': 0, 'saved': 0, 'failed': 0})
            self.shop_stats[shop]['failed'] += 1
            self.logger.error("Following site data didn't retrieved due to error: %s | site: %s | url: %s", msg, shop, req.url if req else '')
            # record detailed issue
            self.record_shop_issue(shop, f"Request error: {msg}", url=(req.url if req else None))
        else:
            self.logger.error("Request failed (no shop meta): %s", msg)
            # record generic issue
            self.record_shop_issue("unknown", f"Request error: {msg}", url=(req.url if req else None))

    def write_shop_item(self, item_dict: dict, shop: str):
        try:
            filename = _safe_shop_filename(shop)
            # Ensure currency header is written once at the top of the file
            if shop not in self._header_written:
                code, source = self.shop_currency.get(shop, (None, None))
                header = {
                    "type": "currency_info",
                    "shop": shop,
                    "currency": code,
                    "currency_source": source,
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

    # ---- Filtering utilities ----
    def _product_matches_filters(self, prod: dict, meta_filters: dict) -> bool:
        filter_pt = meta_filters.get('filter_product_type')
        if filter_pt:
            prod_pt = (prod.get('product_type') or '').strip().lower()
            if prod_pt != filter_pt:
                return False
        filter_tag = meta_filters.get('filter_tag')
        if filter_tag:
            tags_field = prod.get('tags')
            if isinstance(tags_field, str):
                tags_text = tags_field.lower()
                if filter_tag not in [t.strip() for t in tags_text.split(',')]:
                    return False
            elif isinstance(tags_field, list):
                lower_tags = [str(t).strip().lower() for t in tags_field if t is not None]
                if filter_tag not in lower_tags:
                    return False
            else:
                return False
        return True

    # ---- Parsing logic ----
    def parse_products_json(self, response):
        shop = response.meta.get('shop')
        self.shop_stats.setdefault(shop, {'items': 0, 'saved': 0, 'failed': 0})
        meta_filters = {
            'filter_tag': response.meta.get('filter_tag'),
            'filter_product_type': response.meta.get('filter_product_type'),
            'collection': response.meta.get('collection'),
        }

        # handle statuses and record issues
        if response.status in (401, 403):
            self.shop_stats[shop]['failed'] += 1
            msg = f"HTTP {response.status} (unauthorized/forbidden) when requesting {response.url}"
            self.logger.error("Following site data didn't retrieved due to HTTP %s: %s", response.status, shop)
            self.record_shop_issue(shop, msg, url=response.url)
            # skip protected file writing in Zyte mode
            yield scrapy.Request(f"https://{shop}/", callback=self.parse_collection_page, meta={'shop': shop}, dont_filter=True)
            return

        if response.status == 404:
            self.shop_stats[shop]['failed'] += 1
            msg = f"HTTP 404 Not Found for {response.url}"
            self.logger.info("No products.json at %s; falling back to HTML", shop)
            self.record_shop_issue(shop, msg, url=response.url)
            yield scrapy.Request(f"https://{shop}/", callback=self.parse_collection_page, meta={'shop': shop}, dont_filter=True)
            return

        content_type = response.headers.get('Content-Type', b'').decode('utf-8', errors='ignore')
        if 'application/json' in content_type or response.url.endswith('.json'):
            try:
                data = json.loads(response.text)
            except Exception as e:
                self.shop_stats[shop]['failed'] += 1
                msg = f"JSON parse failed for {response.url}: {e}"
                self.logger.error(msg)
                self.record_shop_issue(shop, msg, url=response.url)
                yield scrapy.Request(f"https://{shop}/", callback=self.parse_collection_page, meta={'shop': shop}, dont_filter=True)
                return

            # Try to detect currency from JSON payload using broad keys and symbols
            code, source = self._currency_from_json_shallow(data)
            if code:
                self.shop_currency[shop] = (code, source)
                # Write header early if not already
                if shop not in self._header_written:
                    self.write_shop_item({"note": "header_placeholder"}, shop)  # will write header, then this line
                    # remove placeholder line from stats counters adjustment if needed
            
            products = data.get('products') or []
            if not products:
                self.logger.info("No products returned from JSON for site: %s", shop)

            for prod in products:
                if not self._product_matches_filters(prod, meta_filters):
                    continue

                item = ProductItem()
                item['source'] = shop
                item['product_id'] = prod.get('id')
                item['url'] = prod.get('handle') and f"https://{shop}/products/{prod.get('handle')}"
                item['title'] = prod.get('title')
                item['description'] = prod.get('body_html')
                item['variants'] = prod.get('variants', [])
                item['price'] = prod['variants'][0].get('price') if prod.get('variants') else None

                images = prod.get('images', []) or []
                built_images = []
                for img in images:
                    src = None
                    if isinstance(img, dict):
                        src = img.get('src')
                    elif isinstance(img, str):
                        src = img
                    if src:
                        built_images.append(urljoin(f"https://{shop}", src))
                item['images'] = built_images
                item['image_urls'] = item['images']

                tags_field = prod.get('tags')
                if isinstance(tags_field, str):
                    tags = [t.strip() for t in tags_field.split(',') if t.strip()]
                elif isinstance(tags_field, list):
                    tags = [str(t).strip() for t in tags_field if t is not None and str(t).strip()]
                else:
                    tags = []
                item['tags'] = tags

                item['raw'] = prod

                # Attach currency to output
                cached = self.shop_currency.get(shop)
                currency_code = cached[0] if cached else None
                currency_source = cached[1] if cached else None
                # Some shops include currency in price strings; inspect variants
                if not currency_code:
                    currency_code = self._currency_from_price_text(item.get('price'))
                    if currency_code:
                        currency_source = 'price_text'

                try:
                    item_dict = dict(item)
                except Exception:
                    item_dict = {
                        'source': shop,
                        'product_id': prod.get('id'),
                        'url': item.get('url'),
                        'title': item.get('title'),
                    }
                item_dict['currency'] = currency_code
                item_dict['currency_source'] = currency_source
                if item_dict.get('price') and currency_code:
                    item_dict['price_with_currency'] = f"{currency_code}: {item_dict['price']}"
                else:
                    item_dict['price_with_currency'] = item_dict.get('price')

                # If currency still unknown, schedule a lightweight HTML probe to /collections/all
                if not currency_code and shop not in self.shop_currency:
                    yield scrapy.Request(
                        f"https://{shop}/collections/all",
                        callback=self.parse_currency_page,
                        meta={'shop': shop},
                        dont_filter=True,
                        priority=5,
                    )
                self.write_shop_item(item_dict, shop)
                yield item

            # pagination (since_id)
            if products:
                last_id = products[-1].get('id')
                if last_id:
                    next_url = f"https://{shop}/products.json?limit=250&since_id={last_id}"
                    yield scrapy.Request(
                        next_url,
                        callback=self.parse_products_json,
                        meta={
                            'shop': shop,
                            'filter_tag': meta_filters['filter_tag'],
                            'filter_product_type': meta_filters['filter_product_type'],
                            'collection': meta_filters['collection'],
                            'handle_httpstatus_list': [401, 403, 404],
                        },
                        dont_filter=True,
                    )
        else:
            # HTML fallback
            for href in response.css('a::attr(href)').getall():
                if '/products/' in href:
                    yield response.follow(href, callback=self.parse_product_page, meta={'shop': shop, 'filter_tag': meta_filters['filter_tag'], 'filter_product_type': meta_filters['filter_product_type']})
            for href in response.css('a::attr(href)').getall():
                if '/collections/' in href:
                    if self.collection and f"/collections/{self.collection}" in href:
                        yield response.follow(href, callback=self.parse_collection_page, meta={'shop': shop, 'collection': self.collection})
                    else:
                        yield response.follow(href, callback=self.parse_collection_page, meta={'shop': shop})

    # ---- Currency detection helpers and page ----
    def _currency_from_json_shallow(self, data: dict):
        # Direct keys commonly seen
        for key in ("currency", "currency_code", "shop_currency", "currencyIsoCode", "money_format", "money_with_currency_format"):
            val = data.get(key)
            if isinstance(val, str):
                # Try ISO code inside the value
                m = re.search(r"([A-Z]{3})", val)
                if m:
                    return m.group(1), "json_key"
        # Nested shallow scan
        def scan(obj, depth=0):
            if depth > 3:
                return None
            if isinstance(obj, dict):
                for k, v in obj.items():
                    kl = str(k).lower()
                    if isinstance(v, str):
                        if re.search(r"^[A-Z]{3}$", v):
                            return v
                        if "currency" in kl or "currency_code" in kl or "money" in kl:
                            m = re.search(r"([A-Z]{3})", v)
                            if m:
                                return m.group(1)
                    if isinstance(v, (dict, list)):
                        found = scan(v, depth+1)
                        if found:
                            return found
            elif isinstance(obj, list):
                for elem in obj[:10]:
                    found = scan(elem, depth+1)
                    if found:
                        return found
            return None
        found = scan(data)
        if found:
            return found, "json_nested"
        # Symbol hints inside JSON text
        text = json.dumps(data)[:4000]
        for sym, code in self._SYMBOL_TO_CODE().items():
            if sym in text:
                return code, "json_symbol"
        return None, None

    def _currency_from_html(self, response):
        text = response.text or ""
        # meta tags and structured hints
        for sel in (
            "meta[property='product:price:currency']::attr(content)",
            "meta[itemprop='priceCurrency']::attr(content)",
            "meta[name='og:price:currency']::attr(content)",
            "meta[name='currency']::attr(content)",
        ):
            val = response.css(sel).get()
            if val and re.match(r"^[A-Z]{3}$", val.strip().upper()):
                return val.strip().upper(), "html_meta"
        # JavaScript variables
        js_patterns = [
            r"Shopify\\.currency\\.active\\s*=\\s*['\"]?([A-Z]{3})['\"]?",
            r"Shopify\\.currency\\s*=\\s*['\"]?([A-Z]{3})['\"]?",
            r"currency\\s*[:=]\\s*['\"]([A-Z]{3})['\"]",
            r"Currency\\.current\\s*=\\s*['\"]?([A-Z]{3})['\"]?",
            r"\"currency\"\s*:\s*\"([A-Z]{3})\"",
        ]
        for pat in js_patterns:
            m = re.search(pat, text)
            if m:
                return m.group(1).upper(), "html_script"
        # Visible symbols near price
        combined = " ".join(response.css('.money::text, .price::text, .product-price::text').getall())[:2000]
        for sym, code in self._SYMBOL_TO_CODE().items():
            if sym in combined:
                return code, "html_money_symbol"
        # TLD heuristic
        host = response.url.split('/')[2] if response.url.startswith('http') else ''
        for tld, code in self._TLD_TO_CURRENCY().items():
            if host.endswith('.' + tld) or host.endswith(tld):
                return code, "tld_heuristic"
        return None, None

    def _currency_from_price_text(self, price_text):
        if not price_text:
            return None
        text = str(price_text)
        m = re.search(r"([A-Z]{3})", text)
        if m:
            return m.group(1)
        for sym, code in self._SYMBOL_TO_CODE().items():
            if sym and sym in text:
                return code
        return None

    def _SYMBOL_TO_CODE(self):
        return {
            "$": "USD",
            "€": "EUR",
            "£": "GBP",
            "₹": "INR",
            "¥": "JPY",
            "₽": "RUB",
            "₩": "KRW",
            "₺": "TRY",
            "R$": "BRL",
            "S$": "SGD",
            "₱": "PHP",
            "PKR": "PKR",
            "USD": "USD",
            "EUR": "EUR",
            "GBP": "GBP",
            "AUD": "AUD",
            "CAD": "CAD",
            "INR": "INR",
        }

    def _TLD_TO_CURRENCY(self):
        return {
            "pk": "PKR",
            "us": "USD",
            "uk": "GBP",
            "co.uk": "GBP",
            "in": "INR",
            "jp": "JPY",
            "au": "AUD",
            "ca": "CAD",
            "de": "EUR",
            "fr": "EUR",
            "it": "EUR",
            "es": "EUR",
            "nl": "EUR",
        }

    def parse_currency_page(self, response):
        shop = response.meta.get('shop')
        code, src = self._currency_from_html(response)
        if code:
            self.shop_currency[shop] = (code, src)
        # Ensure header exists before items
        filename = _safe_shop_filename(shop)
        if shop not in self._header_written:
            header = {
                "type": "currency_info",
                "shop": shop,
                "currency": code,
                "currency_source": src,
                "detected_at": _now_iso(),
            }
            with open(filename, "a", encoding="utf-8") as f:
                f.write(json.dumps(header, ensure_ascii=False))
                f.write("\n")
            self._header_written.add(shop)

    def parse_collection_page(self, response):
        shop = response.meta.get('shop')
        self.shop_stats.setdefault(shop, {'items': 0, 'saved': 0, 'failed': 0})

        body_text = response.text.lower()[:2000] if response.text else ""
        if "password" in body_text and ("this store is password protected" in body_text or "password" in response.xpath('//title/text()').get(default="").lower()):
            self.shop_stats[shop]['failed'] += 1
            msg = "Store appears password-protected (HTML)"
            self.logger.error("Store %s appears password-protected (HTML). Recording.", shop)
            self.record_shop_issue(shop, msg, url=response.url)
            # skip protected file writing in Zyte mode
            return

        found_any = False
        for href in response.css('a::attr(href)').getall():
            if '/products/' in href:
                found_any = True
                yield response.follow(href, callback=self.parse_product_page, meta=response.meta)

        if not found_any:
            self.shop_stats[shop]['failed'] += 1
            msg = "No product links found on HTML home/collection"
            self.logger.warning("No product links found on HTML home/collection for site: %s", shop)
            self.record_shop_issue(shop, msg, url=response.url)

    def parse_product_page(self, response):
        # print(response)
        shop = response.meta.get('shop')
        meta_filters = {'filter_tag': response.meta.get('filter_tag'), 'filter_product_type': response.meta.get('filter_product_type')}
        item = ProductItem()
        item['source'] = shop
        item['url'] = response.url
        item['title'] = response.css('h1::text').get() or response.xpath('//title/text()').get()
        item['description'] = ''.join(response.css('.product-description, .description, #product-description ::text').getall()).strip()
        price = response.css('.price::text, .product-price::text, .current-price::text').get()
        item['price'] = price.strip() if price else None
        imgs = response.css('img::attr(src)').getall()
        item['images'] = [response.urljoin(u) for u in imgs]
        item['image_urls'] = item['images']
        item['raw'] = response.text[:1000]

        try:
            item_dict = dict(item)
        except Exception:
            item_dict = {'source': shop, 'url': item.get('url'), 'title': item.get('title')}
        self.write_shop_item(item_dict, shop)
        yield item

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
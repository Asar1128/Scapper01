import scrapy
import re
import json
from datetime import datetime

def _now_iso():
    return datetime.utcnow().isoformat() + "+00:00"

class ShopifyCurrencySpider(scrapy.Spider):
    name = "shopify_currency"

    def __init__(self, shops=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if shops:
            self.shops = [s.strip() for s in shops.split(",")]
        else:
            self.shops = []

    def start_requests(self):
        for shop in self.shops:
            shop = shop.replace('https://', '').replace('http://', '').strip().rstrip('/')
            yield scrapy.Request(
                f"https://{shop}/collections/all",
                callback=self.parse_currency,
                meta={'shop': shop},
                dont_filter=True,
                priority=100,
            )

    def parse_currency(self, response):
        shop = response.meta['shop']
        code = self._extract_currency_from_page_source_json(response)
        if code:
            yield {
                "type": "currency_info",
                "shop": shop,
                "currency": code,
                "detected_at": _now_iso(),
            }

    def _extract_currency_from_page_source_json(self, response):
        # Look for Shopify.currency in page scripts
        for script_text in response.xpath("//script[contains(text(), 'Shopify.currency')]/text()").getall():
            match = re.search(r"Shopify\.currency\s*=\s*(\{.*?\});", script_text)
            if match:
                try:
                    data = json.loads(match.group(1))
                    return data.get("active")
                except Exception:
                    return None
        return None

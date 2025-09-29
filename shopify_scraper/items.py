import scrapy

class ProductItem(scrapy.Item):
    # basic metadata
    source = scrapy.Field()        # shop domain
    product_id = scrapy.Field()
    url = scrapy.Field()
    title = scrapy.Field()
    description = scrapy.Field()

    # prices / variants
    variants = scrapy.Field()
    price = scrapy.Field()
    price_with_currency = scrapy.Field()  # Formatted as "USD: 100.00"

    # images
    images = scrapy.Field()
    image_urls = scrapy.Field()

    # classification
    tags = scrapy.Field()

    # raw payload / debugging
    raw = scrapy.Field()

    # currency detection
    currency = scrapy.Field()         # currency code, e.g. "USD", "PKR"
    currency_source = scrapy.Field()  # where the currency came from: 'json', 'html_meta', 'tld_heuristic', etc.

    # any extra fallback fields (if spider writes them)
    extra = scrapy.Field()
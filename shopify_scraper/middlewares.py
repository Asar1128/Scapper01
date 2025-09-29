import random
from scrapy import signals

class RotateUserAgentMiddleware:
    # Very small UA rotator. For large-scale use, use scrapy-useragents or a maintained list.
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
        " Chrome/117.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko)"
        " Version/14.0 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko)"
        " Chrome/117.0.0.0 Safari/537.36",
    ]

    def process_request(self, request, spider):
        request.headers.setdefault('User-Agent', random.choice(self.user_agents))

# Optional proxy middleware skeleton: configure proxies/proxy pool service and uncomment in settings.
class ProxyMiddleware:
    def process_request(self, request, spider):
        # If you have a pool, set request.meta['proxy'] = "http://user:pass@proxy:port"
        return
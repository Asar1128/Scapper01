import json
from scrapy.exceptions import DropItem
from itemadapter import ItemAdapter

class JsonWriterPipeline:
    def open_spider(self, spider):
        self.file = open(spider.settings.get('OUTPUT_JSON', 'products.json'), 'w', encoding='utf-8')
        self.first = True
        self.file.write('[')

    def close_spider(self, spider):
        self.file.write(']')
        self.file.close()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if not adapter.get('url'):
            raise DropItem("Missing url in %s" % item)
        line = json.dumps(dict(adapter.asdict()), ensure_ascii=False)
        if not self.first:
            self.file.write(',\n')
        self.first = False
        self.file.write(line)
        return item

# Optional: image pipeline is built-in (ImagesPipeline). You only need to enable it in settings.
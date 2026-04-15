import scrapy


class WildfableSpider(scrapy.Spider):
    name = 'wildfable'
    allowed_domains = ['target.com']
    start_urls = ['http://target.com/']

    def parse(self, response):
        pass

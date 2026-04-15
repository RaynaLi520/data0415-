import scrapy


class AnewdaySpider(scrapy.Spider):
    name = 'anewday'
    allowed_domains = ['target.com']
    start_urls = ['http://target.com/']

    def parse(self, response):
        pass

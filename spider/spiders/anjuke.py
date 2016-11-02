# -*- coding: utf-8 -*-
import logging
import scrapy
import geohash
import sqlite3
import datetime
import json
from urllib.parse import urlparse
from itertools import *
from geopy import Point
from geopy.distance import distance, VincentyDistance
from spider.items import Home

class AnjukeSpider(scrapy.Spider):
    name = "anjuke"
    allowed_domains = ["anjuke.com"]

    # 二级地址
    districts = {}

    # 遍历所有的地址
    URL_TEMPLATE = '''http://shanghai.anjuke.com/sale/{}/p{}/#filtersort'''

    def start_requests(self):
        yield scrapy.Request(
            'http://shanghai.anjuke.com/sale/', callback=self.parse_root_page)

    def parse_root_page(self, response):
        """首页
        """

        for district_url in response.selector.xpath('//div[@class="items"][1]/span[@class="elems-l"]/a/@href').extract():
            url = urlparse(district_url)
            district = list(filter(None, url.path.split('/')))[-1]
            self.districts[district] = []
            yield scrapy.Request(url.geturl(),
                                 meta={'district': district},
                                 callback=self.parse_district_page)

    def parse_district_page(self, response):
        """区页面
        """

        district = response.meta['district']
        for block_url in response.selector.xpath('//div[@class="items"][1]/span[@class="elems-l"]/div[@class="sub-items"]/a/@href').extract():
            url = urlparse(block_url)
            block = list(filter(None, url.path.split('/')))[-1]
            self.districts[district].append(block)

            yield scrapy.Request(self.URL_TEMPLATE.format(block, 1),
                                 meta={'district': district, 'block': block, 'page': 1},
                                 callback=self.parse_next_page)

    def parse_next_page(self, response):
        """房屋信息
        """
        meta = response.meta
        for sel in response.selector.xpath('//ul[@class="houselist-mod"]/li'):
            item = Home(district=meta['district'], block=meta['block'])

            detail_sel = sel.xpath('div[@class="house-details"]')

            # house-title
            title_sel = detail_sel.xpath('div[@class="house-title"]/a')
            item['title'] = title_sel.xpath('@title').extract_first().encode('utf-8')
            item['company'] = title_sel.xpath('@data-company').extract_first().encode('utf-8')
            item['url'] = title_sel.xpath('@href').extract_first().encode('utf-8')

            # details-item
            details_item_sel = detail_sel.xpath('div[@class="details-item"][1]')
            item['area'] = details_item_sel.xpath('span[1]/text()').extract_first().encode('utf-8')
            item['type'] = details_item_sel.xpath('span[2]/text()').extract_first().encode('utf-8')
            item['price_per_sqm'] = details_item_sel.xpath('span[3]/text()').extract_first().encode('utf-8')
            item['floor'] = details_item_sel.xpath('span[4]/text()').extract_first().encode('utf-8')
            item['built_date'] = details_item_sel.xpath('span[5]/text()').extract_first().encode('utf-8')

            comm_addr = list(filter(None, detail_sel.xpath('div[@class="details-item"][2]/span/@title').extract_first().split('\xa0', 2)))
            item['community'] = comm_addr[0].encode('utf-8')
            item['address'] = comm_addr[1].encode('utf-8')

            item['price'] = sel.xpath('div[@class="pro-price"]/span[@class="price-det"]/strong/text()').extract_first().encode('utf-8')

            yield item

        # 检查是否还有下一页
        if response.selector.xpath('//div[@class="multi-page"]/a[@class="aNxt"]') is not None:
            meta['page'] = meta['page'] + 1
            yield scrapy.Request(self.URL_TEMPLATE.format(meta['block'], meta['page']),
                                 meta=meta,
                                 callback=self.parse_next_page)

# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class Home(scrapy.Item):
    """安居客的房屋信息
    """
    district = scrapy.Field()
    block = scrapy.Field()
    title = scrapy.Field()
    company = scrapy.Field()
    url = scrapy.Field()

    area = scrapy.Field()   # 面积
    type = scrapy.Field()   # 房型
    price_per_sqm = scrapy.Field()  # 平米价格
    floor = scrapy.Field()  # 楼层
    built_date = scrapy.Field()     # 建造日期
    community = scrapy.Field()  # 小区
    address = scrapy.Field()    # 地址

    price = scrapy.Field()  # 价格

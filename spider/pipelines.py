# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


class HomeRemoveInvalidCharPipeline(object):
    def process_item(self, item, spider):
        if item['title']:
            item['title'] = item['title'].replace('\xb2', '')
        if item['area']:
            item['area'] = item['area'].replace('\xb2', '')
        if item['price_per_sqm']:
            item['price_per_sqm'] = item['price_per_sqm'].replace('\xb2', '')

        return item

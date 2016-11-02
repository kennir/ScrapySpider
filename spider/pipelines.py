# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import sqlite3
import datetime
import logging
from os import path
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher


class HomeSqlStorePipeline(object):
    num_inserted = 0
    cached = 0

    def __init__(self):
        self.conn = None
        dispatcher.connect(self.initialize, signals.engine_started)
        dispatcher.connect(self.finalize, signals.engine_stopped)

    def process_item(self, item, spider):
        try:
            self.conn.execute('''
                              INSERT INTO homes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                              ''',
                              (item['district'],
                               item['block'],
                               item['title'],
                               item['company'],
                               item['url'],
                               item['area'],
                               item['type'],
                               item['price_per_sqm'],
                               item['floor'],
                               item['built_date'],
                               item['community'],
                               item['address'],
                               float(item['price'])
                               ))
            self.num_inserted = self.num_inserted + 1
            self.cached = self.cached + 1
            if self.cached > 100:
                self.conn.commit()
                self.cached = 0

        except:
            spider.getLogger().error("Can't write to database")

        return item

    def initialize(self):
        logging.getLogger().info('-----------------DATABASE READY!--------------------')
        self.conn = sqlite3.connect('anjuke({}).db'.format(
            datetime.datetime.now().strftime("%Y-%m-%d")))
        self.conn.executescript('''
                             DROP TABLE IF EXISTS homes;
                             CREATE TABLE homes (
                                district VARCHAR(32) NOT NULL,
                                block VARCHAR(32) NOT NULL,
                                title TEXT,
                                company TEXT,
                                url TEXT,
                                area TEXT,
                                type TEXT,
                                price_per_sqm TEXT,
                                floor TEXT,
                                built_date TEXT,
                                community TEXT,
                                address TEXT,
                                price REAL
                             );
        ''')
        self.conn.commit()

    def finalize(self):
        if self.conn is not None:
            self.conn.commit()
            self.conn.close()
            self.conn = None
        logging.getLogger().info('%d 条记录被抓取', self.num_inserted)
        logging.getLogger().info('-----------------DATABASE CLOSED!--------------------')

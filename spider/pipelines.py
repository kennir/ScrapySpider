# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import sqlite3
from os import path
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher

class HomeSqlite3Pipeline(object):

    def __init__(self):
        self.conn = None
        dispatcher.connect(self.initialize, signals.engine_started)
        dispatcher.connect(self.finalize, signals.engine_stopped)

    def process_item(self, item, spider):

        return item

    def initialize(self):
        print('------------ INITIALIZE --------')
        if path.exists(self.filename):
            self.conn = sqlite3.connect(self.filename)
        else:
            self.conn = self.create_table(self.filename)

    def finalize(self):
        print('------------ finalize --------')
        if self.conn is not None:
            self.conn.commit()
            self.conn.close()
            self.conn = None

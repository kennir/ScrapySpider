# -*- coding: utf-8 -*-
import scrapy
import json
import sqlite3
import datetime
import logging


class SingleShopSpider(scrapy.Spider):
    name = "SingleShop"
    allowed_domains = ["ele.me"]

    def __init__(self, restaurant_id=None):
        if restaurant_id is None:
            raise ValueError
        self.restaurant_id = restaurant_id
        self.crawl_time = datetime.datetime.now()

    RESTAURANT_URL = '''https://mainsite-restapi.ele.me/shopping/restaurant/{}'''
    MENU_URL = '''https://mainsite-restapi.ele.me/shopping/v1/menu?restaurant_id={}'''

    def start_requests(self):
        yield scrapy.Request(
            url=self.RESTAURANT_URL.format(self.restaurant_id),
            callback=self.parse_restaurant)

    def parse_restaurant(self, response):
        self.json_restaurant = json.loads(response.text)
        yield scrapy.Request(
            url=self.MENU_URL.format(self.restaurant_id),
            callback=self.parse_menu)

    menus = {}

    def parse_menu(self, response):
        json_menus = json.loads(response.text)

        self.prepare_database()
        record_id = self.write_shop()

        for json_menu_catalog in json_menus:
            for json_food in json_menu_catalog['foods']:
                name = json_food['name']
                if name not in self.menus:
                    self.menus[name] = (record_id,
                                        self.crawl_time,
                                        name,
                                        json_food['rating'],
                                        json_food['rating_count'],
                                        SingleShopSpider.get_average_price(
                                            json_food['specfoods']),
                                        json_food['month_sales'], )
        self.write_menus()

    def prepare_database(self):
        self.db_name = '{}({}).db'.format(self.restaurant_id,
                                          self.json_restaurant['name'])
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.executescript('''
                        CREATE TABLE IF NOT EXISTS shop
                            (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            crawl_time DATETIME NOT NULL,
                            name VARCHAR(128) NOT NULL,
                            rating_count INTEGER NOT NULL,
                            recent_order_num INTEGER NOT NULL
                            );
                        CREATE TABLE IF NOT EXISTS menu
                            (
                            shop_record_id INTEGER NOT NULL,
                            crawl_time DATETIME NOT NULL,
                            name VARCHAR(128) NOT NULL,
                            rating TINYINT NOT NULL,
                            rating_count INTEGER NOT NULL,
                            price REAL NOT NULL,
                            month_sales INTEGER NOT NULL
                            );
                    ''')
            conn.commit()

    def write_shop(self):
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute('''INSERT INTO shop(
                        crawl_time,
                        name,
                        rating_count,
                        recent_order_num
                        ) VALUES(?,?,?,?)''',
                           (self.crawl_time, self.json_restaurant['name'],
                            self.json_restaurant['rating_count'],
                            self.json_restaurant['recent_order_num']))
            record_id = cursor.execute('SELECT MAX(id) from shop').fetchone()[
                0]
            conn.commit()
            return record_id

    def write_menus(self):
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.executemany('''INSERT INTO menu VALUES (?,?,?,?,?,?,?)''',
                               self.menus.values())
            conn.commit()

    @staticmethod
    def get_average_price(food_specs):
        prices = sum([float(json_food['price']) for json_food in food_specs])
        num_specs = len(food_specs)
        return prices if num_specs == 0 else prices / num_specs

# -*- coding: utf-8 -*-
import logging
import scrapy
import geohash
import sqlite3
import datetime
import json
from itertools import *

CATEGORIES = {
    '207': {
        'name': '快餐便当',
        'items': [
            {
                'id': 209,
                'name': '盖浇饭'
            },
            {
                'id': 213,
                'name': '米粉面馆'
            },
            {
                'id': 217,
                'name': '饺子馄饨'
            },
            {
                'id': 219,
                'name': '香锅砂锅'
            },
            {
                'id': 215,
                'name': '包子粥店'
            },
            {
                'id': 214,
                'name': '麻辣烫'
            },
            {
                'id': 212,
                'name': '汉堡'
            },
            {
                'id': 216,
                'name': '生煎锅贴'
            },
        ]
    },
    '220': {
        'name': '特色菜系',
        'items': [
            {
                'id': 221,
                'name': '川湘菜'
            },
            {
                'id': 263,
                'name': '其他菜系'
            },
            {
                'id': 232,
                'name': '海鲜'
            },
            {
                'id': 231,
                'name': '火锅烤鱼'
            },
            {
                'id': 225,
                'name': '江浙菜'
            },
            {
                'id': 222,
                'name': '粤菜'
            },
            {
                'id': 223,
                'name': '东北菜'
            },
            {
                'id': 226,
                'name': '西北菜'
            },
            {
                'id': 228,
                'name': '新疆菜'
            },
            {
                'id': 227,
                'name': '鲁菜'
            },
            {
                'id': 224,
                'name': '云南菜'
            },
        ]
    },
    '260': {
        'name': '异国料理',
        'items': [
            {
                'id': 229,
                'name': '日韩料理'
            },
            {
                'id': 211,
                'name': '披萨意面'
            },
            {
                'id': 230,
                'name': '西餐'
            },
            {
                'id': 264,
                'name': '东南亚菜'
            },
        ]
    }
}

# LOCATIONS = {
#     '嘉里中心': 'wtw3esj',
#     '淮海中路': 'wtw3ef9',
#     '浦东国金': 'wtw3syu',
#     '上海春城': 'wtw2fy9',
#     '人民广场': 'wtw3sm0',
#     '龙茗路顾戴路': 'wtw34k0'
# }

LOCATIONS = {
    '嘉里中心': 'wtw3es',
    '淮海中路': 'wtw3ef',
    '浦东国金': 'wtw3sy',
    '上海春城': 'wtw2fy',
    'PeoplesSquare': 'wtw3sm',
    '龙茗路顾戴路': 'wtw34k'
}

class MapGridIterator():
    def __init__(self, location_geohash, depth):
        self._cells = set()
        self._next_batch = set([location_geohash])
        self._computed_cells = set()
        self.max_depth = depth
        self.current_depth = 0

    def __iter__(self):
        return self

    def __next__(self):
        cell = self._next_cell()
        if cell is None:
            raise StopIteration
        return cell,

    def _add_neighbors(self, cell):
        if cell in self._computed_cells:
            return
        n = geohash.neighbors(cell)

        def cond(c):
            return (c in self._computed_cells) or (c in self._cells)

        n[:] = list(filterfalse(cond, n))
        self._next_batch.update(n)
        self._computed_cells.add(cell)

    def _advance_depth(self):
        self._cells = self._next_batch
        self._next_batch = set()
        self.current_depth += 1

    def _take_cell(self):
        if len(self._cells) == 0:
            return None
        cell = self._cells.pop()
        self._add_neighbors(cell)
        return cell

    def _next_cell(self):
        if len(self._cells) == 0 and self.current_depth < self.max_depth:
            self._advance_depth()
        return self._take_cell()


class DatabaseUtil():
    """数据库工具
    """

    def __init__(self, location, depth):
        self.location = location
        self.depth = depth
        self.names = self.generate_database_names()

        # connect and initialize status database
        with sqlite3.connect(self.names['status']) as conn:
            cursor = conn.cursor()
            cursor.executescript('''
                DROP TABLE IF EXISTS grid;
                CREATE TABLE grid
                    (
                    geohash CHARACTER(7) PRIMARY KEY NOT NULL,
                    fetch_status TINYINT DEFAULT 0,
                    commit_date DATETIME
                    );
            ''')

            grid_iter = MapGridIterator(LOCATIONS[location], depth)

            cursor.executemany('''INSERT INTO grid(geohash) VALUES (?);''',
                               grid_iter)
            conn.commit()
            logging.getLogger().info('Grid database ready!')

        # connect and initialize data database
        with sqlite3.connect(self.names['data']) as conn:
            cursor = conn.cursor()
            cursor.executescript('''
                DROP TABLE IF EXISTS restaurants;
                CREATE TABLE restaurants
                    (
                    id INTEGER PRIMARY KEY NOT NULL,
                    name VARCHAR(128) NOT NULL,
                    rating TINYINT,
                    rating_count INTEGER,
                    month_sales INTEGER,
                    phone VARCHAR(16),
                    latitude REAL,
                    longitude REAL,
                    address TEXT,
                    is_premium BOOLEAN,
                    is_new BOOLEAN
                    );

                DROP TABLE IF EXISTS menus;
                CREATE TABLE menus
                    (
                    id INTEGER PRIMARY KEY NOT NULL,
                    restaurant_id INTEGER NOT NULL,
                    name VARCHAR(128) NOT NULL,
                    pinyin_name VARCHAR(128),
                    rating TINYINT,
                    rating_count INTEGER,
                    price REAL,
                    month_sales INTEGER,
                    description TEXT,
                    category_id INTEGER
                    );

                CREATE INDEX restaurant_id_idx ON menus(restaurant_id);
            ''')
            conn.commit()
            logging.getLogger().info('Data database ready!')

            cursor = conn.cursor()
            cursor.executescript('''
                    DROP TABLE IF EXISTS category;
                    CREATE TABLE category
                        (
                        id INTEGER PRIMARY KEY NOT NULL,
                        name VARCHAR(16) NOT NULL,
                        major_id INTEGER NOT NULL,
                        major_name VARCHAR(16) NOT NULL
                        );
                ''')

            for key, value in CATEGORIES.items():
                for item in value['items']:
                    cursor.execute('INSERT INTO category VALUES(?,?,?,?)', (
                        item['id'], item['name'], key, value['name']))

            cursor.executescript('''
                    DROP TABLE IF EXISTS restaurant_categories;
                    CREATE TABLE restaurant_categories
                        (
                        category_id INTEGER NOT NULL,
                        restaurant_id INTEGER NOT NULL
                        );
                ''')
            conn.commit()

    def generate_database_names(self, date=None):
        """生成数据库名称
        :param date: override date, use datetime.now() if None
        :return: names of database
        """
        date = date if date is not None else \
            datetime.datetime.now().strftime("%Y-%m-%d")
        return {
            'date': date,
            'status': '{}({})-{}'.format(self.location, date, 'status.db'),
            'data': '{}({})-{}'.format(self.location, date, 'data.db'),
        }

    def crawl_cell(self):
        """抓取下一个单元
        :return: 单元的GEOHASH
        """
        with sqlite3.connect(
                self.names['status'], isolation_level='EXCLUSIVE',
                timeout=120) as conn:
            cursor = conn.cursor()
            cursor.execute('BEGIN EXCLUSIVE')
            cell = cursor.execute(
                'SELECT geohash FROM grid WHERE fetch_status = 0 LIMIT 1'
            ).fetchone()
            if cell is not None:
                cursor.execute(
                    'UPDATE grid SET fetch_status = 1 WHERE geohash = ?', cell)
            conn.commit()
            return cell

    def finish_crawl_cell(self, cell):
        with sqlite3.connect(
                self.names['status'], isolation_level='EXCLUSIVE',
                timeout=120) as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''UPDATE grid SET fetch_status = 2,commit_date = datetime('now','localtime') WHERE geohash = ?''',
                cell)
            conn.commit()

    def write_restaurants(self, restaurants, categories):
        with sqlite3.connect(
                self.names['data'], isolation_level='EXCLUSIVE',
                timeout=120) as conn:
            cursor = conn.cursor()
            cursor.executemany(
                '''INSERT OR IGNORE INTO restaurants VALUES (?,?,?,?,?,?,?,?,?,?,?)
                ''', restaurants)
            cursor.executemany('''
                INSERT INTO restaurant_categories(category_id,restaurant_id)
                SELECT ?,?
                WHERE NOT EXISTS(SELECT 1 FROM restaurant_categories WHERE category_id = ? AND restaurant_id = ?)
                ''', categories)
            conn.commit()

    def write_menus(self, menus):
        with sqlite3.connect(
                self.names['data'], isolation_level='EXCLUSIVE',
                timeout=120) as conn:
            cursor = conn.cursor()
            cursor.executemany('''
                INSERT INTO menus(restaurant_id,name,pinyin_name,rating,rating_count,price,month_sales,description,category_id)
                VALUES(?,?,?,?,?,?,?,?,?)
            ''', menus)
            conn.commit()


class ElemeSpider(scrapy.Spider):
    name = "eleme"
    allowed_domains = ["ele.me"]

    RESTAURANT_CACHE_SIZE = 100

    restaurant_cache = {}
    category_cache = []

    MENU_CACHE_SIZE = 200
    menu_cache = {}

    # 记录抓去过菜单的Restaurant ID
    menu_crawled_restaurant_ids = set()

    def __init__(self, location=None, depth=30, *args, **kwargs):
        if location not in LOCATIONS:
            logging.getLogger().warning(
                'Unknown location, Use "人民广场" as location')
            location = '人民广场'

        self.dbutil = DatabaseUtil(location, depth)
        super(ElemeSpider, self).__init__(*args, **kwargs)

    def start_requests(self):
        cell = self.dbutil.crawl_cell()
        while cell is not None:
            for key, value in CATEGORIES.items():
                for item in value['items']:
                    tuple = (cell[0], key, item['id'])
                    yield scrapy.Request(
                        self.make_url(tuple),
                        meta={
                            'userdata': tuple
                        },
                        callback=self.parse_restaurant)
            self.dbutil.finish_crawl_cell(cell)
            cell = self.dbutil.crawl_cell()

    DETAIL_URL_TEMPLATE = 'https://mainsite-restapi.ele.me/shopping/v1/menu?restaurant_id={}'

    def parse_restaurant(self, response):
        userdata = response.meta['userdata']
        json_restaurants = json.loads(response.text)
        for json_restaurant in json_restaurants:
            restaurant_id = json_restaurant['id']
            self.restaurant_cache[restaurant_id] = (
                restaurant_id,
                json_restaurant['name'],
                json_restaurant['rating'],
                json_restaurant['rating_count'],
                json_restaurant['recent_order_num'],
                json_restaurant['phone'],
                json_restaurant['latitude'],
                json_restaurant['longitude'],
                json_restaurant['address'],
                json_restaurant['is_premium'],
                json_restaurant['is_new'], )
            self.category_cache.append((userdata[2],
                                        restaurant_id,
                                        userdata[1],
                                        restaurant_id, ))

            # Begin crawl menu
            if restaurant_id not in self.menu_crawled_restaurant_ids:
                yield scrapy.Request(
                    self.DETAIL_URL_TEMPLATE.format(restaurant_id),
                    callback=self.parse_menu)
                self.menu_crawled_restaurant_ids.add(restaurant_id)

        if (len(self.restaurant_cache) >= self.RESTAURANT_CACHE_SIZE):
            self.flush_restaurant_cache()

    def parse_menu(self, response):
        json_menus = json.loads(response.text)
        for menu_catalog in json_menus:
            for json_food in menu_catalog['foods']:
                name = json_food['name']
                if name not in self.menu_cache:
                    self.menu_cache[name] = (
                        json_food['restaurant_id'],
                        name,
                        json_food['pinyin_name'],
                        json_food['rating'],
                        json_food['rating_count'],
                        ElemeSpider.get_average_price(json_food['specfoods']),
                        json_food['month_sales'],
                        json_food['description'],
                        json_food['category_id'], )
        if (len(self.menu_cache) >= self.MENU_CACHE_SIZE):
            self.flush_menu_cache()

    def closed(self, reason):
        self.flush_restaurant_cache()
        self.flush_menu_cache()

    def flush_menu_cache(self):
        self.dbutil.write_menus(self.menu_cache.values())
        self.menu_cache = {}

    def flush_restaurant_cache(self):
        # write cache to database
        self.dbutil.write_restaurants(self.restaurant_cache.values(),
                                      self.category_cache)
        self.restaurant_cache = {}
        self.category_cache = []

    @staticmethod
    def get_average_price(food_specs):
        prices = sum([float(json_food['price']) for json_food in food_specs])
        num_specs = len(food_specs)
        return prices if num_specs == 0 else prices / num_specs

    URL_TEMPLATE = '''https://mainsite-restapi.ele.me/shopping/restaurants?geohash={}&latitude={}&limit=1024&longitude={}&restaurant_category_ids[]={}'''

    def make_url(self, tuple):
        lat, long = geohash.decode(tuple[0])
        return self.URL_TEMPLATE.format(tuple[0], lat, long, tuple[1])

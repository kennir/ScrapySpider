#!/bin/bash

# 进入 /Users/hanks/spider 目录
cd /Users/ken/Developer/projects/2016/ScrapySpider
# 运行爬虫脚本
/usr/local/opt/pyenv/shims/scrapy crawl SingleShop -a restaurant_id=43271 &&
/usr/local/opt/pyenv/shims/scrapy crawl SingleShop -a restaurant_id=730710 &&
/usr/local/opt/pyenv/shims/scrapy crawl SingleShop -a restaurant_id=854644 &&
/usr/local/opt/pyenv/shims/scrapy crawl SingleShop -a restaurant_id=1238594

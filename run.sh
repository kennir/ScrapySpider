#!/bin/bash

# 记录一下开始时间
echo `date` >> log &&
# 进入 /Users/hanks/spider 目录
cd ~/Developer/projects/2016/ScrapySpider
# 运行爬虫脚本
scrapy crawl SingleShop -a restaurant_id=43271 &&
scrapy crawl SingleShop -a restaurant_id=730710 &&
scrapy crawl SingleShop -a restaurant_id=854644 &&
scrapy crawl SingleShop -a restaurant_id=1238594 &&
# 运行完成
echo 'finish' >> log

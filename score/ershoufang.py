# -*- coding: utf-8 -*-
"""
"""
import gevent
from gevent import monkey
from gevent.pool import Pool
monkey.patch_all()
p = Pool(8)

from common.DingTool import DingTool
from common.common import *
from score import get_score
from tqdm import tqdm
import re
import requests
import random
from itertools import zip_longest
from bs4 import BeautifulSoup
from collections import defaultdict
import os
from datetime import datetime
import time

basedir = os.path.abspath(os.path.dirname('.'))


def xiaoqu_detail_spider(url, f):
	"""
	爬取小区详细的信息，比如物业信息等
	"""
	try:
		soup = get_soup(url)
	except:
		print(url, 'xiaoqu_detail_spider', '1' * 50)
		return
	xiaoqu_bases = list()
	types = ['title', 'houseInfo', 'positionInfo', 'tagList']
	try:
		xiaoqu_list = soup.find_all('div', {'class': 'xiaoquInfo'})
	except:
		xiaoqu_list = list()
	for i, xiaoqu in enumerate(xiaoqu_list):
		xq = xiaoqu.find_all('div', {'class': 'xiaoquInfoItem'})
		for info1 in xq:
			info2 = info1.find_all('span')
			tmp = list()
			for info1 in info2:
				content = info1.string
				if type(content) == type(None):
					continue
				tmp.append(content)
				if type(content) != type(None):
					lnglat = info1.get('mendian')
			#print(': '.join(tmp))
			xiaoqu_bases.append(': '.join(tmp))
			f.write('\t' + ': '.join(tmp))
		f.write('\t' + '坐标: %s' % lnglat)

	# 小区均价 月份
	try:
		avg_price = soup.find('div', {'class': 'xiaoquPrice clear'}).find_all('span')
	except:
		return
	avg_price_month = list()
	for price_month in avg_price:
		avg_price_month.append(price_month.string)
	nums_cols1 = [u'小区均价', u'月份']
	for m, n in zip_longest(nums_cols1, avg_price_month):
		if type(n) == type(None):
			n = '-'
		#print(': '.join([m, n]))
		f.write('\t' + ': '.join([m, n]))

	return xiaoqu_bases


def str_replace(str):
	return str.replace('\n', ' ').replace('/', '').replace(' ', '').strip()


def get_pages(soup):
	try:
		d = soup.find('div', {'class': 'page-box house-lst-page-box'}).get('page-data')
		d = eval(d)
		total_pages = d['totalPage']
	except:
		total_pages = 0
	return total_pages


def split_str(lists):
	for key in lists:
		if key:
			return key


def ershoufang_spider(base_file, url, date):
	"""
	爬取一个页面链接中的二手房记录
	"""
	soup = get_soup(url)
	with open(base_file, 'a') as f:
		f.write("url: %s" % url)
		#print('~~~~~~~~~' * 10)
		#print(url)
		house_id = str(url).split('/')[-1].split('.html')[0]
		f.write('\t' + "house_id: %s" % house_id)
		info_dict = defaultdict()
		info_dict['url'] = url
		try:
			sell_title = soup.find_all('div', {'class': 'sellDetailHeader'})
		except:
			return
		for title_actinos in sell_title:
			# 卖房的标题
			title = title_actinos.find('h1', {'class': 'main'}).string
			subtitle = title_actinos.find('div', {'class': 'sub'}).string
			#print('主标题: %s' % title)
			#print('副标题: %s' % subtitle)
			f.write('\t' + '主标题: %s' % title)
			f.write('\t' + '副标题: %s' % subtitle)
			info_dict['title'] = ['主标题: %s' % title, '副标题: %s' % subtitle]
			# 关注量和预约量
			try:
				actions = title_actinos.find_all('div', {'class': 'action'})
			except:
				actions = list()
			info_dict['buyer_nums'] = []
			for action in actions:
				button_num = list()
				button = action.button.string
				num = action.span.string
				nums = ': '.join([button, num])
				#print(nums)
				f.write('\t' + nums)
				info_dict['buyer_nums'].append(nums)
		# 小区名称 所在位置 看房时间
		try:
			xiaoqu_names = soup.find('div', {'class': 'communityName'}).find('a', {'class': 'info'}).string
		except:
			xiaoqu_names = '-'
		#print('小区名称: %s' % xiaoqu_names)
		f.write('\t' + '小区名称: %s' % xiaoqu_names)
		info_dict['xiaoqu_names'] = xiaoqu_names

		# 所在位置
		locs = list()
		try:
			xiaoqu_location = soup.find('div', {'class': 'areaName'}).find('span', {'class': 'info'})
		except:
			xiaoqu_location = list()

		segs = xiaoqu_location.text.split('\xa0')
		for loc in segs:
			if loc:
				locs.append(loc)
		locs_cols = [u'区域', u'片区', u'环数']
		for m, n in zip_longest(locs_cols, locs):
			if type(n) == type(None):
				n = '-'
			#print(': '.join([m, n]))
			f.write('\t' + ': '.join([m, n]))

		# 地铁
		try:
			subway = soup.find('div', {'class': 'areaName'}).find('a', {'class': 'supplement'}).string
		except:
			subway = '-'
		if type(subway) == type(None):
			subway = '-'
		#print('地铁: %s' % subway)
		f.write('\t' + '地铁: %s' % subway)
		info_dict['subway'] = subway

		info_dict['base_info'] = []
		# 总价
		try:
			prices = soup.find_all('div', {'class': 'price'})
			for price in prices:
				total = price.span.string  # + u'万'
				#print('总价: %s' % total)
		except:
			total = '-'
		f.write('\t' + '总价: %s' % total)
		info_dict['base_info'].append('总价: %s' % total)
		# 基本属性
		try:
			bases = soup.find_all('div', {'class': 'base'})
		except:
			bases = []
		for base in bases:
			conts = base.find('div', {'class': 'content'}).find('ul')
			for cont in conts:
				# NavigableString按照字面意义上理解为可遍历字符串，注意此时cont的数据类型
				base_title, base_content = '-', '-'
				for i, key in enumerate(cont):
					if i == 0 and key != '\n':
						base_title = key.string
					if i == 1:
						base_content = key
						base_res = ': '.join([base_title, base_content])
						#print(base_res)
						f.write('\t' + base_res)
						info_dict['base_info'].append(base_res)
		info_dict['transaction_info'] = []
		# 交易属性
		try:
			transactions = soup.find_all('div', {'class': 'transaction'})
		except:
			transactions = []
		for transaction in transactions:
			conts = transaction.find('div', {'class': 'content'}).find('ul')
			for i, cont in enumerate(conts.find_all('li')):
				titles_contents = cont.find_all('span')
				transaction_tmp = list()
				for titles_content in titles_contents:
					tcs = titles_content.string
					# 过滤空格或换行符
					tcs = split_str(re.split(r'[\n\s]', tcs))
					transaction_tmp.append(tcs)
				#print(': '.join(transaction_tmp))
				f.write('\t' + ': '.join(transaction_tmp))
				info_dict['transaction_info'].append(': '.join(transaction_tmp))
		# 户型分布
		try:
			houses = soup.find('div', {'id': 'infoList'})
			houses = houses.find_all('div', {'class': 'col'})  # , type(houses), '00'*30
			house_area_tmp = []
			info_dict['house_info'] = []
			house_info = []
			house_info_str = []

			#print('房间类型 面积 朝向 窗户')
			# f.write('\n' + '房间类型 面积 朝向 窗户')
			for i, house in enumerate(houses):
				house_area_direction = house.string
				house_area_tmp.append(house_area_direction)
				if (i - 3) % 4 == 0:
					house_area = ', '.join(house_area_tmp)
					house_info.append(house_area)
					#print(house_area)
					# f.write('\n' + house_area)
					info_dict['house_info'].append(house_area)
					house_area_tmp = []
			f.write('\t' + "house_info: %s" % '||'.join(house_info))
			# print('  '.join(house_info)
		except:
			f.write('\t' + "house_info: -")
			info_dict['house_info'] = []
		# for key in info_dict:
		#     print(key, info_dict[key]
		f.write('\t' + 'date: %s' % date)
		f.write('\n')
		return ''


def do_ershoufang_spider(date):
	"""
	从链家抽取二手房数据
	:param date:
	:return:
	"""
	time_start = time.time()
	base_file = get_file('ershoufang', date)
	time_end = time.time()
	time_diff = time_end - time_start
	print("获取二手房区域链接耗时:%d" % time_diff)

	with open(base_file, 'wt') as f:
		f.write('')
	# 获取对应链接
	urls_dict = get_urls('ershoufang')

	# 使用多协程
	p = Pool(8)
	tasks = list()
	for region in urls_dict:
		for area in urls_dict[region]:
			pages_url, total_pages = urls_dict[region][area]
			tasks.append(p.spawn(single_process_ershoufang, base_file, pages_url, total_pages,date))

	print("协程 begin")
	time_start = time.time()
	gevent.joinall(tasks)
	time_end = time.time()
	time_diff = time_end - time_start
	print("获取二手房详情耗时:%d" % time_diff)
	print("协程 end")


#NUM = 10
#	for region in urls_dict:
#		print(region, "2" * 20)
#		for i, area in enumerate(urls_dict[region]):
#			pages_url, total_pages = urls_dict[region][area]
#			# 暂时执行单任务
#			# total_pages = 1
#			p = Process(target=single_process_ershoufang, args=(base_file, pages_url, total_pages,date))
#			print('%s-%s Child process will start' % (region, area)), '~' * 20
#			print('1' * 20, i)
#			p.start()
#			print('2' * 20, i)
#			if i % NUM == 0:
#				p.join()
#		p.join()

#	# single run fix
#	for region in urls_dict:
#		for i, area in enumerate(urls_dict[region]):
#			pages_url, total_pages = urls_dict[region][area]
#			single_process_ershoufang(base_file, pages_url, total_pages, date)


# 多进程池
#         p.apply_async(single_process_ershoufang, args=(base_file, pages_url, total_pages,))
# p.close()
# p.join()


def single_process_ershoufang(base_file, pages_url, total_pages, date):
	for i in range(total_pages):
		url = pages_url + "pg%d" % (i + 1)
		print(url)
		try:
			soup = get_soup(url)
			# 获取所有二手房的链接
			tmp = soup.find_all('div', {'class': 'title'})
		except:
			continue
		for urls in tqdm(tmp):
			for key in urls:
				# print(region,area,key,'3'*20
				# print(key.get('href'), '~' * 10
				try:
					url = key.get('href')
				except:
					continue
				if not url:
					continue
				# print(url, '~' * 10
				ershoufang_spider(base_file, url, date)

def get_urls(type):
	"""
		1、爬取北京有哪些区域，比如朝阳区；
		2、进一步这些区域细分到各个片区，比如朝阳区下的北苑；
		这么处理的原因是解决，链家在展示二手房时仅仅展示前3000套房的限制，导致数据抓取不全的问题；
		3、取到片区后，再爬取各片区每一页对应的房源数据
	"""
	urls = defaultdict(dict)
	# 1、爬取北京有哪些区域，比如朝阳区；
	url_region = 'https://bj.lianjia.com/{type}/'.format(type=type)
	soup_region = get_soup(url_region)
	regions = soup_region.find('div', {'data-role': 'ershoufang'}).find_all('a')

	# 获取城区和商圈

	# for region in tqdm(regions):
	#    get_region_area_gevent(urls, region)

	# 使用多协程
	tasks = list()
	for region in regions:  # fix!!!!
		tasks.append(p.spawn(get_region_area_gevent, urls, region))
		print(tasks)
	gevent.joinall(tasks)
	print(f"urls: {urls}")
	time.sleep(2)
	return urls


def get_region_area_gevent(urls, region):
	region_name = region.string
	# if region_name != u'朝阳':
	#     continue

	region_url = region.get('href')
	# 2、进一步这些区域细分到各个片区，比如朝阳区下的北苑；
	url_area = 'https://bj.lianjia.com' + region_url
	soup_area = get_soup(url_area)
	try:
		areas = soup_area.find('div', {'data-role': 'ershoufang'}).find_all('a')
	except Exception as e:
		print(e)
		print(region_name)
		return
	for area in areas:  # fix
		if area.get('title') is not None:
			continue
		area_name = area.string
		area_url = area.get('href')
		# if area_name !=u'太阳宫':
		#     continue

		# 3、取到片区后，再爬取各片区每一页对应的房源数据
		area_url = 'https://bj.lianjia.com' + area_url
		soup_page = get_soup(area_url)
		total_pages = get_pages(soup_page)
		if not total_pages:
			continue
		print(region_name, area_name, area_url)
		urls[region_name][area_name] = [area_url, total_pages]

def get_soup(url):
	try:
		req = requests.get(url, headers=hds[random.randint(0, len(hds) - 1)])
		source_code = req.content
		soup = BeautifulSoup(source_code, 'lxml')
	except (requests.HTTPError) as e:
		print(e)
		return ''
	except Exception as e:
		print(e)
		return
	return soup

def get_file(type, date):
	time.sleep(1)
	basedir = os.path.abspath(os.path.dirname(__file__))
	basedir = f'{basedir}/data/LianJiaScore'
	base_path = basedir + '/%s/' % type + date

	os.makedirs(basedir + '/%s/' % type , exist_ok=True)
	base_file = base_path + '.txt'
	return base_file


def read_ershoufang(date):
	"""
	打分
	:param date:
	:return:
	"""
	base_file = get_file('ershoufang', date)
	score_file = get_file('ershoufang_score', date)

	datas = []
	info = []
	j = 0
	flags = set()
	# 列名
	head_line = \
		["url", "house_id", "title", "subtitle", "buyer_attention", "buyer_see", "xiaoqu_name", "region", "area",
		 "ring_road",
		 "subway",
		 "total",
		 "house_type", "floor", "bulid_area", "house_structure", "use_area",
		 "bulid_type", "orientation", "build_structure", "fitment", "ladder_rate", "warm", "lift",
		 "createtime", "property", "last_buy_time", "house_use", "house_limit", "property_right_belong", "pledge_info",
		 "supplement",
		 "house_info", "date", "defang_area", "total_score", "defang_score", "huxing_score", "chuanghu_score",
		 "chaoxiang_score",
		 "defang_rate", "defang_price_score", "defang_price", "fitment_score"]
	with open(score_file, "w") as write_file:
		write_file.write("\t".join(head_line) + "\n")

	with open(base_file, 'rt') as f, open(score_file, "a") as write_file:
		for tmp in tqdm(f):
			break_flag = 0
			line = tmp.replace('\n', '')
			# print(len(line.split('    '))
			eles = line.split('\t')
			if len(eles) == 34:
				info = []
				for i in range(34):
					try:
						segs = eles[i].split(': ')
						if len(segs) == 2:
							ele = segs[1]
						else:
							ele = ''
						info.append(ele)
					except:
						break_flag = 1
						print("break flag")
			else:
				if len(line.split('\t')) > 1:
					print(len(line.split('\t')), '!' * 30, eles)
				continue

			if break_flag:
				continue
			# 列名 这个写法太坑了 需要优化 用json转df比较好property_right_year
			[url, house_id, title, subtitle, buyer_attention, buyer_see, xiaoqu_name, region, area, ring_road,
			 subway, total, house_type, floor, bulid_area, house_structure, use_area, bulid_type, orientation,
			 build_structure,
			 fitment, ladder_rate, warm, lift, createtime, property, last_buy_time, house_use, house_limit,
			 property_right_belong, pledge_info, supplement, house_info, date] = info
			# 评分相关
			total_score, defang_score, huxing_score, chuanghu_score, chaoxiang_score, defang_area, defang_rate, defang_price_score, defang_price, fitment_score = \
				get_score(
					house_info, bulid_area, total, fitment)

			if house_id in flags:
				# print('*'*30
				continue
			flags.add(house_id)

			tmp = [
				url, house_id, title, subtitle, buyer_attention, buyer_see, xiaoqu_name, region, area, ring_road,
				subway,
				total,
				house_type, floor, bulid_area.replace('㎡', ''), house_structure, use_area.replace('㎡', ''),
				bulid_type, orientation, build_structure, fitment, ladder_rate, warm, lift,
				createtime, property, last_buy_time, house_use, house_limit, property_right_belong, pledge_info,
				supplement,
				house_info, date, defang_area, total_score, defang_score, huxing_score, chuanghu_score, chaoxiang_score,
				defang_rate, defang_price_score, defang_price, fitment_score]

			write_file.write("\t".join(map(str, tmp)) + "\n")

if __name__ == "__main__":
	date_str = datetime.now().strftime("%Y%m%d")
	DingTool.ding_text_msg(f"拉取{date_str} 二手房信息")
	do_ershoufang_spider(date_str)
	read_ershoufang(date_str)
	DingTool.ding_text_msg(f"拉取{date_str} 二手房信息 done.")

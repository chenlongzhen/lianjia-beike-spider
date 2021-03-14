import os, sys
import time
import logging
from datetime import datetime

def get_log(level='DEBUG'):
	_ = os.path.abspath(os.path.dirname(__file__))  # 返回当前文件路径
	root_path = os.path.abspath(os.path.join(_, '..'))  # 返回根目录文件夹
	sys.path.append(root_path)
	# 创建一个logger
	logger = logging.getLogger("quant.log")
	logger.setLevel(level)  # Log等级总开关

	# 创建一个handler，用于写入日志文件
	now = datetime.now()
	rq = now.strftime('%Y%m%d')
	log_path = root_path + '/Logs/'
	os.makedirs(log_path, exist_ok=True)
	log_name = log_path + rq + '.log'
	logfile = log_name
	fh = logging.FileHandler(logfile, mode='w')
	fh.setLevel(level)  # 输出到file的log等级的开关

	# 定义handler的输出格式
	formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
	fh.setFormatter(formatter)

	sh = logging.StreamHandler()  # 往屏幕上输出
	logger.addHandler(fh)
	logger.addHandler(sh)
	return logger

logger = get_log()

if __name__ == "__main__":
	logger = get_log()
	logger.debug("aaaaa")
	logger.info("aaaaa")



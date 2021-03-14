from common.my_log import get_log
from datetime import datetime
import os

# path
_ = os.path.abspath(os.path.dirname(__file__))  # 返回当前文件路径
proj_path = os.path.abspath(os.path.join(_, '..'))  # 返回根目录文件夹

# log
logging = get_log()
# dingding tocken
dingding_open = False
token = "d72740c0ea768e4b737da118e6cc8d816e52b03d7626c40aab8193f21730f6f4"
# dingding url
ding_url = 'https://oapi.dingtalk.com/robot/send?access_token=' + token
# date time
now_date = datetime.now()
now_date_str = now_date.strftime("%Y%m%d")

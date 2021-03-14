"""
钉钉通知
"""
import datetime
import json
import requests

from .my_log import get_log
from .setting import ding_url,logging, dingding_open

class DingTool():
	"""
	dingding
	"""
	@staticmethod
	def ding_text_msg(content):
		try:
			content = f"通知: {content}"
			msg = {
				'msgtype': 'text',
				'text': {'content':
					         content + '\n' + datetime.datetime.now().strftime('%m-%d %H:%M:%S')
				         }
			}

			header = {
				"Content-Type": "application/json",
				"Charset": "UTF-8"
			}
			body = json.dumps(msg)
			if dingding_open:
				msg = requests.post(ding_url, data=body, headers=header)
			logging.info(f"msg: {msg}")

		except Exception as err:
			logging.error(f"钉钉发送失败: {err}")

	@staticmethod
	def ding_markdown_msg(content):
		"""
		发送markdown格式的消息
		:param content:
		:return:
		"""
		try:
			logging.info(content)
			HEADERS = {"Content-Type": "application/json ;charset=utf-8"}
			data = {"actionCard": {"title": "robot send message",
			                       "text": content,
			                       "hideAvatar": "0", "btnOrientation": "0"},
			        "msgtype": "actionCard"}
			String_textMsg = json.dumps(data)
			#print(data)
			#print(url)
			res = data
			if dingding_open:
				res = requests.post(ding_url, data=String_textMsg, headers=HEADERS)
			logging.info(res)
		except Exception as e:
			logging.error(f"钉钉发送失败: {e}")


if __name__ == '__main__':
	robotId = 'd72740c0ea768e4b737da118e6cc8d816e52b03d7626c40aab8193f21730f6f4'
	content = "#测试\n\n![screenshot](https://clzstockbucket.s3.ap-east-1.amazonaws.com/report.png)"
	DingTool.ding_text_msg(content)
	DingTool.ding_markdown_msg(content)
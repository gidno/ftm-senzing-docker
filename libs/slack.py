import requests
import json

from libs.conf import SLACK_WEBHOOK


def send_alert(header, msg, msg_type):
    headers = {"Content-Type": "application/json",
               'charset': 'utf-8'}
    em = ':red_circle:' if msg_type == 'E' else ':large_green_circle:'
    data = {
        'text': f'{em} *{header}*\n{msg}'
    }

    requests.post(SLACK_WEBHOOK, data=json.dumps(data), headers=headers)

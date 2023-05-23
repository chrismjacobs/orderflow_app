import requests
import json
import config

RENDER_API = config.RENDER_API
RENDER_SERVICE = config.RENDER_SERVICE

# url = "https://api.render.com/v1/services/" + RENDER_SERVICE

# headers = {"accept": "application/json", "authorization": 'Bearer ' +  RENDER_API}

# response = requests.get(url, headers=headers)

print(RENDER_SERVICE)

url = "https://api.render.com/v1/services/" + RENDER_SERVICE

headers = {"accept": "application/json", "authorization": 'Bearer ' +  RENDER_API}

response = requests.get(url, headers=headers)

suspended = json.loads(response.text)['suspended']



print(suspended)

surl = url + "/" + 'suspend'
print(surl)

response = requests.post(surl, headers=headers)

print(response)

# print(json.loads(response.text))

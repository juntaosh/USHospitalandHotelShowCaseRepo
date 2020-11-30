import json
import requests

key = "AIzaSyA-"
cx = ""


def getimage(query):
    url = f"https://www.googleapis.com/customsearch/v1?q={query}_logo&num=1&cx={cx}&alt=json&key={key}&searchType=image"
    response = requests.get(url)
    text = response.text
    data = json.loads(text)
    image_link = data['items'][0]['link']
    print(image_link)
    return image_link
    response.close()


def getLargerImage(query):
    url = f"https://www.googleapis.com/customsearch/v1?q={query}&num=1&cx={cx}&alt=json&key={key}&searchType=image"
    response = requests.get(url)
    text = response.text
    data = json.loads(text)
    image_link = data['items'][0]['link']
    return image_link
    response.close()


def getCovidData(state):
    url = f"https://api.covidtracking.com/v1/states/{state}/current.json"
    response = requests.get(url)
    text = response.text
    data = json.loads(text)
    response.close()
    return data

def getHistoricData(state):
    url = f"https://api.covidtracking.com/v1/states/{state}/daily.json"
    response = requests.get(url)
    text = response.text
    data = json.loads(text)
    result = []
    counter = 0
    for api_data in data:
        r = []
        r.append(counter)
        counter += 1
        if api_data["hospitalizedCurrently"] is None:
            r.append(0)
        else:
            r.append(api_data["hospitalizedCurrently"])
        if api_data["positiveIncrease"] is None:
            r.append(0)
        else:
            r.append(api_data["positiveIncrease"])
        if api_data["deathIncrease"] is None:
            r.append(0)
        else:
            r.append(api_data["deathIncrease"])
        result.append(r)
    response.close()
    return result
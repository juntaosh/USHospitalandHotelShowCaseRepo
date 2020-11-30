import requests
import json
f = open("General_Hotel_Data.json")
hotels = json.load(f)
response = requests.patch(url='https://projecthoteldatabase.firebaseio.com/projecthoteldatabase.json', json=hotels)
if response.status_code != 200:
    print(response.status_code)
f.close()

f = open("avg_score.json")
scores = json.load(f)
scores = scores['Hotels']
for score in scores:
    url = 'https://projecthoteldatabase.firebaseio.com/projecthoteldatabase/Hotels/' + score + '.json'
    response = requests.patch(url=url, json=scores[score])
f.close()

f = open("rating_map.json")
ratings = json.load(f)
rating = ratings['Hotels']
for r in rating:
    url = 'https://projecthoteldatabase.firebaseio.com/projecthoteldatabase/Hotels/' + r + '.json'
    response = requests.patch(url=url, json=rating[r])
f.close()

f = open("reviews.json")
review = json.load(f)
response = requests.patch(url='https://projecthoteldatabase.firebaseio.com/projecthoteldatabase.json', json=review)
if response.status_code != 200:
    print(response.status_code)
f.close()
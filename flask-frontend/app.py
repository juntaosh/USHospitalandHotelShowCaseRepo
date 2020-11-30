'''
pip install flask
pip install flask-mysql
pip install cryptography
'''

from flask import Flask, render_template, request, redirect
from flaskext.mysql import MySQL
import autocorrect
import pyrebase
from geopy.distance import geodesic
from math import log10, floor
import operator
import difflib
from getimage import getimage, getLargerImage, getCovidData, getHistoricData

app = Flask(__name__)

# initialize MySQL and create connection to database
app.config['MYSQL_DATABASE_USER'] = 'dsci551_project'
app.config['MYSQL_DATABASE_PASSWORD'] = 'dsci551_project'
app.config['MYSQL_DATABASE_DB'] = 'hospitals'
app.config['MYSQL_DATABASE_HOST'] = 'localhost'
mysql = MySQL()
mysql.init_app(app)
connection = mysql.connect()
cursor = connection.cursor()

hos_tables = ['hospital_info', 'complications', 'timely_care', 'infections', 'general_ratings', 'general_comparisons',
              'overall_ratings']
hos_overall_list = 'o.`Facility ID`, h.`Facility Name`, o.`Hospital overall rating`, o.`Average National Comparison`, o.`Average Rating in Complications`, o.`Average Death Rate`,o.`Average Score in Timely and Effective Care`, o.`Average Number of Infections`, o.`Meets criteria for promoting interoperability of EHRs`'
hos_concat_add = 'CONCAT(h.`Facility ID`, h.`Facility Name`, h.Address, h.City, h.State, h.`ZIP Code`, h.`County Name`, h.`Phone Number`)'
hos_order_by = 'o.`Hospital overall rating` desc, o.`Average National Comparison` asc, o.`Average Rating in Complications` desc, o.`Average Death Rate` asc, o.`Average Score in Timely and Effective Care` desc,  o.`Average Number of Infections` asc'
G_API = 'AIzaSyC3MsyehmbIcBoTu67uEmO3QYHaXrK_em0'

past_search = []
firebase_data = []
reviews = []
hotel_info = []
hospital_info = []
coviddata = {}


def execute_query(query):
    # return query result
    cursor.execute(query)
    data = cursor.fetchall()
    headers = [i[0] for i in cursor.description]
    return data, headers


# endpoint for search
@app.route('/', methods=['GET', 'POST'])
def search():
    if request.method == "POST":
        # fetch keyword
        keyword = request.form.getlist('hotel')[0]
        # search = search.split()
        # keyword, db = ' '.join(search[:-1]), search[-1]
        # if db.lower() == 'hospital':
        query = f"SELECT {hos_overall_list} FROM hospital_info h JOIN overall_ratings o WHERE h.`Facility ID` = o.`Facility ID` AND {hos_concat_add} LIKE '%{keyword}%' ORDER BY {hos_order_by}"
        if keyword == '':
            data, headers = execute_query(
                f"SELECT {hos_overall_list}  FROM hospital_info h JOIN overall_ratings o WHERE h.`Facility ID` = o.`Facility ID` ORDER BY {hos_order_by} LIMIT 10")
        else:
            data, headers = execute_query(query)
        new = request.form.getlist('hotel')[0]  # Record as past searches
        if len(past_search) != 0:
            if len(past_search) == 3 and new not in past_search:
                del past_search[-1]
                past_search.insert(0, new)
            elif len(past_search) != 1 and new in past_search:
                index = past_search.index(new)
                del past_search[index]
                past_search.insert(0, new)
            elif new not in past_search:
                past_search.insert(0, new)
        else:
            past_search.insert(0, new)
        return render_template('index.html',
                               headers=headers[1:],
                               objects=data,
                               givenQuery=request.form.getlist('hotel')[0])
    return render_template('index.html')


@app.route('/get_ratings/<string:fk>', methods=['GET', 'POST'])
def get_ratings(fk):
    queries = [f"SELECT * FROM {table} WHERE `Facility ID`={fk}" for table in hos_tables]
    data = [execute_query(query) for query in queries]
    retrieveHotelInfo(data[0][0][0][-8],
                      data[0][0][0][-4],
                      data[0][0][0][-3],
                      data[0][0][0][1])
    image = getLargerImage(data[0][0][0][1])
    coviddata = getCovidData(data[0][0][0][4])
    test_data = getHistoricData(data[0][0][0][4])
    data_size = len(test_data)-1
    return render_template('get_ratings.html', image=image, address=data[0][0], address_h=data[0][1],
                           complications=data[1][0], complications_h=data[1][1],
                           timely_care=data[2][0], timely_care_h=data[2][1],
                           infections=data[3][0], infections_h=data[3][1],
                           general=data[4][0], general_h=data[4][1],
                           general_comp=data[5][0], general_comp_h=data[5][1],
                           overall_ratings=data[6][0], overall_ratings_h=data[6][1],
                           coviddata=coviddata, test_data=test_data, data_size=data_size
                           )


@app.route('/get_hotels', methods=['GET', 'POST'])
def get_hotels():
    sorted_data = sorted(firebase_data, key=operator.itemgetter('Distance'))
    return render_template('get_hotels.html', data=sorted_data,
                           hasNextHidden="" if (len(firebase_data) > 20) else "hidden",
                           hasPrevHidden="hidden")

@app.route('/get_hotels/sortByRating', methods=['GET', 'POST'])
def getHotelRatingSorted():
    sorted_data = sorted(firebase_data, key=operator.itemgetter('Avg_rating'), reverse=True)
    return render_template('get_hotels.html', data=sorted_data,
                           hasNextHidden="" if (len(firebase_data) > 20) else "hidden",
                           hasPrevHidden="hidden")

@app.route('/get_hotels/sortByName', methods=['GET', 'POST'])
def getHotelNameSorted():
    sorted_data = sorted(firebase_data, key=operator.itemgetter('Name'))
    return render_template('get_hotels.html', data=sorted_data,
                           hasNextHidden="" if (len(firebase_data) > 20) else "hidden",
                           hasPrevHidden="hidden")

@app.route('/get_hotels/sortByType', methods=['GET', 'POST'])
def getHotelTypeSorted():
    sorted_data = sorted(firebase_data, key=operator.itemgetter('Type'))
    return render_template('get_hotels.html', data=sorted_data,
                           hasNextHidden="" if (len(firebase_data) > 20) else "hidden",
                           hasPrevHidden="hidden")

@app.route('/get_hotels/sortByDistance', methods=['GET', 'POST'])
def getHotelDistanceSorted():
    sorted_data = sorted(firebase_data, key=operator.itemgetter('Distance'))
    return render_template('get_hotels.html', data=sorted_data,
                           hasNextHidden="" if (len(firebase_data) > 20) else "hidden",
                           hasPrevHidden="hidden")


@app.route('/hotel_detail/<string:hotel_name>', methods=['GET', 'POST'])
def fetch_hotel_details(hotel_name):
    reviews.clear()
    hotel_info.clear()
    hotel_dict = {}
    new_avg = 0.000
    for hotel in firebase_data:
        if hotel['Name'] == hotel_name:
            review_ids = hotel['Ratings']
            hotel_info.append(hotel_name)
            hotel_info.append(hotel['Latitude'])
            hotel_info.append(hotel['Longitude'])
            hotel_dict = hotel
            for review_id in review_ids:
                data = db.child("projecthoteldatabase/Reviews").order_by_child('Id').equal_to(review_id).get()
                reviews.append(normalizeReview(data.val()[str(review_id)]))
            new_avg = removeDuplicates()
    sorted_rating = sorted(reviews, key=operator.itemgetter('Score'), reverse=True)
    imageURL = getLargerImage(hotel_name)
    coviddata = getCovidData(hotel_dict['State'])
    test_data = getHistoricData(hotel_dict['State'])
    data_size = len(test_data)-1
    return render_template('hotel_detail.html',
                           reviews=sorted_rating,
                           hospital_lat=hospital_info[1],
                           hospital_long=hospital_info[2],
                           hotel_lat=hotel_info[1],
                           hotel_long=hotel_info[2],
                           hotel_dict=hotel_dict,
                           given_hospital=hospital_info[0],
                           new_avg=new_avg,
                           imageURL=imageURL,
                           coviddata=coviddata,
                           test_data=test_data,
                           data_size=data_size)


def normalizeReview(f_data):
    normalized_data = {'Id': f_data['Id'], 'Hotel': f_data['ReviewedHotel'], 'Score': f_data['ReviewedScore'],
                       'review': f_data['ReviewedText'], 'Title': f_data['ReviewedTitle']}
    return normalized_data


def removeDuplicates():
    duplicate_reviews = []
    counter_a = 0
    counter_b = 0
    for review_a in reviews:
        for review_b in reviews:
            if review_a == review_b:
                counter_b += 1
                continue
            else:
                ratio = difflib.SequenceMatcher(None, str(review_a), str(review_b)).ratio()
                if ratio > 0.80:
                    index = sorted([counter_a, counter_b])
                    if index not in duplicate_reviews:
                        duplicate_reviews.append(index)
                counter_b += 1
        counter_a += 1
    for review in duplicate_reviews:
        del review[review[0]]
    new_avg = 0.00
    for review in reviews:
        new_avg += float(review['Score'])
    new_avg /= len(reviews)
    return new_avg


def noquote(s):
    return s


def round_to_1(x):
    return round(x, -int(floor(log10(abs(x)))))


def retrieveHotelInfo(state, long, lat, hospital_name):
    print("State: %s, Long: %s, Lat: %s" % (state, long, lat))
    hospital_info.clear()
    hospital_info.append(hospital_name)
    hospital_info.append(lat)
    hospital_info.append(long)
    print(hospital_info)
    data = db.child("projecthoteldatabase/Hotels").order_by_child('State').equal_to(state).get()
    firebase_data.clear()
    for x in data.each():
        # firebase_data.append((x.val(), calculateDistance(x.val(), long, lat)))
        firebase_data.append(normalizeData(x.val(), long, lat))


def normalizeData(f_data, long, lat):
    normalized_data = {}
    normalized_data['Address'] = f_data['Address']
    normalized_data['Avg_rating'] = round_to_1(float(f_data['Avg_rating']))
    normalized_data['City'] = f_data['City']
    normalized_data['Id'] = f_data['Id']
    normalized_data['Latitude'] = f_data['Latitude']
    normalized_data['Longitude'] = f_data['Longitude']
    normalized_data['Name'] = f_data['Name']
    normalized_data['Ratings'] = f_data['Ratings']
    normalized_data['State'] = f_data['State']
    normalized_data['Type'] = f_data['Type']
    normalized_data['Zip'] = f_data['Zip']
    hospital = (lat, long)
    hotel = (f_data['Latitude'], f_data['Longitude'])
    normalized_data['Distance'] = int(geodesic(hospital, hotel).miles)
    return normalized_data


def getAutoCorrection(input):
    if input[-1] == ' ':
        return []  # return empty suggestions when user starts entering a new word
    else:
        inputs = input.split()
        if len(inputs) == 1:  # return correction on this word, removing all white spaces.
            return autocorrect.candidates(inputs[0])[0:5]
        else:  # keep the original characters before the last word, fix that word and append to the prior characters
            original_text = input[0:(len(input) - len(inputs[-1]))]
            candidates = autocorrect.candidates(inputs[-1])[0:5]
            results = []
            for candidate in candidates:
                results.append(original_text + candidate)
            return results


def getAutoComplete(input):
    return []  # TODO from google place API


@app.route('/autocomplete', methods=['GET'])
def autocomplete():
    query = request.args["query"]
    return {"history": past_search,
            "autocorrect": getAutoCorrection(query),
            "autocomplete": getAutoComplete(query)}


if __name__ == '__main__':
    config = {
        "apiKey": "",
        "authDomain": "",
        "databaseURL": "",
        "storageBucket": ""
    }
    firebase = pyrebase.initialize_app(config)
    db = firebase.database()
    pyrebase.pyrebase.quote = noquote
    app.run(debug=True)

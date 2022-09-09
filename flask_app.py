

from ast import Global
from re import T
from flask import Flask, jsonify, request, render_template, redirect
from redis import Redis
import matplotlib.pyplot as plt
import json

app = Flask(__name__)

#Recieves data from sparkapp, converts to json object, then puts into redis
@app.route('/updateData_languageCount', methods=['POST'])
def updateData():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('languageCount', json.dumps(data))
    return jsonify({'msg': 'success'})
@app.route('/updateData_pushCount', methods=['POST'])
def updateData2():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('pushCount', json.dumps(data))
    return jsonify({'msg': 'success'})
@app.route('/updateData_starCount', methods=['POST'])
def updateData3():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('starCount', json.dumps(data))
    return jsonify({'msg': 'success'})
@app.route('/updateData_wordCount', methods=['POST'])
def updateData4():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('wordCount', json.dumps(data))
    return jsonify({'msg': 'success'})


@app.route('/', methods=['GET'])
def index():
    r = Redis(host='redis', port=6379)

    #Tries to retrieve data, then convert json into python dict, else displays waiting for data
    try:
        q1_data = r.get('languageCount')
        q2_data = r.get('pushCount')
        q3_data = r.get('starCount')
        q4_data= r.get('wordCount')
        q1_data = json.loads(q1_data)
        q2_data = json.loads(q2_data)
        q3_data = json.loads(q3_data)
        q4_data = json.loads(q4_data)
    except TypeError:
        return "waiting for data"
    
    #Repository count by language, accesses indexes of counts of each language, if none then set to 0
    try:
        py_index = q1_data['Language'].index('Python')
        py_count = q1_data['Total_count'][py_index]
        r_index = q1_data['Language'].index('Ruby')
        ruby_count = q1_data['Total_count'][r_index]
        c_index = q1_data['Language'].index('C')
        c_count = q1_data['Total_count'][c_index]
    except ValueError:
        py_count = 0
        ruby_count = 0
        c_count = 0  
        
    #Repository push count by language, accesses index of push counts for each language, if none then set to 0
    try:
        py_index = q2_data['Language'].index('Python')
        py_push_count = q2_data['Push_count'][py_index]
        r_index = q2_data['Language'].index('Ruby')
        ruby_push_count = q2_data['Push_count'][r_index]
        c_index = q2_data['Language'].index('C')
        c_push_count = q2_data['Push_count'][c_index]
    except ValueError:
        py_push_count = 0
        ruby_push_count = 0
        c_push_count = 0  
    #py_push_count=py_push_count, ruby_push_count=ruby_push_count,c_push_count=c_push_count

    #Repository star count by language, accesses index of star count for each language, if none then set to 0
    try:
        py_index = q3_data['Language'].index('Python')
        py_stars = int(q3_data['Average_star'][py_index])
        ruby_index = q3_data['Language'].index('Ruby')
        ruby_stars = int(q3_data['Average_star'][ruby_index])
        c_index = q3_data['Language'].index('C')
        c_stars = int(q3_data['Average_star'][c_index])
    except ValueError:
        py_stars = 0
        c_stars=0
        ruby_stars=0
        
    #creates bar chart for q3: Average stars by programming language, outputs as png.
    x = [1, 2,3]
    height = [py_stars,ruby_stars,c_stars]
    tick_label = ['Python', 'Ruby','c']
    plt.bar(x, height, tick_label=tick_label, width=0.8, color=['tab:orange', 'tab:blue','tab:red'])
    plt.ylabel('star average')
    plt.xlabel('Programming Language')
    plt.title('Average star count by language')
    plt.savefig('streaming/static/image/q3_chart.png')
    
    #Repository word count by language, gets all indexes of each language
    try:
        #gets only first 11 indexes of word
        py_index = [i for i, x in enumerate(q4_data['Language']) if x == "Python"][0:11]
        ruby_index = [i for i, x in enumerate(q4_data['Language']) if x == "Ruby"][0:11]
        c_index = [i for i, x in enumerate(q4_data['Language']) if x == "C"][0:11]
        
        py_words,c_words,ruby_words = [],[],[]
        #Creates list of lists containing word,count
        for p in py_index:
            py_words.append([q4_data['word'][p], q4_data['count'][p]])
        for r in ruby_index:
            ruby_words.append([q4_data['word'][r], q4_data['count'][r]])
        for c in c_index:
            c_words.append([q4_data['word'][c], q4_data['count'][c]])
        
          
    except ValueError:
        py_words =[]
        c_words=[]
        ruby_words=[]
    
    
    return render_template('index.html', py_count=py_count, ruby_count=ruby_count, c_count=c_count,url='static/image/q3_chart.png',py_words=py_words,c_words=c_words,ruby_words=ruby_words)

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')










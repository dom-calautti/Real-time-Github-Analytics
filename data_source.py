
import sys
import socket
import random
import time
import json
import requests
import time
import os
import datetime
from datetime import timezone

p_out, r_out, c_out= "","",""
var_delimiter="\t"
repo_delimiter = "\n"
#github PAT authentication
token = os.environ['TOKEN']

#sets up tcp/sending data environment
TCP_IP = "0.0.0.0"
TCP_PORT = 9999
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
# if the connection is accepted, proceed
conn, addr = s.accept()
print("Connected... Starting sending data.")
dt = datetime.datetime.now(timezone.utc)
utc_time = dt.replace(tzinfo=timezone.utc)
utc_timestamp = utc_time.timestamp()
print("Time started:    "+ str(utc_timestamp))

while True:
    try:
        #notes the starttime of the proccess
        start_time = time.time_ns()

        
        #Sends request to Github API to recieve python repositories and then loads as json file.
        python_repos = 'https://api.github.com/search/repositories?q=+language:Python&sort=updated&order=desc&per_page=50'
        p_res = requests.get(python_repos, headers={"Authorization":token})
        p_json = p_res.json()
        
        #Sends request to Github API to recieve Ruby repositories and then loads as json file.
        ruby_repos = 'https://api.github.com/search/repositories?q=+language:Ruby&sort=updated&order=desc&per_page=50'
        r_res = requests.get(ruby_repos, headers={"Authorization":token})
        r_json = r_res.json()
        
        #Sends request to Github API to recieve C repositories and then loads as json file.
        c_repos = 'https://api.github.com/search/repositories?q=+language:C&sort=updated&order=desc&per_page=50'
        c_res = requests.get(c_repos, headers={"Authorization":token})
        c_json = c_res.json()
        
        #Goes through json file to extract relevant information about repository, encodes the data, sends it by tcp to port 9999, empties string.
        if "items" in p_json:
            for p in p_json["items"]:
                p_out += "Python" + var_delimiter + str(p["full_name"])+ var_delimiter+ str(p["stargazers_count"])+ var_delimiter+ str(p["description"]) + var_delimiter+ p["pushed_at"] +repo_delimiter
            print(p_out)
            p_out = str.encode(p_out)
            conn.send(p_out)
            p_out=""
            
        #Goes through json file to extract relevant information about repository, encodes the data, and sends it by tcp to port 9999, empties string.
        if "items" in r_json:
            for r in r_json["items"]:
                r_out += "Ruby" + var_delimiter + str(r["full_name"])+ str(var_delimiter)+ str(r["stargazers_count"])+ var_delimiter+ str(r["description"]) + var_delimiter+ str(r["pushed_at"]) + repo_delimiter
            print(r_out)
            r_out = str.encode(r_out)
            conn.send(r_out)
            r_out=""

        #Goes through json file to extract relevant information about repository, encodes the data, and sends it by tcp to port 9999, empties string.
        if "items" in c_json:
            for c in c_json["items"]:
                c_out += "C" + var_delimiter + str(c["full_name"])+ var_delimiter+ str(c["stargazers_count"]) + var_delimiter+ str(c["description"]) + var_delimiter+ c["pushed_at"] + repo_delimiter
            print(c_out)
            c_out = str.encode(c_out)
            conn.send(c_out)
            c_out = ""

        #Calculates sleeptime of 15 seconds that considers the start time, so runtime is not a factor and sleep is consistenly 15seconds.
        execution_time = (time.time_ns() - start_time)/1000000000
        if execution_time < 15:
            time.sleep( 15 - execution_time )


    except KeyboardInterrupt:


        s.shutdown(socket.SHUT_RD)

A big data system that performs real-time streaming analytics for public repositories hosted on GitHub. 
The system runs a stream processing pipeline, where the live data stream to be analyzed is coming from GitHub API. An Apache Spark cluster processes the data stream. 
A web application receives the output from Spark and visualizes the analysis result. This program is a multi-container system using on Docker.

**To run the program use the command:** \
    $ docker-compose up \
 **In a seperate terminal window use the command:** \
    $ docker exec streaming_spark_1 spark-submit /streaming/spark_app.py \
    **(Before running manually put all of the repository files inside a folder named streaming)**

The web application with the real-time charts will be listening on port 5000 of the Docker host.

## Output 

**Terminal output format :** 

![image](https://user-images.githubusercontent.com/67840155/190014293-b92a23ec-fb6d-4bc0-b068-d1a397d3b6f3.png)
\
**Web Application** 

![image](https://user-images.githubusercontent.com/67840155/190014038-41401ede-0bed-4aea-b23a-4b5c199e2072.png)

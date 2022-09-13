A big data system that performs real-time streaming analytics for public repositories hosted on GitHub. 
The system runs a stream processing pipeline, where the live data stream to be analyzed is coming from GitHub API. An Apache Spark cluster processes the data stream. 
A web application receives the output from Spark and visualizes the analysis result. This program is a multi-container system using on Docker.

To run the program: \
    $ docker-compose up \
    $ docker exec streaming_spark_1 spark-submit /streaming/spark_app.py

The web application with the real-time charts will be listening on port 5000 of the Docker host.

## Output

**Terminal output format :**
```
Application start timestamp in UTC:application end timestamp in UTC
Python:#collected repo (requirement 3(i)):average number of stars (requirement 3(iii))
Ruby2:#collected repo (requirement 3(i)):average number of stars (requirement 3(iii))
C:#collected repo (requirement 3(i)):average number of stars (requirement 3(iii))
Python:a comma-separated list with ten tuples, each of which contains a frequent word (top 10) and its number of occurrences (requirement 3(iv))
Ruby:a comma-separated list with ten tuples, each of which contains a frequent word (top 10) and its number of occurrences (requirement 3(iv))
C:a comma-separated list with ten tuples, each of which contains a frequent word (top 10) and its number of occurrences (requirement 3(iv))
```
**For example**
```
1648771200:1648778400
Python:4096:6.4
Ruby:2048:12.8
C:1024:1.6
Python:(You,1000),(need,999),(to,998),(deploy,997),(the,996),(streaming,995),(analysis,994),(system,993),(and,992),(keep,991)
Ruby:(it,1024),(running,512),(for,256),(at,128),(least,64),(two,32),(hours,16),(on,8),(your,4),(machine,2)
C:(Then,100),(you,99),(need,98),(to,97),(prepare,96),(a,95),(text,94),(file,93),(which,92),(consists,91)
```

# spotify-pipe
This repository provides a general example of working with the API and Airflow using the Python. 

The purpose of this ETL process is to get information about what songs the user has listened to on his account during a day. 
Every 24 hours this process is repeated, due to the fact that the Airflow orchestration tool is used.  

Over time, with daily running, it will be possible to analyse how user's taste in music is changing or not. 
As well as obtaining statistics such as: 
- Top bands
- Top songs 
- Average duration
- Time spent listening to music per day

The data is collected by the Spotify API, 
the resulting json file is parsed within the written function, 
which in the intermediate result gives a pandas dataframe with cleaned up data. 

Then the database connection is established [In this case a local MySQL database is used]. 
Then - loaded into the database.

In schedule.py file basic exmaple of defining dags is presented. 

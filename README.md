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

Then the database connection is established [In this case MySQL is used]. 

In order to secure my code, and not to use the password and username of the database directly, 
I used the os library to replace direct values with environmental variables.

Then - loaded into the database.

The problem that the token disappears quite quickly - its relevance time is only 1 hour - can be solved in the following ways. 
  - Manual update before starting the pipe
  - Using a refresh_token, which is provided by the spotify itself. 

To start with, I created a project here[https://developer.spotify.com/dashboard/applications]
Where I could generate **CLIENT_ID, SCOPE, and REDIRECT_URI**. 
To get a long term token, run the following command 
``` cmd
curl -d client_id=$CLIENT_ID -d client_secret=$CLIENT_SECRET -d grant_type=authorization_code -d code=$CODE -d redirect_uri=$REDIRECT_URI https://accounts.spotify.com/api/token
```

This resulted in a json file with the required information
```json
{
    "access_token": token1",
    "token_type": "Bearer",
    "expires_in": 3600,
    "refresh_token": "token2",
    "scope": "user-read-recently-played"
}
```

In schedule.py file basic exmaple of defining dags is presented. 



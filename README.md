# ***IMPORTANT*** 

This projects requires the installation of AstroCLI, refer to below link on how:

**https://www.astronomer.io/docs/astro/cli/install-cli/**


## **Documentation**

This is a project that uses Airflow to automate getting a random spotify artist's top album/track, below describes the workflow or DAG:

## Extract
Using the spotipy API, extract the top tracks from a random artist in an array eg. 

["Bruno Mars", "Lady Gaga", "The Weeknd", "Kendrick Lamar", "Billie Eilish", "Coldplay", "SZA", "Rihanna", "Bad Bunny", "Taylor Swift"]


and load the data into a dataframe

you can configure the array if you want.
## Transform 
Transform the dataframe into more meaningful data like changing the release date column to a **datetime** datatype
## Load
Load the dataframe into postgres database


Packages used can be inferred in the requirements.txt
- spotipy
- pandas

## **To Run**

in terminal type:

astro dev start

## **Extra Config**

You might need to change the postgres port if you already have postgres port installed in your **local**, refer to below link on how:

**https://www.astronomer.io/docs/astro/cli/configure-cli/#:~:text=Any%20string-,postgres.port,-The%20port%20for**

# ***IMPORTANT*** 

This projects requires the installation of AstroCLI, refer to below link on how:

**https://www.astronomer.io/docs/astro/cli/install-cli/**


## **Documentation**

This is a project that uses Airflow to automate getting a random spotify artist's top album/track, below describes the workflow or DAG:

## Extract
Using the spotipy API, extract the top album from a random artist ["Bruno Mars", "Lady Gaga", "The Weeknd", "Kendrick Lamar", "Billie Eilish", "Coldplay", "SZA", "Rihanna", "Bad Bunny", "Taylor Swift"]

you can configure the array if you want.
## Transform 

## Load

Packages used can be inferred in the requirements.txt
- spotipy
- pandas

## **To Run**

in terminal type:

astro dev start

## **Extra Config**

You might need to change the postgres port if you already have postgres port installed in your **local**, refer to below link on how:

**https://www.astronomer.io/docs/astro/cli/configure-cli/#:~:text=Any%20string-,postgres.port,-The%20port%20for**

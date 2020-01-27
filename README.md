# Historical and realtime analytics platform: tracking trends in sentiment towards entities in news vs social media 

The data: GDELT dataset (https://www.gdeltproject.org/), Twitter archive (https://archive.org/details/twitterstream?and%5B%5D=year%3A%222018%22h)

Pipeline: S3 -> Spark -> Postgres -> Dash
          Stream data -> Pulsar -> Postgres -> Dash


import csv
import random
import pandas as pd
from pyspark.sql import SparkSession

song_ids = [f"song_{i}" for i in range(1, 201)]
genres = ["Pop", "Rock", "Jazz", "Hip-Hop", "Electronic"]
moods = ["Happy", "Sad", "Energetic", "Chill"]

with open("songs_metadata.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["song_id", "title", "artist", "genre", "mood"])
    for song_id in song_ids:
        title = f"Title_{random.randint(1, 1000)}"
        artist = f"Artist_{random.randint(1, 300)}"
        genre = random.choice(genres)
        mood = random.choice(moods)
        writer.writerow([song_id, title, artist, genre, mood])
df = pd.read_csv("songs_metadata.csv")
#print(df["song_id"])




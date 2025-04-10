from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime

# Initialize Spark Session
spark = SparkSession.builder.appName("MusicAnalytics").getOrCreate()

# Load datasets
logs = spark.read.option("header", True).csv("listening_logs.csv")
songs = spark.read.option("header", True).csv("songs_metadata.csv")

# Preprocess
logs = logs.withColumn("timestamp", to_timestamp("timestamp")) \
           .withColumn("duration_sec", col("duration_sec").cast("int"))
songs = songs.dropDuplicates(["song_id"])

# Join logs with song metadata
enriched = logs.join(songs, "song_id")

# Task 1: User's Favorite Genre
genre_count = enriched.groupBy("user_id", "genre").count()
window_spec = Window.partitionBy("user_id").orderBy(col("count").desc())
favorite_genre = genre_count.withColumn("rank", row_number().over(window_spec)) \
                            .filter(col("rank") == 1) \
                            .drop("rank")
favorite_genre.write.csv("output/user_favorite_genres", header=True)

# Task 2: Average Listen Time per Song
avg_duration = enriched.groupBy("song_id", "title").agg(avg("duration_sec").alias("avg_duration_sec"))
avg_duration.write.csv("output/avg_listen_time_per_song", header=True)

# Task 3: Top 10 Most Played Songs This Week
current_week_start = datetime(2025, 3, 23)
week_df = enriched.filter(col("timestamp") >= lit(current_week_start))
top_songs = week_df.groupBy("song_id", "title").count().orderBy(col("count").desc()).limit(10)
top_songs.write.csv("output/top_songs_this_week", header=True)

# Task 4: Recommend "Happy" Songs to Sad Listeners
mood_counts = enriched.groupBy("user_id", "mood").count()
top_mood = mood_counts.withColumn("rank", row_number().over(Window.partitionBy("user_id").orderBy(col("count").desc())))
top_mood = top_mood.filter(col("rank") == 1)
sad_users = top_mood.filter(col("mood") == "Sad").select("user_id")
user_song_history = enriched.select("user_id", "song_id").distinct()
happy_songs = songs.filter(col("mood") == "Happy").select("song_id", "title", "artist").distinct()
recommendations = sad_users.crossJoin(happy_songs) \
    .join(user_song_history, ["user_id", "song_id"], "left_anti") \
    .withColumn("rank", row_number().over(Window.partitionBy("user_id").orderBy(rand()))) \
    .filter(col("rank") <= 3) \
    .drop("rank")
recommendations.write.csv("output/happy_recommendations", header=True)

# Task 5: Genre Loyalty Score
total_counts = enriched.groupBy("user_id").agg(count("*").alias("total_plays"))
genre_counts = enriched.groupBy("user_id", "genre").count()
loyalty = genre_counts.join(total_counts, "user_id") \
                      .withColumn("loyalty_score", col("count") / col("total_plays"))
top_loyalty = loyalty.withColumn("rank", row_number().over(Window.partitionBy("user_id").orderBy(col("loyalty_score").desc()))) \
                     .filter((col("rank") == 1) & (col("loyalty_score") > 0.8)) \
                     .select("user_id", "genre", "loyalty_score")
top_loyalty.write.csv("output/genre_loyalty_scores", header=True)

# Task 6: Night Owl Users (12AM–5AM)
night_logs = enriched.withColumn("hour", hour("timestamp")) \
                     .filter((col("hour") >= 0) & (col("hour") < 5))
night_users = night_logs.groupBy("user_id").count().filter(col("count") >= 5)
night_users.write.csv("output/night_owl_users", header=True)

# Task 7: Enriched Logs
enriched.write.csv("output/enriched_logs", header=True)

spark.stop()

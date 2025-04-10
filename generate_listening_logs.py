import csv
import random
from datetime import datetime, timedelta

user_ids = [f"user_{i}" for i in range(1, 101)]
song_ids = [f"song_{i}" for i in range(1, 201)]
start_date = datetime(2025, 3, 18)

with open("listening_logs.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["user_id", "song_id", "timestamp", "duration_sec"])
    for _ in range(10000):
        user = random.choice(user_ids)
        song = random.choice(song_ids)
        timestamp = start_date + timedelta(seconds=random.randint(0, 604800))  # one week
        duration = random.randint(30, 300)
        writer.writerow([user, song, timestamp.strftime("%Y-%m-%d %H:%M:%S"), duration])

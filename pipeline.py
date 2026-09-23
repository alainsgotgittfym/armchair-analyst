import logging
import random
from datetime import datetime, timedelta
import duckdb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def fetch_couch_potato_logs(num_records: int = 15) -> list[dict]:
    genres = ["sci-fi", "comedy", "drama", "anime", "horror", "documentary"]
    users = [101, 102, 103, 104, 105]
    movies = [f"m_{i}" for i in range(1, 10)]
    
    raw_logs = []
    base_time = datetime.now()

    for _ in range(num_records):
        duration = random.choice([3600, 5400, 7200, -500, 0])
        watched = random.choice([0, 1200, 3600, 5400, 8000, -10])
        genre = random.choice(genres + [None])
        
        is_broken_date = random.random() < 0.1
        timestamp = "broken_timestamp_lol" if is_broken_date else (base_time - timedelta(minutes=random.randint(0, 10000))).isoformat()

        raw_logs.append({
            "user_id": random.choice(users),
            "movie_id": random.choice(movies),
            "duration_sec": duration,
            "watched_sec": watched,
            "genre": genre,
            "timestamp": timestamp
        })

    return raw_logs


def sanitize_garbage(logs: list[dict]) -> pd.DataFrame:
    valid_logs = []
    
    for log in logs:
        try:
            if log["duration_sec"] <= 0 or log["watched_sec"] < 0:
                continue
            if log["watched_sec"] > log["duration_sec"]:
                continue
            
            log["timestamp"] = datetime.fromisoformat(log["timestamp"])
            valid_logs.append(log)
        except (ValueError, TypeError):
            continue

    return pd.DataFrame(valid_logs)


def crunch_lazy_metrics(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    df["completion_rate"] = df["watched_sec"] / df["duration_sec"]

    features = df.groupby("user_id").agg(
        total_watched_min=("watched_sec", lambda x: round(x.sum() / 60, 2)),
        avg_completion_rate=("completion_rate", lambda x: round(x.mean(), 2)),
        total_views=("movie_id", "count"),
        favorite_genre=("genre", lambda x: x.mode()[0] if not x.empty else "unknown")
    ).reset_index()

    return features


def save_to_duckdb(df: pd.DataFrame, db_path: str = "analytics.db") -> None:
    if df.empty:
        logging.warning("DataFrame is empty, skipping DB export.")
        return

    con = duckdb.connect(db_path)
    con.execute("CREATE TABLE IF NOT EXISTS user_features AS SELECT * FROM df WHERE 1=0")
    con.execute("INSERT INTO user_features SELECT * FROM df")
    con.close()
    
    logging.info(f"Successfully saved features to DuckDB database: {db_path}")


def run_pipeline() -> None:
    logging.info("Waking up the pipeline...")
    
    raw_data = fetch_couch_potato_logs(num_records=20)
    cleaned_df = sanitize_garbage(raw_data)
    
    logging.info(f"Dumped {len(raw_data)} raw records. Filtered down to {len(cleaned_df)} non-trash records.")
    
    features_df = crunch_lazy_metrics(cleaned_df)
    save_to_duckdb(features_df)
    
    print("\nLazy Couch Potato Analytics:")
    print(features_df.to_string(index=False))


if __name__ == "__main__":
    run_pipeline()
import os
import re
from datetime import datetime
from glob import glob

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine


def load_env_file(path: str = ".env") -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


def get_db_url() -> str:
    host = os.environ.get("POSTGRES_CONN_HOST", "localhost")
    port = os.environ.get("POSTGRES_CONN_PORT", "5432")
    user = os.environ.get("ELT_DATABASE_USERNAME", "yt_api_user")
    password = os.environ.get("ELT_DATABASE_PASSWORD", "")
    db = os.environ.get("ELT_DATABASE_NAME", "elt_db")
    if host == "postgres":
        host = "localhost"
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


def load_from_postgres() -> pd.DataFrame:
    engine = create_engine(get_db_url())
    query = """
        SELECT
            "Video_ID",
            "Video_Title",
            "Upload_DATE",
            "Duration",
            "Video_Type",
            "Video_Views",
            "Likes_Count",
            "Comments_Count"
        FROM core.yt_api
    """
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df


def parse_date_from_filename(path: str) -> datetime | None:
    m = re.search(r"YT_data_(\\d{4}-\\d{2}-\\d{2})\\.json$", path)
    if not m:
        return None
    return datetime.strptime(m.group(1), "%Y-%m-%d")


def load_latest_json() -> pd.DataFrame:
    files = glob("data/YT_data_*.json")
    if not files:
        raise FileNotFoundError("No JSON files found in data/")
    files_sorted = sorted(files, key=lambda p: parse_date_from_filename(p) or datetime.min)
    latest = files_sorted[-1]
    df = pd.read_json(latest)
    return df


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    # Normalize column names to the core schema
    rename_map = {
        "video_id": "Video_ID",
        "title": "Video_Title",
        "published_at": "Upload_DATE",
        "duration": "Duration",
        "view_count": "Video_Views",
        "like_count": "Likes_Count",
        "comment_count": "Comments_Count",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Ensure numeric columns
    for col in ["Video_Views", "Likes_Count", "Comments_Count"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # Ensure timestamp
    if "Upload_DATE" in df.columns:
        df["Upload_DATE"] = pd.to_datetime(df["Upload_DATE"], errors="coerce")

    return df


def ratio(numer: int, denom: int) -> float:
    if denom == 0:
        return 0.0
    return numer / denom


def render_metrics(df: pd.DataFrame) -> None:
    views = int(df["Video_Views"].sum()) if "Video_Views" in df.columns else 0
    likes = int(df["Likes_Count"].sum()) if "Likes_Count" in df.columns else 0
    comments = int(df["Comments_Count"].sum()) if "Comments_Count" in df.columns else 0

    like_ratio = ratio(likes, views)
    comment_ratio = ratio(comments, views)

    m1, m2, m3 = st.columns(3)
    m1.metric("Total Views", f"{views:,}")
    m2.metric("Like-to-View Ratio", f"{like_ratio:.2%}")
    m3.metric("Comment-to-View Ratio", f"{comment_ratio:.2%}")


def render_top10(df: pd.DataFrame) -> None:
    if "Video_Views" not in df.columns:
        st.warning("Video_Views column missing.")
        return
    top10 = df.sort_values("Video_Views", ascending=False).head(10)
    st.subheader("Top 10 Videos by Views")
    st.dataframe(
        top10[
            [
                "Video_Title",
                "Video_Views",
                "Likes_Count",
                "Comments_Count",
                "Upload_DATE",
            ]
        ],
        use_container_width=True,
    )
    st.bar_chart(top10.set_index("Video_Title")["Video_Views"])


def main() -> None:
    load_env_file()
    st.set_page_config(page_title="YouTube ELT Metrics", layout="wide")
    st.title("YouTube ELT Metrics")

    source = st.sidebar.selectbox(
        "Data Source",
        ["Postgres (core.yt_api)", "Latest JSON file"],
    )

    try:
        if source.startswith("Postgres"):
            df = load_from_postgres()
        else:
            df = load_latest_json()
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        st.stop()

    df = normalize_df(df)

    if df.empty:
        st.warning("No data found.")
        st.stop()

    render_metrics(df)
    render_top10(df)


if __name__ == "__main__":
    main()

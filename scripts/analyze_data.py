"""
Quick data analysis script for your Spotify data
"""
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# Set display options
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_rows', 50)

# Connect to database
db_path = 'data/duckdb/spotify.duckdb'
conn = duckdb.connect(db_path, read_only=True)

print("=" * 80)
print("🎵 SPOTIFY LISTENING ANALYTICS 🎵")
print("=" * 80)

# 1. Overview Stats
print("\n📊 OVERVIEW")
print("-" * 80)

total_plays = conn.execute("""
    SELECT COUNT(*) FROM marts.fct_listening_history
""").fetchone()[0]

unique_tracks = conn.execute("""
    SELECT COUNT(DISTINCT track_id) FROM marts.dim_tracks
""").fetchone()[0]

unique_artists = conn.execute("""
    SELECT COUNT(DISTINCT artist_id) FROM marts.dim_artists
""").fetchone()[0]

print(f"Total Plays: {total_plays:,}")
print(f"Unique Tracks: {unique_tracks:,}")
print(f"Unique Artists: {unique_artists:,}")

# 2. Top Artists
print("\n👤 TOP 10 ARTISTS")
print("-" * 80)
top_artists = conn.execute("""
    SELECT 
        artist_name,
        total_plays,
        unique_tracks,
        ROUND(avg_popularity, 1) as avg_popularity
    FROM marts.dim_artists
    ORDER BY total_plays DESC
    LIMIT 10
""").fetchdf()
print(top_artists.to_string(index=False))

# 3. Top Tracks
print("\n🎵 TOP 10 TRACKS")
print("-" * 80)
top_tracks = conn.execute("""
    SELECT 
        track_name,
        artist_name,
        total_plays,
        avg_popularity
    FROM marts.dim_tracks
    ORDER BY total_plays DESC
    LIMIT 10
""").fetchdf()
print(top_tracks.to_string(index=False))

# 4. Listening Patterns by Hour
print("\n⏰ LISTENING PATTERNS BY HOUR OF DAY")
print("-" * 80)
hourly_patterns = conn.execute("""
    SELECT 
        hour_of_day,
        total_plays,
        unique_tracks
    FROM analytics.listening_patterns_hourly
    ORDER BY hour_of_day
""").fetchdf()

if not hourly_patterns.empty:
    print(hourly_patterns.to_string(index=False))
    
    # Create a simple bar chart
    plt.figure(figsize=(12, 6))
    plt.bar(hourly_patterns['hour_of_day'], hourly_patterns['total_plays'])
    plt.xlabel('Hour of Day')
    plt.ylabel('Number of Plays')
    plt.title('Listening Activity by Hour')
    plt.xticks(range(0, 24))
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig('data/exports/listening_by_hour.png', dpi=300, bbox_inches='tight')
    print("\n📊 Chart saved to: data/exports/listening_by_hour.png")

# 5. Recent Listening History
print("\n🎧 RECENT LISTENING (Last 10 plays)")
print("-" * 80)
recent = conn.execute("""
    SELECT 
        played_at,
        track_name,
        artist_name,
        album_name
    FROM marts.fct_listening_history
    ORDER BY played_at DESC
    LIMIT 10
""").fetchdf()
print(recent.to_string(index=False))

# 6. Daily Top Tracks
print("\n📅 TOP TRACKS BY DAY")
print("-" * 80)
daily_tracks = conn.execute("""
    SELECT 
        play_date,
        track_name,
        artist_name,
        play_count,
        row_number
    FROM analytics.top_tracks_daily
    WHERE row_number <= 3
    ORDER BY play_date DESC, row_number
    LIMIT 15
""").fetchdf()

if not daily_tracks.empty:
    print(daily_tracks.to_string(index=False))

# 7. Audio Features Analysis (if available)
print("\n🎼 AUDIO FEATURES ANALYSIS")
print("-" * 80)
try:
    audio_analysis = conn.execute("""
        SELECT 
            analysis_date,
            avg_energy,
            avg_danceability,
            avg_valence as avg_positivity
        FROM analytics.audio_features_analysis
        ORDER BY analysis_date DESC
        LIMIT 7
    """).fetchdf()
    
    if not audio_analysis.empty:
        print(audio_analysis.to_string(index=False))
    else:
        print("No audio features data available (requires Spotify Premium)")
except:
    print("Audio features not available (requires Spotify Premium)")

# Export data to CSV for external analysis
print("\n💾 EXPORTING DATA")
print("-" * 80)

# Export top artists
top_artists.to_csv('data/exports/top_artists.csv', index=False)
print("✓ Exported: data/exports/top_artists.csv")

# Export top tracks
top_tracks.to_csv('data/exports/top_tracks.csv', index=False)
print("✓ Exported: data/exports/top_tracks.csv")

# Export listening history
all_history = conn.execute("""
    SELECT 
        played_at,
        track_name,
        artist_name,
        album_name,
        duration_minutes,
        popularity
    FROM marts.fct_listening_history
    ORDER BY played_at DESC
""").fetchdf()
all_history.to_csv('data/exports/listening_history.csv', index=False)
print(f"✓ Exported: data/exports/listening_history.csv ({len(all_history):,} records)")

conn.close()

print("\n" + "=" * 80)
print("✅ Analysis Complete!")
print("=" * 80)
print("\nExported files are in: data/exports/")
print("You can open these CSV files in Excel, Google Sheets, or any data viz tool!")


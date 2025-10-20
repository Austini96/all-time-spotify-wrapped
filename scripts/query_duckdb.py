"""
DuckDB Query Helper Script
Run this locally to explore your Spotify data
"""
import duckdb
import pandas as pd
from pathlib import Path
import sys

# Set pandas display options for better readability
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 50)


def connect_db(db_path='data/duckdb/spotify.duckdb'):
    """Connect to DuckDB database"""
    if not Path(db_path).exists():
        print(f"❌ Database not found at: {db_path}")
        print("Make sure you've run the Airflow pipeline first!")
        sys.exit(1)
    
    return duckdb.connect(db_path)


def show_overview(conn):
    """Show database overview"""
    print("=" * 80)
    print("SPOTIFY DATA WAREHOUSE OVERVIEW")
    print("=" * 80)
    
    # Show all tables
    print("\n📊 TABLES:")
    tables = conn.execute("SHOW TABLES").fetchdf()
    print(tables)
    
    # Show counts for each table
    print("\n📈 ROW COUNTS:")
    for table in tables['name']:
        count = conn.execute(f"SELECT COUNT(*) as count FROM {table}").fetchone()[0]
        print(f"  {table}: {count:,} rows")
    
    print()


def show_raw_data(conn):
    """Show raw data samples"""
    print("=" * 80)
    print("RAW DATA SAMPLES")
    print("=" * 80)
    
    # Raw tracks
    print("\n🎵 RECENT TRACKS (Last 5):")
    tracks = conn.execute("""
        SELECT 
            played_at,
            track_name,
            artist_name,
            album_name,
            popularity
        FROM raw_spotify_tracks
        ORDER BY played_at DESC
        LIMIT 5
    """).fetchdf()
    print(tracks)
    
    # Top artists
    print("\n👤 TOP ARTISTS (By play count):")
    artists = conn.execute("""
        SELECT 
            artist_name,
            COUNT(*) as play_count
        FROM raw_spotify_tracks
        GROUP BY artist_name
        ORDER BY play_count DESC
        LIMIT 5
    """).fetchdf()
    print(artists)
    
    # Audio features (if available)
    try:
        feature_count = conn.execute("SELECT COUNT(*) FROM raw_spotify_audio_features").fetchone()[0]
        if feature_count > 0:
            print("\n🎼 AUDIO FEATURES SAMPLE:")
            features = conn.execute("""
                SELECT 
                    id,
                    danceability,
                    energy,
                    tempo,
                    valence
                FROM raw_spotify_audio_features
                LIMIT 5
            """).fetchdf()
            print(features)
    except:
        print("\n⚠️  Audio features not available")


def show_analytics(conn):
    """Show analytics data"""
    print("\n" + "=" * 80)
    print("ANALYTICS & INSIGHTS")
    print("=" * 80)
    
    # Listening patterns by hour
    print("\n⏰ LISTENING PATTERNS BY HOUR:")
    hourly = conn.execute("""
        SELECT 
            EXTRACT(HOUR FROM played_at) as hour,
            COUNT(*) as plays
        FROM raw_spotify_tracks
        GROUP BY hour
        ORDER BY hour
        LIMIT 10
    """).fetchdf()
    if not hourly.empty:
        print(hourly)
    else:
        print("No data yet")
    
    # Most popular tracks
    print("\n🔥 MOST PLAYED TRACKS:")
    top_tracks = conn.execute("""
        SELECT 
            track_name,
            artist_name,
            COUNT(*) as play_count
        FROM raw_spotify_tracks
        GROUP BY track_name, artist_name
        ORDER BY play_count DESC
        LIMIT 5
    """).fetchdf()
    print(top_tracks)


def run_custom_query(conn, query):
    """Run a custom SQL query"""
    print("\n" + "=" * 80)
    print("CUSTOM QUERY RESULTS")
    print("=" * 80)
    print(f"\nQuery: {query}\n")
    
    try:
        result = conn.execute(query).fetchdf()
        print(result)
        return result
    except Exception as e:
        print(f"❌ Error: {e}")
        return None


def interactive_mode(conn):
    """Interactive query mode"""
    print("\n" + "=" * 80)
    print("INTERACTIVE MODE")
    print("=" * 80)
    print("Enter SQL queries (or 'exit' to quit)")
    print("Example: SELECT * FROM raw_spotify_tracks LIMIT 5;")
    print()
    
    while True:
        query = input("SQL> ").strip()
        
        if query.lower() in ['exit', 'quit', 'q']:
            break
        
        if not query:
            continue
        
        try:
            result = conn.execute(query).fetchdf()
            print(result)
            print()
        except Exception as e:
            print(f"❌ Error: {e}\n")


def main():
    """Main function"""
    print("\n🎵 Spotify DuckDB Query Tool 🎵\n")
    
    # Connect to database
    conn = connect_db()
    
    # Show options
    print("What would you like to do?")
    print("  1. Show database overview")
    print("  2. Show raw data samples")
    print("  3. Show analytics & insights")
    print("  4. Run custom query")
    print("  5. Interactive mode")
    print("  6. Show everything")
    print("  0. Exit")
    
    choice = input("\nEnter choice (0-6): ").strip()
    
    if choice == '1':
        show_overview(conn)
    elif choice == '2':
        show_raw_data(conn)
    elif choice == '3':
        show_analytics(conn)
    elif choice == '4':
        query = input("Enter SQL query: ").strip()
        run_custom_query(conn, query)
    elif choice == '5':
        interactive_mode(conn)
    elif choice == '6':
        show_overview(conn)
        show_raw_data(conn)
        show_analytics(conn)
    elif choice == '0':
        print("Goodbye!")
    else:
        print("Invalid choice")
    
    conn.close()


if __name__ == "__main__":
    main()


"""
Manually emit OpenLineage events for complete dataset lineage
Includes extended history, playlists, and full column-level lineage from raw sources
"""
import os
import requests
from datetime import datetime
import uuid

MARQUEZ_URL = os.getenv('OPENLINEAGE_URL', 'http://marquez-api:5000')
NAMESPACE = os.getenv('OPENLINEAGE_NAMESPACE', 'spotify_analytics')


def emit_openlineage_event(job_name, inputs=None, outputs=None, column_lineage=None, run_id=None):
    """
    Emit an OpenLineage event to Marquez with column-level lineage
    
    Args:
        job_name: Name of the job
        inputs: List of dicts with 'name' and optionally 'fields' (schema)
        outputs: List of dicts with 'name' and optionally 'fields' (schema)
        column_lineage: Dict mapping output columns to input columns
        run_id: Optional run ID (generated if not provided)
    """
    if run_id is None:
        run_id = str(uuid.uuid4())
    
    event_time = datetime.utcnow().isoformat() + 'Z'
    
    # Format input datasets with schema
    input_datasets = []
    if inputs:
        for inp in inputs:
            dataset = {
                "namespace": NAMESPACE,
                "name": inp['name'] if isinstance(inp, dict) else inp
            }
            # Add schema if provided
            if isinstance(inp, dict) and 'fields' in inp:
                dataset["facets"] = {
                    "schema": {
                        "_producer": "spotify_analytics_manual",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
                        "fields": inp['fields']
                    }
                }
            input_datasets.append(dataset)
    
    # Format output datasets with schema and column lineage
    output_datasets = []
    if outputs:
        for out in outputs:
            dataset = {
                "namespace": NAMESPACE,
                "name": out['name'] if isinstance(out, dict) else out
            }
            
            facets = {}
            
            # Add schema if provided
            if isinstance(out, dict) and 'fields' in out:
                facets["schema"] = {
                    "_producer": "spotify_analytics_manual",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
                    "fields": out['fields']
                }
            
            # Add column lineage if provided
            if column_lineage:
                facets["columnLineage"] = {
                    "_producer": "spotify_analytics_manual",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ColumnLineageDatasetFacet.json",
                    "fields": column_lineage
                }
            
            if facets:
                dataset["facets"] = facets
            
            output_datasets.append(dataset)
    
    event = {
        "eventType": "COMPLETE",
        "eventTime": event_time,
        "run": {
            "runId": run_id,
            "facets": {}
        },
        "job": {
            "namespace": NAMESPACE,
            "name": job_name,
            "facets": {}
        },
        "inputs": input_datasets,
        "outputs": output_datasets,
        "producer": "spotify_analytics_manual"
    }
    
    try:
        response = requests.post(
            f"{MARQUEZ_URL}/api/v1/lineage",
            json=event,
            headers={"Content-Type": "application/json"}
        )
        if response.status_code in [200, 201]:
            col_info = f", {len(column_lineage)} column mappings" if column_lineage else ""
            print(f"✓ Emitted lineage for {job_name}")
            print(f"  Inputs: {len(input_datasets)}, Outputs: {len(output_datasets)}{col_info}")
        else:
            print(f"✗ Failed to emit lineage: {response.status_code}")
            print(f"  Response: {response.text}")
    except Exception as e:
        print(f"✗ Error emitting lineage: {e}")


def emit_dbt_lineage():
    """Emit comprehensive lineage for all dbt models including extended history and playlists"""
    
    print("\n🔗 Emitting complete dbt model lineage to Marquez...")
    print(f"   Marquez URL: {MARQUEZ_URL}")
    print(f"   Namespace: {NAMESPACE}\n")
    
    # ========================================
    # STAGING MODELS
    # ========================================
    
    # 1. stg_spotify_tracks (from API)
    emit_openlineage_event(
        job_name="dbt.stg_spotify_tracks",
        inputs=[{
            "name": "raw_spotify_tracks",
            "fields": [
                {"name": "played_at", "type": "TIMESTAMP"},
                {"name": "track_id", "type": "VARCHAR"},
                {"name": "track_name", "type": "VARCHAR"},
                {"name": "artist_id", "type": "VARCHAR"},
                {"name": "artist_name", "type": "VARCHAR"},
                {"name": "album_id", "type": "VARCHAR"},
                {"name": "album_name", "type": "VARCHAR"},
                {"name": "album_release_date", "type": "VARCHAR"},
                {"name": "duration_ms", "type": "INTEGER"},
                {"name": "popularity", "type": "INTEGER"},
                {"name": "explicit", "type": "BOOLEAN"},
            ]
        }],
        outputs=[{
            "name": "staging.stg_spotify_tracks",
            "fields": [
                {"name": "play_id", "type": "INTEGER"},
                {"name": "played_at", "type": "TIMESTAMP"},
                {"name": "played_date", "type": "DATE"},
                {"name": "track_id", "type": "VARCHAR"},
                {"name": "track_name", "type": "VARCHAR"},
                {"name": "artist_id", "type": "VARCHAR"},
                {"name": "artist_name", "type": "VARCHAR"},
                {"name": "album_id", "type": "VARCHAR"},
                {"name": "album_name", "type": "VARCHAR"},
                {"name": "duration_ms", "type": "INTEGER"},
                {"name": "popularity", "type": "INTEGER"},
            ]
        }],
        column_lineage={
            "played_at": {"inputFields": [{"namespace": NAMESPACE, "name": "raw_spotify_tracks", "field": "played_at"}]},
            "track_id": {"inputFields": [{"namespace": NAMESPACE, "name": "raw_spotify_tracks", "field": "track_id"}]},
            "track_name": {"inputFields": [{"namespace": NAMESPACE, "name": "raw_spotify_tracks", "field": "track_name"}]},
            "artist_id": {"inputFields": [{"namespace": NAMESPACE, "name": "raw_spotify_tracks", "field": "artist_id"}]},
            "artist_name": {"inputFields": [{"namespace": NAMESPACE, "name": "raw_spotify_tracks", "field": "artist_name"}]},
            "album_id": {"inputFields": [{"namespace": NAMESPACE, "name": "raw_spotify_tracks", "field": "album_id"}]},
            "album_name": {"inputFields": [{"namespace": NAMESPACE, "name": "raw_spotify_tracks", "field": "album_name"}]},
            "duration_ms": {"inputFields": [{"namespace": NAMESPACE, "name": "raw_spotify_tracks", "field": "duration_ms"}]},
            "popularity": {"inputFields": [{"namespace": NAMESPACE, "name": "raw_spotify_tracks", "field": "popularity"}]},
        }
    )
    
    # 2. stg_spotify_extended_history (from extended streaming history)
    emit_openlineage_event(
        job_name="dbt.stg_spotify_extended_history",
        inputs=[{
            "name": "raw_spotify_extended_history",
            "fields": [
                {"name": "ts", "type": "TIMESTAMP"},
                {"name": "platform", "type": "VARCHAR"},
                {"name": "ms_played", "type": "INTEGER"},
                {"name": "conn_country", "type": "VARCHAR"},
                {"name": "master_metadata_track_name", "type": "VARCHAR"},
                {"name": "master_metadata_album_artist_name", "type": "VARCHAR"},
                {"name": "master_metadata_album_album_name", "type": "VARCHAR"},
                {"name": "spotify_track_uri", "type": "VARCHAR"},
                {"name": "reason_start", "type": "VARCHAR"},
                {"name": "reason_end", "type": "VARCHAR"},
                {"name": "shuffle", "type": "BOOLEAN"},
                {"name": "skipped", "type": "BOOLEAN"},
            ]
        }],
        outputs=[{
            "name": "staging.stg_spotify_extended_history",
            "fields": [
                {"name": "played_at", "type": "TIMESTAMP"},
                {"name": "played_date", "type": "DATE"},
                {"name": "platform", "type": "VARCHAR"},
                {"name": "ms_played", "type": "INTEGER"},
                {"name": "conn_country", "type": "VARCHAR"},
                {"name": "track_id", "type": "VARCHAR"},
                {"name": "track_name", "type": "VARCHAR"},
                {"name": "artist_id", "type": "VARCHAR"},
                {"name": "artist_name", "type": "VARCHAR"},
                {"name": "album_id", "type": "VARCHAR"},
                {"name": "album_name", "type": "VARCHAR"},
                {"name": "spotify_track_uri", "type": "VARCHAR"},
                {"name": "reason_start", "type": "VARCHAR"},
                {"name": "reason_end", "type": "VARCHAR"},
                {"name": "shuffle", "type": "BOOLEAN"},
                {"name": "skipped", "type": "BOOLEAN"},
            ]
        }],
        column_lineage={
            "played_at": {"inputFields": [{"namespace": NAMESPACE, "name": "raw_spotify_extended_history", "field": "ts"}]},
            "platform": {"inputFields": [{"namespace": NAMESPACE, "name": "raw_spotify_extended_history", "field": "platform"}]},
            "ms_played": {"inputFields": [{"namespace": NAMESPACE, "name": "raw_spotify_extended_history", "field": "ms_played"}]},
            "conn_country": {"inputFields": [{"namespace": NAMESPACE, "name": "raw_spotify_extended_history", "field": "conn_country"}]},
            "track_name": {"inputFields": [{"namespace": NAMESPACE, "name": "raw_spotify_extended_history", "field": "master_metadata_track_name"}]},
            "artist_name": {"inputFields": [{"namespace": NAMESPACE, "name": "raw_spotify_extended_history", "field": "master_metadata_album_artist_name"}]},
            "album_name": {"inputFields": [{"namespace": NAMESPACE, "name": "raw_spotify_extended_history", "field": "master_metadata_album_album_name"}]},
            "spotify_track_uri": {"inputFields": [{"namespace": NAMESPACE, "name": "raw_spotify_extended_history", "field": "spotify_track_uri"}]},
            "reason_start": {"inputFields": [{"namespace": NAMESPACE, "name": "raw_spotify_extended_history", "field": "reason_start"}]},
            "reason_end": {"inputFields": [{"namespace": NAMESPACE, "name": "raw_spotify_extended_history", "field": "reason_end"}]},
            "shuffle": {"inputFields": [{"namespace": NAMESPACE, "name": "raw_spotify_extended_history", "field": "shuffle"}]},
            "skipped": {"inputFields": [{"namespace": NAMESPACE, "name": "raw_spotify_extended_history", "field": "skipped"}]},
        }
    )
    
    # 3. stg_spotify_audio_features
    emit_openlineage_event(
        job_name="dbt.stg_spotify_audio_features",
        inputs=[{"name": "raw_spotify_audio_features"}],
        outputs=[{"name": "staging.stg_spotify_audio_features"}]
    )
    
    # 4. stg_spotify_artists
    emit_openlineage_event(
        job_name="dbt.stg_spotify_artists",
        inputs=[{"name": "raw_spotify_artists"}],
        outputs=[{"name": "staging.stg_spotify_artists"}]
    )
    
    # 5. stg_spotify_playlists (NEW)
    emit_openlineage_event(
        job_name="dbt.stg_spotify_playlists",
        inputs=[{
            "name": "raw_spotify_playlists",
            "fields": [
                {"name": "playlist_id", "type": "VARCHAR"},
                {"name": "playlist_name", "type": "VARCHAR"},
                {"name": "owner_id", "type": "VARCHAR"},
                {"name": "is_owner", "type": "BOOLEAN"},
                {"name": "is_public", "type": "BOOLEAN"},
                {"name": "total_tracks", "type": "INTEGER"},
            ]
        }],
        outputs=[{
            "name": "staging.stg_spotify_playlists",
            "fields": [
                {"name": "playlist_id", "type": "VARCHAR"},
                {"name": "playlist_name", "type": "VARCHAR"},
                {"name": "owner_id", "type": "VARCHAR"},
                {"name": "is_owner", "type": "BOOLEAN"},
                {"name": "is_public", "type": "BOOLEAN"},
                {"name": "total_tracks", "type": "INTEGER"},
            ]
        }],
        column_lineage={
            "playlist_id": {"inputFields": [{"namespace": NAMESPACE, "name": "raw_spotify_playlists", "field": "playlist_id"}]},
            "playlist_name": {"inputFields": [{"namespace": NAMESPACE, "name": "raw_spotify_playlists", "field": "playlist_name"}]},
            "owner_id": {"inputFields": [{"namespace": NAMESPACE, "name": "raw_spotify_playlists", "field": "owner_id"}]},
            "is_owner": {"inputFields": [{"namespace": NAMESPACE, "name": "raw_spotify_playlists", "field": "is_owner"}]},
            "is_public": {"inputFields": [{"namespace": NAMESPACE, "name": "raw_spotify_playlists", "field": "is_public"}]},
            "total_tracks": {"inputFields": [{"namespace": NAMESPACE, "name": "raw_spotify_playlists", "field": "total_tracks"}]},
        }
    )
    
    # 6. stg_spotify_playlist_tracks (NEW)
    emit_openlineage_event(
        job_name="dbt.stg_spotify_playlist_tracks",
        inputs=[{
            "name": "raw_spotify_playlist_tracks",
            "fields": [
                {"name": "playlist_id", "type": "VARCHAR"},
                {"name": "track_id", "type": "VARCHAR"},
                {"name": "added_at", "type": "TIMESTAMP"},
                {"name": "position", "type": "INTEGER"},
            ]
        }],
        outputs=[{
            "name": "staging.stg_spotify_playlist_tracks",
            "fields": [
                {"name": "playlist_id", "type": "VARCHAR"},
                {"name": "track_id", "type": "VARCHAR"},
                {"name": "added_at", "type": "TIMESTAMP"},
                {"name": "position", "type": "INTEGER"},
            ]
        }],
        column_lineage={
            "playlist_id": {"inputFields": [{"namespace": NAMESPACE, "name": "raw_spotify_playlist_tracks", "field": "playlist_id"}]},
            "track_id": {"inputFields": [{"namespace": NAMESPACE, "name": "raw_spotify_playlist_tracks", "field": "track_id"}]},
            "added_at": {"inputFields": [{"namespace": NAMESPACE, "name": "raw_spotify_playlist_tracks", "field": "added_at"}]},
            "position": {"inputFields": [{"namespace": NAMESPACE, "name": "raw_spotify_playlist_tracks", "field": "position"}]},
        }
    )
    
    # ========================================
    # MARTS - DIMENSION TABLES
    # ========================================
    
    # 7. dim_tracks (combines API + extended history)
    emit_openlineage_event(
        job_name="dbt.dim_tracks",
        inputs=[
            {"name": "staging.stg_spotify_tracks"},
            {"name": "staging.stg_spotify_extended_history"},
            {"name": "staging.stg_spotify_audio_features"}
        ],
        outputs=[{
            "name": "marts.dim_tracks",
            "fields": [
                {"name": "track_id", "type": "VARCHAR"},
                {"name": "track_name", "type": "VARCHAR"},
                {"name": "artist_id", "type": "VARCHAR"},
                {"name": "artist_name", "type": "VARCHAR"},
                {"name": "total_plays", "type": "INTEGER"},
                {"name": "danceability", "type": "DOUBLE"},
                {"name": "energy", "type": "DOUBLE"},
                {"name": "valence", "type": "DOUBLE"},
            ]
        }],
        column_lineage={
            "track_id": {"inputFields": [
                {"namespace": NAMESPACE, "name": "staging.stg_spotify_tracks", "field": "track_id"},
                {"namespace": NAMESPACE, "name": "staging.stg_spotify_extended_history", "field": "track_id"}
            ]},
            "track_name": {"inputFields": [
                {"namespace": NAMESPACE, "name": "staging.stg_spotify_tracks", "field": "track_name"},
                {"namespace": NAMESPACE, "name": "staging.stg_spotify_extended_history", "field": "track_name"}
            ]},
            "danceability": {"inputFields": [{"namespace": NAMESPACE, "name": "staging.stg_spotify_audio_features", "field": "danceability"}]},
            "energy": {"inputFields": [{"namespace": NAMESPACE, "name": "staging.stg_spotify_audio_features", "field": "energy"}]},
            "valence": {"inputFields": [{"namespace": NAMESPACE, "name": "staging.stg_spotify_audio_features", "field": "valence"}]},
        }
    )
    
    # 8. dim_artists (combines API + extended history)
    emit_openlineage_event(
        job_name="dbt.dim_artists",
        inputs=[
            {"name": "staging.stg_spotify_tracks"},
            {"name": "staging.stg_spotify_extended_history"},
            {"name": "staging.stg_spotify_artists"}
        ],
        outputs=[{
            "name": "marts.dim_artists",
            "fields": [
                {"name": "artist_id", "type": "VARCHAR"},
                {"name": "artist_name", "type": "VARCHAR"},
                {"name": "genres", "type": "VARCHAR"},
                {"name": "total_plays", "type": "INTEGER"},
                {"name": "total_listen_time_hours", "type": "DOUBLE"},
            ]
        }],
        column_lineage={
            "artist_id": {"inputFields": [
                {"namespace": NAMESPACE, "name": "staging.stg_spotify_artists", "field": "artist_id"},
                {"namespace": NAMESPACE, "name": "staging.stg_spotify_extended_history", "field": "artist_id"}
            ]},
            "artist_name": {"inputFields": [
                {"namespace": NAMESPACE, "name": "staging.stg_spotify_artists", "field": "artist_name"},
                {"namespace": NAMESPACE, "name": "staging.stg_spotify_extended_history", "field": "artist_name"}
            ]},
            "genres": {"inputFields": [{"namespace": NAMESPACE, "name": "staging.stg_spotify_artists", "field": "genres"}]},
        }
    )
    
    # 9. dim_albums (combines API + extended history)
    emit_openlineage_event(
        job_name="dbt.dim_albums",
        inputs=[
            {"name": "staging.stg_spotify_tracks"},
            {"name": "staging.stg_spotify_extended_history"}
        ],
        outputs=[{
            "name": "marts.dim_albums",
            "fields": [
                {"name": "album_id", "type": "VARCHAR"},
                {"name": "album_name", "type": "VARCHAR"},
                {"name": "artist_id", "type": "VARCHAR"},
                {"name": "total_plays", "type": "INTEGER"},
            ]
        }],
        column_lineage={
            "album_id": {"inputFields": [
                {"namespace": NAMESPACE, "name": "staging.stg_spotify_tracks", "field": "album_id"},
                {"namespace": NAMESPACE, "name": "staging.stg_spotify_extended_history", "field": "album_id"}
            ]},
            "album_name": {"inputFields": [
                {"namespace": NAMESPACE, "name": "staging.stg_spotify_tracks", "field": "album_name"},
                {"namespace": NAMESPACE, "name": "staging.stg_spotify_extended_history", "field": "album_name"}
            ]},
        }
    )
    
    # 10. dim_playlists (NEW)
    emit_openlineage_event(
        job_name="dbt.dim_playlists",
        inputs=[
            {"name": "staging.stg_spotify_playlists"},
            {"name": "staging.stg_spotify_playlist_tracks"}
        ],
        outputs=[{
            "name": "marts.dim_playlists",
            "fields": [
                {"name": "playlist_id", "type": "VARCHAR"},
                {"name": "playlist_name", "type": "VARCHAR"},
                {"name": "total_tracks", "type": "INTEGER"},
                {"name": "actual_track_count", "type": "INTEGER"},
            ]
        }],
        column_lineage={
            "playlist_id": {"inputFields": [{"namespace": NAMESPACE, "name": "staging.stg_spotify_playlists", "field": "playlist_id"}]},
            "playlist_name": {"inputFields": [{"namespace": NAMESPACE, "name": "staging.stg_spotify_playlists", "field": "playlist_name"}]},
            "total_tracks": {"inputFields": [{"namespace": NAMESPACE, "name": "staging.stg_spotify_playlists", "field": "total_tracks"}]},
            "actual_track_count": {"inputFields": [{"namespace": NAMESPACE, "name": "staging.stg_spotify_playlist_tracks", "field": "track_id"}]},
        }
    )
    
    # ========================================
    # MARTS - FACT TABLE
    # ========================================
    
    # 11. fct_listening_history (complete fact table with extended history + playlists)
    emit_openlineage_event(
        job_name="dbt.fct_listening_history",
        inputs=[
            {"name": "staging.stg_spotify_tracks"},
            {"name": "staging.stg_spotify_extended_history"},
            {"name": "staging.stg_spotify_audio_features"},
            {"name": "staging.stg_spotify_artists"},
            {"name": "staging.stg_spotify_playlist_tracks"},
            {"name": "staging.stg_spotify_playlists"}
        ],
        outputs=[{
            "name": "marts.fct_listening_history",
            "fields": [
                {"name": "play_id", "type": "INTEGER"},
                {"name": "played_at", "type": "TIMESTAMP"},
                {"name": "track_id", "type": "VARCHAR"},
                {"name": "track_name", "type": "VARCHAR"},
                {"name": "artist_id", "type": "VARCHAR"},
                {"name": "artist_name", "type": "VARCHAR"},
                {"name": "data_source", "type": "VARCHAR"},
                {"name": "platform", "type": "VARCHAR"},
                {"name": "conn_country", "type": "VARCHAR"},
                {"name": "playlists_containing_track", "type": "VARCHAR"},
                {"name": "shuffle", "type": "BOOLEAN"},
                {"name": "skipped", "type": "BOOLEAN"},
                {"name": "danceability", "type": "DOUBLE"},
                {"name": "energy", "type": "DOUBLE"},
                {"name": "valence", "type": "DOUBLE"},
            ]
        }],
        column_lineage={
            "played_at": {"inputFields": [
                {"namespace": NAMESPACE, "name": "staging.stg_spotify_tracks", "field": "played_at"},
                {"namespace": NAMESPACE, "name": "staging.stg_spotify_extended_history", "field": "played_at"}
            ]},
            "track_id": {"inputFields": [
                {"namespace": NAMESPACE, "name": "staging.stg_spotify_tracks", "field": "track_id"},
                {"namespace": NAMESPACE, "name": "staging.stg_spotify_extended_history", "field": "track_id"}
            ]},
            "platform": {"inputFields": [{"namespace": NAMESPACE, "name": "staging.stg_spotify_extended_history", "field": "platform"}]},
            "conn_country": {"inputFields": [{"namespace": NAMESPACE, "name": "staging.stg_spotify_extended_history", "field": "conn_country"}]},
            "shuffle": {"inputFields": [{"namespace": NAMESPACE, "name": "staging.stg_spotify_extended_history", "field": "shuffle"}]},
            "skipped": {"inputFields": [{"namespace": NAMESPACE, "name": "staging.stg_spotify_extended_history", "field": "skipped"}]},
            "playlists_containing_track": {"inputFields": [
                {"namespace": NAMESPACE, "name": "staging.stg_spotify_playlists", "field": "playlist_name"}
            ]},
            "danceability": {"inputFields": [{"namespace": NAMESPACE, "name": "staging.stg_spotify_audio_features", "field": "danceability"}]},
            "energy": {"inputFields": [{"namespace": NAMESPACE, "name": "staging.stg_spotify_audio_features", "field": "energy"}]},
            "valence": {"inputFields": [{"namespace": NAMESPACE, "name": "staging.stg_spotify_audio_features", "field": "valence"}]},
        }
    )
    
    # ========================================
    # ANALYTICS MODELS
    # ========================================
    
    # 12. playlist_analysis (NEW)
    emit_openlineage_event(
        job_name="dbt.playlist_analysis",
        inputs=[
            {"name": "marts.dim_playlists"},
            {"name": "marts.fct_listening_history"},
            {"name": "staging.stg_spotify_playlist_tracks"}
        ],
        outputs=[{
            "name": "analytics.playlist_analysis",
            "fields": [
                {"name": "playlist_id", "type": "VARCHAR"},
                {"name": "playlist_name", "type": "VARCHAR"},
                {"name": "played_tracks", "type": "INTEGER"},
                {"name": "total_plays", "type": "INTEGER"},
                {"name": "played_percentage", "type": "DOUBLE"},
                {"name": "avg_valence", "type": "DOUBLE"},
                {"name": "avg_energy", "type": "DOUBLE"},
                {"name": "playlist_mood", "type": "VARCHAR"},
            ]
        }],
        column_lineage={
            "playlist_id": {"inputFields": [{"namespace": NAMESPACE, "name": "marts.dim_playlists", "field": "playlist_id"}]},
            "playlist_name": {"inputFields": [{"namespace": NAMESPACE, "name": "marts.dim_playlists", "field": "playlist_name"}]},
            "played_tracks": {"inputFields": [
                {"namespace": NAMESPACE, "name": "marts.fct_listening_history", "field": "track_id"},
                {"namespace": NAMESPACE, "name": "staging.stg_spotify_playlist_tracks", "field": "track_id"}
            ]},
            "avg_valence": {"inputFields": [{"namespace": NAMESPACE, "name": "marts.fct_listening_history", "field": "valence"}]},
            "avg_energy": {"inputFields": [{"namespace": NAMESPACE, "name": "marts.fct_listening_history", "field": "energy"}]},
        }
    )
    
    # 13. top_tracks_daily
    emit_openlineage_event(
        job_name="dbt.top_tracks_daily",
        inputs=[{"name": "marts.fct_listening_history"}],
        outputs=[{"name": "analytics.top_tracks_daily"}],
        column_lineage={
            "track_id": {"inputFields": [{"namespace": NAMESPACE, "name": "marts.fct_listening_history", "field": "track_id"}]},
            "track_name": {"inputFields": [{"namespace": NAMESPACE, "name": "marts.fct_listening_history", "field": "track_name"}]},
            "artist_name": {"inputFields": [{"namespace": NAMESPACE, "name": "marts.fct_listening_history", "field": "artist_name"}]},
        }
    )
    
    # 14. top_artists_daily
    emit_openlineage_event(
        job_name="dbt.top_artists_daily",
        inputs=[{"name": "marts.fct_listening_history"}],
        outputs=[{"name": "analytics.top_artists_daily"}],
        column_lineage={
            "artist_id": {"inputFields": [{"namespace": NAMESPACE, "name": "marts.fct_listening_history", "field": "artist_id"}]},
            "artist_name": {"inputFields": [{"namespace": NAMESPACE, "name": "marts.fct_listening_history", "field": "artist_name"}]},
        }
    )
    
    # 15. listening_patterns_hourly
    emit_openlineage_event(
        job_name="dbt.listening_patterns_hourly",
        inputs=[{"name": "marts.fct_listening_history"}],
        outputs=[{"name": "analytics.listening_patterns_hourly"}],
        column_lineage={
            "played_date": {"inputFields": [{"namespace": NAMESPACE, "name": "marts.fct_listening_history", "field": "played_date"}]},
            "played_hour": {"inputFields": [{"namespace": NAMESPACE, "name": "marts.fct_listening_history", "field": "played_hour"}]},
        }
    )
    
    # 16. audio_features_analysis
    emit_openlineage_event(
        job_name="dbt.audio_features_analysis",
        inputs=[{"name": "marts.fct_listening_history"}],
        outputs=[{"name": "analytics.audio_features_analysis"}],
        column_lineage={
            "danceability": {"inputFields": [{"namespace": NAMESPACE, "name": "marts.fct_listening_history", "field": "danceability"}]},
            "energy": {"inputFields": [{"namespace": NAMESPACE, "name": "marts.fct_listening_history", "field": "energy"}]},
            "valence": {"inputFields": [{"namespace": NAMESPACE, "name": "marts.fct_listening_history", "field": "valence"}]},
        }
    )
    
    print("\n✅ Successfully emitted lineage for 16 dbt models")
    print("   - 6 staging models (including extended history & playlists)")
    print("   - 5 mart models (dimensions + fact)")
    print("   - 5 analytics models (including playlist analysis)")
    print(f"\n🔍 View lineage at: http://localhost:3000\n")


if __name__ == "__main__":
    emit_dbt_lineage()

select distinct on (track_id)
    track_id,
    track_name,
    primary_artist_id,
    primary_artist_name,
    album_id,
    album_name,
    duration_ms
from {{ ref('stg_spotify_events') }}
order by track_id, played_at desc

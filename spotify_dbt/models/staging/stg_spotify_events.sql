-- Clean, typed view over raw events.
select
    event_id,
    event_timestamp                       as played_at,
    track_id,
    track_name,
    artist_names,
    artist_ids,
    artist_ids ->> 0                      as primary_artist_id,
    artist_names ->> 0                    as primary_artist_name,
    album_id,
    album_name,
    duration_ms,
    progress_ms,
    is_playing
from {{ source('raw', 'spotify_events') }}
where track_id is not null
  and event_timestamp is not null

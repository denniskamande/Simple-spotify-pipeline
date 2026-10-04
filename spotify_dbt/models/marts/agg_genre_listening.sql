select
    genre_group,
    count(*)                              as tracks_played,
    count(distinct primary_artist_id)     as unique_artists,
    round(sum(listened_ms) / 60000.0, 1)  as minutes_listened
from {{ ref('fct_listening_events') }}
group by genre_group

-- One row per play. listened_ms = time until the next play started,
-- capped at the track length (so long pauses don't inflate listening time).
-- For the most recent play it falls back to the remaining track time.
-- genre_group comes from the primary artist.
with events as (
    select
        *,
        lead(played_at) over (order by played_at) as next_played_at
    from {{ ref('stg_spotify_events') }}
)

select
    e.event_id,
    e.played_at,
    e.played_at at time zone '{{ var("local_timezone") }}'                          as played_at_local,
    (e.played_at at time zone '{{ var("local_timezone") }}')::date                  as play_date,
    extract(hour from e.played_at at time zone '{{ var("local_timezone") }}')::int  as play_hour,
    extract(isodow from e.played_at at time zone '{{ var("local_timezone") }}')::int as iso_day_of_week,
    e.track_id,
    e.track_name,
    e.primary_artist_id,
    e.primary_artist_name,
    coalesce(g.genre_group, 'Unknown')                                              as genre_group,
    e.album_id,
    e.album_name,
    e.duration_ms,
    greatest(
        0,
        least(
            e.duration_ms,
            coalesce(
                (extract(epoch from (e.next_played_at - e.played_at)) * 1000)::bigint,
                e.duration_ms - coalesce(e.progress_ms, 0)
            )
        )
    )::bigint                                                                       as listened_ms
from events e
left join {{ ref('stg_artist_genres') }} g
  on g.artist_id = e.primary_artist_id

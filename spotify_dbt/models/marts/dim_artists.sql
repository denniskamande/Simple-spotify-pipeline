select
    a.artist_id,
    a.artist_name,
    coalesce(g.genre_group, 'Unknown') as genre_group,
    g.top_genre,
    g.genres_list
from (
    select artist_id, max(artist_name) as artist_name
    from {{ ref('fct_event_artists') }}
    group by artist_id
) a
left join {{ ref('stg_artist_genres') }} g
  on g.artist_id = a.artist_id

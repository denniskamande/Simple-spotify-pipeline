-- One row per artist with genre tags and a broad genre_group.
-- Group = the artist's top-voted tag that matches seeds/genre_map.csv
-- (priority only breaks ties within a single tag).
-- seeds/artist_genre_overrides.csv wins over everything.
with g as (
    select artist_id, artist_name, genres
    from {{ source('raw', 'artist_genres') }}
),

tags as (
    select g.artist_id, t.tag, t.pos
    from g, jsonb_array_elements_text(g.genres) with ordinality as t(tag, pos)
),

matched as (
    select
        tags.artist_id,
        m.genre_group,
        row_number() over (
            partition by tags.artist_id
            order by tags.pos, m.priority, m.keyword
        ) as rn
    from tags
    join {{ ref('genre_map') }} m
      on tags.tag like '%' || m.keyword || '%'
)

select
    g.artist_id,
    g.artist_name,
    g.genres ->> 0 as top_genre,
    (select string_agg(x, ', ') from jsonb_array_elements_text(g.genres) as x) as genres_list,
    coalesce(
        o.genre_group,
        matched.genre_group,
        case when jsonb_array_length(g.genres) = 0 then 'Unknown' else 'Other' end
    ) as genre_group
from g
left join matched
  on matched.artist_id = g.artist_id and matched.rn = 1
left join {{ ref('artist_genre_overrides') }} o
  on lower(o.artist_name) = lower(g.artist_name)

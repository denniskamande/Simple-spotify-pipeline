-- Bridge table: one row per (play, credited artist). Use for collaborations.
select
    e.event_id,
    i.artist_id,
    n.artist_name,
    (i.pos = 1) as is_primary
from {{ ref('stg_spotify_events') }} e,
     lateral jsonb_array_elements_text(e.artist_ids)   with ordinality as i(artist_id, pos),
     lateral jsonb_array_elements_text(e.artist_names) with ordinality as n(artist_name, pos2)
where i.pos = n.pos2

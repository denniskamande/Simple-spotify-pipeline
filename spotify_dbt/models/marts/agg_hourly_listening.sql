-- Feeds the day-of-week x hour heatmap.
select
    iso_day_of_week,
    case iso_day_of_week
        when 1 then 'Mon' when 2 then 'Tue' when 3 then 'Wed' when 4 then 'Thu'
        when 5 then 'Fri' when 6 then 'Sat' when 7 then 'Sun'
    end                                       as day_name,
    play_hour,
    count(*)                                  as tracks_played,
    round(sum(listened_ms) / 60000.0, 1)      as minutes_listened
from {{ ref('fct_listening_events') }}
group by iso_day_of_week, play_hour

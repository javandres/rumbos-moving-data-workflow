{{ config(
    materialized = 'table'
) }}

SELECT
    staypoint_id,
    DATE,
    "group",
    track_id,
    user_id,
    ciudad,
    COUNT(*) AS positionfixes_count
FROM
    {{ source(
        'public',
        '_positionfixes'
    ) }}
GROUP BY
    staypoint_id,
    DATE,
    "group",
    track_id,
    user_id,
    ciudad

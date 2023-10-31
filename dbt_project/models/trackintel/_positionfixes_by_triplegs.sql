{{ config(
    materialized = 'table'
) }}

SELECT
    tripleg_id,
    DATE,
    "group",
    track_id,
    user_id,
    COUNT(*) AS positionfixes_count
FROM
    {{ source(
        'public',
        '_positionfixes'
    ) }}
GROUP BY
    tripleg_id,
    DATE,
    "group",
    track_id,
    user_id

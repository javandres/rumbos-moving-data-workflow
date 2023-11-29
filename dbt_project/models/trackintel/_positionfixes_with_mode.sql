{{ config(
    materialized = 'table'
) }}

SELECT
    p.*,
    tl.mode
FROM
    {{ source(
        'public',
        '_positionfixes'
    ) }}
    p,
    {{ ref("_triplegs_length") }}
    tl
WHERE
    tripleg_id IS NOT NULL
    AND tl.id = p.tripleg_id
    AND tl.track_id = p.track_id
UNION
SELECT
    p.*,
    (
        CASE
            WHEN (
                sp.prev_tripleg_mode = 'slow_mobility'
                AND next_tripleg_mode = 'slow_mobility'
            ) THEN 'slow_mobility'
            ELSE next_tripleg_mode
        END
    ) AS MODE
FROM
    {{ source(
        'public',
        '_positionfixes'
    ) }}
    p,
    {{ ref("_staypoints_with_triplegs") }}
    sp
WHERE
    staypoint_id IS NOT NULL
    AND sp.id = p.staypoint_id
    AND sp.track_id = p.track_id

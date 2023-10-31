{{ config(
    materialized = 'table',
    pre_hook = "set lc_time = 'es_EC.utf8'",
) }}

SELECT
    id,
    tl.user_id AS codigo_num,
    codigo,
    TYPE,
    MODE,
    started_at,
    finished_at,
    to_char(
        started_at,
        'TMDay'
    ) AS day_of_week,
    geom AS geometry,
    st_length(st_transform(geom, 32717)) AS recorrido_metros,
    duration_seconds,
    pbt.date,
    pbt.group,
    pbt.track_id
FROM
    {{ source(
        'public',
        '_triplegs'
    ) }}
    tl
    LEFT JOIN {{ ref("_positionfixes_by_triplegs") }}
    pbt
    ON pbt.tripleg_id = tl.id
    AND pbt.user_id = tl.user_id

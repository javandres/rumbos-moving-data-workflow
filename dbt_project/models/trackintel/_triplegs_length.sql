{{ config(
    materialized = 'table',
    pre_hook = "set lc_time = 'es_EC.utf8'",
) }}

SELECT
    user_id AS codigo_num,
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
    duration_seconds
FROM
    {{ source(
        'public',
        '_triplegs'
    ) }}

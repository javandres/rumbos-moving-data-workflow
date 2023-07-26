{{ config(
    materialized = 'table',
    pre_hook = "set lc_time = 'es_EC.utf8'",
) }}

SELECT
    track_id,
    codigo,
    TYPE,
    TO_DATE(
        DATE,
        'YYYYMMDD'
    ) AS DATE,
    to_char(TO_DATE(DATE, 'YYYYMMDD'), 'TMDay') AS day_of_week,
    st_makeline(
        geometry
        ORDER BY
            TIME
    ) AS geometry
FROM
    {{ source(
        'public',
        'trajectories'
    ) }}
GROUP BY
    track_id,
    codigo,
    TYPE,
    DATE

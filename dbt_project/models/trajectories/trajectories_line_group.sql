{{ config(
    materialized = 'table',
    pre_hook = "set lc_time = 'es_EC.utf8'",
) }}

WITH trajs AS (

    SELECT
        "group",
        codigo,
        TYPE,
        TO_DATE(
            DATE,
            'YYYYMMDD'
        ) AS DATE,
        ciudad,
        modalidad,
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
        "group",
        codigo,
        TYPE,
        DATE,
        ciudad,
        modalidad
)
SELECT
    *,
    st_length(st_transform(geometry, 32717)) AS recorrido_metros
FROM
    trajs

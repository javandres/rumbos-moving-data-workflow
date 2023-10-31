{{ config(
    materialized = 'table'
) }}

WITH sp AS (

    SELECT
        codigo,
        TYPE,
        "date",
        COUNT(codigo) AS COUNT,
        MIN(duration_seconds) AS MIN,
        MAX(duration_seconds) AS MAX,
        AVG(duration_seconds) AS AVG,
        SUM(duration_seconds) AS SUM
    FROM
        {{ ref(
            '_staypoints_actividad'
        ) }}
    GROUP BY
        codigo,
        TYPE,
        "date"
    ORDER BY
        codigo,
        TYPE,
        "date"
)
SELECT
    sp_persona.codigo,
    sp_persona.date,
    sp_persona.count AS staypoints_persona_count,
    sp_persona.min AS staypoints_persona_duration_min,
    sp_persona.max AS staypoints_persona_duration_max,
    sp_persona.avg AS staypoints_persona_duration_avg,
    sp_persona.sum AS staypoints_persona_duration,
    sp_artefacto.count AS staypoints_artefacto_count,
    sp_artefacto.min AS staypoints_artefacto_duration_min,
    sp_artefacto.max AS staypoints_artefacto_duration_max,
    sp_artefacto.avg AS staypoints_artefacto_duration_avg,
    sp_artefacto.sum AS staypoints_artefacto_duration
FROM
    sp sp_persona
    LEFT JOIN sp sp_artefacto
    ON sp_persona.codigo = sp_artefacto.codigo
    AND sp_persona.date = sp_artefacto.date
    AND sp_artefacto.type = 'artefacto'
WHERE
    sp_persona.type = 'persona'

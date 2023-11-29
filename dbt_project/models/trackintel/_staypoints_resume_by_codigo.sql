{{ config(
    materialized = 'table'
) }}

SELECT
    codigo,
    ciudad,
    COUNT(DATE) AS tracks,
    SUM(staypoints_persona_count) AS staypoints_persona_count,
    SUM(staypoints_persona_duration) AS staypoints_persona_duration,
    MIN(staypoints_persona_duration) AS staypoints_persona_duration_min,
    MAX(staypoints_persona_duration) AS staypoints_persona_duration_max,
    AVG(staypoints_persona_duration) AS staypoints_persona_duration_avg,
    SUM(staypoints_artefacto_count) AS staypoints_artefacto_count,
    SUM(staypoints_artefacto_duration) AS staypoints_artefacto_duration,
    MIN(staypoints_artefacto_duration) AS staypoints_artefacto_duration_min,
    MAX(staypoints_artefacto_duration) AS staypoints_artefacto_duration_max,
    AVG(staypoints_artefacto_duration) AS staypoints_artefacto_duration_avg
FROM
    {{ ref('_staypoints_resume_by_date') }}
GROUP BY
    codigo,
    ciudad
ORDER BY
    codigo

{{ config(
    materialized = 'table'
) }}

SELECT
    *
FROM
    (
        SELECT
            codigo,
            ciudad,
            COUNT(DATE) AS tracks,
            SUM(recorrido_metros_persona) AS recorrido_metros_persona_sum,
            SUM(recorrido_metros_persona) / COUNT(DATE) AS recorrido_metros_persona_avg,
            SUM(duration_seconds_persona) AS duration_seconds_persona_sum,
            SUM(duration_seconds_persona) / COUNT(DATE) AS duration_seconds_persona_avg,
            SUM(recorrido_metros_artefacto) AS recorrido_metros_artefacto_sum,
            SUM(recorrido_metros_artefacto) / COUNT(DATE) AS recorrido_metros_artefacto_avg,
            SUM(duration_seconds_artefacto) AS duration_seconds_artefacto_sum,
            SUM(duration_seconds_artefacto) / COUNT(DATE) AS duration_seconds_artefacto_avg,
            SUM(recorrido_metros_persona_slow_mobility) AS recorrido_metros_persona_slow_mobility_sum,
            SUM(recorrido_metros_persona_slow_mobility) / COUNT(DATE) AS recorrido_metros_persona_slow_mobility_avg,
            SUM(duration_seconds_persona_slow_mobility) AS duration_seconds_persona_slow_mobility_sum,
            SUM(duration_seconds_persona_slow_mobility) / COUNT(DATE) AS duration_seconds_persona_slow_mobility_avg,
            SUM(recorrido_metros_persona_motorized_mobility) AS recorrido_metros_persona_motorized_mobility_sum,
            SUM(recorrido_metros_persona_motorized_mobility) / COUNT(DATE) AS recorrido_metros_persona_motorized_mobility_avg,
            SUM(duration_seconds_persona_motorized_mobility) AS duration_seconds_persona_motorized_mobility_sum,
            SUM(duration_seconds_persona_motorized_mobility) / COUNT(DATE) AS duration_seconds_persona_motorized_mobility_avg,
            SUM(recorrido_metros_artefacto_slow_mobility) AS recorrido_metros_artefacto_slow_mobility_sum,
            SUM(recorrido_metros_artefacto_slow_mobility) / COUNT(DATE) AS recorrido_metros_artefacto_slow_mobility_avg,
            SUM(duration_seconds_artefacto_slow_mobility) AS duration_seconds_artefacto_slow_mobility_sum,
            SUM(duration_seconds_artefacto_slow_mobility) / COUNT(DATE) AS duration_seconds_artefacto_slow_mobility_avg,
            SUM(
                recorrido_metros_artefacto_motorized_mobility
            ) / COUNT(DATE) AS recorrido_metros_artefacto_motorized_mobility_sum,
            SUM(
                recorrido_metros_artefacto_motorized_mobility
            ) / COUNT(DATE) AS recorrido_metros_artefacto_motorized_mobility_avg,
            SUM(duration_seconds_artefacto_motorized_mobility) AS duration_seconds_artefacto_motorized_mobility_sum,
            SUM(duration_seconds_artefacto_motorized_mobility) / COUNT(DATE) AS duration_seconds_artefacto_motorized_mobility_avg,
            SUM(recorrido_metros_persona_line) AS recorrido_metros_persona_line_sum,
            SUM(recorrido_metros_persona_line) / COUNT(DATE) AS recorrido_metros_persona_line_avg,
            SUM(recorrido_metros_artefacto_line) AS recorrido_metros_artefacto_line_sum,
            SUM(recorrido_metros_artefacto_line) / COUNT(DATE) AS recorrido_metros_artefacto_line_avg
        FROM
            {{ ref('_triplegs_resume_by_date') }}
        GROUP BY
            codigo,
            ciudad
    ) A
ORDER BY
    codigo

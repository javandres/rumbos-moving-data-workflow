{{ config(
    materialized = 'table'
) }}

SELECT
    COUNT(*) AS COUNT,
    SUM(recorrido_metros_persona_sum) AS recorrido_metros_persona_sum,
    SUM(recorrido_metros_persona_avg) / COUNT(*) AS recorrido_metros_persona_avg,
    SUM(duration_seconds_persona_sum) AS duration_seconds_persona_sum,
    SUM(duration_seconds_persona_avg) / COUNT(*) AS duration_seconds_persona_avg,
    SUM(recorrido_metros_artefacto_sum) AS recorrido_metros_artefacto_sum,
    SUM(recorrido_metros_artefacto_avg) / COUNT(*) AS recorrido_metros_artefacto_avg,
    SUM(duration_seconds_artefacto_sum) AS duration_seconds_artefacto_sum,
    SUM(duration_seconds_artefacto_avg) / COUNT(*) AS duration_seconds_artefacto_avg,
    SUM(recorrido_metros_persona_slow_mobility_sum) AS recorrido_metros_persona_slow_mobility_sum,
    SUM(recorrido_metros_persona_slow_mobility_avg) / COUNT(*) AS recorrido_metros_persona_slow_mobility_avg,
    SUM(duration_seconds_persona_slow_mobility_sum) AS duration_seconds_persona_slow_mobility_sum,
    SUM(duration_seconds_persona_slow_mobility_avg) / COUNT(*) AS duration_seconds_persona_slow_mobility_avg,
    SUM(recorrido_metros_persona_motorized_mobility_sum) AS recorrido_metros_persona_motorized_mobility_sum,
    SUM(recorrido_metros_persona_motorized_mobility_avg) / COUNT(*) AS recorrido_metros_persona_motorized_mobility_avg,
    SUM(duration_seconds_persona_motorized_mobility_sum) AS duration_seconds_persona_motorized_mobility_sum,
    SUM(duration_seconds_persona_motorized_mobility_avg) / COUNT(*) AS duration_seconds_persona_motorized_mobility_avg,
    SUM(recorrido_metros_artefacto_slow_mobility_sum) AS recorrido_metros_artefacto_slow_mobility_sum,
    SUM(recorrido_metros_artefacto_slow_mobility_avg) / COUNT(*) AS recorrido_metros_artefacto_slow_mobility_avg,
    SUM(duration_seconds_artefacto_slow_mobility_sum) AS duration_seconds_artefacto_slow_mobility_sum,
    SUM(duration_seconds_artefacto_slow_mobility_avg) / COUNT(*) AS duration_seconds_artefacto_slow_mobility_avg,
    SUM(recorrido_metros_artefacto_motorized_mobility_sum) AS recorrido_metros_artefacto_motorized_mobility_sum,
    SUM(recorrido_metros_artefacto_motorized_mobility_avg) / COUNT(*) AS recorrido_metros_artefacto_motorized_mobility_avg,
    SUM(duration_seconds_artefacto_motorized_mobility_sum) AS duration_seconds_artefacto_motorized_mobility_sum,
    SUM(duration_seconds_artefacto_motorized_mobility_avg) / COUNT(*) AS duration_seconds_artefacto_motorized_mobility_avg,
    SUM(recorrido_metros_persona_line_sum) AS recorrido_metros_persona_line_sum,
    SUM(recorrido_metros_persona_line_avg) / COUNT(*) AS recorrido_metros_persona_line_avg,
    SUM(recorrido_metros_artefacto_line_sum) AS recorrido_metros_artefacto_line_sum,
    SUM(recorrido_metros_artefacto_line_avg) / COUNT(*) AS recorrido_metros_artefacto_line_avg
FROM
    {{ ref('_triplegs_resume_by_codigo') }}

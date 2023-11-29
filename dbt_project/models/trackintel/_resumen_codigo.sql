{{ config(
    materialized = 'table'
) }}

SELECT
    sp.codigo,
    sp.ciudad,
    sp.tracks,
    sp.staypoints_persona_count,
    sp.staypoints_persona_duration,
    sp.staypoints_persona_duration_min,
    sp.staypoints_persona_duration_max,
    sp.staypoints_persona_duration_avg,
    sp.staypoints_artefacto_count,
    sp.staypoints_artefacto_duration,
    sp.staypoints_artefacto_duration_min,
    sp.staypoints_artefacto_duration_max,
    sp.staypoints_artefacto_duration_avg,
    tl.recorrido_metros_persona_sum AS mov_recorrido_metros_persona_sum,
    tl.recorrido_metros_persona_avg AS mov_recorrido_metros_persona_avg,
    tl.duration_seconds_persona_sum AS mov_duration_seconds_persona_sum,
    tl.duration_seconds_persona_avg AS mov_duration_seconds_persona_avg,
    tl.recorrido_metros_artefacto_sum AS mov_recorrido_metros_artefacto_sum,
    tl.recorrido_metros_artefacto_avg AS mov_recorrido_metros_artefacto_avg,
    tl.duration_seconds_artefacto_sum AS mov_duration_seconds_artefacto_sum,
    tl.duration_seconds_artefacto_avg AS mov_duration_seconds_artefacto_avg,
    tl.recorrido_metros_persona_slow_mobility_sum AS mov_recorrido_metros_persona_slow_mobility_sum,
    tl.recorrido_metros_persona_slow_mobility_avg AS mov_recorrido_metros_persona_slow_mobility_avg,
    tl.duration_seconds_persona_slow_mobility_sum AS mov_duration_seconds_persona_slow_mobility_sum,
    tl.duration_seconds_persona_slow_mobility_avg AS mov_duration_seconds_persona_slow_mobility_avg,
    tl.recorrido_metros_persona_motorized_mobility_sum AS mov_recorrido_metros_persona_motorized_mobility_sum,
    tl.recorrido_metros_persona_motorized_mobility_avg AS mov_recorrido_metros_persona_motorized_mobility_avg,
    tl.duration_seconds_persona_motorized_mobility_sum AS mov_duration_seconds_persona_motorized_mobility_sum,
    tl.duration_seconds_persona_motorized_mobility_avg AS mov_duration_seconds_persona_motorized_mobility_avg,
    tl.recorrido_metros_artefacto_slow_mobility_sum AS mov_recorrido_metros_artefacto_slow_mobility_sum,
    tl.recorrido_metros_artefacto_slow_mobility_avg AS mov_recorrido_metros_artefacto_slow_mobility_avg,
    tl.duration_seconds_artefacto_slow_mobility_sum AS mov_duration_seconds_artefacto_slow_mobility_sum,
    tl.duration_seconds_artefacto_slow_mobility_avg AS mov_duration_seconds_artefacto_slow_mobility_avg,
    tl.recorrido_metros_artefacto_motorized_mobility_sum AS mov_recorrido_metros_artefacto_motorized_mobility_sum,
    tl.recorrido_metros_artefacto_motorized_mobility_avg AS mov_recorrido_metros_artefacto_motorized_mobility_avg,
    tl.duration_seconds_artefacto_motorized_mobility_sum AS mov_duration_seconds_artefacto_motorized_mobility_sum,
    tl.duration_seconds_artefacto_motorized_mobility_avg AS mov_duration_seconds_artefacto_motorized_mobility_avg,
    tl.recorrido_metros_persona_line_sum AS recorrido_metros_persona_line_sum,
    tl.recorrido_metros_persona_line_avg AS recorrido_metros_persona_line_avg,
    tl.recorrido_metros_artefacto_line_sum AS recorrido_metros_artefacto_line_sum,
    tl.recorrido_metros_artefacto_line_avg AS recorrido_metros_artefacto_line_avg
FROM
    {{ ref('_staypoints_resume_by_codigo') }}
    sp
    LEFT JOIN {{ ref('_triplegs_resume_by_codigo') }}
    tl
    ON sp.codigo = tl.codigo

{{ config(
    materialized = 'table'
) }}

WITH triplegs AS (

    SELECT
        codigo,
        TYPE,
        SUM(recorrido_metros) AS recorrido_metros,
        SUM(duration_seconds) AS duration_seconds
    FROM
        {{ ref(
            '_triplegs_length'
        ) }}
    GROUP BY
        codigo,
        TYPE
),
triplegs_mode AS (
    SELECT
        codigo,
        TYPE,
        MODE,
        SUM(recorrido_metros) AS recorrido_metros,
        SUM(duration_seconds) AS duration_seconds
    FROM
        {{ ref(
            '_triplegs_length'
        ) }}
    GROUP BY
        codigo,
        TYPE,
        MODE
),
resume AS (
    SELECT
        tplg_persona.codigo AS codigo,
        tplg_persona.recorrido_metros AS recorrido_metros_persona,
        tplg_persona.duration_seconds AS duration_seconds_persona,
        tplg_artefacto.recorrido_metros AS recorrido_metros_artefacto,
        tplg_artefacto.duration_seconds AS duration_seconds_artefacto,
        triplegs_mode_persona.recorrido_metros AS recorrido_metros_persona_slow_mobility,
        triplegs_mode_persona.duration_seconds AS duration_seconds_persona_slow_mobility,
        triplegs_mode_artefacto.recorrido_metros AS recorrido_metros_artefacto_motorized_mobility,
        triplegs_mode_artefacto.duration_seconds AS duration_seconds_aretefacto_motorized_mobility
    FROM
        triplegs tplg_persona
        LEFT JOIN triplegs tplg_artefacto
        ON tplg_persona.codigo = tplg_artefacto.codigo
        AND tplg_artefacto.type = 'artefacto'
        LEFT JOIN triplegs_mode triplegs_mode_persona
        ON tplg_persona.codigo = triplegs_mode_persona.codigo
        AND triplegs_mode_persona.type = 'persona'
        AND triplegs_mode_persona.mode = 'slow_mobility'
        LEFT JOIN triplegs_mode triplegs_mode_artefacto
        ON tplg_persona.codigo = triplegs_mode_artefacto.codigo
        AND triplegs_mode_artefacto.type = 'artefacto'
        AND triplegs_mode_artefacto.mode = 'motorized_mobility'
    WHERE
        tplg_persona.type = 'persona'
    ORDER BY
        tplg_persona.codigo
)
SELECT
    tr.*,
    tlr.recorrido_metros_persona AS recorrido_metros_persona_line,
    tlr.recorrido_metros_artefacto AS recorrido_metros_artefacto_line
FROM
    resume tr
    LEFT JOIN {{ ref('trajectories_line_resume') }}
    tlr
    ON tr.codigo = tlr.codigo

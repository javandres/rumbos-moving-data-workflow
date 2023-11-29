{{ config(
    materialized = 'table'
) }}

WITH triplegs AS (

    SELECT
        codigo,
        TYPE,
        DATE,
        SUM(recorrido_metros) AS recorrido_metros,
        SUM(duration_seconds) AS duration_seconds
    FROM
        {{ ref(
            '_triplegs_length'
        ) }}
    GROUP BY
        codigo,
        TYPE,
        DATE
),
triplegs_mode AS (
    SELECT
        codigo,
        TYPE,
        DATE,
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
        DATE,
        MODE
),
resume AS (
    SELECT
        tplg_persona.codigo AS codigo,
        tplg_persona.date AS DATE,
        tplg_persona.recorrido_metros AS recorrido_metros_persona,
        tplg_persona.duration_seconds AS duration_seconds_persona,
        tplg_artefacto.recorrido_metros AS recorrido_metros_artefacto,
        tplg_artefacto.duration_seconds AS duration_seconds_artefacto,
        triplegs_mode_persona_slow.recorrido_metros AS recorrido_metros_persona_slow_mobility,
        triplegs_mode_persona_slow.duration_seconds AS duration_seconds_persona_slow_mobility,
        triplegs_mode_persona_motor.recorrido_metros AS recorrido_metros_persona_motorized_mobility,
        triplegs_mode_persona_motor.duration_seconds AS duration_seconds_persona_motorized_mobility,
        triplegs_mode_artefacto_slow.recorrido_metros AS recorrido_metros_artefacto_slow_mobility,
        triplegs_mode_artefacto_slow.duration_seconds AS duration_seconds_artefacto_slow_mobility,
        triplegs_mode_artefacto_motor.recorrido_metros AS recorrido_metros_artefacto_motorized_mobility,
        triplegs_mode_artefacto_motor.duration_seconds AS duration_seconds_artefacto_motorized_mobility
    FROM
        triplegs tplg_persona
        LEFT JOIN triplegs tplg_artefacto
        ON tplg_persona.codigo = tplg_artefacto.codigo
        AND tplg_persona.date = tplg_artefacto.date
        AND tplg_artefacto.type = 'artefacto'
        LEFT JOIN triplegs_mode triplegs_mode_persona_slow
        ON tplg_persona.codigo = triplegs_mode_persona_slow.codigo
        AND tplg_persona.date = triplegs_mode_persona_slow.date
        AND triplegs_mode_persona_slow.type = 'persona'
        AND triplegs_mode_persona_slow.mode = 'slow_mobility'
        LEFT JOIN triplegs_mode triplegs_mode_persona_motor
        ON tplg_persona.codigo = triplegs_mode_persona_motor.codigo
        AND tplg_persona.date = triplegs_mode_persona_motor.date
        AND triplegs_mode_persona_motor.type = 'persona'
        AND (
            triplegs_mode_persona_motor.mode = 'motorized_mobility'
            OR triplegs_mode_persona_motor.mode = 'fast_mobility'
        )
        LEFT JOIN triplegs_mode triplegs_mode_artefacto_slow
        ON tplg_persona.codigo = triplegs_mode_artefacto_slow.codigo
        AND tplg_persona.date = triplegs_mode_artefacto_slow.date
        AND triplegs_mode_artefacto_slow.type = 'artefacto'
        AND triplegs_mode_artefacto_slow.mode = 'slow_mobility'
        LEFT JOIN triplegs_mode triplegs_mode_artefacto_motor
        ON tplg_persona.codigo = triplegs_mode_artefacto_motor.codigo
        AND tplg_persona.date = triplegs_mode_artefacto_motor.date
        AND triplegs_mode_artefacto_motor.type = 'artefacto'
        AND (
            triplegs_mode_artefacto_motor.mode = 'motorized_mobility'
            OR triplegs_mode_artefacto_motor.mode = 'fast_mobility'
        )
    WHERE
        tplg_persona.type = 'persona'
    ORDER BY
        tplg_persona.codigo
)
SELECT
    tr.*,
    tlr.ciudad,
    tlr.recorrido_metros_persona AS recorrido_metros_persona_line,
    tlr.recorrido_metros_artefacto AS recorrido_metros_artefacto_line
FROM
    resume tr
    LEFT JOIN {{ ref('trajectories_line_resume') }}
    tlr
    ON tr.codigo = tlr.codigo
    AND tr.date = tlr.date
ORDER BY
    tr.codigo,
    tr.date

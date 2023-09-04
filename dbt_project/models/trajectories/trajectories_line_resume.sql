{{ config(
    materialized = 'table',
) }}

WITH traj_line AS (

    SELECT
        codigo,
        TYPE,
        SUM(recorrido_metros) AS recorrido_metros
    FROM
        {{ ref('trajectories_line') }}
    GROUP BY
        codigo,
        TYPE
)
SELECT
    traj_line_persona.codigo AS codigo,
    traj_line_persona.recorrido_metros AS recorrido_metros_persona,
    traj_line_artefacto.recorrido_metros AS recorrido_metros_artefacto
FROM
    traj_line traj_line_persona
    LEFT JOIN traj_line traj_line_artefacto
    ON traj_line_persona.codigo = traj_line_artefacto.codigo
    AND traj_line_artefacto.type = 'artefacto'
WHERE
    traj_line_persona.type = 'persona'

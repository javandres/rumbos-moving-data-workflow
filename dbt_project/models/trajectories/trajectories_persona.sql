{{ config(
    materialized = 'table',
) }}

SELECT
    *
FROM
    {{ source(
        'public',
        'trajectories'
    ) }}
WHERE
    TYPE = 'persona'

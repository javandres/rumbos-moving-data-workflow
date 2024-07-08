{{ config(
    materialized = 'table',
    post_hook = 'create index if not exists "{{ this.name }}__index_on_ID" on {{ this }} ("id")'
) }}

SELECT
    *
FROM
    {{ ref(
        '_staypoints_with_triplegs'
    ) }}
    sp
WHERE
   next_tripleg_mode <> 'motorized_mobility'

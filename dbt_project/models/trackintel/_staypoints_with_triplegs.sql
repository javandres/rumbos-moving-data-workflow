{{ config(
    materialized = 'table'
) }}

SELECT
    sp.*,
    tripleg_prev.*,
    tripleg_next.*,
    pfs.date,
    pfs.group,
    pfs.track_id,
    pfs.positionfixes_count
FROM
    {{ source(
        'public',
        '_staypoints'
    ) }}
    sp
    INNER JOIN LATERAL (
        SELECT
            tl.id AS prev_tripleg_id,
            tl.mode AS prev_tripleg_mode
        FROM
            {{ ref(
                '_triplegs_length'
            ) }}
            tl
        WHERE
            tl.codigo = sp.codigo
            AND tl.type = sp.type
            AND tl.finished_at <= sp.started_at
        ORDER BY
            tl.finished_at DESC
        LIMIT
            1
    ) tripleg_prev
    ON TRUE
    INNER JOIN LATERAL (
        SELECT
            tl.id AS next_tripleg_id,
            tl.mode AS next_tripleg_mode
        FROM
            {{ ref(
                '_triplegs_length'
            ) }}
            tl
        WHERE
            tl.codigo = sp.codigo
            AND tl.type = sp.type
            AND tl.started_at >= sp.finished_at
        ORDER BY
            tl.finished_at ASC
        LIMIT
            1
    ) tripleg_next
    ON TRUE
    LEFT JOIN {{ ref('_positionfixes_by_staypoints') }}
    pfs
    ON pfs.user_id = sp.user_id
    AND pfs.staypoint_id = sp.id

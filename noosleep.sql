BEGIN;
WITH candidate_events AS (
    SELECT 
        e.global_seq,
        e.id,
        e.stream_id AS stream_id_fk,
        s.stream_id,
        e.seq,
        e.type,
        e.causation_id,
        e.correlation_id,
        e.payload,
        e.created_at,
        pg_try_advisory_xact_lock(hashtext('test-group'), hashtext(s.id::text)) as lock_obtained
    FROM events e
    JOIN streams s ON e.stream_id = s.id
    LEFT JOIN offsets o ON o.stream_id = e.stream_id 
        AND o.group_id = 'test-group'
    WHERE e.global_seq > COALESCE(o.global_seq, 0)
    ORDER BY e.global_seq
),
next_event AS (
    SELECT *
    FROM candidate_events
    WHERE lock_obtained = true
    LIMIT 1
),
locked_offset AS (
    INSERT INTO offsets (stream_id, group_id, global_seq)
    SELECT 
        ne.stream_id_fk,
        'test-group',
        0
    FROM next_event ne
    ON CONFLICT (group_id, stream_id) DO UPDATE 
    SET global_seq = offsets.global_seq
    RETURNING id, stream_id, global_seq
)
SELECT 
    ne.*,
    lo.id AS offset_id
FROM next_event ne
JOIN locked_offset lo ON ne.stream_id_fk = lo.stream_id;
COMMIT;

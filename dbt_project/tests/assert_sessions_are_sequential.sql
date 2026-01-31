-- Test to ensure session numbers are sequential with no gaps
-- Sessions should be numbered 1, 2, 3, ... with no missing numbers
WITH session_gaps AS (
    SELECT
        session_number,
        LAG(session_number) OVER (ORDER BY session_number) AS prev_session,
        session_number - LAG(session_number) OVER (ORDER BY session_number) AS gap
    FROM (
        SELECT DISTINCT session_number
        FROM {{ ref('fct_listening_history') }}
        WHERE session_number IS NOT NULL
    ) sessions
)
SELECT
    session_number,
    prev_session,
    gap
FROM session_gaps
WHERE gap > 1  -- Gap greater than 1 indicates missing session numbers

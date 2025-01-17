CREATE OR REPLACE VIEW view_mb_deadheads AS SELECT
    YMTH,
    SUBSTR(CAST(YMTH AS TEXT), 0, 5) AS Year,
    Service,
    ROUND(SUM("Pull-In/Out Distance (A)"), 2) AS "Deadhead Miles",
    ROUND(SUM("Pull-In/Out Hours (A)"), 2) AS "Deadhead Hours"
FROM CR0174
GROUP BY
    YMTH,
    Service
;
/*
 *
 */
SELECT
    --YMTH,
    SUM(CASE WHEN Label LIKE '% Ridership' THEN VALUE ELSE 0 END) AS UPT
    --SUM(CASE WHEN Label LIKE '% Ridership' THEN VALUE ELSE 0 END) AS VRM,
    --SUM(CASE WHEN Label LIKE '% Ridership' THEN VALUE ELSE 0 END) AS VRH,

FROM view_mb_stats
WHERE
    YMTH = :ymth
    AND Mode = :mode
;

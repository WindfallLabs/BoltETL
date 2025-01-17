/*
 * ...
 */
SELECT *
FROM view_mb_stats
WHERE YMTH = {ymth}
UNION
SELECT *
FROM view_dr_stats
WHERE YMTH = {ymth}
ORDER BY Seq
;

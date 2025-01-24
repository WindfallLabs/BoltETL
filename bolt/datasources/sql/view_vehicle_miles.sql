/*
 *
 */ 
CREATE OR REPLACE VIEW view_vehicle_miles AS
SELECT
    YMTH,
    Service,
    ROUND(SUM("Revenue Distance (A)"), 2) AS Rev,  -- I think this isn't normalized by number of weekdays, sats, and suns
    ROUND(SUM("Pull-In/Out Distance (A)"), 2) AS DH,
    ROUND(SUM("Revenue Distance (A)" + "Pull-In/Out Distance (A)"), 2) AS TVM
FROM cr0174
GROUP BY
    YMTH, Service
;
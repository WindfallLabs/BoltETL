/* Motor-Bus Stats for Monthly NTD Reporting */
CREATE OR REPLACE VIEW view_mb_stats AS
WITH service_types AS (
    SELECT 'Weekday' as service
    UNION ALL SELECT 'Saturday'
    UNION ALL SELECT 'Sunday'
),
combined_metrics AS (
    SELECT 
        s.service,
        r.YMTH,
        r."Ridership",
        r."Revenue Miles (Avg)",
        r."Revenue Hours",
        r."Passenger Miles",
        d."Deadhead Miles",
        d."Deadhead Hours"
    FROM service_types s
    LEFT JOIN NTDMonthly r
        ON r."Service" = s.service
    LEFT JOIN view_mb_deadheads d
        ON d."Service" = s.service
        AND d.YMTH = r.YMTH
)
-- Final selection
SELECT
    Seq,
    YMTH,
    CAST(SUBSTR(CAST(YMTH AS TEXT), 0, 5) AS INTEGER) AS Year,
    'MB' AS Mode,
    "Label",
    ROUND("Value", 2) AS Value
FROM (
    -- Ridership
    SELECT
        1 AS Seq,
        YMTH,
        service || ' Ridership' as "Label",
        "Ridership" as "Value"
    FROM combined_metrics
    
    UNION ALL
    
    -- Revenue Miles
    SELECT
        2 AS Seq,
        YMTH,
        service || ' Revenue Miles (Avg)' as "Label",
        "Revenue Miles (Avg)" as "Value"
    FROM combined_metrics
    
    UNION ALL
    
    -- Revenue Hours
    SELECT
        3 AS Seq,
        YMTH,
        service || ' Revenue Hours' as "Label",
        "Revenue Hours" as "Value"
    FROM combined_metrics
    
    UNION ALL
    
    -- Passenger Miles
    SELECT
        4 AS Seq,
        YMTH,
        service || ' Passenger Miles' as "Label",
        "Passenger Miles" as "Value"
    FROM combined_metrics
    
    UNION ALL

    -- Total Vehicle Miles
    SELECT
        5 AS Seq,
        YMTH,
        service || ' Total Vehicle Miles' as "Label",
        --"Total Vehicle Miles (DR)" as "Value"
        0.0 AS "Label"  -- TODO: WIP
    FROM ViaS10

    UNION ALL

    -- Total Vehicle Hours
    SELECT
        6 AS Seq,
        YMTH,
        service || ' Total Vehicle Hours' as "Label",
        --"Total Vehicle Hours (DR)" as "Value"
        0.0 AS "Label"  -- TODO: WIP
    FROM ViaS10

    UNION ALL
    
    -- Deadhead Miles
    SELECT
        7 AS Seq,
        YMTH,
        service || ' Deadhead Miles' as "Label",
        "Deadhead Miles" as "Value"
    FROM combined_metrics
    
    UNION ALL
    
    -- Deadhead Hours
    SELECT
        8 AS Seq,
        YMTH,
        service || ' Deadhead Hours' as "Label",
        "Deadhead Hours" as "Value"
    FROM combined_metrics
);

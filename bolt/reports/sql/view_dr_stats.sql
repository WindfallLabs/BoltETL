/* Demand-Response Stats for Monthly NTD Reporting */
CREATE OR REPLACE VIEW view_dr_stats AS
SELECT 
    YMTH,
    'DR' AS Mode,
    "Label",
    ROUND("Value", 2) AS Value
FROM (
    -- Ridership
    SELECT 
        YMTH,
        service || ' Ridership' as "Label",
        "Ridership (DR)" as "Value"
    FROM ViaS10

    UNION ALL

    -- Revenue Miles
    SELECT 
        YMTH,
        service || ' Revenue Miles' as "Label",
        "Vehicle Revenue Miles (DR)" as "Value"
    FROM ViaS10

    UNION ALL

    -- Revenue Hours
    SELECT 
        YMTH,
        service || ' Revenue Hours' as "Label",
        "Vehicle Revenue Hours (DR)" as "Value"
    FROM ViaS10

    UNION ALL

    -- Passenger Miles
    SELECT 
        YMTH,
        service || ' Passenger Miles' as "Label",
        "Passenger Miles (DR)" as "Value"
    FROM ViaS10

    UNION ALL

    -- Total Vehicle Miles
    SELECT 
        YMTH,
        service || ' Total Vehicle Miles' as "Label",
        "Total Vehicle Miles (DR)" as "Value"
    FROM ViaS10

    UNION ALL

    -- Total Vehicle Hours
    SELECT 
        YMTH,
        service || ' Total Vehicle Hours' as "Label",
        "Total Vehicle Hours (DR)" as "Value"
    FROM ViaS10

    UNION ALL

    -- Deadhead Miles
    SELECT 
        YMTH,
        service || ' Deadhead Miles' as "Label",
        "Deadhead Miles" as "Value"
    FROM ViaS10

    UNION ALL

    -- Deadhead Hours
    SELECT 
        YMTH,
        service || ' Deadhead Hours' as "Label",
        "Deadhead Hours" as "Value"
    FROM ViaS10
);

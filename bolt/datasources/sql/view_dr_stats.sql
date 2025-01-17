/* Demand-Response Stats for Monthly NTD Reporting */
CREATE OR REPLACE VIEW view_dr_stats AS
SELECT
    Seq,
    YMTH,
    CAST(SUBSTR(CAST(YMTH AS TEXT), 0, 5) AS INTEGER) AS Year,
    'DR' AS Mode,
    "Label",
    ROUND("Value", 2) AS Value
FROM (
    -- Ridership
    SELECT
        9 AS Seq,
        YMTH,
        service || ' Ridership' as "Label",
        "Ridership (DR)" as "Value"
    FROM ViaS10

    UNION ALL

    -- Revenue Miles
    SELECT
        10 AS Seq,
        YMTH,
        service || ' Revenue Miles' as "Label",
        "Vehicle Revenue Miles (DR)" as "Value"
    FROM ViaS10

    UNION ALL

    -- Revenue Hours
    SELECT
        11 AS Seq,
        YMTH,
        service || ' Revenue Hours' as "Label",
        "Vehicle Revenue Hours (DR)" as "Value"
    FROM ViaS10

    UNION ALL

    -- Passenger Miles
    SELECT
        12 AS Seq,
        YMTH,
        service || ' Passenger Miles' as "Label",
        "Passenger Miles (DR)" as "Value"
    FROM ViaS10

    UNION ALL

    -- Total Vehicle Miles
    SELECT
        13 AS Seq,
        YMTH,
        service || ' Total Vehicle Miles' as "Label",
        "Total Vehicle Miles (DR)" as "Value"
    FROM ViaS10

    UNION ALL

    -- Total Vehicle Hours
    SELECT
        14 AS Seq,
        YMTH,
        service || ' Total Vehicle Hours' as "Label",
        "Total Vehicle Hours (DR)" as "Value"
    FROM ViaS10

    UNION ALL

    -- Deadhead Miles
    SELECT
        15 AS Seq,
        YMTH,
        service || ' Deadhead Miles' as "Label",
        "Deadhead Miles" as "Value"
    FROM ViaS10

    UNION ALL

    -- Deadhead Hours
    SELECT
        16 AS Seq,
        YMTH,
        service || ' Deadhead Hours' as "Label",
        "Deadhead Hours" as "Value"
    FROM ViaS10
);

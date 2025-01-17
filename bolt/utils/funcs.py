"""Misc Functions"""

def fiscal_year(year_month: int|str) -> str:
    """Calculate a US Federal fiscal year from a YearMonth."""
    year = int(str(year_month)[:4])
    month = int(str(year_month)[4:])

    if month >= 7:
        return f"FY{str(year + 1)[2:4]}"
    else:
        return f"FY{str(year)[2:4]}"

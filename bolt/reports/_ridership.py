import pandas as pd


# FROM CLAUDE.ai (converted SQL to pandas)
# TODO: working....
def transform_ntd_data(df):
    """
    Transform NTD data from wide to long format, adding service type metrics.

    Parameters:
    df (pandas.DataFrame): Input DataFrame with columns as specified in the schema

    Returns:
    pandas.DataFrame: Transformed data in long format with separate Service column
    """
    # Create service types
    service_types = pd.DataFrame({"Service": ["Weekday", "Saturday", "Sunday"]})

    # Create a cross join between service types and unique YMTHs
    ymth_values = df["YMTH"].unique()
    combined_base = (
        service_types.assign(key=1)
        .merge(pd.DataFrame({"YMTH": ymth_values}).assign(key=1), on="key")
        .drop("key", axis=1)
    )

    # Merge with the main data
    combined_metrics = combined_base.merge(df, how="left", on=["Service", "YMTH"])

    # Create the long format transformations
    metrics = [
        ("Ridership", "Ridership"),
        ("Revenue Miles (Avg)", "Revenue Miles"),
        ("Revenue Hours", "Revenue Hours"),
        ("Passenger Miles", "Passenger Miles"),
        ("Deadhead Miles", "Deadhead Miles"),
        ("Deadhead Hours", "Deadhead Hours"),
    ]

    # Initialize an empty list to store DataFrames
    dfs = []

    # Create separate DataFrames for each metric
    for col_name, metric_name in metrics:
        if col_name in combined_metrics.columns:
            temp_df = combined_metrics[["YMTH", "Service", col_name]].copy()
            temp_df["Label"] = metric_name
            temp_df = temp_df.rename(columns={col_name: "Value"})
            dfs.append(temp_df[["YMTH", "Service", "Label", "Value"]])

    # Additional placeholder metrics (Total Vehicle Miles and Hours)
    for metric in ["Total Vehicle Miles", "Total Vehicle Hours"]:
        for service in service_types["Service"]:
            temp_df = pd.DataFrame(
                {"YMTH": ymth_values, "Service": service, "Label": metric, "Value": 0.0}
            )
            dfs.append(temp_df)

    # Combine all DataFrames
    result = pd.concat(dfs, ignore_index=True)

    # Add Mode column and sort
    result["Mode"] = "MB"
    result = result.sort_values(["YMTH", "Service", "Label"]).reset_index(drop=True)

    return result[["YMTH", "Mode", "Service", "Label", "Value"]]

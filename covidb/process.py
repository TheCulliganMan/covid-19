import pandas as pd
import numpy as np
import us
import datetime
from covidb.config import RAW_OUTPUT_CSV, ENHANCED_OUTPUT_CSV, get_todays_output_name


def is_state(row, column="Province/State"):
    if len(row[column].split(", ")) > 1:
        return False
    state = us.states.lookup(row[column])
    if state:
        return True
    return False

def get_county(row, column="Province/State"):
    split = row[column].split(", ")
    if len(split) == 2:
        county = split[0]
        return county
    return np.NaN

def get_state(row, column="Province/State"):
    if row["is_full_state"]:
        return us.states.lookup(row[column])
    split = row[column].split(", ")
    if len(split) == 2:
        state = us.states.lookup(split[1])
        if state:
            return state
    return np.NaN


def enhance_data(save_data_as_csv=True):
    df = pd.read_csv(RAW_OUTPUT_CSV)
    df = df.loc[df["Country/Region"] == "US"].reset_index(drop=True)
    df["is_full_state"] = df.apply(is_state, axis=1)
    df["county"] = df.apply(get_county, axis=1)
    df["state"] = df.apply(get_state, axis=1)
    df["Date"] = pd.to_datetime(df["Date"]).dt.date
    if save_data_as_csv:
        df.to_csv(ENHANCED_OUTPUT_CSV, index=False)
    return df

def get_todays_data(save_data_as_csv=True):
    df = enhance_data(save_data_as_csv=save_data_as_csv)
    today_df = df.loc[df["Date"] == df["Date"].max()].copy()
    day_previous = df.loc[df["Date"] == (df["Date"].max() - datetime.timedelta(days=1))]
    def get_previous_day(row):
        if row["is_full_state"] is True:
            yesterday = day_previous.loc[
                (day_previous["state"] == row["state"]) & \
                pd.isna(day_previous["county"])
            ]
        else:
            yesterday = day_previous.loc[
                (day_previous["county"] == row["county"]) & \
                (day_previous["state"] == day_previous["state"])
            ]
        if len(yesterday):
            return yesterday.iloc[0]["Confirmed"]
        return 0
    today_df["confirmed_yesterday"] = today_df.apply(get_previous_day, axis=1)
    today_df["confirmed_delta_today"] = today_df["Confirmed"] - today_df["confirmed_yesterday"]
    today_df = today_df.sort_values("confirmed_delta_today", ascending=False).reset_index(drop=True)
    if save_data_as_csv:
        todays_name = get_todays_output_name()
        today_df.to_csv(todays_name, index=False)
    return {"today": today_df, "all_data": df}


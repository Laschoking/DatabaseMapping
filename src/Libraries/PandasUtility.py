import pandas as pd

def add_series_to_df(series, df):
    if not df.empty:
        df = pd.concat([df, series.to_frame().T], ignore_index=True)
    else:
        df = pd.DataFrame(series).T
    return df


def is_series_in_df(series, df):
    return df.astype(str).eq(series.astype(str)).all(axis=1).any()
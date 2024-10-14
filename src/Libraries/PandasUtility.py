import pandas as pd

def add_series_to_df(series, df):
    if not df.empty:
        df = pd.concat([df, series.to_frame().T], ignore_index=True)
    else:
        df = pd.DataFrame(series).T
    return df


def is_series_in_df(series, df):
    #cols = df[series.index]
    # Reduce the columns of df to fit the series
    return df[series.index].astype(str).eq(series.astype(str)).all(axis=1).any()



def get_mapping_id(new_mapping, existing_mappings_df) -> (int, bool):
    """ Retrieve the mapping_func id from MappingSetup"""
    # If mapping_setup is in the DB already, use the existing Mapping_Identifier
    matches = existing_mappings_df[
        ['expansion', 'anchor_quantile', 'importance_weight', 'dynamic', 'metric','struct_ratio']].eq(new_mapping).all(axis=1)
    if matches.any():
        # The index which has the match is exactly the mapping_id we are looking for
        curr_mapping_id = existing_mappings_df.loc[matches.idxmax(), 'mapping_id']
        return curr_mapping_id, True

    else:
        # Add new entry for mapping_df
        curr_mapping_id = len(existing_mappings_df)
        return curr_mapping_id, False


def skip_current_computation(mapping_id,db_config_id, df,run_nr) -> list:
    if not run_nr:
        raise ValueError("no versions given")
    result_key_df = df[['mapping_id', 'db_config_id','run_nr']]
    current_keys = pd.Series({'mapping_id': mapping_id, 'db_config_id': db_config_id, "run_nr": 0})

    todo_runs = []
    # Iterate through all version
    for nr in run_nr:
        current_keys.at['run_nr'] = nr
        if not is_series_in_df(series=current_keys,df=result_key_df):
            todo_runs.append(nr)
    return todo_runs

def flatten_json(nested_json, exclude=['']):

    """Flatten json object with nested keys into a single level.
        Args:
            nested_json: A nested json object.
            exclude: Keys to exclude from output.
        Returns:
            The flattened json object if successful, None otherwise.
    """
    out = {}

    def flatten(x, name='', exclude=exclude):
        if type(x) is dict:
            for a in x:
                if a not in exclude: flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(nested_json)
    return out


def psql_insert_copy(table, conn, keys, data_iter):
    
    """
    Execute SQL statement inserting data to make loading quicker
    
    ----------Parameters----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str column names
    data_iter : Iterable that iterates the values to be inserted
    """

    from io import StringIO
    import csv

    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)


def load_to_db(df, table_name, engine):
    
    '''
    Create a function to load data from dataframes to database.

    ----------Parameters----------
    df : dataframe object
    table_name : name of database table
    engine : sqlalchemy.engine.base.Engine object with connection to database

    Returns:
    If no table exists: new data is loaded to a new table in the database with duplicate rows removed,
                        and primary key assigned to column index 0.
    If table exists: new data is added to existing table in the database.
    '''
    
    from sqlalchemy import inspect
    import pandas as pd

    # Drop duplicate rows
    df = df.drop_duplicates()

    # Assign DataFrame name
    df.name = table_name

    # Check if table exists in database
    inspector = inspect(engine)
    exists = inspector.has_table(f'{df.name}_store')
    
    
    #------------------- 1. If the table doesn't exist in database -------------------

    if exists == False:

        # Create table and load DataFrame to database
        df.to_sql(f'{df.name}_store', engine, index=False, method=psql_insert_copy)

        # Assign primary key
        engine.execute(f'ALTER TABLE {df.name}_store ADD PRIMARY KEY({df.columns[0]})')

        # Confirm execution
        print(f'{df.name}_store table created')
        print('New data loaded to ' + f'{df.name}_store' + ' table')

    
    #------------------- 2. If the table does exist in database -------------------

    elif exists == True:
        print(f'{df.name}_store' + ' table exists')
        
        
        ################### 2.1 Choose tables that require updating and not appending
        
        if df.name == 'competitions' or df.name == 'team_season' or df.name == 'player_season':
            
            # Load existing table from database
            db_table = pd.read_sql_query(f'select * from {df.name}_store', con=engine)
            
            # Append database dataframe with new dataframe and update values
            merged_df = pd.concat([db_table.set_index(f'{db_table.columns[0]}'),df.set_index(f'{df.columns[0]}')], axis=0, join='outer')
            
            # Drop duplicates
            updated_df = merged_df.reset_index().drop_duplicates(f'{db_table.columns[0]}', keep='last')
            
            # Load DataFrame to database
            updated_df.to_sql(f'{df.name}_store', engine, index=False, if_exists='replace', method=psql_insert_copy)
            
            # Assign primary key
            engine.execute(f'ALTER TABLE {df.name}_store ADD PRIMARY KEY({df.columns[0]})')

            print(f'{df.name}_store table data updated')
        
        ################### 2.2 Choose tables that require appending
        
        else:

            # Load existing table from database
            db_table = pd.read_sql_query(f'select * from {df.name}_store', con=engine)

            # Find unique rows between database table and new data
            unique_rows = df[~df.iloc[:, 0].isin(db_table.iloc[:, 0])]

            try:
                # Try to load unique rows. Note this will fail if there is a new column
                unique_rows.to_sql(f'{df.name}_store', engine, index=False, if_exists='append', method=psql_insert_copy)

                # Confirm execution
                print(f'New data appended to {df.name}_store')

            except:
                # Replace existing data database table with the old and new data
                print(f'{df.name}_store columns do not match')

                new_df = pd.concat([db_table, df])
                new_df.to_sql(f'{df.name}_store', engine, index=False, if_exists='replace', method=psql_insert_copy)

                # Confirm execution
                print(f'New data appended to {df.name}_store')

                # Remove variables
                del new_df

            # Remove variables
            del [unique_rows, db_table]

    print("-" * 40)
    
    

def team_match_calulations():
    
    team_match_dict = {
         'player_match_minutes': 'sum',
         'player_match_np_xg_per_shot': 'sum',
         'player_match_np_xg': 'sum',
         'player_match_np_shots': 'sum',
         'player_match_goals': 'sum',
         'player_match_np_goals': 'sum',
         'player_match_xa': 'sum',
         'player_match_key_passes': 'sum',
         'player_match_op_key_passes': 'sum',
         'player_match_assists': 'sum',
         'player_match_through_balls': 'sum',
         'player_match_passes_into_box': 'sum',
         'player_match_op_passes_into_box': 'sum',
         'player_match_touches_inside_box': 'sum',
         'player_match_tackles': 'sum',
         'player_match_interceptions': 'sum',
         'player_match_possession': 'mean',
         'player_match_dribbled_past': 'sum',
         'player_match_dribbles_faced': 'sum',
         'player_match_dribbles': 'sum',
         'player_match_challenge_ratio': 'mean',
         'player_match_fouls': 'sum',
         'player_match_dispossessions': 'sum',
         'player_match_long_balls': 'sum',
         'player_match_successful_long_balls': 'sum',
         'player_match_long_ball_ratio': 'mean',
         'player_match_shots_blocked': 'sum',
         'player_match_clearances': 'sum',
         'player_match_aerials': 'sum',
         'player_match_successful_aerials': 'sum',
         'player_match_aerial_ratio': 'mean',
         'player_match_passes': 'sum',
         'player_match_successful_passes': 'sum',
         'player_match_passing_ratio': 'mean',
         'player_match_op_passes': 'sum',
         'player_match_forward_passes': 'sum',
         'player_match_backward_passes': 'sum',
         'player_match_sideways_passes': 'sum',
         'player_match_op_f3_passes': 'sum',
         'player_match_op_f3_forward_passes': 'sum',
         'player_match_op_f3_backward_passes': 'sum',
         'player_match_op_f3_sideways_passes': 'sum',
         'player_match_np_shots_on_target': 'sum',
         'player_match_crosses': 'sum',
         'player_match_successful_crosses': 'sum',
         'player_match_crossing_ratio': 'mean',
         'player_match_penalties_won': 'sum',
         'player_match_passes_inside_box': 'sum',
         'player_match_op_xa': 'sum',
         'player_match_op_assists': 'sum',
         'player_match_pressured_long_balls': 'sum',
         'player_match_unpressured_long_balls': 'sum',
         'player_match_aggressive_actions': 'sum',
         'player_match_turnovers': 'sum',
         'player_match_crosses_into_box': 'sum',
         'player_match_sp_xa': 'sum',
         'player_match_op_shots': 'sum',
         'player_match_touches': 'sum',
         'player_match_pressure_regains': 'sum',
         'player_match_box_cross_ratio': 'mean',
         'player_match_deep_progressions': 'sum',
         'player_match_shot_touch_ratio': 'mean',
         'player_match_fouls_won': 'sum',
         'player_match_xgchain': 'sum',
         'player_match_op_xgchain': 'sum',
         'player_match_xgbuildup': 'sum',
         'player_match_op_xgbuildup': 'sum',
         'player_match_xgchain_per_possession': 'sum',
         'player_match_op_xgchain_per_possession': 'sum',
         'player_match_xgbuildup_per_possession': 'sum',
         'player_match_op_xgbuildup_per_possession': 'sum',
         'player_match_pressures': 'sum',
         'player_match_pressure_duration_total': 'sum',
         'player_match_pressure_duration_avg': 'mean',
         'player_match_pressured_action_fails': 'sum',
         'player_match_counterpressures': 'sum',
         'player_match_counterpressure_duration_total': 'sum',
         'player_match_counterpressure_duration_avg': 'mean',
         'player_match_counterpressured_action_fails': 'sum',
         'player_match_obv': 'sum',
         'player_match_obv_pass': 'sum',
         'player_match_obv_shot': 'sum',
         'player_match_obv_defensive_action': 'sum',
         'player_match_obv_dribble_carry': 'sum',
         'player_match_obv_gk': 'sum',
         'player_match_deep_completions': 'sum',
         'player_match_ball_recoveries': 'sum',
         'player_match_np_psxg': 'sum',
         'player_match_penalties_faced': 'sum',
         'player_match_penalties_conceded': 'sum',
         'player_match_fhalf_ball_recoveries': 'sum'
        }
    
    return team_match_dict
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
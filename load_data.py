import pandas as pd
import os
import numpy as np

def get_table(year):
    if int(year) < 2018:
        url = 'http://seshat.datasd.org/pd/vehicle_stops_{}_datasd_v1.csv'.format(year)
        df =  pd.read_csv(url)
        
    else: # year >= 2018
        url = 'http://seshat.datasd.org/pd/ripa_stops_datasd_v1.csv'
        df = pd.read_csv(url)
        
    return df
  

# helper functions for cleaning / merging
def c_bool(string):
    if (string == 'Y') | (string =='y'):
        return 1
    elif (string == 'N') | (string == 'n'):
        return 0
    else: 
        return np.nan

def make_bool(cols, df):
    for col in cols:
        if col not in list(df.columns):
            continue
        df[col] = df[col].apply(lambda x: c_bool(x))
    return df

def check_outcome(row):    
    to_check = ['arrest_made', 'property_seized']
    for check in to_check:
        if row.loc[check] == 1:
            return check
    return np.nan
    
def create_outcome(df):
    outcome = []
    for i in range(df.shape[0]):
        outcome.append(check_outcome(df.loc[i]))
    df['outcome'] = outcome
    return df

def add_stop_reason(df):
    url = 'http://seshat.datasd.org/pd/ripa_stop_reason_datasd.csv'
    reason = pd.read_csv(url)
    return df.merge(reason, on = 'stop_id').drop_duplicates()


def add_stop_outcome(df):
    url = 'http://seshat.datasd.org/pd/ripa_stop_result_datasd.csv'
    outcome = pd.read_csv(url)
    return df.merge(outcome, on = 'stop_id').drop_duplicates()

def add_data(df):
    df = add_stop_reason(df)
    df = add_stop_outcome(df)    
    return df


def clean_results(year, df):
    if int(year) < 2018:        
        # do something
        mapper = {'stop_id': 'stop_id', 'stop_cause': 'stop_cause', 'service_area': 'service_area', 
                  'subject_race': 'driver_race', 'subject_sex': 'driver_sex', 'subject_age': 'driver_age',
                  'date_stop': 'stop_date',  'time_stop': 'stop_time', 'sd_resident': 'sd_resident',
                  'arrested': 'arrest_made', 'searched': 'search_conducted', 'contraband_found': 'contraband_found',
                  'property_seized': 'property_seized'
                 }
        col_keep = list(mapper.keys())
        change_bool = ['sd_resident', 'search_conducted', 'contraband_found', 'property_seized', 'arrest_made']
        df = df[col_keep]
        df = df.rename(columns=mapper)
        df = make_bool(change_bool, df)
        df = create_outcome(df)
            
    else: # year >= 2018
        # do something else
        mapper = {'stop_id': 'stop_id', 'stop_cause': 'stop_cause', 'service_area': 'service_area', 
                  'subject_race': 'driver_race', 'gend': 'driver_sex', 'subject_age': 'driver_age',
                  'date_stop': 'stop_date',  'time_stop': 'stop_time', 'sd_resident': 'sd_resident',
                  'arrested': 'arrested', 'searched': 'searched', 'contraband_found': 'contraband_found',
                  'property_seized': 'property_seized', 'exp_years': 'exp_years', 'stopduration': 'stop_duration',
                  'address_city': 'address_city', 'beat': 'beat', 'pid': 'driver_id', 
                  'perceived_limited_english': 'perceived_limited_english'
                 }
        col_keep = list(mapper.keys())
        df = df.rename(columns=mapper)
        df = make_bool(change_bool, df)
        df = add_data(df)
    
    return df 



# The ingestion pipeline should take in the year (between 2014 and 2019) as a parameter
def load_data(years, outpath):
    """
    >>> load_data(["2015"], "data")
    >>> os.path.exists("data")
    >>> True
    >>> os.path.isfile("data/sdvehicle_stops_2015.csv")
    >>> True
    >>> df = pd.read_csv("data/sdvehicle_stops_2015.csv")
    >>> 'stop_id' in list(df.columns)
    >>> True
    >>> 'stop_cause' in list(df.columns)
    >>> type(df['sd_resident'][0]) == int
    >>> True
    """
    for year in years:
        if not os.path.exists(outpath):
            os.mkdir(outpath)

            # save df as csv
            path = '{}/sdvehicle_stops_{}.csv'.format(outpath, year)
            
            results = get_table(year)
            
            # do some cleaning
            clean_ = clean_results(year, results)    
            clean_.to_csv(path)   
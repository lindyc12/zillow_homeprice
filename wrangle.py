import pandas as pd
import os
from env import user, password, host

def get_db_url(database):
    return f'mysql+pymysql://{user}:{password}@{host}/{database}'


def get_zillow_data():

    filename = "zillow.csv"

    if os.path.isfile(filename):
        return pd.read_csv(filename)
    else:
        return get_new_zillow_data()

def get_new_zillow_data():

    sql = """
   select 
    bedroomcnt, bathroomcnt, calculatedfinishedsquarefeet, taxvaluedollarcnt, yearbuilt, taxamount, poolcnt
    from properties_2017
    join predictions_2017 as pred using (id)
    join propertylandusetype as prop using (propertylandusetypeid)
    where prop.propertylandusedesc = "Single Family Residential"
    and pred.transactiondate like "2017%%"
"""
    return pd.read_sql(sql, get_db_url("zillow"))




def optimize_types(df):
    # Convert to integers
    df["yearbuilt"] = df["yearbuilt"].astype(int)
    df["bedroomcnt"] = df["bedroomcnt"].astype(int)    
    df["taxvaluedollarcnt"] = df["taxvaluedollarcnt"].astype(int)
    df["calculatedfinishedsquarefeet"] = df["calculatedfinishedsquarefeet"].astype(int)
    return df



def handle_outliers(df):
    df = df[df.bathroomcnt <= 5]
    
    df = df[df.bedroomcnt <= 6]

    df = df[df.taxvaluedollarcnt < 2_000_000]

    df = df[df.calculatedfinishedsquarefeet < 5000]

    return df



def rename_cols(df): #rename columns
    df = df.rename(columns = {'bedroomcnt':'bedroom', 'bathroomcnt':'bathroom', 'calculatedfinishedsquarefeet':'SqFt', 'taxvaluedollarcnt':'home_value', 'yearbuilt':'year_built', 'taxamount':'tax_amount', 'poolcnt':'pool'})
    return df


def wrangle_zillow():
 
    df = get_zillow_data()

    #df = optimize_types(df)

    df = handle_outliers(df)

    df = rename_cols(df)

    return df

import pandas as pd
import sqlite3
from zipfile import ZipFile
import os
from urllib.request import urlopen

#%%
GEOGS = ["BlockGr", "CD", "County", "MCD", "Nation", "Place", "SLDL", "SLDU", "State", "Tract"]
DBNAME = "cvap.sqlite"

race_dict = {"Total": "total", 
             "Not Hispanic or Latino": "not_hispanic", 
             "American Indian or Alaska Native Alone": "aian", 
             "Asian Alone": "asian", 
             "Black or African American Alone": "black", 
             "Native Hawaiian or Other Pacific Islander Alone": "pacific", 
             "White Alone": "white",
             "American Indian or Alaska Native and White": "aian_white", 
             "Asian and White": "asian_white", 
             "Black or African American and White": "black_white",
             "American Indian or Alaska Native and Black or African American": "aian_black", 
             "Remainder of Two or More Race Responses": "two_or_more_remainder", 
             "Hispanic or Latino": "hispanic"}

#%%
def create_cvap_df(year, geog, download_dir = os.getcwd()):
    
    zip = ZipFile(os.path.join(download_dir, "CVAP_{1}-{0}_ACS_csv_files.zip".format(year, year - 4)))
    df = pd.read_csv(zip.open(geog + ".csv"), encoding = "LATIN1")
    
    # Lowercase all column names
    df.columns = map(str.lower, df.columns)    
    
    # Alter GEOID for compatibility with API and TIGER/Line joining
    df["geoid"] = df["geoid"].str.split("US").str[1]
    
    # Update LNTITLE with appropriate abbreviations
    df["lntitle"].replace(to_replace = race_dict, inplace = True)
 
    # Remove unused columns, rename columns
    df.drop(columns = ["lnnumber", "geoname"], inplace = True)
    dict_rename = {"tot_est": "", "tot_moe": "moe",
                   "adu_est": "vap", "adu_moe": "vap_moe", 
                   "cit_est": "citizen", "cit_moe": "citizen_moe",
                   "cvap_est": "cvap", "cvap_moe": "cvap_moe"}
    df.rename(columns = dict_rename, inplace = True)
    
    # Pivot, flatten multilevel column index
    df = df.pivot(index = "geoid", columns = "lntitle")
    df.columns = ["_".join(col[-1::-1]).strip("_") for col in df.columns.values]
    df.reset_index(inplace = True)   
    
    return df

#%%
def push_df_to_sqlite(df, year, geog, db = DBNAME):
   
    conn = sqlite3.connect(db)
    df.to_sql(geog.lower() + "_" + str(year), conn, if_exists = "replace", index = False)
    conn.close()

#%%
def get_cvap(year, data_dir = os.getcwd()):

    # Test year to make sure it is available
    available_years = list(range(2009, 2018))
    available_years.append(2000)

    if year not in available_years:
        # Message or raise error?
        return

    url = "http://www2.census.gov/programs-surveys/decennial/rdo/datasets/{0}/{0}-cvap/CVAP_{1}-{0}_ACS_csv_files.zip".format(year, year - 4)
    
    basename = url.split("/")[-1]
    
    file_data = urlopen(url)  
    data_to_write = file_data.read()
    with open(os.path.join(data_dir, basename), "wb") as f:  
        f.write(data_to_write)
    
#%%
tmp = create_cvap_df(2017, "County", "download")

push_df_to_sqlite(tmp, 2017, "County", os.path.join("data", DBNAME))


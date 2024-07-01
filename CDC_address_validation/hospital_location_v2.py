import pandas as pd
import sys
from geopy.distance import geodesic
# from tqdm import tqdm
import numpy as np
import os.path
sys.path.insert(0, r'C:\Users\owaghmare\Desktop\projects')
from sql_conn import call_db


def preProcess_newCDC(CDC_2024, lihi5_list):

    def parse_lat_long(row):
        # (lon, lat)
        if isinstance(row, str):
            row = row.replace('POINT',"").replace('(', "").replace(")", "").strip().split(" ")
            row.reverse()
            # (lat, lon)
            return row
        else:
            return []

    # converting from string to list (lat, lon)
    CDC_2024['geocoded_hospital_address'] = CDC_2024['geocoded_hospital_address'].apply(parse_lat_long)

    # splitting the (lat, lon) list column into two columns
    CDC_2024[['address_latitude','address_longitude']] = pd.DataFrame(CDC_2024.geocoded_hospital_address.tolist(), index= CDC_2024.index)
    CDC_2024.drop(columns = ["geocoded_hospital_address", "geohash"] , inplace = True)
    CDC_2024['data_year'] = 2024.0

    # correcting the datatypes:
    CDC_2024 = CDC_2024.astype({
        "address_latitude": float,
        "address_longitude": float
    })

    # replacing records with zip code 00000 and fips_code 00000 to null
        # df.where: Where cond is True, keep the original value. Where False, replace with corresponding value from other
    CDC_2024["zip"] = CDC_2024["zip"].where(CDC_2024["zip"] != "00000", np.nan) 
    CDC_2024["fips_code"] = CDC_2024["fips_code"].where(CDC_2024["fips_code"] != "00000", np.nan) 
    # fill missing ccn values
    CDC_2024['ccn'] = CDC_2024['ccn'].fillna(CDC_2024['hhs_id'].str.replace("C","").str.replace("-A", "").str.strip())
    CDC_2024 = CDC_2024.drop_duplicates()

    # select only records that are in LIHI5 list
    CDC_2024 = CDC_2024.loc[CDC_2024.ccn.isin(lihi5_list.prvdr_num), :]

    return CDC_2024



def find_distance(cor1, cor2):
    """finds distance between two coordinates (lat, long)

    Args:
        cor1 (set): new cdc coordinates (lat, long)
        cor2 (set): lihi 4 coordinates (lat, long)
    
    Returns:
        distance (float): distance between two coordinates
    """

    return geodesic(cor1, cor2).meters if not ((np.isnan(cor1).any()) or (np.isnan(cor2).any())) else np.nan


def compute_between_CDC_distance(CDC_2024, CDC_2023):
    print("computing distance for common hospitals:")
    common_records = CDC_2024.loc[CDC_2024.hhs_id.isin(CDC_2023.hhs_id), :]
    not_common_records = CDC_2024.loc[~CDC_2024.hhs_id.isin(CDC_2023.hhs_id),:]
    lst = list()
    for id_ in common_records.hhs_id.unique():
        cor1 = CDC_2024.loc[CDC_2024.hhs_id == id_, ["address_latitude", "address_longitude"]].values[0]
        cor2 = CDC_2023.loc[CDC_2023.hhs_id == id_, ["address_latitude", "address_longitude"]].values[0]
        distance = find_distance(cor1, cor2)
        lst.append([id_, distance])

    distance_df = pd.DataFrame(lst, columns = ["hhs_id","distance"])
    CDC_2024 = pd.merge(CDC_2024, distance_df, how = 'left')

    return CDC_2024


def identify_anomalous_distances(CDC_2024, CDC_2023, ahd_2022):
    """identify hospitals with more than 1000 distance, add 2023 CDC and 2022 ahd data to it

    Args:
        CDC_2024 (_type_): pandas dataframe
        CDC_2023 (_type_): pandas dataframe
        ahd_2022 (_type_): pandas dataframe

    Returns:
        _type_: pandas dataframe
    """
    CDC_processed = CDC_2024.loc[CDC_2024.distance > 1000, :]
    CDC_processed = CDC_processed.merge(CDC_2023, on= ['ccn', 'hhs_id'], how = "left", suffixes = ["_CDC_2024", "_CDC_2023"])
    CDC_processed = CDC_processed.merge(ahd_2022, left_on = "ccn" , right_on = "cms_certification_number", how = "left", suffixes = [None, "_ahd"])
    # CDC_processed.to_csv(r"lihi5-inclusivity-personal\CDC_address_validation\CDC_2024_anomalies.csv", index = False)

    return CDC_processed


def remove_duplicate_campuses(CDC_2024):
    lst = list()
    for ccn, new_group in CDC_2024.groupby(by = ['ccn']):
        if new_group.shape[0] > 1:
            # dist = pd.DataFrame(index = new_group.hhs_id.unique(), columns = new_group.hhs_id.unique())
            dist = []
            for i, campus_id_1 in enumerate(new_group.hhs_id.unique()):
                cor1 = new_group.loc[new_group.hhs_id == campus_id_1,  ["address_latitude", "address_longitude"]].values[0]
                for j, campus_id_2 in enumerate(new_group.hhs_id.unique()):
                    if i < j:
                        cor2 =  new_group.loc[new_group.hhs_id == campus_id_2,  ["address_latitude", "address_longitude"]].values[0]
                        d = find_distance(cor1, cor2)
                        if d == 0:
                            dist.append([ccn[0], campus_id_2, 1])
                    # dist.loc[campus_id_1, campus_id_2] = d

            if dist not in lst: 
                lst.extend(dist)
    df = pd.DataFrame(lst, columns = ['ccn', 'hhs_id', 'duplicate']).drop_duplicates()
    CDC_2024 = CDC_2024.merge(df, how = 'left', on = ['ccn', 'hhs_id'])
    CDC_2024 = CDC_2024.drop_duplicates()
    CDC_2024 = CDC_2024.loc[CDC_2024.duplicate != 1, :]
    return CDC_2024


def to_csv(df, path):
    # Prepend dtypes to the top of df (from path)
    df.loc[-1] = df.dtypes
    df.index = df.index + 1
    df.sort_index(inplace=True)
    # Then save it to a csv
    df.to_csv(path, index=False)


def read_csv(path):
    # Read types first line of csv
    dtypes = pd.read_csv(path, nrows=1).iloc[0].to_dict()
    # Read the rest of the lines with the types from above
    return pd.read_csv(path, dtype=dtypes, skiprows=[1])


def process_cleaned_data(anomalies, CDC_2024):
    anomalies = anomalies.loc[:, ['hhs_id','ccn', 'chosen_latitude', 'chosen_longitude']]
    CDC_2024 = CDC_2024.merge(anomalies, on=['ccn','hhs_id'], how = 'left')
    CDC_2024['chosen_longitude'] = CDC_2024['chosen_longitude'].fillna(CDC_2024['address_longitude'])
    CDC_2024['chosen_latitude'] = CDC_2024['chosen_latitude'].fillna(CDC_2024['address_latitude'])
    CDC_2024.drop(["address_longitude","address_latitude"],inplace=True,axis=1)
    CDC_2024 = CDC_2024.rename(columns = {'chosen_longitude': 'address_longitude',
                                          'chosen_latitude': 'address_latitude'})
    return CDC_2024


lihi5_list = call_db('lihi_website', 'gref__2022lihi5_genhosplist')
# ahd_2022 = call_db('overuse', 'dat__2022ahd')
CDC_2023 = call_db('downunder', 'ref__lihi4_hhs_id')
# CDC_2024_anomalies = pd.read_excel(r'lihi5-inclusivity-personal\CDC_address_validation\CDC_2024_anomalies.xlsx', converters={'ccn':str,'zip': '{:0>5}'.format, 'fips_code': '{:0>5}'.format})
# CDC_2024 = pd.read_csv(r'lihi5-inclusivity-personal\CDC_address_validation\HHS_IDs_20240124.csv', converters={'zip': '{:0>5}'.format, 'fips_code': '{:0>5}'.format})
# CDC_2024 = preProcess_newCDC(CDC_2024, lihi5_list)
# CDC_2024 = compute_between_CDC_distance(CDC_2024, CDC_2023)
# identify_anomalous_distances(CDC_2024, CDC_2023, ahd_2022)
# CDC_2024 = remove_duplicate_campuses(CDC_2024)
# to_csv(CDC_2024, r"lihi5-inclusivity-personal\CDC_address_validation\CDC_2024_v2.csv")

# remove_duplicate_campuses(CDC_2024)
# CDC_processed = identify_anomalous_distances(CDC_2024, CDC_2023, ahd_2022)
# 
# CDC_2024


CDC_2024_anomalies = pd.read_excel(r'lihi5-inclusivity-personal\CDC_address_validation\processed_data\CDC_2024_anomalies.xlsx', converters={'ccn':str,'zip': '{:0>5}'.format, 'fips_code': '{:0>5}'.format})
CDC_2024 = pd.read_excel(r'lihi5-inclusivity-personal\CDC_address_validation\processed_data\CDC_2024_v2.xlsx', converters={'ccn':str,'zip': '{:0>5}'.format, 'fips_code': '{:0>5}'.format})
CDC_2024 = process_cleaned_data(CDC_2024_anomalies, CDC_2024)
CDC_2024 = CDC_2024.loc[CDC_2024.ccn.isin(lihi5_list.prvdr_num.unique()),:]
CDC_2024.to_excel(r'lihi5-inclusivity-personal\CDC_address_validation\processed_data\CDC_2024_finalized.xlsx', index = False)
import pandas as pd
import sys
from geopy.distance import geodesic
# from tqdm import tqdm
import numpy as np
import os.path
sys.path.insert(0, r'C:\Users\owaghmare\Desktop\projects')
from sql_conn import call_db


def preProcess_newData(new_cdc):

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
    new_cdc['geocoded_hospital_address'] = new_cdc['geocoded_hospital_address'].apply(parse_lat_long)

    # splitting the (lat, lon) list column into two columns
    new_cdc[['address_latitude','address_longitude']] = pd.DataFrame(new_cdc.geocoded_hospital_address.tolist(), index= new_cdc.index)
    new_cdc.drop(columns = ["geocoded_hospital_address", "geohash"] , inplace = True)
    new_cdc['data_year'] = 2024.0

    # correcting the datatypes:
    new_cdc = new_cdc.astype({
        "address_latitude": float,
        "address_longitude": float
    })

    # replacing records with zip code 00000 and fips_code 00000 to null
        # df.where: Where cond is True, keep the original value. Where False, replace with corresponding value from other
    new_cdc["zip"] = new_cdc["zip"].where(new_cdc["zip"] != "00000", np.nan) 
    new_cdc["fips_code"] = new_cdc["fips_code"].where(new_cdc["fips_code"] != "00000", np.nan) 

    if new_cdc['ccn'].isna().sum() !=0:
        new_cdc.loc[new_cdc['ccn'].isna(),:].to_csv(r"lihi5-inclusivity-personal\CDC_address_validation\CDC_hospitals_with_null_ccn.csv", index = False)

    return new_cdc


def create_list(new_cdc, lihi5_list, CDC_2023):
    """This function aims to identify hospitals that are present in both, LIHI5 and new CDC data, and create a new dataframe from it.

    Args:
        new_cdc (dataframe): new CDC data
        lihi5_list (dataframe): LIHI 5 draft list
        CDC_2023 (dataframe): LIHI 4 HHS ID table, the table we want to recreate using the new CDC data and LIHI5 list

    Returns:
        CDC_2024 (dataframe): Records of hospitals from the latest CDC data that are present in LIHI 5 draft list
    """

    # result = pd.merge(lihi5_list, new_cdc, left_on='prvdr_num', right_on='ccn')
    # print("# unique CCN id's that are present in both CDC and LIHI 5 list: {}".format(result['prvdr_num'].nunique()))
    # print("# unique providers in the LIHI 5 draft list: {}".format(lihi5_list['prvdr_num'].nunique()))
    # print("# of missing hospitals form CDC data: {}".format(lihi5_list['prvdr_num'].nunique() - result['prvdr_num'].nunique()))

    # x = lihi5_list.loc[~lihi5_list['prvdr_num'].isin(result['prvdr_num']), : ]
    # x.to_csv(r"LIHI_Inclusivity\missing_hospitals_from_CDC.csv", index = False)
    # print("Information about hospitals that are missing from the CDC data but are present in the LIHI 5 list:\n{}".format(x))

    # # it is possible that these hospitals exist in the bew CDC data but have Null CCN id's, a quick check revealed that all of these hospitals are present in the null_ccn file

    # # lets check if these hospitals are present in the LIHI4 list:
    # print("# of missing hospitals from new CDC data: {}".format(CDC_2023.loc[CDC_2023.ccn.isin(x.prvdr_num), "hhs_id"].shape[0]))

    print("# of distinct CCN id's that are present in both, new CDC data and LIHI 5 data: {}".format(new_cdc.loc[new_cdc.ccn.isin(lihi5_list.prvdr_num), :].ccn.nunique()))
    print("# of distinct CCN id's in LIHI 5 data: {}".format(lihi5_list.prvdr_num.nunique()))
    print("# of missing CCN id's from the CDC data: {}".format(lihi5_list.prvdr_num.nunique() - new_cdc.loc[new_cdc.ccn.isin(lihi5_list.prvdr_num), :].ccn.nunique()))

    # it is possible that these hospitals exist in the new CDC data but have Null CCN id's, a quick check revealed that all of these hospitals are the new CDC data

    # lets identify which hospitals are missing from the CDC data:
        # step 1: get CCN id's for missing hospitals
        # step 2: filter new CDC data using these CCN id's
    
    a = lihi5_list.prvdr_num.unique()
    b = new_cdc.loc[new_cdc.ccn.isin(lihi5_list.prvdr_num), :].ccn.unique()
    missing_ccn = [x for x in a if x not in b]
    to_add = new_cdc.loc[new_cdc.hhs_id.str.contains("|".join(missing_ccn)), :]
    to_add = to_add.loc[to_add.ccn.isna(), :]

    # adding CCN values to these records
    to_add['ccn'] = to_add['ccn'].fillna(to_add['hhs_id'].str.replace("C","").str.replace("-A", "").str.strip())

    # now we have all hospitals that were mentioned in the LIHI 5 draft list. Creating a dataframe
    CDC_2024 = new_cdc.loc[new_cdc.ccn.isin(lihi5_list.prvdr_num), :]
    CDC_2024 = CDC_2024.append(to_add)

    print("After processing: # of distinct CCN id's in LIHI 5: {}".format(lihi5_list.prvdr_num.nunique()))
    print("After processing: # of distinct CCN id's in CDC_2024: {}".format(CDC_2024.ccn.nunique()))


    return CDC_2024

def find_distance(cor1, cor2):
    """finds distance between two coordinates (lat, long)

    Args:
        cor1 (set): new cdc coordinates (lat, long)
        cor2 (set): lihi 4 coordinates (lat, long)
    
    Returns:
        distance (float): distance between two coordinates
    """

    return geodesic(cor1, cor2).meters if not (np.isnan(cor1).any() and np.isnan(cor2).any()) else np.nan


def compare_campus():

    return


def compare_hhs():

    return


def compare(CDC_2024, CDC_2023):
    """This function first identifies hospitals that are present in both LIHI 5 and LIHI 4, then compares their addresses. (CDC with ref__lihi4_hhs_id)
    compare address steps: 
        1. HHS_ID validation: Difference in HHS-ID's between CDC_2024 and CDC_2023 data
            a. Hospitals with the same number of campuses (HHS-IDs) but the coordinates are different
            b. Hospitals with fewer campuses than last year
            c. Hospitals with more campuses than last year
        2. counts of hospitals where:
            a. There are new campuses in the CDC data
            b. There are missing campuses in the CDC data
            c. The campus addresses are different in the CDC data
        3. Compute distance between coordinated listed in CDC 2024 and CDC 2023 data

    Args:
        CDC_2024 (_type_): latest CDC data for LIHI 5 hospitals
        CDC_2023 (_type_): lihi 4 data

     Returns:
        CDC_2024 (dataframe): Added flag variable which indicates a change in address
    """
    
    # since we have a one to many mapping for CCN id, we will be using HHS_ID for this filtering
    common_records = CDC_2024.loc[CDC_2024.ccn.isin(CDC_2023.ccn), :]
    not_common_records = CDC_2024.loc[~CDC_2024.ccn.isin(CDC_2023.ccn),:]

    print("# of common hospitals in CDC_2024 and LIHI 5: {}".format(common_records['ccn'].nunique()))
    print("# of hospitals that are new in CDC_2024: {}".format(not_common_records.ccn.nunique()))

    print("validating address for common hospitals: ")

    lst = list()
    for id, new_group in common_records.groupby(by = ["ccn"]):
        old_group = CDC_2023.loc[CDC_2023.ccn == id, :]
        x = pd.merge(new_group, old_group, on = ['hhs_id'], how = 'outer', indicator = True)
        different_values = x[x['_merge'] != 'both']
        if different_values.shape[0] == 0:
            new_group['hhs_id_discrepancy'] = 0
            lst.append(new_group)
        else:
            # for both and left only
            x = new_group.loc[new_group.hhs_id.isin(different_values.loc[different_values._merge != "right_only", 'hhs_id'])]
            x = x.append(new_group.loc[new_group.ccn == id]).drop_duplicates()
            x['hhs_id_discrepancy'] = 0
            x.loc[x.hhs_id.isin(different_values.hhs_id), "hhs_id_discrepancy"] = 1


            # for right only
            y = CDC_2023.loc[CDC_2023.hhs_id.isin(different_values.loc[different_values._merge == "right_only", 'hhs_id'])]
            y['hhs_id_discrepancy'] = 0
            y.loc[y.hhs_id.isin(different_values.hhs_id),"hhs_id_discrepancy"] = 1
            
            x = pd.concat([x,y], ignore_index= True)
            lst.append(x)

    x = pd.concat(lst, ignore_index = True)
    not_common_records['hhs_id_discrepancy'] = 0
    CDC_2024 = pd.concat([x, not_common_records], ignore_index= True).drop_duplicates()
    CDC_2024.loc[CDC_2024.hhs_id_discrepancy == 0, 'data_'] = "both"
    CDC_2024.loc[(CDC_2024.hhs_id_discrepancy == 1) & (CDC_2024.data_year == 2024.0), 'data_'] = "new CDC"
    CDC_2024.loc[(CDC_2024.hhs_id_discrepancy == 1) & (CDC_2024.data_year != 2024.0), 'data_'] = "old CDC"

    # writing records that have a new campus or a campus shutdown: 247 CCN id's with a discrepency in hhs_id
    # CDC_2024.loc[CDC_2024.ccn.isin(CDC_2024.loc[CDC_2024.data_ != "both"].ccn)].to_csv(r"lihi5-inclusivity-personal\CDC_address_validation\change_in_hospital_campus.csv", index = False)

    

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

    # writing the file
    # CDC_2024.to_csv(r"lihi5-inclusivity-personal\CDC_address_validation\CDC_2024.csv", index = False)

    return

lihi5_list = call_db('lihi_website', 'gref__2022lihi5_genhosplist')
CDC_2023 = call_db('downunder', 'ref__lihi4_hhs_id')
new_cdc = pd.read_csv(r'lihi5-inclusivity-personal\CDC_address_validation\HHS_IDs_20240124.csv', converters={'zip': '{:0>5}'.format, 'fips_code': '{:0>5}'.format})
new_cdc = preProcess_newData(new_cdc)
CDC_2024 = create_list(new_cdc, lihi5_list, CDC_2023)
CDC_2024 = compare(CDC_2024, CDC_2023)

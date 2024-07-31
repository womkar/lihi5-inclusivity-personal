import pandas as pd
import sys
sys.path.insert(0, r'C:\Users\owaghmare\Desktop\projects')
from sql_conn import call_table



lihi5_list = call_table('lihi_website', 'gref__2022lihi5_genhosplist')
CDC_2024_anomalies = pd.read_excel(r'lihi5-inclusivity-personal\CDC_address_validation\CDC_2024_anomalies.xlsx', converters={'ccn':str,'zip': '{:0>5}'.format, 'fips_code': '{:0>5}'.format})
CDC_2024_anomalies
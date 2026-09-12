
## Library

import concurrent.futures
import pandas as pd
from google.cloud import bigquery
from extract_from_netsuite import pull_data_by_sql
from decimal import Decimal

#### The code ####
def get_max_uniquekey():
    df = pull_data_by_sql(query="SELECT count(distinct uniquekey) as total_row FROM transactionline", return_df=True)
    return int(df["total_row"].iloc[0])


)
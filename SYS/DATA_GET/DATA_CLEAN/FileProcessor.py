import os
from SYS.URI import raw_data
from SYS.URI import cln_data
from datetime import datetime


class FileProcessor:
    @staticmethod
    def get_dates(in_month, in_day):
        return "{}-{}-{}".format(2016, in_month, in_day), "{}-{}-{}".format(2016, in_month, in_day + 1)

    @staticmethod
    def in_parquet_uri(in_candidate, is_top):
        in_candidate = in_candidate.replace(" ", "-")
        parquet_folder = os.path.join(raw_data, "{}-{}".format(is_top, in_candidate))
        parquet_uri = os.path.join(raw_data, "{}-{}/*".format(is_top, in_candidate))
        if os.path.exists(parquet_folder) is False:
            print(parquet_folder)
            print("{} [System]: Does not exist {}-{}'s data.".format(datetime.now(), is_top, in_candidate))
            has_folder = False
        else:
            has_folder = True
        return parquet_uri, has_folder

    @staticmethod
    def out_parquet_uri(in_candidate, is_stop):
        parquet_file = os.path.join(cln_data, "{}-{}.parquet.gzip".format(is_stop, in_candidate.replace(" ", "-")))
        if os.path.exists(cln_data) is False:
            os.makedirs(cln_data, exist_ok=True)
        return parquet_file

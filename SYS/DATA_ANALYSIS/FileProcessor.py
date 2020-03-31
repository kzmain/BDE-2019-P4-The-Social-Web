import os
from SYS.URI import cln_data
from datetime import datetime


class FileProcessor:
    @staticmethod
    def get_dates(in_month, in_day):
        return "{}-{}-{}".format(2016, in_month, in_day), "{}-{}-{}".format(2016, in_month, in_day + 1)

    @staticmethod
    def in_parquet_uri(in_candidate, is_top):
        in_candidate = in_candidate.replace(" ", "-")
        parquet_folder = os.path.join(cln_data, "{}-{}.parquet.gzip".format(is_top, in_candidate))
        parquet_uri = os.path.join(parquet_folder, "*.parquet")
        if os.path.exists(parquet_folder) is False:
            print(parquet_folder)
            print("{} [System]: Does not exist {}-{}'s data.".format(datetime.now(), is_top, in_candidate))
            has_folder = False
        else:
            has_folder = True
        return parquet_uri, has_folder

    @staticmethod
    def out_parquet_uri(in_candidate, is_stop, out_dir):
        in_candidate = in_candidate.replace(" ", "-")
        parquet_file = os.path.join(out_dir, "{}-{}.parquet.gzip".format(is_stop, in_candidate))
        if os.path.exists(out_dir) is False:
            os.makedirs(out_dir, exist_ok=True)
        return parquet_file

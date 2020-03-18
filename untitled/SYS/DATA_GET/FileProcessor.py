import os
from SYS.URI import raw_data as d
from datetime import datetime


class FileProcessor:
    @staticmethod
    def get_dates(in_month, in_day):
        return "{}-{}-{}".format(2016, in_month, in_day), "{}-{}-{}".format(2016, in_month, in_day + 1)

    @staticmethod
    def get_file_dir_uri(in_date, in_candidate, is_top):
        in_candidate = in_candidate.replace(" ", "-")
        in_fd = os.path.join(d, "{}-{}".format(is_top, in_candidate))
        in_fn = os.path.join(d, "{}-{}/{}-{}.parquet.gzip".format(is_top, in_candidate, in_candidate, in_date))
        if os.path.exists(in_fd) is False:
            os.makedirs(in_fd, exist_ok=True)
        if os.path.exists(in_fn):
            print("{} [System]: Existed {}'s data. ({}-{})".format(datetime.now(), in_date, in_candidate, is_top))
            in_fe = True
        else:
            in_fe = False
        return in_fd, in_fn, in_fe

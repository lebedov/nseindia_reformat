#!/usr/bin/env python

"""
Parse file into several CSV files.

"""

import csv, os.path, tempfile, shutil
from datetime import datetime

def parse_orders_data(in_file_name, N_lines=100000, work_dir='./'):
    """

    Notes
    -----
    Assumes equity derivatives market orders data format.

    Saves generated CSV data in multiple files names containing a
    fixed number of rows.
    """

    # Number of seconds between the epoch and 1/1/1980:
    time_adjust = (datetime(1980, 1, 1, 0, 0, 0)-\
                   datetime(1970, 1, 1, 0, 0, 0)).total_seconds()

    with open(in_file_name, 'rb') as f_in:
        fd, name = tempfile.mkstemp(dir=work_dir)
        f = os.fdopen(fd, 'wb')
        w = csv.writer(f)
        last_order_number_date = ''
        count = 0
        for line in iter(f_in):
            count += 1
            record_indicator = line[0:2]
            segment = line[2:6]
            order_number = line[6:22]

            # Close and rename the output file when the order number
            # date changes:
            if (count % N_lines) == 0:
                f.close()
                shutil.move(name, os.path.join(work_dir, '%i.csv' % count))
                print '%i.csv' % count
                fd, name = tempfile.mkstemp(dir=work_dir)
                f = os.fdopen(fd, 'wb')
                w = csv.writer(f)

            trans_time = line[22:36]

            # Convert number of jiffies (1/65535 s = 1 jiffie) since
            # 1/1/1980 into a readable time/date format (UTC); note that we
            # need to adjust the number of seconds to offset from the
            # Unix epoch so as to enable usage of Python's time module:
            trans_time_sec = int(trans_time)/65535.0+time_adjust
            trans_time = \
              datetime.strftime(datetime.utcfromtimestamp(trans_time_sec), \
                                '%m/%d/%Y %H:%M:%S.%f')

            buy_sell_indicator = line[36:37]
            activity_type = line[37:38]
            symbol = line[38:48].strip('b')
            instrument = line[48:54]

            # Only extract data with instrument == FUTIDX or
            # FUTSTK:
            if instrument not in ['FUTIDX', 'FUTSTK']:
                continue
            expiry_date = \
              datetime.strftime(datetime.strptime(line[54:63], '%d%b%Y'),
                                '%m/%d/%Y')
            strike_price = int(line[63:71])
            option_type = line[71:73]
            volume_disclosed = int(line[73:81])
            volume_original = int(line[81:89])
            limit_price = '%.2f' % float(line[89:95]+'.'+line[95:97])
            trigger_price = '%.2f' % float(line[97:105]+'.'+line[103:105])
            mkt_flag = line[105:106]
            on_stop_flag = line[106:107]
            io_flag = line[107:108]
            spread_comb_type = line[108:109]
            algo_indicator = line[109:110]
            client_id_flag = line[110:111]

            row = [record_indicator,
                   segment,
                   order_number,
                   trans_time,
                   buy_sell_indicator,
                   activity_type,
                   symbol,
                   instrument,
                   expiry_date,
                   strike_price,
                   option_type,
                   volume_disclosed,
                   volume_original,
                   limit_price,
                   trigger_price,
                   mkt_flag,
                   on_stop_flag,
                   io_flag,
                   spread_comb_type,
                   algo_indicator,
                   client_id_flag]
            w.writerow(row)

        # Close and rename the last file:
        f.close()
        shutil.move(name, os.path.join(work_dir, '%i.csv' % count))
        print '%i.csv' % count

def parse_trades_data():
    pass

if __name__ == '__main__':
    parse_orders_data('/home/lev/india_maglaras/nse/fao/FAO_Orders_14092012.DAT',
                      100000, '/home/lev/india_maglaras/nse/fao/')

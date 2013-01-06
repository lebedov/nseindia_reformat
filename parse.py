#!/usr/bin/env python

"""
Parse India data files into multiple CSV files.
"""

import csv, os.path, tempfile, shelve, shutil
from datetime import datetime

def parse_orders_data(in_file_name, work_dir='./', N_lines=100000):
    """
    Parses input data file and saves generated CSV data in multiple files names,
    each of which contains all orders associated with a specific security.
    
    Parameters
    ----------
    in_file_name : str
        Input file name.
    work_dir : str
        Directory in which to save the CSV files.
    N_lines : int
        Number of lines at which to print a progress indicator.
        
    Notes
    -----
    Assumes equity derivatives market orders data format.
    Assumes that the directory `work_dir` exists.

    """

    # Number of seconds between the epoch and 1/1/1980:
    time_adjust = (datetime(1980, 1, 1, 0, 0, 0)-\
                   datetime(1970, 1, 1, 0, 0, 0)).total_seconds()

    with open(in_file_name, 'rb') as f_in:
        db = {}
        count = 0
        for line in iter(f_in):
            count += 1
            if (count % N_lines) == 0:
                print count
            
            record_indicator = line[0:2]
            segment = line[2:6]
            order_number = line[6:22]
            trans_date_time = line[22:36]

            # Convert number of jiffies (1/65536 s = 1 jiffie) since
            # 1/1/1980 into a readable time/date format (UTC); note that we
            # need to adjust the number of seconds to offset from the
            # Unix epoch so as to enable usage of Python's time module:
            trans_date_time_sec = int(trans_date_time)/65536.0+time_adjust

            # Split the transaction time into a date and time of day:
            trans_date = \
              datetime.strftime(datetime.utcfromtimestamp(trans_date_time_sec), \
                                '%m/%d/%Y')
            trans_time = \
              datetime.strftime(datetime.utcfromtimestamp(trans_date_time_sec), \
                                '%H:%M:%S.%f')
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
            algo_ind = line[109:110]
            client_id_flag = line[110:111]

            row = [record_indicator,
                   segment,
                   order_number,
                   trans_date,
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
                   algo_ind,
                   client_id_flag]
            if not db.has_key(symbol):
                f = open(os.path.join(work_dir, symbol + '-orders.csv'), 'wb')
                db[symbol] = {'handle': f, 
                              'writer': csv.writer(f)}
            db[symbol]['writer'].writerow(row)
            
    # Close files:
    for symbol in db.keys():
        db[symbol]['handle'].close()
    
def parse_trades_data(in_file_name, work_dir='./', N_lines=100000):
    """
    Parses input data file and saves generated CSV data in multiple files names,
    each of which contains all trades associated with a specific security.

    Parameters
    ----------
    in_file_name : str
        Input file name.
    work_dir : str
        Directory in which to save the CSV files.
    N_lines : int
        Number of lines at which to print a progress indicator.

    Notes
    -----
    Assumes equity derivatives market trades data format.
    Assumes that the directory `work_dir` exists.
    
    """

    # Number of seconds between the epoch and 1/1/1980:
    time_adjust = (datetime(1980, 1, 1, 0, 0, 0)-\
                   datetime(1970, 1, 1, 0, 0, 0)).total_seconds()

    with open(in_file_name, 'rb') as f_in:
        db = {}
        count = 0
        for line in iter(f_in):
            count += 1
            if (count % N_lines) == 0:
                print count

            record_indicator = line[0:2]
            segment = line[2:6]
            trade_number = line[6:22]
            trade_date_time = line[22:36]

            # Convert number of jiffies (1/65536 s = 1 jiffie) since
            # 1/1/1980 into a readable time/date format (UTC); note that we
            # need to adjust the number of seconds to offset from the
            # Unix epoch so as to enable usage of Python's time module:
            trade_date_time_sec = int(trade_date_time)/65536.0+time_adjust

            # Split the trade time into a date and time of day:
            trade_date = \
              datetime.strftime(datetime.utcfromtimestamp(trade_date_time_sec), \
                                '%m/%d/%Y')
            trade_time = \
              datetime.strftime(datetime.utcfromtimestamp(trade_date_time_sec), \
                                '%H:%M:%S.%f')
            symbol = line[36:46].strip('b')
            instrument = line[46:52]

            # Only extract data with instrument == FUTIDX or
            # FUTSTK:
            if instrument not in ['FUTIDX', 'FUTSTK']:
                continue
            expiry_date = \
              datetime.strftime(datetime.strptime(line[52:61], '%d%b%Y'),
                                '%m/%d/%Y')
            strike_price = '%.2f' % float(line[61:67]+'.'+line[67:69])
            option_type = line[69:71]
            trade_price = '%.2f' % float(line[71:77]+'.'+line[77:79])
            trade_quantity = int(line[79:87])
            buy_order_num = line[87:103]
            buy_algo_ind = line[103:104]
            buy_client_id_flag = line[104:105]
            sell_order_num = line[105:121]
            sell_algo_ind = line[121:122]
            sell_client_id_flag = line[122:123]

            row = [record_indicator,
                   segment,
                   trade_number,
                   trade_date,
                   trade_time,
                   symbol,
                   instrument,
                   expiry_date,
                   strike_price,
                   option_type,
                   trade_price,
                   trade_quantity,
                   buy_order_num,
                   buy_algo_ind,
                   buy_client_id_flag,
                   sell_order_num,
                   sell_algo_ind,
                   sell_client_id_flag]
            if not db.has_key(symbol):
                f = open(os.path.join(work_dir, symbol + '-trades.csv'), 'wb')
                db[symbol] = {'handle': f, 
                              'writer': csv.writer(f)}
            db[symbol]['writer'].writerow(row)

    # Close files:
    for symbol in db.keys():
        db[symbol]['handle'].close()
        
if __name__ == '__main__':

    # Assumes that all of the input data is in a single file:
    parse_orders_data('/home/lev/india_maglaras/nse/fao/FAO_Orders_14092012.DAT',
                      '/home/lev/india_maglaras/nse/fao/orders/')
    # parse_trades_data('/home/lev/india_maglaras/nse/fao/FAO_Trades_28092012.DAT',
    #                   '/home/lev/india_maglaras/nse/fao/trades/')

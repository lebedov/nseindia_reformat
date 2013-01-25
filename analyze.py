#!/usr/bin/env python

"""
Analyze parsed India trades data.
"""

import csv, datetime, sys
import numpy as np
import pandas

def rount_ceil_minute(d):
    """
    Round to nearest minute after the specified time.

    Parameters
    ----------
    d : datetime.datetime
       A time expressed using the `datetime.datetime` class.
       
    """
    
    return d-datetime.timedelta(seconds=d.second,
                                microseconds=d.microsecond,
                                minutes=(-1 if d.second != 0 or d.microsend != 0 else 0))

def sample(df, delta, date_time_col, *data_cols):
    """
    Sample data at a specific sampling time interval.

    Return a table of data containing at least two columns; one column contains times
    separated by the specified sampling interval, while the others contain the
    data points associated with the most recent times in the original table
    prior to each successive sampling time.
    
    Parameters
    ----------
    df : pandas.DataFrame
        Data to sample. Must contain columns with the names in `data_cols` and
        `date_time_col`.
    delta : datetime.timedelta
        Sampling interval.
    date_time_col : str
        Name of date/time.
    data_cols : tuple of str 
        Name(s) of data column.
        
    Returns
    -------
    out : pandas.DataFrame
        DataFrame containing column of resampled data points and sampling times.

    Notes 
    -----
    Sampling begins at 9:15 AM and ends at 3:30 PM.
    
    """
    
    result_dict = {date_time_col: []}        
    for col in data_cols:
        result_dict[col] = []

    # if len(df) == 0:
    #     return result_dict
    
    temp = df.irow(0)[date_time_col]    
    t_start = datetime.datetime(temp.year, temp.month, temp.day, 9, 15, 0, 0)
    t_end = datetime.datetime(temp.year, temp.month, temp.day, 15, 30, 0, 0)
    
    data_dict_last = {}
    for col in data_cols:        
        data_dict_last[col] = df.irow(0)[col]

    t = t_start
    i = 0
    while t <= t_end:

        # Save the sample time:
        result_dict[date_time_col].append(t)

        # Store the data associated with the first event as the sampled value
        # between 9:15 AM and the time of first event when the
        # latter occurs later than the former:                    
        if t >= df[date_time_col].min():

            # Only update the data point stored at each time point when 
            # a time point in the original series is passed:
            while i < len(df) and t >= df.irow(i)[date_time_col]:
                for col in data_cols:
                    data_dict_last[col] = df.irow(i)[col]
                i += 1  
            
        for col in data_cols:
            result_dict[col].append(data_dict_last[col])
        t += delta        
    return pandas.DataFrame(result_dict)
    
def analyze(file_name):
    """
    Analyze parsed India data in specified file name.

    Parameters
    ----------
    file_name : str
        Name of CSV file containing parsed India data.
        
    Returns
    -------
    output : list
        Results of analysis. These include the following (in order):

        number of trades with quantity below Q1
        number of trades with quantity below Q2
        number of trades with quantity below Q3
        maximum trade quantity
        minimum trade quantity
        mean trade quantity
        number of trades with interarrival times below Q1
        number of trades with interarrival times below Q2
        number of trades with interarrival times below Q3
        maximum daily trade volume
        minimum daily trade volume
        mean daily trade volume
        median daily trade volume
        mean trade price over entire month
        standard deviation of trade price sampled over 3 minutes in bps
        mean returns sampled over 3 minutes in bps
        standard deviation of returns sampled over 3 minutes in bps
        sum of absolute values of returns sampled over 3 minutes in bps        
        maximum daily price in bps (for each business day of 9/2012)
        minimum daily price in bps (for each business day of 9/2012)
        
    """

    df = pandas.read_csv(file_name, header=None, 
                         names=['record_indicator',
                                'segment',
                                'trade_number',
                                'trade_date',
                                'trade_time',
                                'symbol',
                                'instrument',
                                'expiry_date',
                                'strike_price',
                                'option_type',
                                'trade_price',
                                'trade_quantity',
                                'buy_order_num',
                                'buy_algo_ind',
                                'buy_client_id_flag',
                                'sell_order_num',
                                'sell_algo_ind',
                                'sell_client_id_flag'])    

    # Find quartiles of number of trades:
    trade_quant_q1 = df['trade_quantity'].quantile(0.25)
    trade_quant_q2 = df['trade_quantity'].quantile(0.50)
    trade_quant_q3 = df['trade_quantity'].quantile(0.75)

    # Trade quantity stats:
    max_trade_quant = df['trade_quantity'].max()
    min_trade_quant = df['trade_quantity'].min()
    mean_trade_quant = df['trade_quantity'].mean()

    # Convert trade date/times to datetime.timedelta and join the column to the
    # original data:
    s_trade_date_time = \
      df[['trade_date', 'trade_time']].apply(lambda x: \
        datetime.datetime.strptime(x[0] + ' ' + x[1], '%m/%d/%Y %H:%M:%S.%f'),
          axis=1)
    s_trade_date_time.name = 'trade_date_time'
    df = df.join(s_trade_date_time)
    
    # Compute trade interarrival times for each day (i.e., the interval between
    # the last trade on one day and the first day on the following day should
    # not be regarded as an interarrival time). Note that this returns the times
    # in nanoseconds:
    s_inter_time = df.groupby('trade_date')['trade_date_time'].apply(lambda x: x.diff())
    
    # Exclude the NaNs that result because of the application of the diff()
    # method to each group of trade times:
    s_inter_time = s_inter_time[s_inter_time.notnull()]

    if len(s_inter_time) > 0:
        
        # Convert interarrival times from nanoseconds to seconds:
        s_inter_time = s_inter_time.apply(lambda x: x*10**-9)  
    
        # Find interarrival time quartiles:    
        inter_time_q1 = s_inter_time.quantile(0.25)
        inter_time_q2 = s_inter_time.quantile(0.50)
        inter_time_q3 = s_inter_time.quantile(0.75)
    else:

        # If there are not enough trades per day to compute interarrival times,
        # set the number of times to 0 for each quantile:
        inter_time_q1 = 0
        inter_time_q2 = 0
        inter_time_q3 = 0
        
    # Compute the daily traded volume:
    s_daily_vol = \
      df.groupby('trade_date')['trade_quantity'].apply(sum)
    df_daily_vol = pandas.DataFrame({'trade_date': s_daily_vol.index,
                                     'trade_quantity': s_daily_vol.values})
    
    # Set the number of trades for days on which no trades were recorded to 0:
    sept_days = map(lambda d: '09/%02i/2012' % d, 
                    [3, 4, 5, 6, 7, 10, 11, 12, 13, 14, 17, 18, 19, 20, 21, 24,
                     25, 26, 27, 28])        
    df_daily_vol = \
      df_daily_vol.combine_first(pandas.DataFrame({'trade_date': sept_days, 
                                 'trade_quantity': np.zeros(len(sept_days))}))

    # Compute daily volume stats:
    max_daily_vol = df_daily_vol['trade_quantity'].max()
    min_daily_vol = df_daily_vol['trade_quantity'].min()
    mean_daily_vol = df_daily_vol['trade_quantity'].mean()
    median_daily_vol = df_daily_vol['trade_quantity'].median()

    # Sample trade prices every 3 minutes for each day of the month and combine
    # into a single DataFrame:
    # XXX need to handle empty groups
    df_trade_price_res = \
      df.groupby('trade_date').apply(lambda d: \
         sample(d, datetime.timedelta(minutes=3), 
                'trade_date_time', 'trade_price', 'trade_date'))

    # Compute the average price trade for the entire month of data:
    mean_trade_price = df['trade_price'].mean()
    
    # Compute standard deviation of sampled prices:
    std_trade_price_res = df_trade_price_res['trade_price'].std()

    # Compute returns:
    s_returns = \
      df_trade_price_res.groupby('trade_date').apply( \
        lambda x: x['trade_price'].diff()/x['trade_price'])
    s_returns = s_returns[s_returns.notnull()]
    
    if len(s_returns) > 0:

        # Compute average and standard deviation of returns in bps:           
        mean_returns = s_returns.mean()*10000
        std_returns = s_returns.std()*10000
        sum_abs_returns = sum(abs(s_returns))*10000
    else:

        # If there are too few trades with which to compute returns, set the
        # return statistics to 0:
        mean_returns = 0.0
        std_returns = 0.0
        sum_abs_returns = 0.0
        
    # Set the trade price for days on which no trades were recorded to 0:
    df_price = df[['trade_date', 'trade_price']]
    df_price = df_price.combine_first(pandas.DataFrame({'trade_date': sept_days, 
                                 'trade_price': np.zeros(len(sept_days))}))

    # Compute the daily maximum and minimum trade price expressed in basis
    # points away from the daily opening price:
    daily_price_max_list = map(lambda x: 10000*int(x),
      df_price.groupby('trade_date')['trade_price'].apply(max)-df.ix[0]['trade_price'])
    daily_price_min_list = map(lambda x: 10000*int(x),
      df_price.groupby('trade_date')['trade_price'].apply(min)-df.ix[0]['trade_price'])

    return [trade_quant_q1, trade_quant_q2, trade_quant_q3,
            max_trade_quant, min_trade_quant, mean_trade_quant,
            inter_time_q1, inter_time_q2, inter_time_q3,
            max_daily_vol, min_daily_vol, mean_daily_vol, 
            median_daily_vol, mean_trade_price, std_trade_price_res, 
            mean_returns, std_returns, sum_abs_returns] + daily_price_max_list + daily_price_min_list

if __name__ == '__main__':    
    if len(sys.argv) == 1:
        print 'need to specify input files'
        sys.exit(0)

    w = csv.writer(sys.stdout)
    for file_name in sys.argv[1:]:
        row = analyze(file_name)    
        w.writerow([file_name] + row)

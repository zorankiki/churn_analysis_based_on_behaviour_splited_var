import pandas as pd
import numpy as np
import ast
import math
from lifelines import CoxTimeVaryingFitter

#### Helpers ####
import os
import sys
sys.path.insert(0, os.path.abspath('../'))
from helpers.s3_bucket_utils import S3BucketUtils
from helpers import db_utils
from helpers import settings

bucket = S3BucketUtils()
################

def get_dummies_merge(df, column_name, prefix, drop, drop_first):
    if prefix:
        if drop_first:
            df = pd.merge(df, pd.get_dummies(df[column_name], prefix = column_name, drop_first=True), left_index = True, right_index = True)
        else:
            df = pd.merge(df, pd.get_dummies(df[column_name], prefix = column_name, drop_first=False), left_index = True, right_index = True)
    else:
        df = pd.merge(df, pd.get_dummies(df[column_name], drop_first=True), left_index = True, right_index = True)

    if drop:
        df = df.drop(column_name, axis = 1)
    return df

def get_number_of_spots_with_var_equal_to_1(vars_, df, column_name):
    spots_that_cancelled = df[df['event']==True]['spot_id'].unique()
    total_number_of_spots = []
    total_number_of_spots_perc = []
    number_of_spots_that_cancelled = []
    for i in range(0, len(vars_)):
        total_number_of_spots.append(df[df[column_name]==vars_[i]]['spot_id'].nunique())
        total_number_of_spots_perc.append((df[df[column_name]==vars_[i]]['spot_id'].nunique()/df['spot_id'].nunique())*100)
        number_of_spots_that_cancelled.append(df[(df[column_name]==vars_[i])&\
                                            (df['spot_id'].isin(spots_that_cancelled))]['spot_id'].nunique())

    res = pd.DataFrame(data = pd.concat([pd.Series(vars_), pd.Series(total_number_of_spots), pd.Series(total_number_of_spots_perc), pd.Series(number_of_spots_that_cancelled)],axis = 1))
    res.rename(columns = {0:'variable', 1:'total_number_of_spots', 2:'total_number_of_spots_%', 3:'number_of_cancelled_spots'}, inplace = True)
    res.sort_values('total_number_of_spots', ascending = False, inplace = True)
    res.reset_index(drop = True, inplace = True)

    return res

def get_data_for_the_MV_Cox_model(date_of_analysis, hs_filename, spots_set, with_wo_CB, event_date, columns, data_dir, for_the_report = 'yes/no', C = 1, C1 = 1):

    date_dir = date_of_analysis.replace('-', '_')

    ## read data from s3 ##
    data = bucket.load_csv_from_s3(file_name = data_dir + 'data/' + date_dir +\
                             '/data_tv_with_maybe_problematic_spots_all.csv')

    HS_spots = bucket.\
    load_csv_from_s3(file_name='churn_analysis/data/'+date_dir+'/'+hs_filename)
    HS_spots.drop(HS_spots[(HS_spots.duplicated('Spot ID'))&(HS_spots['Premium'].isnull())].index, inplace  =True)

    data = data.merge(HS_spots[['Spot ID', 'Contract type']].rename(columns = {'Spot ID':'spot_id'}), on = 'spot_id')


    ## drop duplicates just in case ##
    data.drop_duplicates(inplace = True)
    
    ###  year_when_become_customer  ###
    data = get_dummies_merge(df=data, column_name='year_become_customer', prefix=True, drop=False, drop_first=False)

    data['limits'] = data['limits'].apply(lambda x: ast.literal_eval(x))
    data['left_limit'] = data['limits'].apply(lambda x: x[0])
    data['right_limit'] = data['limits'].apply(lambda x: x[1])
    data.sort_values(['spot_id', 'time'], ascending = [True, True], inplace = True)

    data['canc_conf_event']  = data['event']

    ## choose event date we're going to use ##
    # if event_date == 'canc_conf':
    #     data['event'] = data['canc_conf_event']
    #     data.drop(data[data['more_than_60_days_passed_since_canc_req']==1].index, inplace = True)
    # elif event_date == 'canc_req':
    data['event'] = data['canc_req_event']
    data.drop(data[(data['requested_cancellation']==True)&\
    (data['left_limit']>=data['cancellation_requested'])].index, inplace = True)

    data.reset_index(drop = True, inplace = True)


    ## get spot categories and metro areas (choose a subset of values present in all data sets) ##
    data_smallest_set = \
    bucket.load_csv_from_s3(file_name = 'churn_analysis/' + 'data/' + date_dir +\
                                 '/data_tv_CAN_CANCEL_spots_wo_CB_wo_151617.csv')
    data_smallest_set.drop(data_smallest_set[data_smallest_set['their_own_website']=='dontKnow'].index, inplace = True)
    data_cols = pd.DataFrame(data_smallest_set.columns, columns = ['columns'])
    spot_category_vars = data_smallest_set['spot_category'].unique()
    metro_area_vars = data_smallest_set['metro_area'].unique()

    spot_categories_spots_numbers = get_number_of_spots_with_var_equal_to_1(spot_category_vars, data_smallest_set, 'spot_category')
    metro_areas_spots_numbers = get_number_of_spots_with_var_equal_to_1(metro_area_vars, data_smallest_set, 'metro_area')

    spot_categories_for_the_model = spot_categories_spots_numbers[spot_categories_spots_numbers['total_number_of_spots_%']>1]['variable'].unique()
    metro_areas_for_the_model = metro_areas_spots_numbers[metro_areas_spots_numbers['total_number_of_spots_%']>1]['variable'].unique()


    data.loc[(data['spot_category'].isin(spot_categories_for_the_model)==False)|\
         (data['spot_category']=='dontKnow')|\
         (data['spot_category'].isnull()), 'spot_category'] = 'other'

    data.loc[(data['metro_area'].isin(metro_areas_for_the_model)==False)|\
             (data['metro_area']=='dontKnow')|\
             (data['metro_area'].isnull()), 'metro_area'] = 'other'

    ## mark all spot categories and metro areas that have less than 50 spots (in the biggest data set) as other ##
    data_biggest_set = \
    bucket.load_csv_from_s3(file_name = 'churn_analysis/' + 'data/' + date_dir +\
                                 '/data_tv_with_maybe_problematic_spots_all.csv')
    spot_categories_value_counts =\
    data_biggest_set[['spot_id', 'spot_category']].drop_duplicates()['spot_category'].value_counts().\
    reset_index().rename(columns = {'index':'spot_category', 'spot_category':'num_of_spots'})
    metro_areas_value_counts =\
    data_biggest_set[['spot_id', 'metro_area']].drop_duplicates()['metro_area'].value_counts().\
    reset_index().rename(columns = {'index':'metro_area', 'metro_area':'num_of_spots'})

    data.loc[(data['spot_category'].isin(spot_categories_for_the_model))&\
    (data['spot_category'].isin(spot_categories_value_counts[spot_categories_value_counts['num_of_spots']<50]['spot_category'].\
                               unique())), 'spot_category'] = 'other'

    data.loc[(data['metro_area'].isin(metro_areas_for_the_model))&\
        (data['metro_area'].isin(metro_areas_value_counts[metro_areas_value_counts['num_of_spots']<50]['metro_area'].\
                                   unique())), 'metro_area'] = 'other'


    data = get_dummies_merge(df=data, column_name='spot_category', prefix=True, drop=False, drop_first=False)
    if for_the_report != 'yes':
        data.drop('spot_category_Bar or Restaurant', axis = 1, inplace = True)

    data = get_dummies_merge(df=data, column_name='metro_area', prefix=True, drop=False, drop_first=False)
    if for_the_report != 'yes':
        data.drop(['metro_area_other'], axis = 1, inplace = True)

    data_cols = pd.DataFrame(data.columns, columns = ['columns'])
    spot_category_vars = list(data_cols[data_cols['columns'].apply(lambda x: 'spot_category_' in x)]['columns'].unique())
    metro_area_vars = list(data_cols[data_cols['columns'].apply(lambda x: 'metro_area_' in x)]['columns'].unique())

    ### their own website dummies ###
    data = get_dummies_merge(df=data, column_name='their_own_website', prefix=True, drop=False, drop_first=False)

    ## rename tickets.total ##
    data.rename(columns = {'tickets.total':'Number.of.tickets.last.month'}, inplace = True)

    ## add the is_corona variable ##
    data['is_corona'] = 0
    data.loc[data['limits'].apply(lambda x: x[1][0:7]>='2020-03' and x[0][0:7]<'2021-06'), 'is_corona'] = 1


    ## add log2 transformation for 'Website.Views.last.month.total' and 'Website.Views.per.month.avg' ##
    data['Website.Views.last.month.total.log2'] = data['Website.Views.last.month.total'].apply(lambda x: math.log2((x/C)+1))

    ## add normalized 'Website.Views.last.month.total' and 'Website.Views.per.month.avg' ##
    data['Website.Views.last.month.total.div100'] = data['Website.Views.last.month.total']/100
    data['Website.Views.last.month.total.div1000'] = data['Website.Views.last.month.total']/1000

    ## adding normalized numerical variables ##
    vars_for_norm = ['New.email.subscriber.signups.last.month', 'New.email.subscribers.allinclusive.last.month',\
    'Fb.page.likes.analysis.last.month', 'Fb.page.likes.analysis.avg',\
    'Number.of.tickets.last.month']

    for var in vars_for_norm:
        data[var+'.log2'] = data[var].apply(lambda x: math.log2((x/C1)+1))
        data[var+'.div10'] = data[var]/10
        data[var+'.div100'] = data[var]/100
        data[var+'.div1000'] = data[var]/1000


    ## get dummy variables for Contract type ##
    data.loc[(data['Contract type'].isnull())|(data['Contract type']=='0'), 'Contract type'] = 'Monthly'
    data = get_dummies_merge(df = data, column_name='Contract type', prefix=False, drop=False, drop_first = False)

    ############# prepare df to fit the model with ###############
    base_cols = ['spot_id', 'time', 'event']

    ## calculate final max time and min time ##
    if 'final_max_time' in data.columns:
        data.drop('final_max_time', axis = 1, inplace = True)
    data = data.merge(data.groupby('spot_id')['time'].max().reset_index().rename(columns = {'time':'final_max_time'}),\
           on = 'spot_id')
    if 'min_time' in data.columns:
        data.drop('min_time', axis = 1, inplace = True)
    data = data.merge(data.groupby('spot_id')['time'].min().reset_index().rename(columns = {'time':'min_time'}),\
           on = 'spot_id')

    # ## drop spots with one record ##
    # data.drop(data[data['min_time']==data['final_max_time']].index, inplace = True)

    # ## final variables to fit the model with ##
    # columns = columns + spot_category_vars + metro_area_vars

    # df_timeline = data.copy()
    # df_timeline.drop(df_timeline[df_timeline['right_limit']==date_of_analysis].index, inplace = True)
    # df_timeline = df_timeline[sorted(columns)]
    #
    # df_timeline['stop'] = df_timeline['time']
    # df_timeline['start'] = df_timeline['stop']-1
    # df_timeline.drop('time', axis = 1, inplace = True)
    #
    # ## base df ##
    # min_start = df_timeline.groupby('spot_id')['start'].min().reset_index()
    # max_stop = df_timeline.groupby('spot_id')['stop'].max().reset_index()
    #
    # base_df = \
    # min_start.merge(max_stop, on = 'spot_id').merge(df_timeline[['spot_id', 'stop', 'event']], on = ['spot_id', 'stop'])

    return data

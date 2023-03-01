import pandas as pd
import numpy as np

#### Helpers ####
import os
import sys
sys.path.insert(0, os.path.abspath('../'))
from helpers.s3_bucket_utils import S3BucketUtils
from helpers import settings

bucket = S3BucketUtils()
################

churn_based_on_behaviour_dir = 'churn_analysis_based_on_behaviour/'

def calculate_churn_risk(date_of_analysis, df_timeline, vars_, data_all_spots, ctv, model_type, model_number, with_add_vars, p_limit = 0.5, event_date_type='cancellation_requested/cancellation_confirmed', spots_set='ALL/CAN_CANCEL', with_wo_CB='with_CB/wo_CB', with_dontKnow_non_SH_site_spots = True, with_spot_categories_metro_areas=True):
    date_dir = date_of_analysis.replace('-', '_')

    churn_risk_prediction_path = 'data/'+date_dir+'/exports/churn_risk_prediction/'+model_type+'/model_'+str(model_number)+'/'
    churn_risk_prediction_with_add_important_vars_path = 'data/'+date_dir+'/exports/churn_risk_prediction_with_add_important_vars/'+model_type+'/model_'+str(model_number)+'/'

    p_limit = str(p_limit).split('.')[0]+'_'+str(p_limit).split('.')[1]

    df_timeline = df_timeline.merge(df_timeline.groupby('spot_id')['stop'].max().reset_index().rename(columns = {'stop':'final_stop'}), on = 'spot_id')

    #Final max time and min time
    if 'final_max_time' in data_all_spots.columns:
        data_all_spots.drop('final_max_time', axis = 1, inplace = True)
    data_all_spots = data_all_spots.merge(data_all_spots.groupby('spot_id')['time'].max().reset_index().rename(columns = {'time':'final_max_time'}),\
           on = 'spot_id')
    if 'min_time' in data_all_spots.columns:
        data_all_spots.drop('min_time', axis = 1, inplace = True)
    data_all_spots = data_all_spots.merge(data_all_spots.groupby('spot_id')['time'].min().reset_index().rename(columns = {'time':'min_time'}),\
           on = 'spot_id')


    data_all_spots.drop_duplicates(inplace = True)

    spots_survived = data_all_spots[(data_all_spots['time']==data_all_spots['final_max_time'])&\
                  (data_all_spots['event']==False)]['spot_id'].unique()
    if 'has/had_their_own_website' in vars_:
        spots_survived_vars_ = data_all_spots[(data_all_spots['time']==data_all_spots['final_max_time'])&\
                    (data_all_spots['event']==False)][['spot_id','ga_profile_id', 'domain', 'spot_category', 'metro_area', 'start', 'time', 'maybe_problematic',\
                    'Date Cancellation Requested', 'Date Cancellation Confirmed', 'end', \
                    'Reason for Cancelling Survey', 'Delinquent ', 'premium_service_hs']+vars_]
    else:
        spots_survived_vars_ = data_all_spots[(data_all_spots['time']==data_all_spots['final_max_time'])&\
                    (data_all_spots['event']==False)][['spot_id','ga_profile_id', 'domain', 'spot_category', 'metro_area', 'start','has/had_their_own_website', 'time', 'maybe_problematic',\
                    'Date Cancellation Requested', 'Date Cancellation Confirmed', 'end', \
                    'Reason for Cancelling Survey', 'Delinquent ', 'premium_service_hs']+vars_]

    spots_survived_vars_['time'] = spots_survived_vars_['time'] + 1

    baseline_cum_haz = ctv.baseline_cumulative_hazard_.reset_index().rename(columns = {'index':'time'})
    baseline_cum_haz = baseline_cum_haz.merge(spots_survived_vars_['time'], on = 'time', how = 'outer').drop_duplicates().sort_values('time').reset_index(drop = True)
    baseline_cum_haz_interpolated = baseline_cum_haz.interpolate(method = 'linear')
    spots_survived_vars_ = spots_survived_vars_.merge(baseline_cum_haz_interpolated)

    S = np.exp(-(spots_survived_vars_['baseline hazard'])*np.exp((spots_survived_vars_[vars_]*ctv.params_).sum(axis = 1)))
    H = np.ones(len(S))-S

    spots_survived_vars_['churn_prediction'] = H

    churn_risk_sorted_spots = spots_survived_vars_.\
    sort_values('churn_prediction', ascending = False)[['spot_id', 'ga_profile_id', 'domain', 'spot_category', 'metro_area', 'has/had_their_own_website', 'start', 'time', 'maybe_problematic', \
    'Date Cancellation Requested', 'Date Cancellation Confirmed', 'end', \
    'Reason for Cancelling Survey', 'Delinquent ', 'premium_service_hs', 'churn_prediction']]

    churn_risk_sorted_spots.drop_duplicates(inplace = True)
    churn_risk_sorted_spots.reset_index(drop = True, inplace = True)

    if event_date_type == 'cancellation_confirmed':
        file_name = 'churn_risk_based_on_'+spots_set+'_spots_'+with_wo_CB+'_cancellation_confirmed_p_below_'+p_limit+'.csv'
    elif event_date_type == 'cancellation_requested':
        file_name = 'churn_risk_based_on_'+spots_set+'_spots_'+with_wo_CB+'_cancellation_requested_p_below_'+p_limit+'.csv'

    spots_info = bucket.\
    load_csv_from_s3(file_name='churn_analysis/data/'+date_dir+'/all_spots_clean_ext.csv')
    churn_risk_sorted_spots = churn_risk_sorted_spots.merge(spots_info[['spot_id', 'name']], on = 'spot_id', how = 'left')
    churn_risk_sorted_spots = churn_risk_sorted_spots[['spot_id', 'name', 'ga_profile_id', 'domain', 'spot_category', 'metro_area', 'has/had_their_own_website', 'start', 'time', 'maybe_problematic', \
    'Date Cancellation Requested', 'Date Cancellation Confirmed', 'end', \
    'Reason for Cancelling Survey', 'Delinquent ', 'premium_service_hs', 'churn_prediction']]

    if with_add_vars == False:
        if not os.path.exists(churn_risk_prediction_path):
            os.makedirs(churn_risk_prediction_path)
        churn_risk_sorted_spots.to_csv(churn_risk_prediction_path+file_name, index = False)
        bucket.store_csv_to_s3(data_frame = churn_risk_sorted_spots, \
        file_name = file_name, \
        dir = '/'+churn_based_on_behaviour_dir + churn_risk_prediction_path)
    else:
        if not os.path.exists(churn_risk_prediction_with_add_important_vars_path):
            os.makedirs(churn_risk_prediction_with_add_important_vars_path)
        churn_risk_sorted_spots.to_csv(churn_risk_prediction_with_add_important_vars_path+file_name, index = False)
        bucket.store_csv_to_s3(data_frame = churn_risk_sorted_spots, \
        file_name = file_name, \
        dir = '/'+churn_based_on_behaviour_dir + churn_risk_prediction_with_add_important_vars_path)

    # return (churn_risk_sorted_spots, spots_survived_vars_)

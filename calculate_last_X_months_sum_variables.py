import pandas as pd
import get_all_vars
import yaml
import numpy as np

#### Helpers ####
import os
import sys
sys.path.insert(0, os.path.abspath('../'))
from helpers.s3_bucket_utils import S3BucketUtils
from helpers import db_utils
from helpers import settings

bucket = S3BucketUtils()
################

## dir names ##
regular_churn_dir = 'churn_analysis/'
churn_based_on_behaviour_dir = 'churn_analysis_based_on_behaviour/'


def get_key_based_on_value_in_a_dict(dict_, value_of_interest):
    for key, value in dict_.items():
        if value == value_of_interest:
            return key

def get_last_w_months_sum(df, col_name, last_w_months_avg_col_name, w):
    df[last_w_months_avg_col_name] = df.groupby('spot_id')[col_name].\
    apply(lambda x: x.rolling(window=w, min_periods=0).sum())

    return df

def read_estimated_periods_and_thresholds(filename, dir_, all_vars_base_names):
    print(dir_+filename)
    estimated_periods_and_thresholds = \
    bucket.load_csv_from_s3(file_name = dir_ + filename)

    estimated_periods_and_thresholds['internal_variable_name'] = \
    estimated_periods_and_thresholds['variable_name'].apply(lambda x: get_key_based_on_value_in_a_dict(dict_=all_vars_base_names,\
                                                                                                  value_of_interest=x))

    return estimated_periods_and_thresholds

def get_last_X_months_sum_names(period_to_look_at, base_var):
    if period_to_look_at == 'last_month':
        return base_var + '_last_month_sum'
    else:
        X = int(period_to_look_at.split('_')[1])
        return base_var+'_last_'+str(X)+'_months_sum'

def get_vars_to_use(estimated_periods_and_thresholds, all_vars_base_names, var_, vars_to_be_used):
    period_to_look_at_started = estimated_periods_and_thresholds[estimated_periods_and_thresholds['internal_variable_name']==var_]['period_to_look_at_started'].\
    values[0]
    period_to_look_at_stopped = estimated_periods_and_thresholds[estimated_periods_and_thresholds['internal_variable_name']==var_]['period_to_look_at_stopped'].\
    values[0]

    vars_to_be_used.loc[var_, 'var_to_use_started'] = \
    get_last_X_months_sum_names(period_to_look_at=period_to_look_at_started, base_var=all_vars_base_names[var_])
    vars_to_be_used.loc[var_, 'var_to_use_stopped'] = \
    get_last_X_months_sum_names(period_to_look_at=period_to_look_at_stopped, base_var=all_vars_base_names[var_])

    for i in range(0, len([x for x in estimated_periods_and_thresholds.columns if 'covid_period_to_look_at_stopped' in x])):
        period_to_look_at_tmp =\
        estimated_periods_and_thresholds[estimated_periods_and_thresholds['internal_variable_name']==var_]['covid_period_to_look_at_stopped_'+str(i+1)].\
        values[0]

        if period_to_look_at_tmp==period_to_look_at_tmp:
            vars_to_be_used.loc[var_, 'var_to_use_stopped_'+str(i+1)] = \
            get_last_X_months_sum_names(period_to_look_at=period_to_look_at_tmp, base_var=all_vars_base_names[var_])

    return vars_to_be_used

def create_total_sum_variables(df, all_vars, all_vars_base_names, estimated_periods_and_thresholds):
    cols_started = ['period_to_look_at_started']+\
    [x for x in estimated_periods_and_thresholds.columns if 'covid_period_to_look_at_started' in x]
    cols_stopped = ['period_to_look_at_stopped']+\
    [x for x in estimated_periods_and_thresholds.columns if 'covid_period_to_look_at_stopped' in x]
    cols_started = ['var_to_use_started'+x.split('started')[1] for x in cols_started]
    cols_stopped = ['var_to_use_stopped'+x.split('stopped')[1] for x in cols_stopped]

    vars_to_be_used = pd.DataFrame(columns = ['internal_variable_name']+cols_started+cols_stopped)
    vars_to_be_used['internal_variable_name'] = all_vars
    vars_to_be_used.set_index('internal_variable_name', inplace = True)

    estimated_periods_and_thresholds.to_csv('estimated_periods_and_thresholds.csv',index=False)
    for var_ in all_vars:
        print(var_)
        periods_we_need = \
        estimated_periods_and_thresholds[estimated_periods_and_thresholds['internal_variable_name']==var_]['periods_to_look_at_we_need'].values[0]
        for period_to_look_at in periods_we_need:
            if period_to_look_at == 'last_month':
                df[all_vars_base_names[var_]+'_last_month_sum'] = df[var_]
            else:
                w = int(period_to_look_at.split('_')[1])

                if all_vars_base_names[var_]+'_last_'+str(w)+'_months_sum' not in df.columns:
                        df = get_last_w_months_sum(df=df, col_name=var_, \
                                                     last_w_months_avg_col_name=all_vars_base_names[var_]+'_last_'+str(w)+'_months_sum',\
                                                     w = w)

        if var_=='QR.code.menu.scans.total' or var_ == 'QR.code.flyer.scans.total':
            downloaded_qrcode_flyers_period = \
            estimated_periods_and_thresholds[estimated_periods_and_thresholds['internal_variable_name']=='Downloaded.qrcode.flyers.total']['period_to_look_at_stopped'].values[0]
            if downloaded_qrcode_flyers_period == 'last_month':
                df[all_vars_base_names[var_]+'_last_month_sum'] = df[var_]
            else:
                w = int(downloaded_qrcode_flyers_period.split('_')[1])

                if all_vars_base_names[var_]+'_last_'+str(w)+'_months_sum' not in df.columns:
                        df = get_last_w_months_sum(df=df, col_name=var_, \
                                                     last_w_months_avg_col_name=all_vars_base_names[var_]+'_last_'+str(w)+'_months_sum',\
                                                     w = w)

        vars_to_be_used = \
        get_vars_to_use(estimated_periods_and_thresholds=estimated_periods_and_thresholds, all_vars_base_names=all_vars_base_names,\
                   var_=var_, vars_to_be_used=vars_to_be_used)

    return (df, vars_to_be_used)

def get_number_of_months(period):
    if period==period:
        if period == 'last_month':
            return 1
        else:
            return int(period.split('_')[1])
    else:
        return np.nan


def read_yaml_file(file_path):
    with open(file_path) as file:
        f = yaml.load(file, Loader=yaml.FullLoader)
    return f


def get_covid_parameters():
    ## read covid period restrictions parameters ##
    covid_period_limits = read_yaml_file(r'./parameters/covid_period.yaml')
    covid_period_to_look_at = read_yaml_file(r'./parameters/covid_period_to_look_at.yaml')
    covid_thresholds = read_yaml_file(r'./parameters/covid_thresholds.yaml')

    covid_period_parameters = \
    pd.DataFrame(columns = ['variable_name', 'period_start', 'period_end', 'period_to_look_at_stopped', 'threshold_stopped'])

    for key in covid_period_limits.keys():
        for i in range(0, int(len(covid_period_limits[key])/2)):
            idx = len(covid_period_parameters)
            covid_period_parameters.loc[idx, 'variable_name'] = key
            covid_period_parameters.loc[idx, 'period_start'] = covid_period_limits[key]['start_'+str(i+1)]
            covid_period_parameters.loc[idx, 'period_end'] = covid_period_limits[key]['end_'+str(i+1)]
            covid_period_parameters.loc[idx, 'period_to_look_at_stopped'] = covid_period_to_look_at[key]['period_to_look_at_stopped_'+str(i+1)]
            covid_period_parameters.loc[idx, 'threshold_stopped'] = \
            covid_thresholds[key]['threshold_stopped_'+str(i+1)]

    inquiries_and_changed_inquiry_status_vars = read_yaml_file(r'./parameters/for_properly_used_inquiries_vars.yaml')
    for var_ in covid_period_parameters['variable_name'].unique():
        inquiries_var_key = \
        get_key_based_on_value_in_a_dict(dict_=inquiries_and_changed_inquiry_status_vars['inquiries_vars_base_names'],\
                                    value_of_interest=var_)
        if inquiries_var_key:
            changed_inquiry_status_var = \
            inquiries_and_changed_inquiry_status_vars['changed_inquiry_status_vars_base_names'][inquiries_var_key]

            addition_changed_inquiry_status_var_params = \
            covid_period_parameters[covid_period_parameters['variable_name']==var_].copy()
            addition_changed_inquiry_status_var_params['variable_name'].replace(var_, changed_inquiry_status_var, inplace = True)

            covid_period_parameters = pd.concat([covid_period_parameters, addition_changed_inquiry_status_var_params])

    return covid_period_parameters


def main(date_of_analysis, dir_to_use, period_to_look_at_thresholds_filename, data_tv_filename, vars_periods_to_look_at_thresholds_to_use_filename):
    date_dir = date_of_analysis.replace('-', '_')

    ## get variables we're calculating last X months sum values for and their base names ##
    (all_vars, all_vars_base_names) = get_all_vars.main()
    
    ## read estimated periods to look at and threshold values from s3 ##
    estimated_periods_and_thresholds = \
    read_estimated_periods_and_thresholds(filename=period_to_look_at_thresholds_filename,\
                                    dir_=churn_based_on_behaviour_dir+'data/'+date_dir+'/'+dir_to_use, all_vars_base_names=all_vars_base_names)
    

    ## get covid related parameters ##
    covid_period_parameters = get_covid_parameters()

    ## read the data set for churn with all spots ##
    df = bucket.load_csv_from_s3(file_name = regular_churn_dir + 'data/' + date_dir + \
                        '/data_tv_with_maybe_problematic_spots_all.csv')

    maximum_num_of_covid_periods = \
    covid_period_parameters.groupby(['variable_name'])['period_to_look_at_stopped'].nunique().max()

    for i in range(0, maximum_num_of_covid_periods):
        estimated_periods_and_thresholds['covid_period_to_look_at_stopped_'+str(i+1)] = np.nan
        estimated_periods_and_thresholds['covid_threshold_stopped_'+str(i+1)] = np.nan
        estimated_periods_and_thresholds['covid_period_start_'+str(i+1)] = np.nan
        estimated_periods_and_thresholds['covid_period_end_'+str(i+1)] = np.nan

    estimated_periods_and_thresholds.set_index('variable_name', inplace = True)
    for var_ in covid_period_parameters['variable_name'].unique():
        for i in range(0, len(covid_period_parameters[(covid_period_parameters['variable_name']==var_)])):
            estimated_periods_and_thresholds.loc[var_, 'covid_period_to_look_at_stopped_'+str(i+1)] = \
            covid_period_parameters[(covid_period_parameters['variable_name']==var_)]['period_to_look_at_stopped'].iloc[i]
            estimated_periods_and_thresholds.loc[var_, 'covid_threshold_stopped_'+str(i+1)] = \
            covid_period_parameters[(covid_period_parameters['variable_name']==var_)]['threshold_stopped'].iloc[i]
            estimated_periods_and_thresholds.loc[var_, 'covid_period_start_'+str(i+1)] = \
            covid_period_parameters[(covid_period_parameters['variable_name']==var_)]['period_start'].iloc[i]
            estimated_periods_and_thresholds.loc[var_, 'covid_period_end_'+str(i+1)] = \
            covid_period_parameters[(covid_period_parameters['variable_name']==var_)]['period_end'].iloc[i]

    estimated_periods_and_thresholds['periods_to_look_at_we_need'] = estimated_periods_and_thresholds.\
    apply(lambda x: list(dict.fromkeys([x['period_to_look_at_stopped'], x['period_to_look_at_started']]+\
                                      [x[y] for y in estimated_periods_and_thresholds.columns if 'covid_period_to_look_at' in y and x[y]==x[y]]).keys()), axis = 1)
    estimated_periods_and_thresholds.reset_index(inplace = True)

    ## vars to be used are last X months sum variables - we'll use those to create categorical ones ##
    (df, vars_to_be_used) = create_total_sum_variables(df=df, all_vars=all_vars, all_vars_base_names=all_vars_base_names,\
                               estimated_periods_and_thresholds=estimated_periods_and_thresholds)

    ## drop some columns we don't need from df ##
    cols_to_drop = \
    [x for x in df.columns if 'avg.3months' in x or 'last3months.average' in x or 'uses' in x \
    or 'stopped_posting_' in x or 'DEPRECATED' in x or 'spot_category_' in x or 'metro_area_' in x]
    more_cols_to_drop = ['max_time', 'date_conf_date_req_diff','sessions_start',\
    'oldest_archive', 'oldest_archive_url', 'oldest_archive_date',\
    'oldest_archive_year_month','start_archive_date_diff',\
    'start_archive_date_diff_days', 'sessions_start_archive_date_diff_days',\
    'starts_posting_date', 'posted_before']
    more_cols_to_drop = [x for x in more_cols_to_drop if x in df.columns]
    cols_to_drop = cols_to_drop + more_cols_to_drop
    df.drop(cols_to_drop, axis = 1, inplace = True)

    ## save df with vars to be used to s3 ##
    bucket.store_csv_to_s3(data_frame = df, \
    file_name = data_tv_filename, \
    dir = '/' + churn_based_on_behaviour_dir + 'data/'+date_dir+'/'+dir_to_use)

    estimated_periods_and_thresholds = \
    estimated_periods_and_thresholds[['variable_name', 'internal_variable_name', 'period_to_look_at_started',\
                                     'period_to_look_at_stopped', 'threshold_started', 'threshold_stopped']+\
                                    [x for x in estimated_periods_and_thresholds.columns if 'covid_' in x]].\
    merge(vars_to_be_used.reset_index(), on = ['internal_variable_name'])

    estimated_periods_and_thresholds['number_of_months_started'] = \
    estimated_periods_and_thresholds['period_to_look_at_started'].apply(lambda x: get_number_of_months(x))
    estimated_periods_and_thresholds['number_of_months_stopped'] = \
    estimated_periods_and_thresholds['period_to_look_at_stopped'].apply(lambda x: get_number_of_months(x))

    for i in range(0, len([x for x in estimated_periods_and_thresholds.columns if 'covid_period_to_look_at' in x])):
        estimated_periods_and_thresholds['covid_number_of_months_stopped_'+str(i+1)] = \
        estimated_periods_and_thresholds['covid_period_to_look_at_stopped_'+str(i+1)].apply(lambda x: get_number_of_months(x))


    estimated_periods_and_thresholds.rename(columns = {'variable_name':'variable_base_name'}, inplace = True)

    bucket.store_csv_to_s3(data_frame = estimated_periods_and_thresholds, \
    file_name = vars_periods_to_look_at_thresholds_to_use_filename, \
    dir = '/' + churn_based_on_behaviour_dir + 'data/'+date_dir+'/'+dir_to_use)


    return (df, estimated_periods_and_thresholds)

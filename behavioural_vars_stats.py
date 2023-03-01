import pandas as pd
import numpy as np
import yaml
import get_all_vars

#### Helpers ####
import os
import sys
sys.path.insert(0, os.path.abspath('../'))
from helpers.s3_bucket_utils import S3BucketUtils
from helpers import db_utils
from helpers import settings

bucket = S3BucketUtils()
################

churn_based_on_behaviour_dir = 'churn_analysis_based_on_behaviour/'

def get_fields_for_each_variable(all_vars, all_vars_base_names, did_something_before_vars, did_something_last_month_vars, did_something_before_and_didnt_last_month_vars, changed_inquiry_status_to_properly_used_names):
    fields = []
    for var_ in all_vars:
        fields.extend([did_something_before_vars[var_], did_something_last_month_vars[var_], \
         did_something_before_and_didnt_last_month_vars[var_]])

        if 'changed' in var_.lower() and 'inquiry' in var_.lower():
            fields.append('had_'+changed_inquiry_status_to_properly_used_names[all_vars_base_names[var_]]+'_before_stopped_and_resumed')
        else:
            fields.append('had_'+all_vars_base_names[var_]+'_before_stopped_and_resumed')

    return fields

data_sets = ['data_tv_ALL_spots_with_CB_wo_151617.csv',\
             'data_tv_ALL_spots_wo_CB_wo_151617.csv',\
             'data_tv_CAN_CANCEL_spots_with_CB_wo_151617.csv',\
             'data_tv_CAN_CANCEL_spots_wo_CB_wo_151617.csv']

def main(date_of_analysis, dir_to_use, data_tv_filename, vars_periods_to_look_at_thresholds_to_use_filename, testing_diff_parameters):
    date_dir = date_of_analysis.replace('-', '_')

    with open(r'./parameters/for_properly_used_inquiries_vars.yaml') as file:
        for_properly_used_inquiries_vars = yaml.load(file, Loader=yaml.FullLoader)

    inquiries_vars_base_names = \
    for_properly_used_inquiries_vars['inquiries_vars_base_names']
    changed_inquiry_status_vars_base_names = \
    for_properly_used_inquiries_vars['changed_inquiry_status_vars_base_names']
    changed_inquiry_status_to_properly_used_names = for_properly_used_inquiries_vars['changed_inquiry_status_to_properly_used']

    (all_vars, all_vars_base_names) = get_all_vars.main()

    vars_periods_to_look_at_thresholds_to_use = \
    bucket.\
    load_csv_from_s3(file_name=churn_based_on_behaviour_dir + 'data/'+date_dir+'/'+dir_to_use+\
                     vars_periods_to_look_at_thresholds_to_use_filename)

    ## did something before, did something last month, did something before and didnt last month ##
    first_month_spot_did_something_vars = dict(zip(all_vars_base_names.keys(), ['had_'+x+'_first_month' for x in all_vars_base_names.values()]))
    did_something_before_vars = dict(zip(all_vars_base_names.keys(), ['had_'+x+'_before' for x in all_vars_base_names.values()]))
    did_something_last_month_vars = dict(zip(vars_periods_to_look_at_thresholds_to_use['internal_variable_name'].unique(), vars_periods_to_look_at_thresholds_to_use[['variable_base_name', 'period_to_look_at_stopped']].\
    apply(lambda x: 'had_'+x['variable_base_name']+'_'+x['period_to_look_at_stopped'], axis = 1).values))
    did_something_before_and_didnt_last_month_vars = \
    dict(zip(vars_periods_to_look_at_thresholds_to_use['internal_variable_name'].unique(),\
        vars_periods_to_look_at_thresholds_to_use[['variable_base_name', 'period_to_look_at_stopped']].\
        apply(lambda x: 'had_'+x['variable_base_name']+'_before_and_didnt_'+x['period_to_look_at_stopped'], axis = 1).values))

    vars_periods_to_look_at_thresholds_to_use.set_index('variable_base_name', inplace = True)
    for key in all_vars_base_names.keys():
        if 'changed' in key.lower() and 'inquiry' in key.lower():
            period_to_look_at_started_tmp = \
            vars_periods_to_look_at_thresholds_to_use.loc[all_vars_base_names[key], 'period_to_look_at_started']

            period_to_look_at_stopped_tmp = \
            vars_periods_to_look_at_thresholds_to_use.loc[all_vars_base_names[key], 'period_to_look_at_stopped']

            did_something_before_vars[key] = \
            'had_'+for_properly_used_inquiries_vars['changed_inquiry_status_to_properly_used'][all_vars_base_names[key]]+'_before'

            did_something_last_month_vars[key] = \
            'had_'+for_properly_used_inquiries_vars['changed_inquiry_status_to_properly_used'][all_vars_base_names[key]]+'_'+period_to_look_at_stopped_tmp

            did_something_before_and_didnt_last_month_vars[key] = \
            'had_'+for_properly_used_inquiries_vars['changed_inquiry_status_to_properly_used'][all_vars_base_names[key]]+'_before_and_didnt_'+period_to_look_at_stopped_tmp

    export_fields = get_fields_for_each_variable(all_vars=all_vars, all_vars_base_names=all_vars_base_names,\
                                                 did_something_before_vars=did_something_before_vars, \
                                                 did_something_last_month_vars=did_something_last_month_vars,\
                                                 did_something_before_and_didnt_last_month_vars=did_something_before_and_didnt_last_month_vars, \
                                                 changed_inquiry_status_to_properly_used_names=changed_inquiry_status_to_properly_used_names)
    export = pd.DataFrame(index = [x.split('_wo_151617')[0] for x in data_sets[0:1]],\
                         columns = export_fields)
    filename = data_sets[0].split('_wo_151617')[0]
    if testing_diff_parameters == True:
        df = \
        bucket.load_csv_from_s3(file_name='churn_analysis_based_on_behaviour/data/'+date_dir+'/'+\
                                dir_to_use+filename+'_wo_151617_'+\
                                data_tv_filename.split('.csv')[0].split('_')[-1]+'.csv')
    else:
        df = \
        bucket.load_csv_from_s3(file_name='churn_analysis_based_on_behaviour/data/'+date_dir+'/'+\
                                dir_to_use+filename+'_wo_151617.csv')

    for var_ in all_vars:
        df = df.merge(df[df[did_something_last_month_vars[var_]]==1].groupby('spot_id')['left_limit'].max().\
                      reset_index().rename(columns = {'left_limit':did_something_last_month_vars[var_]+'_last_month'}),\
             on = 'spot_id', how = 'left')
        df = df.merge(df[df[did_something_before_and_didnt_last_month_vars[var_]]==1].groupby('spot_id')['left_limit'].min().\
                      reset_index().rename(columns = {'left_limit':did_something_before_and_didnt_last_month_vars[var_]+'_first_month'}),\
                  on = 'spot_id', how = 'left')

        did_something_before_spots =\
        df[df[did_something_before_vars[var_]]==1]['spot_id'].unique()
        did_something_last_X_months_spots = \
        df[df[did_something_last_month_vars[var_]]==1]['spot_id'].unique()
        did_something_before_and_didnt_last_X_months_spots = \
        df[df[did_something_before_and_didnt_last_month_vars[var_]]==1]['spot_id'].unique()
        did_something_before_stopped_and_resumed_spots = \
        df[(df[did_something_last_month_vars[var_]+'_last_month']>\
           df[did_something_before_and_didnt_last_month_vars[var_]+'_first_month'])]['spot_id'].unique()


        export.loc[filename, did_something_before_vars[var_]] = len(did_something_before_spots)
        export.loc[filename, did_something_last_month_vars[var_]] = len(did_something_last_X_months_spots)
        export.loc[filename, did_something_before_and_didnt_last_month_vars[var_]] = \
        (len(did_something_before_and_didnt_last_X_months_spots), round(100*(len(did_something_before_and_didnt_last_X_months_spots)/len(did_something_before_spots)), 2))

        if 'changed' in var_.lower() and 'inquiry' in var_.lower():
            export.loc[filename, 'had_'+changed_inquiry_status_to_properly_used_names[all_vars_base_names[var_]]+'_before_stopped_and_resumed'] = \
            (len(did_something_before_stopped_and_resumed_spots), round(100*(len(did_something_before_stopped_and_resumed_spots)/len(did_something_before_and_didnt_last_X_months_spots)), 2))
        else:
            export.loc[filename, 'had_'+all_vars_base_names[var_]+'_before_stopped_and_resumed'] = \
            (len(did_something_before_stopped_and_resumed_spots), round(100*(len(did_something_before_stopped_and_resumed_spots)/len(did_something_before_and_didnt_last_X_months_spots)), 2))


    if not os.path.exists('data/'+date_dir+'/'+dir_to_use+'exports/'):
        os.makedirs('data/'+date_dir+'/'+dir_to_use+'exports/')

    export = export.transpose()

    if testing_diff_parameters == True:
        export.to_csv('data/'+date_dir+'/'+dir_to_use+'exports/started_stopped_resumed_spots_stats_'+\
                      data_tv_filename.split('.csv')[0].split('_')[-1]+'.csv')
        bucket.\
        store_csv_to_s3(data_frame = export, \
                        file_name = 'started_stopped_resumed_spots_stats_'+\
                      data_tv_filename.split('.csv')[0].split('_')[-1]+'.csv', \
                        dir = '/' + churn_based_on_behaviour_dir + 'data/'+date_dir+'/'+dir_to_use+'exports/')
    else:
        export.to_csv('data/'+date_dir+'/'+dir_to_use+'exports/started_stopped_resumed_spots_stats.csv')
        bucket.\
        store_csv_to_s3(data_frame = export, \
                        file_name = 'started_stopped_resumed_spots_stats.csv', \
                        dir = '/' + churn_based_on_behaviour_dir + 'data/'+date_dir+'/'+dir_to_use+'exports/')

    resumed_vars = [x for x in export.index.unique() if 'resumed' in x]

    more_than_20_perc = export.loc[resumed_vars][export.loc[resumed_vars][filename].apply(lambda x: x[1]>20)]
    less_than_20_perc = export.loc[resumed_vars][export.loc[resumed_vars][filename].apply(lambda x: x[1]<=20)]

    if testing_diff_parameters == True:
        more_than_20_perc.\
        to_csv('data/'+date_dir+'/'+dir_to_use+'exports/more_than_20_perc_spots_resumed_'+\
               data_tv_filename.split('.csv')[0].split('_')[-1]+'.csv')
        bucket.\
        store_csv_to_s3(data_frame = more_than_20_perc, \
                        file_name = 'more_than_20_perc_spots_resumed_'+\
                        data_tv_filename.split('.csv')[0].split('_')[-1]+'.csv', \
                        dir = '/' + churn_based_on_behaviour_dir + 'data/'+date_dir+'/'+dir_to_use+'exports/')

        less_than_20_perc.\
        to_csv('data/'+date_dir+'/'+dir_to_use+'exports/20_perc_or_less_spots_resumed_'+\
               data_tv_filename.split('.csv')[0].split('_')[-1]+'.csv')
        bucket.\
        store_csv_to_s3(data_frame = less_than_20_perc,\
                        file_name = '20_perc_or_less_spots_resumed_'+\
                        data_tv_filename.split('.csv')[0].split('_')[-1]+'.csv', \
                        dir = '/' + churn_based_on_behaviour_dir + 'data/'+date_dir+'/'+dir_to_use+'exports/')
    else:
        more_than_20_perc.\
        to_csv('data/'+date_dir+'/'+dir_to_use+'exports/more_than_20_perc_spots_resumed.csv')
        bucket.\
        store_csv_to_s3(data_frame = more_than_20_perc, \
                        file_name = 'more_than_20_perc_spots_resumed.csv', \
                        dir = '/' + churn_based_on_behaviour_dir + 'data/'+date_dir+'/'+dir_to_use+'exports/')

        less_than_20_perc.\
        to_csv('data/'+date_dir+'/'+dir_to_use+'exports/20_perc_or_less_spots_resumed.csv')
        bucket.\
        store_csv_to_s3(data_frame = less_than_20_perc,\
                        file_name = '20_perc_or_less_spots_resumed.csv', \
                        dir = '/' + churn_based_on_behaviour_dir + 'data/'+date_dir+'/'+dir_to_use+'exports/')

    return export

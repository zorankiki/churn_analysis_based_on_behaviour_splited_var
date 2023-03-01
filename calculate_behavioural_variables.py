import calculate_last_X_months_sum_variables
import pandas as pd
import get_all_vars
import numpy as np
import yaml

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
# dir_to_use = 'testing_different_period_to_look_at_threshold_values/'

vars_that_require_visiting_admin = ['Changed.post.picture.total', \
                                   'Changed.post.text.total', \
                                   # 'Changed.picture.or.text.total', \
                                   'Posts.disliked.total',\
                                   'Posts.liked.total',\
                                   'Posts.seen.total',\
                                   'Preview.page.views.facebook.total', \
                                   'Preview.page.views.email.total', \
                                   'Preview.page.views.twitter.total', \
                                   'Number.of.requests.for.new.text.fragment.total',\
                                   'Added.events.manually.Edited.events.total',\
                                   'Added.food.Edited.food.total',\
                                   'Added.specials.Edited.specials.total',\
                                   'Visited.events.page.total',\
                                   'Visited.food.page.total',\
                                   'Visited.inquiries.pages.total',\
                                    'Visited.special.page.total',\
                                    'Visited.stats.page.total',\
                                    'Visited.regular.flyers.page.total',\
                                    'Visited.qrcode.flyers.page.total',\
                                    'Downloaded.regular.flyers.total',\
                                    'Downloaded.qrcode.flyers.total',\
                                    'Text.fragment.suggestion.applied.total'
                                   ]

vars_that_require_preview_page_views = ['Changed.post.picture.total', \
                                   'Changed.post.text.total', \
                                   # 'Changed.picture.or.text.total', \
                                   'Posts.disliked.total',\
                                   'Posts.liked.total',\
                                   'Posts.seen.total',\
                                   'Number.of.requests.for.new.text.fragment.total'
                                   ]

vars_that_require_visiting_food_page = ['Added.food.Edited.food.total']

vars_that_require_visiting_specials_page = ['Added.specials.Edited.specials.total']

vars_that_require_visiting_qrcode_flyers_page = ['Downloaded.qrcode.flyers.total']

vars_that_require_visiting_regular_flyers_page = ['Downloaded.regular.flyers.total']


vars_of_interest = {'Visited.admin.total':vars_that_require_visiting_admin,\
                   'Preview.page.views.facebook.total':vars_that_require_preview_page_views,\
                   'Visited.food.page.total':vars_that_require_visiting_food_page,\
                   'Visited.special.page.total':vars_that_require_visiting_specials_page,\
                   'Visited.qrcode.flyers.page.total':vars_that_require_visiting_qrcode_flyers_page,\
                   'Visited.regular.flyers.page.total':vars_that_require_visiting_regular_flyers_page}

def get_key_based_on_value_in_a_dict(dict_, value_of_interest):
    for key, value in dict_.items():
        if value == value_of_interest:
            return key


def first_month_spot_did_something(spot, var_, var_to_use, number_of_months, threshold):
    return spot[(spot['time']>=number_of_months)&\
 (spot[var_to_use]>threshold)]['left_limit'].min()

def properly_used_inquiries_first_month(spot, var_, number_of_months, changed_status_var_to_use, inquiries_var_to_use, changed_status_threshold, inquiries_threshold):
    return spot[(spot['time']>=number_of_months)&\
 (spot[changed_status_var_to_use]>changed_status_threshold)&\
               (spot[inquiries_var_to_use]>inquiries_threshold)]['left_limit'].min()


def did_something_last_month(spot, var_, var_to_use, did_something_before_vars, threshold):
    df_res =  pd.DataFrame(spot[['time', var_to_use, did_something_before_vars[var_]]].\
    apply(lambda x: 1 if x[did_something_before_vars[var_]]==1 and x[var_to_use]>threshold else 0, axis = 1))

    return pd.concat([spot[['spot_id', 'time']], df_res], axis = 1)

def did_something_last_month_covid_correction(spot, var_, var_to_use, number_of_months, did_something_before_vars, threshold):
    df_res = pd.DataFrame(spot[['time', var_to_use, did_something_before_vars[var_]]].\
    apply(lambda x: 1 if x[did_something_before_vars[var_]]==1 and \
          x[var_to_use]>threshold else 0, axis = 1))

    return pd.concat([spot[['spot_id', 'time']], df_res], axis = 1)

def properly_used_inquiries_last_month(spot, var_, did_something_before_vars, changed_status_var_to_use, inquiries_var_to_use, changed_status_threshold, inquiries_threshold):
    df_res =  pd.DataFrame(spot[['time', did_something_before_vars[var_], changed_status_var_to_use, inquiries_var_to_use]].\
    apply(lambda x: 1 if x[did_something_before_vars[var_]]==1 and x[changed_status_var_to_use]>changed_status_threshold\
          and x[inquiries_var_to_use]>inquiries_threshold else 0, axis = 1))

    return pd.concat([spot[['spot_id', 'time']], df_res], axis = 1)

def properly_used_inquiries_last_month_covid_correction(spot, var_, number_of_months, did_something_before_vars, changed_status_var_to_use, inquiries_var_to_use, changed_status_threshold, inquiries_threshold):
    df_res = pd.DataFrame(spot[['time', did_something_before_vars[var_], changed_status_var_to_use, inquiries_var_to_use]].\
    apply(lambda x: 1 if x[did_something_before_vars[var_]]==1 and x[changed_status_var_to_use]>changed_status_threshold and\
               x[inquiries_var_to_use]>inquiries_threshold else 0, axis = 1))

    return pd.concat([spot[['spot_id', 'time']], df_res], axis = 1)


# def did_something_before_and_didnt_last_month(spot, var_, var_to_use, did_something_before_vars, threshold, downloaded_qrcode_flyers_period_to_look_at):
#     if var_ == 'Downloaded.qrcode.flyers.total':
#         df_res = pd.DataFrame(spot[['time', var_to_use, did_something_before_vars[var_],\
#                                    'qr_code_menu_scans_'+downloaded_qrcode_flyers_period_to_look_at+'_sum',\
#                                     'other_non_contactless_menu_qr_flyer_scans_'+downloaded_qrcode_flyers_period_to_look_at+'_sum']].\
#         apply(lambda x: 1 if x[did_something_before_vars[var_]]==1 and x[var_to_use]<=threshold and \
#               x['qr_code_menu_scans_'+downloaded_qrcode_flyers_period_to_look_at+'_sum']==0 and \
#               x['other_non_contactless_menu_qr_flyer_scans_'+downloaded_qrcode_flyers_period_to_look_at+'_sum']==0 else 0, axis = 1))
#     else:
#         df_res = pd.DataFrame(spot[['time', var_to_use, did_something_before_vars[var_]]].\
#         apply(lambda x: 1 if x[did_something_before_vars[var_]]==1 and x[var_to_use]<=threshold else 0, axis = 1))
#
#     return pd.concat([spot[['spot_id', 'time']], df_res], axis = 1)

def did_something_before_and_didnt_last_month(spot, var_, var_to_use, did_something_before_vars, threshold, downloaded_qrcode_flyers_period_to_look_at):
    df_res = pd.DataFrame(spot[['time', var_to_use, did_something_before_vars[var_]]].\
    apply(lambda x: 1 if x[did_something_before_vars[var_]]==1 and x[var_to_use]<=threshold else 0, axis = 1))

    return pd.concat([spot[['spot_id', 'time']], df_res], axis = 1)

def did_something_before_and_didnt_last_month_covid_correction(spot, var_, var_to_use, number_of_months, did_something_before_vars, did_something_last_month_vars, threshold):
    df_res = pd.DataFrame(spot[['time', var_to_use, did_something_before_vars[var_], did_something_last_month_vars[var_]]].\
    apply(lambda x: 1 if x[did_something_before_vars[var_]]==1 and \
          x[var_to_use]<=threshold else 0, axis = 1))

    return pd.concat([spot[['spot_id', 'time']], df_res], axis = 1)

def did_something_before_and_didnt_last_month_correction(spot, var_, var_to_use, did_something_before_vars, did_something_last_month_vars):
    df_res = pd.DataFrame(spot[['time', var_to_use, did_something_before_vars[var_], did_something_last_month_vars[var_]]].\
    apply(lambda x: 1 if x[did_something_before_vars[var_]]==1 and x[did_something_last_month_vars[var_]]==0 else 0, axis = 1))

    return pd.concat([spot[['spot_id', 'time']], df_res], axis = 1)

def properly_used_inquiries_before_and_didnt_last_month(spot, var_, did_something_before_vars, changed_status_var_to_use, inquiries_var_to_use, changed_status_threshold, inquiries_threshold):
    df_res = pd.DataFrame(spot[['time', did_something_before_vars[var_], changed_status_var_to_use, inquiries_var_to_use]].\
    apply(lambda x: 1 if x[did_something_before_vars[var_]]==1 and (x[changed_status_var_to_use]<=changed_status_threshold or\
                                                                    x[inquiries_var_to_use]<=inquiries_threshold) else 0, axis = 1))

    return pd.concat([spot[['spot_id', 'time']], df_res], axis = 1)

def properly_used_inquiries_before_and_didnt_last_month_covid_correction(spot, var_, number_of_months, did_something_before_vars, did_something_last_month_vars, changed_status_var_to_use, inquiries_var_to_use, changed_status_threshold, inquiries_threshold):
    df_res = pd.DataFrame(spot[['time', did_something_before_vars[var_], did_something_last_month_vars[var_], changed_status_var_to_use, inquiries_var_to_use]].\
    apply(lambda x: 1 if x[did_something_before_vars[var_]]==1\
          and (x[changed_status_var_to_use]<=changed_status_threshold or\
               x[inquiries_var_to_use]<=inquiries_threshold) else 0, axis = 1))

    return pd.concat([spot[['spot_id', 'time']], df_res], axis = 1)


def drop_unnecessary_columns(df):
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

    return cols_to_drop


def main(date_of_analysis, dir_to_use, period_to_look_at_thresholds_filename, data_tv_filename, vars_periods_to_look_at_thresholds_to_use_filename, testing_diff_parameters):
    date_dir = date_of_analysis.replace('-', '_')

    (df, vars_periods_to_look_at_thresholds_to_use) = \
    calculate_last_X_months_sum_variables.main(date_of_analysis=date_of_analysis, dir_to_use=dir_to_use, \
                                           period_to_look_at_thresholds_filename=period_to_look_at_thresholds_filename, data_tv_filename=data_tv_filename, \
                                           vars_periods_to_look_at_thresholds_to_use_filename=vars_periods_to_look_at_thresholds_to_use_filename)

    # df = bucket.\
    # load_csv_from_s3(file_name=churn_based_on_behaviour_dir + 'data/'+date_dir+'/'+dir_to_use+data_tv_filename)
    # vars_periods_to_look_at_thresholds_to_use = \
    # bucket.\
    # load_csv_from_s3(file_name=churn_based_on_behaviour_dir + 'data/'+date_dir+'/'+dir_to_use+vars_periods_to_look_at_thresholds_to_use_filename)


    with open(r'./parameters/for_properly_used_inquiries_vars.yaml') as file:
        for_properly_used_inquiries_vars = yaml.load(file, Loader=yaml.FullLoader)

    inquiries_vars_base_names = \
    for_properly_used_inquiries_vars['inquiries_vars_base_names']
    changed_inquiry_status_vars_base_names = \
    for_properly_used_inquiries_vars['changed_inquiry_status_vars_base_names']

    (all_vars, all_vars_base_names) = get_all_vars.main()

    ## did something before, did something last month, did something before and didnt last month ##
    first_month_spot_did_something_vars = dict(zip(all_vars_base_names.keys(), ['had_'+x+'_first_month' for x in all_vars_base_names.values()]))
    did_something_before_vars = dict(zip(all_vars_base_names.keys(), ['had_'+x+'_before' for x in all_vars_base_names.values()]))
    did_something_last_month_vars = dict(zip(vars_periods_to_look_at_thresholds_to_use['internal_variable_name'].unique(), vars_periods_to_look_at_thresholds_to_use[['variable_base_name', 'period_to_look_at_stopped']].\
    apply(lambda x: 'had_'+x['variable_base_name']+'_'+x['period_to_look_at_stopped'], axis = 1).values))
    did_something_before_and_didnt_last_month_vars = \
    dict(zip(vars_periods_to_look_at_thresholds_to_use['internal_variable_name'].unique(),\
            vars_periods_to_look_at_thresholds_to_use[['variable_base_name', 'period_to_look_at_stopped']].\
            apply(lambda x: 'had_'+x['variable_base_name']+'_before_and_didnt_'+x['period_to_look_at_stopped'], axis = 1).values))


    vars_periods_to_look_at_thresholds_to_use.\
    set_index('variable_base_name', inplace = True)

    downloaded_qrcode_flyers_period_to_look_at = \
    vars_periods_to_look_at_thresholds_to_use.loc['downloaded_qrcode_flyers']['period_to_look_at_stopped']

    for var_ in all_vars:
        if first_month_spot_did_something_vars[var_] in df.columns:
            df.drop(first_month_spot_did_something_vars[var_], axis = 1, inplace = True)

        if did_something_before_vars[var_] in df.columns:
            df.drop(did_something_before_vars[var_], axis = 1, inplace = True)

        if did_something_last_month_vars[var_] in df.columns:
            df.drop(did_something_last_month_vars[var_], axis = 1, inplace = True)

        if did_something_before_and_didnt_last_month_vars[var_] in df.columns:
            df.drop(did_something_before_and_didnt_last_month_vars[var_], axis = 1, inplace = True)


        if 'changed' in var_.lower() and 'inquiry' in var_.lower():
            print(var_)
            base_var_name = all_vars_base_names[var_]

            inquiry_type = \
            get_key_based_on_value_in_a_dict(dict_=changed_inquiry_status_vars_base_names, value_of_interest=base_var_name)

            changed_status_var_to_use_started = \
            vars_periods_to_look_at_thresholds_to_use.loc[all_vars_base_names[var_], 'var_to_use_started']
            changed_status_number_of_months_started = \
            vars_periods_to_look_at_thresholds_to_use.loc[all_vars_base_names[var_], 'number_of_months_started']
            changed_status_threshold_started = \
            vars_periods_to_look_at_thresholds_to_use.loc[all_vars_base_names[var_], 'threshold_started']

            inquiries_var_to_use_started = \
            vars_periods_to_look_at_thresholds_to_use.loc[inquiries_vars_base_names[inquiry_type], 'var_to_use_started']
            inquiries_number_of_months_started = \
            vars_periods_to_look_at_thresholds_to_use.loc[inquiries_vars_base_names[inquiry_type], 'number_of_months_started']
            inquiries_threshold_started = \
            vars_periods_to_look_at_thresholds_to_use.loc[inquiries_vars_base_names[inquiry_type], 'threshold_started']

            ### before ###
            df = df.merge(df.groupby('spot_id')[['time', changed_status_var_to_use_started, inquiries_var_to_use_started, 'left_limit']].\
            apply(lambda x: properly_used_inquiries_first_month(spot=x, var_=var_, number_of_months=changed_status_number_of_months_started, changed_status_var_to_use=changed_status_var_to_use_started, inquiries_var_to_use=inquiries_var_to_use_started, changed_status_threshold=changed_status_threshold_started, inquiries_threshold=inquiries_threshold_started)).reset_index().\
            rename(columns = {0:first_month_spot_did_something_vars[var_]}), on = 'spot_id', how = 'left')

            df.loc[(df['left_limit']>=df[first_month_spot_did_something_vars[var_]]), did_something_before_vars[var_]] = 1
            df[did_something_before_vars[var_]].fillna(0, inplace = True)

            changed_status_var_to_use_stopped = \
            vars_periods_to_look_at_thresholds_to_use.loc[all_vars_base_names[var_], 'var_to_use_stopped']
            changed_status_number_of_months_stopped = \
            vars_periods_to_look_at_thresholds_to_use.loc[all_vars_base_names[var_], 'number_of_months_stopped']
            changed_status_threshold_stopped = \
            vars_periods_to_look_at_thresholds_to_use.loc[all_vars_base_names[var_], 'threshold_stopped']

            inquiries_var_to_use_stopped = \
            vars_periods_to_look_at_thresholds_to_use.loc[inquiries_vars_base_names[inquiry_type], 'var_to_use_stopped']
            inquiries_number_of_months_stopped = \
            vars_periods_to_look_at_thresholds_to_use.loc[inquiries_vars_base_names[inquiry_type], 'number_of_months_stopped']
            inquiries_threshold_stopped = \
            vars_periods_to_look_at_thresholds_to_use.loc[inquiries_vars_base_names[inquiry_type], 'threshold_stopped']

            ### last X months ###
            df = df.merge(df.groupby('spot_id')[['spot_id', 'time', changed_status_var_to_use_stopped, inquiries_var_to_use_stopped, did_something_before_vars[var_]]].\
            apply(lambda x: properly_used_inquiries_last_month(spot=x, var_=var_, did_something_before_vars=did_something_before_vars, changed_status_var_to_use=changed_status_var_to_use_stopped, inquiries_var_to_use=inquiries_var_to_use_stopped, changed_status_threshold=changed_status_threshold_stopped, inquiries_threshold=inquiries_threshold_stopped)).\
                                rename(columns = {0:did_something_last_month_vars[var_]}),\
                                on  = ['spot_id', 'time'], how = 'left')


            ### before and didnt last X months ###
            df = df.merge(df.groupby('spot_id')[['spot_id', 'time', changed_status_var_to_use_stopped, inquiries_var_to_use_stopped, did_something_before_vars[var_]]].\
            apply(lambda x: properly_used_inquiries_before_and_didnt_last_month(spot=x, var_=var_, did_something_before_vars=did_something_before_vars, changed_status_var_to_use=changed_status_var_to_use_stopped, inquiries_var_to_use=inquiries_var_to_use_stopped, changed_status_threshold=changed_status_threshold_stopped, inquiries_threshold=inquiries_threshold_stopped)).\
                                rename(columns = {0:did_something_before_and_didnt_last_month_vars[var_]}),\
                                on  = ['spot_id', 'time'], how = 'left')

            ## corrections to the did something before and didnt last X months variable during Covid period ##
            covid_period_cols = [x for x in vars_periods_to_look_at_thresholds_to_use.columns if 'covid_period_to_look_at' in x]

            for i in range(0, len(covid_period_cols)):
                if vars_periods_to_look_at_thresholds_to_use[vars_periods_to_look_at_thresholds_to_use['internal_variable_name']==var_]['covid_period_to_look_at_stopped_'+str(i+1)].values[0]==vars_periods_to_look_at_thresholds_to_use[vars_periods_to_look_at_thresholds_to_use['internal_variable_name']==var_]['covid_period_to_look_at_stopped_'+str(i+1)].values[0]:
                    print(i)
                    print(var_)
                    changed_status_var_to_use_stopped = \
                    vars_periods_to_look_at_thresholds_to_use.loc[all_vars_base_names[var_], 'var_to_use_stopped_'+str(i+1)]
                    changed_status_number_of_months_stopped = \
                    vars_periods_to_look_at_thresholds_to_use.loc[all_vars_base_names[var_], 'covid_number_of_months_stopped_'+str(i+1)]
                    changed_status_threshold_stopped = \
                    vars_periods_to_look_at_thresholds_to_use.loc[all_vars_base_names[var_], 'covid_threshold_stopped_'+str(i+1)]

                    inquiries_var_to_use_stopped = \
                    vars_periods_to_look_at_thresholds_to_use.loc[inquiries_vars_base_names[inquiry_type], 'var_to_use_stopped_'+str(i+1)]
                    inquiries_number_of_months_stopped = \
                    vars_periods_to_look_at_thresholds_to_use.loc[inquiries_vars_base_names[inquiry_type], 'covid_number_of_months_stopped_'+str(i+1)]
                    inquiries_threshold_stopped = \
                    vars_periods_to_look_at_thresholds_to_use.loc[inquiries_vars_base_names[inquiry_type], 'covid_threshold_stopped_'+str(i+1)]


                    period_start = \
                    vars_periods_to_look_at_thresholds_to_use.loc[all_vars_base_names[var_], 'covid_period_start_'+str(i+1)]
                    period_end = \
                    vars_periods_to_look_at_thresholds_to_use.loc[all_vars_base_names[var_], 'covid_period_end_'+str(i+1)]

                    df = df.merge(df[(df['left_limit']>=period_start)&\
                      (df['left_limit']<=period_end)].groupby('spot_id')[['spot_id', 'time', changed_status_var_to_use_stopped, inquiries_var_to_use_stopped, did_something_before_vars[var_], did_something_last_month_vars[var_]]].\
                    apply(lambda x: properly_used_inquiries_before_and_didnt_last_month_covid_correction(x, var_, changed_status_number_of_months_stopped, did_something_before_vars, did_something_last_month_vars, changed_status_var_to_use_stopped, inquiries_var_to_use_stopped,\
                                                                                              changed_status_threshold_stopped, inquiries_threshold_stopped)).\
                    rename(columns = {0:did_something_before_and_didnt_last_month_vars[var_]+'_covid_correction'}),\
                             on = ['spot_id', 'time'], how = 'left')


                    df = df.merge(df[(df['left_limit']>=period_start)&\
                      (df['left_limit']<=period_end)].groupby('spot_id')[['spot_id', 'time', changed_status_var_to_use_stopped, inquiries_var_to_use_stopped, did_something_before_vars[var_]]].\
                    apply(lambda x: properly_used_inquiries_last_month_covid_correction(x, var_, changed_status_number_of_months_stopped, did_something_before_vars, changed_status_var_to_use_stopped, inquiries_var_to_use_stopped,\
                                                                                              changed_status_threshold_stopped, inquiries_threshold_stopped)).\
                    rename(columns = {0:did_something_last_month_vars[var_]+'_covid_correction'}),\
                             on = ['spot_id', 'time'], how = 'left')


                    df.loc[(df[did_something_before_and_didnt_last_month_vars[var_]+'_covid_correction'].isnull()),\
                          did_something_before_and_didnt_last_month_vars[var_]+'_covid_correction'] = \
                    df.loc[(df[did_something_before_and_didnt_last_month_vars[var_]+'_covid_correction'].isnull()),\
                          did_something_before_and_didnt_last_month_vars[var_]]

                    df[did_something_before_and_didnt_last_month_vars[var_]] = \
                    df[did_something_before_and_didnt_last_month_vars[var_]+'_covid_correction']
                    df.drop(did_something_before_and_didnt_last_month_vars[var_]+'_covid_correction', axis = 1, inplace = True)


                    df.loc[(df[did_something_last_month_vars[var_]+'_covid_correction'].isnull()),\
                           did_something_last_month_vars[var_]+'_covid_correction'] = \
                    df.loc[(df[did_something_last_month_vars[var_]+'_covid_correction'].isnull()),\
                          did_something_last_month_vars[var_]]

                    df[did_something_last_month_vars[var_]] = \
                    df[did_something_last_month_vars[var_]+'_covid_correction']
                    df.drop(did_something_last_month_vars[var_]+'_covid_correction', axis = 1, inplace = True)

            ### rename changed inquiry status to properly used ###
            df.rename(columns = {did_something_before_vars[var_]:'had_properly_used_'+inquiry_type+'_inquiries_before'},\
                     inplace = True)
            df.rename(columns = {did_something_last_month_vars[var_]:'had_properly_used_'+inquiry_type+'_inquiries_last'+did_something_last_month_vars[var_].split('_last')[1]},\
                                inplace = True)
            df.rename(columns = {did_something_before_and_didnt_last_month_vars[var_]:'had_properly_used_'+inquiry_type+'_inquiries_before_and_didnt_last'+did_something_before_and_didnt_last_month_vars[var_].split('_last')[1]}, \
                       inplace = True)
        else:
            ### before ###
            var_to_use_started = \
            vars_periods_to_look_at_thresholds_to_use.loc[all_vars_base_names[var_], 'var_to_use_started']
            number_of_months_started = \
            vars_periods_to_look_at_thresholds_to_use.loc[all_vars_base_names[var_], 'number_of_months_started']
            threshold_started = \
            vars_periods_to_look_at_thresholds_to_use.loc[all_vars_base_names[var_], 'threshold_started']

            df = df.merge(df.groupby('spot_id')[['time', var_to_use_started, 'left_limit']].\
            apply(lambda x: first_month_spot_did_something(x, var_, var_to_use_started, number_of_months_started, threshold_started)).reset_index().\
            rename(columns = {0:first_month_spot_did_something_vars[var_]}), on = 'spot_id', how = 'left')

            df.loc[(df['left_limit']>=df[first_month_spot_did_something_vars[var_]]), did_something_before_vars[var_]] = 1
            df[did_something_before_vars[var_]].fillna(0, inplace = True)

            var_to_use_stopped = \
            vars_periods_to_look_at_thresholds_to_use.loc[all_vars_base_names[var_], 'var_to_use_stopped']
            number_of_months_stopped = \
            vars_periods_to_look_at_thresholds_to_use.loc[all_vars_base_names[var_], 'number_of_months_stopped']
            threshold_stopped = \
            vars_periods_to_look_at_thresholds_to_use.loc[all_vars_base_names[var_], 'threshold_stopped']

            ### last X months ###
            df = df.merge(df.groupby('spot_id')[['spot_id', 'time', var_to_use_stopped, did_something_before_vars[var_]]].\
            apply(lambda x: did_something_last_month(x, var_, var_to_use_stopped, did_something_before_vars, threshold_stopped)).\
                                    rename(columns = {0:did_something_last_month_vars[var_]}),\
                                    on  = ['spot_id', 'time'], how = 'left')


            ### before and didnt last X months ###
            df = df.merge(df.groupby('spot_id')[['spot_id', 'time', var_to_use_stopped, did_something_before_vars[var_],\
                                                'qr_code_menu_scans_'+downloaded_qrcode_flyers_period_to_look_at+'_sum',\
                                                 'other_non_contactless_menu_qr_flyer_scans_'+downloaded_qrcode_flyers_period_to_look_at+'_sum']].\
            apply(lambda x: did_something_before_and_didnt_last_month(x, var_, var_to_use_stopped, did_something_before_vars, threshold_stopped,\
                                                                     downloaded_qrcode_flyers_period_to_look_at)).\
                                    rename(columns = {0:did_something_before_and_didnt_last_month_vars[var_]}),\
                                    on  = ['spot_id', 'time'], how = 'left')

            ## corrections to the did something before and didnt last X months variable during Covid period ##
            covid_period_cols = [x for x in vars_periods_to_look_at_thresholds_to_use.columns if 'covid_period_to_look_at' in x]

            for i in range(0, len(covid_period_cols)):
                if vars_periods_to_look_at_thresholds_to_use[vars_periods_to_look_at_thresholds_to_use['internal_variable_name']==var_]['covid_period_to_look_at_stopped_'+str(i+1)].values[0]==vars_periods_to_look_at_thresholds_to_use[vars_periods_to_look_at_thresholds_to_use['internal_variable_name']==var_]['covid_period_to_look_at_stopped_'+str(i+1)].values[0]:
                    print(i)
                    print(var_)
                    var_to_use_stopped = \
                    vars_periods_to_look_at_thresholds_to_use.loc[all_vars_base_names[var_], 'var_to_use_stopped_'+str(i+1)]
                    number_of_months_stopped = \
                    vars_periods_to_look_at_thresholds_to_use.loc[all_vars_base_names[var_], 'covid_number_of_months_stopped_'+str(i+1)]
                    threshold_stopped = \
                    vars_periods_to_look_at_thresholds_to_use.loc[all_vars_base_names[var_], 'covid_threshold_stopped_'+str(i+1)]

                    period_start = \
                    vars_periods_to_look_at_thresholds_to_use.loc[all_vars_base_names[var_], 'covid_period_start_'+str(i+1)]
                    period_end = \
                    vars_periods_to_look_at_thresholds_to_use.loc[all_vars_base_names[var_], 'covid_period_end_'+str(i+1)]

                    if did_something_before_and_didnt_last_month_vars[var_]+'_covid_correction' in df.columns:
                        df.drop(did_something_before_and_didnt_last_month_vars[var_]+'_covid_correction', axis = 1, inplace = True)
                    if did_something_last_month_vars[var_]+'_covid_correction' in df.columns:
                        df.drop(did_something_last_month_vars[var_]+'_covid_correction', axis = 1, inplace = True)

                    df = df.merge(df[(df['left_limit']>=period_start)&\
                      (df['left_limit']<=period_end)].groupby('spot_id')[['spot_id', 'time', var_to_use_stopped, did_something_before_vars[var_], did_something_last_month_vars[var_]]].\
                    apply(lambda x: did_something_before_and_didnt_last_month_covid_correction(x, var_, var_to_use_stopped, number_of_months_stopped, did_something_before_vars, did_something_last_month_vars, threshold_stopped)).\
                    rename(columns = {0:did_something_before_and_didnt_last_month_vars[var_]+'_covid_correction'}),\
                             on = ['spot_id', 'time'], how = 'left')

                    df = df.merge(df[(df['left_limit']>=period_start)&\
                      (df['left_limit']<=period_end)].groupby('spot_id')[['spot_id', 'time', var_to_use_stopped, did_something_before_vars[var_]]].\
                    apply(lambda x: did_something_last_month_covid_correction(x, var_, var_to_use_stopped, number_of_months_stopped, did_something_before_vars, threshold_stopped)).\
                    rename(columns = {0:did_something_last_month_vars[var_]+'_covid_correction'}),\
                             on = ['spot_id', 'time'], how = 'left')


                    df.loc[(df[did_something_before_and_didnt_last_month_vars[var_]+'_covid_correction'].isnull()),\
                          did_something_before_and_didnt_last_month_vars[var_]+'_covid_correction'] = \
                    df.loc[(df[did_something_before_and_didnt_last_month_vars[var_]+'_covid_correction'].isnull()),\
                          did_something_before_and_didnt_last_month_vars[var_]]

                    df[did_something_before_and_didnt_last_month_vars[var_]] = \
                    df[did_something_before_and_didnt_last_month_vars[var_]+'_covid_correction']
                    df.drop(did_something_before_and_didnt_last_month_vars[var_]+'_covid_correction', axis = 1, inplace = True)


                    df.loc[(df[did_something_last_month_vars[var_]+'_covid_correction'].isnull()),\
                          did_something_last_month_vars[var_]+'_covid_correction'] = \
                    df.loc[(df[did_something_last_month_vars[var_]+'_covid_correction'].isnull()),\
                          did_something_last_month_vars[var_]]

                    df[did_something_last_month_vars[var_]] = \
                    df[did_something_last_month_vars[var_]+'_covid_correction']
                    df.drop(did_something_last_month_vars[var_]+'_covid_correction', axis = 1, inplace = True)

    ## save df before corrections ##
    if testing_diff_parameters == True:
        bucket.store_csv_to_s3(data_frame = df, \
        file_name = 'data_tv_with_maybe_problematic_spots_all_before_corrections_'+\
                               data_tv_filename.split('.csv')[0].split('_')[-1]+'.csv', \
        dir = '/' + churn_based_on_behaviour_dir + 'data/'+date_dir+'/'+dir_to_use)
    else:
        bucket.store_csv_to_s3(data_frame = df, \
        file_name = 'data_tv_with_maybe_problematic_spots_all_before_corrections.csv', \
        dir = '/' + churn_based_on_behaviour_dir + 'data/'+date_dir+'/'+dir_to_use)


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

    ## fill in missing mixpanel events ##
    # for the did something before variables #
    for key in vars_of_interest.keys():
        df.loc[(df[did_something_before_vars[key]]==0)&\
          (df[[did_something_before_vars[x] for x in vars_of_interest[key]]].\
          apply(lambda x: np.any(np.array(x)==1), axis = 1)), did_something_before_vars[key]] = 1

    # for the did something last X months variables #
    number_of_months_stopped_dict = \
    dict(zip(vars_periods_to_look_at_thresholds_to_use['internal_variable_name'].tolist(),\
            vars_periods_to_look_at_thresholds_to_use['number_of_months_stopped'].tolist()))

    for key in vars_of_interest.keys():
        vars_to_take_into_account = [x for x in vars_of_interest[key] if number_of_months_stopped_dict[x]<=number_of_months_stopped_dict[key]]
        if len(vars_to_take_into_account)>0:
            df.loc[(df[did_something_last_month_vars[key]]==0)&\
                  (df[[did_something_last_month_vars[x] for x in vars_to_take_into_account]].\
                  apply(lambda x: np.any(np.array(x)==1), axis = 1)), did_something_last_month_vars[key]] = 1

    for var_ in vars_of_interest.keys():

        var_to_use_stopped = \
        vars_periods_to_look_at_thresholds_to_use.loc[all_vars_base_names[var_], 'var_to_use_stopped']
        number_of_months_stopped = \
        vars_periods_to_look_at_thresholds_to_use.loc[all_vars_base_names[var_], 'number_of_months_stopped']
        threshold_stopped = \
        vars_periods_to_look_at_thresholds_to_use.loc[all_vars_base_names[var_], 'threshold_stopped']


        if did_something_before_and_didnt_last_month_vars[var_] in df.columns:
            df.drop(did_something_before_and_didnt_last_month_vars[var_], axis = 1, inplace = True)

        df = df.merge(df.groupby('spot_id')[['spot_id', 'time', var_to_use_stopped, did_something_before_vars[var_], did_something_last_month_vars[var_]]].\
        apply(lambda x: did_something_before_and_didnt_last_month_correction(x, var_, var_to_use_stopped, did_something_before_vars, \
                                                                             did_something_last_month_vars)).\
                            rename(columns = {0:did_something_before_and_didnt_last_month_vars[var_]}),\
                            on  = ['spot_id', 'time'], how = 'left')

    ### add behavioural vars to all data sets ###
    behavioural_vars = list(first_month_spot_did_something_vars.values())+list(did_something_before_vars.values())+list(did_something_last_month_vars.values())+\
    list(did_something_before_and_didnt_last_month_vars.values())

    for spots_set in ['ALL', 'CAN_CANCEL']:
        for with_wo_CB in ['with_CB', 'wo_CB']:
            df_subset = bucket.load_csv_from_s3(file_name = 'churn_analysis/data/' + date_dir +\
                             '/data_tv_'+spots_set+'_spots_'+with_wo_CB+'_wo_151617.csv')
            cols_to_drop = drop_unnecessary_columns(df_subset)
            df_subset.drop(cols_to_drop, axis = 1, inplace = True)
            df_subset = df_subset.merge(df[['spot_id', 'left_limit']+behavioural_vars],\
               on = ['spot_id', 'left_limit'])

            ## save each data set ##
            if testing_diff_parameters == True:
                bucket.store_csv_to_s3(data_frame = df_subset, \
                file_name = 'data_tv_'+spots_set+'_spots_'+with_wo_CB+'_wo_151617_'+\
                                       data_tv_filename.split('.csv')[0].split('_')[-1]+'.csv', \
                dir = '/' + churn_based_on_behaviour_dir + 'data/'+date_dir+'/'+dir_to_use)
            else:
                bucket.store_csv_to_s3(data_frame = df_subset, \
                file_name = 'data_tv_'+spots_set+'_spots_'+with_wo_CB+'_wo_151617.csv', \
                dir = '/' + churn_based_on_behaviour_dir + 'data/'+date_dir+'/'+dir_to_use)

    spots_set = 'ALL'
    with_wo_CB = 'with_CB'
    df_subset = bucket.load_csv_from_s3(file_name = 'churn_analysis/data/' + date_dir +\
                             '/data_tv_'+spots_set+'_spots_'+with_wo_CB+'_wo_151617.csv')

    cols_to_drop = drop_unnecessary_columns(df_subset)
    df_subset.drop(cols_to_drop, axis = 1, inplace = True)
    df_subset = df_subset.merge(df[['spot_id', 'left_limit']+behavioural_vars],\
       on = ['spot_id', 'left_limit'])

    ## save each data set ##
    if testing_diff_parameters == True:
        bucket.store_csv_to_s3(data_frame = df_subset, \
        file_name = 'data_tv_'+spots_set+'_spots_'+with_wo_CB+'_wo_151617_'+\
                               data_tv_filename.split('.csv')[0].split('_')[-1]+'.csv', \
        dir = '/' + churn_based_on_behaviour_dir + 'data/'+date_dir+'/'+dir_to_use)
    else:
        bucket.store_csv_to_s3(data_frame = df_subset, \
        file_name = 'data_tv_'+spots_set+'_spots_'+with_wo_CB+'_wo_151617.csv', \
        dir = '/' + churn_based_on_behaviour_dir + 'data/'+date_dir+'/'+dir_to_use)



    ## save df with vars to be used to s3 ##
    if testing_diff_parameters == True:
        bucket.store_csv_to_s3(data_frame = df, \
        file_name = 'data_tv_with_maybe_problematic_spots_all_'+\
                               data_tv_filename.split('.csv')[0].split('_')[-1]+'.csv', \
        dir = '/' + churn_based_on_behaviour_dir + 'data/'+date_dir+'/'+dir_to_use)
    else:
        bucket.store_csv_to_s3(data_frame = df, \
        file_name = 'data_tv_with_maybe_problematic_spots_all.csv', \
        dir = '/' + churn_based_on_behaviour_dir + 'data/'+date_dir+'/'+dir_to_use)

    return df
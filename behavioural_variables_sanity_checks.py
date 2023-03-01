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


def main(date_of_analysis):
    date_dir = date_of_analysis.replace('-', '_')

    df = bucket.load_csv_from_s3(file_name = 'churn_analysis_based_on_behaviour/data/' + date_dir +\
                         '/data_tv_with_maybe_problematic_spots_all.csv')


    df_orig = bucket.load_csv_from_s3(file_name = 'churn_analysis_based_on_behaviour/data/' + date_dir +\
                                 '/data_tv_with_maybe_problematic_spots_all_before_corrections.csv')

    with open(r'./parameters/for_properly_used_inquiries_vars.yaml') as file:
        for_properly_used_inquiries_vars = yaml.load(file, Loader=yaml.FullLoader)

    inquiries_vars_base_names = \
    for_properly_used_inquiries_vars['inquiries_vars_base_names']
    changed_inquiry_status_vars_base_names = \
    for_properly_used_inquiries_vars['changed_inquiry_status_vars_base_names']
    changed_inquiry_status_to_properly_used_names = for_properly_used_inquiries_vars['changed_inquiry_status_to_properly_used']

    (all_vars, all_vars_base_names) = get_all_vars.main()

    vars_periods_to_look_at_thresholds_to_use = \
    bucket.load_csv_from_s3(file_name = 'churn_analysis_based_on_behaviour/data/' + date_dir +\
                                 '/vars_periods_to_look_at_thresholds_to_use.csv')

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

    behavioural_vars = list(first_month_spot_did_something_vars.values())+list(did_something_before_vars.values())+list(did_something_last_month_vars.values())+\
    list(did_something_before_and_didnt_last_month_vars.values())

    df_orig_test = df_orig.copy()
    df_orig_test = df_orig_test[['spot_id', 'left_limit', 'time'] + all_vars + behavioural_vars]
    for col in all_vars + behavioural_vars:
        df_orig_test.rename(columns = {col:col+'_old'}, inplace = True)

    df_test = df.copy()
    df_test = df_test[['spot_id', 'left_limit', 'time'] + all_vars + behavioural_vars]
    df_test = df_orig_test.merge(df_test, on = ['spot_id', 'left_limit', 'time'])

    #################### SANITY CHECKS #####################################
    print('Are the data frames the same lenght before and after corrections?')
    if len(df)==len(df_orig):
        print('YES!')
    else:
        print('NO!!!')

    print('Are all variables with different did something before variables among admin variables of interest?')
    vars_check = []
    for var_ in all_vars:
        if len(df_test[(df_test[did_something_before_vars[var_]+'_old']!=df_test[did_something_before_vars[var_]])])>0:
            vars_check.append(var_)
    if len([x for x in vars_check if x not in vars_of_interest])==0:
        print('YES!')
    else:
        print('NO!!!')

    print('Are all new values for the did something before variables 1?')
    new_values = []
    for var_ in all_vars:
        if len(df_test[(df_test[did_something_before_vars[var_]+'_old']!=df_test[did_something_before_vars[var_]])])>0:
            new_values.extend(df_test[(df_test[did_something_before_vars[var_]+'_old']!=\
                                       df_test[did_something_before_vars[var_]])][did_something_before_vars[var_]].unique())
    if len([x for x in new_values if x!=1])==0:
        print('YES!')
    else:
        print('NO!!!')

    # for the did something last X months variables #
    number_of_months_started_dict = \
    dict(zip(vars_periods_to_look_at_thresholds_to_use['internal_variable_name'].tolist(),\
            vars_periods_to_look_at_thresholds_to_use['number_of_months_started'].tolist()))
    print('Do all dependent variables for last X months have lower number of months than the variable of interest?')
    not_okay_flag = []
    for var_ in all_vars:
        if len(df_test[(df_test[did_something_last_month_vars[var_]+'_old']!=df_test[did_something_last_month_vars[var_]])])>0:
            if len([number_of_months_started_dict[x] for x in vars_of_interest[var_] if number_of_months_started_dict[x]>number_of_months_started_dict[var_]])>0:
                   not_okay_flag.append(1)
    if len(not_okay_flag)==0:
        print('YES!')
    else:
        print('NO!!!')

    print('Are all new values for did something last X months 1?')
    new_values = []
    for var_ in all_vars:
        if len(df_test[(df_test[did_something_last_month_vars[var_]+'_old']!=df_test[did_something_last_month_vars[var_]])])>0:
            new_values.extend(df_test[(df_test[did_something_last_month_vars[var_]+'_old']!=\
                            df_test[did_something_last_month_vars[var_]])][did_something_last_month_vars[var_]].unique())
    if len([x for x in new_values if x!=1])==0:
        print('YES!')
    else:
        print('NO!!!')

    print('Are there any cases where did something last X months = 1 and did something before = 0?')
    faulty_records = []
    for var_ in all_vars:
        if len(df[(df[did_something_last_month_vars[var_]]==1)&\
          (df[did_something_before_vars[var_]]==0)])>0:
            faulty_records.extend(df[(df[did_something_last_month_vars[var_]]==1)&\
          (df[did_something_before_vars[var_]]==0)].index.unique())
    if len(faulty_records)==0:
        print('NO!')
    else:
        print('YES!!!')

    print('Are there any cases where did something before and didnt last X months = 1 and did something before = 0?')
    faulty_records = []
    for var_ in all_vars:
        if len(df[(df[did_something_before_and_didnt_last_month_vars[var_]]==1)&\
          (df[did_something_before_vars[var_]]==0)])>0:
            faulty_records.extend(df[(df[did_something_before_and_didnt_last_month_vars[var_]]==1)&\
          (df[did_something_before_vars[var_]]==0)].index.unique())
    if len(faulty_records)==0:
        print('NO!')
    else:
        print('YES!!!')

    print('Are all variables with different did something before and didnt last X months values among variables of interest?')
    vars_check = []
    for var_ in all_vars:
        if len(df_test[(df_test[did_something_before_and_didnt_last_month_vars[var_]+'_old']!=df_test[did_something_before_and_didnt_last_month_vars[var_]])])>0:
            vars_check.append(var_)
    if len([x for x in vars_check if x not in vars_of_interest])==0:
        print('YES!')
    else:
        print('NO!!!')

    print('Are there any records where there is a different did something before and didnt last month variable and all others are the same?')
    faulty_records = []
    for var_ in all_vars:
        if len(df_test[(df_test[did_something_before_and_didnt_last_month_vars[var_]+'_old']!=df_test[did_something_before_and_didnt_last_month_vars[var_]])&\
            (df_test[did_something_before_vars[var_]+'_old']==df_test[did_something_before_vars[var_]])&\
            (df_test[did_something_last_month_vars[var_]+'_old']==df_test[did_something_last_month_vars[var_]])])>0:
            print(var_)
            faulty_records.extend(df_test[(df_test[did_something_before_and_didnt_last_month_vars[var_]+'_old']!=df_test[did_something_before_and_didnt_last_month_vars[var_]])&\
            (df_test[did_something_before_vars[var_]+'_old']==df_test[did_something_before_vars[var_]])&\
            (df_test[did_something_last_month_vars[var_]+'_old']==df_test[did_something_last_month_vars[var_]])])
    if len(faulty_records)==0:
        print('NO!')
    else:
        print('YES!!!')

    print('Are there any records where did something before = 1 and did something last X months = 0 in the first month the before var becomes 1?')
    faulty_records = []
    for var_ in all_vars:
        if len(df_test[(df_test[did_something_before_vars[var_]]==1)&\
           (df_test[did_something_last_month_vars[var_]]==0)&\
           (df_test[first_month_spot_did_something_vars[var_]]==df_test['left_limit'])])>0:
            faulty_records.extend(df_test[(df_test[did_something_before_vars[var_]]==1)&\
           (df_test[did_something_last_month_vars[var_]]==0)&\
           (df_test[first_month_spot_did_something_vars[var_]]==df_test['left_limit'])].index.unique())
    if len(faulty_records)==0:
        print('NO!')
    else:
        print('YES!!!')

    print('Are there any records where the did_something_before is 1 and the other 2 are both 0?')
    # faulty_records = dict.fromkeys(all_vars)
    faulty_records = []
    for var_ in all_vars:
    #     faulty_records[var_] = []
        if len(df[(df[did_something_before_and_didnt_last_month_vars[var_]]==0)&\
                  (df[did_something_last_month_vars[var_]]==0)&\
                 (df[did_something_before_vars[var_]]==1)])>0:
            faulty_records.append(df[(df[did_something_before_and_didnt_last_month_vars[var_]]==0)&\
                  (df[did_something_last_month_vars[var_]]==0)&\
                                          (df[did_something_before_vars[var_]]==1)].index)
    #         faulty_records[var_].extend(df[(df[did_something_before_and_didnt_last_month_vars[var_]]==0)&\
    #               (df[did_something_last_month_vars[var_]]==0)&\
    #                                       (df[did_something_before_vars[var_]]==1)].index)

    if len(faulty_records)>0:
        print('YES!!!')
    else:
        print('NO!')

    print('Value counts for new vars for records where there is a different did something before and didnt last X months value: ')
    faulty_records = []
    for var_ in all_vars:
        if len(df_test[(df_test[did_something_before_and_didnt_last_month_vars[var_]+'_old']!=df_test[did_something_before_and_didnt_last_month_vars[var_]])])>0:
            print(var_)
            print(len(df_test[(df_test[did_something_before_and_didnt_last_month_vars[var_]+'_old']!=df_test[did_something_before_and_didnt_last_month_vars[var_]])]))
            print('did something last X months: ')
            print(df_test[(df_test[did_something_before_and_didnt_last_month_vars[var_]+'_old']!=\
                           df_test[did_something_before_and_didnt_last_month_vars[var_]])][did_something_last_month_vars[var_]].value_counts())
            print('did something before and didnt last X months: ')
            print(df_test[(df_test[did_something_before_and_didnt_last_month_vars[var_]+'_old']!=\
                           df_test[did_something_before_and_didnt_last_month_vars[var_]])][did_something_before_and_didnt_last_month_vars[var_]].value_counts())

            if len(df_test[(df_test[did_something_before_and_didnt_last_month_vars[var_]+'_old']!=df_test[did_something_before_and_didnt_last_month_vars[var_]])])!=\
            (df_test[(df_test[did_something_before_and_didnt_last_month_vars[var_]+'_old']!=\
                           df_test[did_something_before_and_didnt_last_month_vars[var_]])][did_something_last_month_vars[var_]].value_counts().sum()):
                faulty_records.append(1)
            if len(df_test[(df_test[did_something_before_and_didnt_last_month_vars[var_]+'_old']!=df_test[did_something_before_and_didnt_last_month_vars[var_]])])!=\
            (df_test[(df_test[did_something_before_and_didnt_last_month_vars[var_]+'_old']!=\
                           df_test[did_something_before_and_didnt_last_month_vars[var_]])][did_something_before_and_didnt_last_month_vars[var_]].value_counts().sum()):
                faulty_records.append(1)
    print('*****************')
    if len(faulty_records)==0:
        print('OK!')
    else:
        print('NOT OK!!!')
    print('*****************')

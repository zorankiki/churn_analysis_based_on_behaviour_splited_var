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

def main(date_of_analysis, variables_to_use_for_the_model = []):
    date_dir = date_of_analysis.replace('-', '_')

    vars_periods_to_look_at_thresholds_to_use = \
    bucket.load_csv_from_s3(file_name = churn_based_on_behaviour_dir + 'data/' + date_dir +\
                     '/vars_periods_to_look_at_thresholds_to_use.csv')

    period_to_look_at_started =\
    vars_periods_to_look_at_thresholds_to_use[['variable_base_name', 'period_to_look_at_started', 'number_of_months_started']].\
    apply(lambda x: (x['variable_base_name'], x['period_to_look_at_started']), axis=1).tolist()

    period_to_look_at_stopped =\
    vars_periods_to_look_at_thresholds_to_use[['variable_base_name', 'period_to_look_at_stopped', 'number_of_months_stopped']].\
    apply(lambda x: (x['variable_base_name'], x['period_to_look_at_stopped']), axis=1).tolist()

    (all_vars, all_vars_base_names) = get_all_vars.main()

    with open(r'./parameters/for_properly_used_inquiries_vars.yaml') as file:
        for_properly_used_inquiries_vars = yaml.load(file, Loader=yaml.FullLoader)

    inquiries_vars_base_names = \
    for_properly_used_inquiries_vars['inquiries_vars_base_names']
    changed_inquiry_status_vars_base_names = \
    for_properly_used_inquiries_vars['changed_inquiry_status_vars_base_names']


    if len(variables_to_use_for_the_model)>0:
        period_to_look_at_started = \
        [x for x in period_to_look_at_started if x[0] in variables_to_use_for_the_model]

        period_to_look_at_stopped = \
        [x for x in period_to_look_at_stopped if x[0] in variables_to_use_for_the_model]

        all_vars_base_names = \
        dict(filter(lambda x: x[1] in variables_to_use_for_the_model, all_vars_base_names.items()))

        variables_to_use_for_the_model = [x for x in variables_to_use_for_the_model if\
        x not in list(all_vars_base_names.values())]

    ## did something before, did something last month ##
    did_something_before_vars = ['had_'+x+'_before' for x in all_vars_base_names.values()]
    did_something_last_month_vars = ['had_'+x[0]+'_'+x[1] for x in period_to_look_at_stopped]

    for key in all_vars_base_names.keys():
        if 'changed' in key.lower() and 'inquiry' in key.lower():
            period_to_look_at_started_tmp = vars_periods_to_look_at_thresholds_to_use[vars_periods_to_look_at_thresholds_to_use['variable_base_name']==\
                                                                             all_vars_base_names[key]]['period_to_look_at_started'].iloc[0]

            period_to_look_at_stopped_tmp = vars_periods_to_look_at_thresholds_to_use[vars_periods_to_look_at_thresholds_to_use['variable_base_name']==\
                                                                             all_vars_base_names[key]]['period_to_look_at_stopped'].iloc[0]



            did_something_before_vars.append('had_'+for_properly_used_inquiries_vars['changed_inquiry_status_to_properly_used'][all_vars_base_names[key]]+'_before')
            did_something_before_vars.remove('had_'+all_vars_base_names[key]+'_before')

            did_something_last_month_vars.append('had_'+for_properly_used_inquiries_vars['changed_inquiry_status_to_properly_used'][all_vars_base_names[key]]+'_'+period_to_look_at_stopped_tmp)
            did_something_last_month_vars.remove('had_'+all_vars_base_names[key]+'_'+period_to_look_at_stopped_tmp)


    return (variables_to_use_for_the_model, did_something_before_vars, did_something_last_month_vars)

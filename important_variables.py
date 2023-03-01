import get_started_doing_something_variables
import get_stopped_doing_something_variables
import get_all_vars
import yaml

import pandas as pd
import read_a_combination_of_variables
import os
import sys
sys.path.insert(0, os.path.abspath('../'))
from helpers.s3_bucket_utils import S3BucketUtils
from helpers import settings

bucket = S3BucketUtils()

with open(r'./parameters/for_properly_used_inquiries_vars.yaml') as file:
    for_properly_used_inquiries_vars = yaml.load(file, Loader=yaml.FullLoader)

def get_model_number(var_, model_names):
    if len(model_names[model_names['model_name']==var_]['model_number'])>0:
        model_number = model_names[model_names['model_name']==var_]['model_number'].iloc[0]
    else:
        multiple_vars = model_names[(model_names['model_name'].apply(lambda x: ',' in x))]
        for i in range(0, len(multiple_vars)):
            if var_ in multiple_vars['model_name'].iloc[i]:
                model_number = multiple_vars['model_number'].iloc[i]
                break
    return model_number

def get_pairs_of_variables(churn_based_on_behaviour_dir, date_dir, model_type):
    vars_periods_to_look_at_thresholds_to_use = \
    bucket.load_csv_from_s3(file_name = churn_based_on_behaviour_dir + 'data/' + date_dir +\
                     '/vars_periods_to_look_at_thresholds_to_use.csv')

    if date_dir < '2022_01_01':
        vars_periods_to_look_at_thresholds_to_use['period_to_look_at_started'] = \
        vars_periods_to_look_at_thresholds_to_use['period_to_look_at']
        vars_periods_to_look_at_thresholds_to_use['period_to_look_at_stopped'] = \
        vars_periods_to_look_at_thresholds_to_use['period_to_look_at']

        vars_periods_to_look_at_thresholds_to_use['number_of_months_started'] = \
        vars_periods_to_look_at_thresholds_to_use['number_of_months']
        vars_periods_to_look_at_thresholds_to_use['number_of_months_stopped'] = \
        vars_periods_to_look_at_thresholds_to_use['number_of_months']


    period_to_look_at_started =\
    vars_periods_to_look_at_thresholds_to_use[['variable_base_name', 'period_to_look_at_started', 'number_of_months_started']].\
    apply(lambda x: (x['variable_base_name'], x['period_to_look_at_started']), axis=1).tolist()
    period_to_look_at_stopped =\
    vars_periods_to_look_at_thresholds_to_use[['variable_base_name', 'period_to_look_at_stopped', 'number_of_months_stopped']].\
    apply(lambda x: (x['variable_base_name'], x['period_to_look_at_stopped']), axis=1).tolist()


    (all_vars, all_vars_base_names) = get_all_vars.main()
    first_var = dict.fromkeys(all_vars_base_names.values())
    second_var = dict.fromkeys(all_vars_base_names.values())

    if model_type == 'started_doing_something':
        for i in range(0, len(period_to_look_at_started)):
            first_var[period_to_look_at_stopped[i][0]] = 'had_'+period_to_look_at_stopped[i][0]+'_'+period_to_look_at_stopped[i][1]
            second_var[period_to_look_at_stopped[i][0]] = \
            'had_'+period_to_look_at_stopped[i][0]+'_before_and_didnt_'+period_to_look_at_stopped[i][1]

        for key in first_var.keys():
            if 'changed' in key.lower() and 'inquiry' in key.lower():
                period_to_look_at_started_tmp = vars_periods_to_look_at_thresholds_to_use[vars_periods_to_look_at_thresholds_to_use['variable_base_name']==\
                                                                                 key]['period_to_look_at_started'].iloc[0]
                period_to_look_at_stopped_tmp = vars_periods_to_look_at_thresholds_to_use[vars_periods_to_look_at_thresholds_to_use['variable_base_name']==\
                                                                                 key]['period_to_look_at_stopped'].iloc[0]

                first_var[key] = 'had_'+for_properly_used_inquiries_vars['changed_inquiry_status_to_properly_used'][key]+'_'+period_to_look_at_stopped_tmp
        for key in second_var.keys():
            if 'changed' in key.lower() and 'inquiry' in key.lower():
                period_to_look_at_started_tmp = vars_periods_to_look_at_thresholds_to_use[vars_periods_to_look_at_thresholds_to_use['variable_base_name']==\
                                                                                 key]['period_to_look_at_started'].iloc[0]
                period_to_look_at_stopped_tmp = vars_periods_to_look_at_thresholds_to_use[vars_periods_to_look_at_thresholds_to_use['variable_base_name']==\
                                                                                 key]['period_to_look_at_stopped'].iloc[0]

                second_var[key] = 'had_'+for_properly_used_inquiries_vars['changed_inquiry_status_to_properly_used'][key]+'_before_and_didnt_'+period_to_look_at_stopped_tmp

    elif model_type == 'stopped_doing_something':
        for i in range(0, len(period_to_look_at_started)):
            first_var[period_to_look_at_started[i][0]] = 'had_'+period_to_look_at_started[i][0]+'_before'
            second_var[period_to_look_at_stopped[i][0]] = 'had_'+period_to_look_at_stopped[i][0]+'_'+period_to_look_at_stopped[i][1]

        for key in first_var.keys():
            if 'changed' in key.lower() and 'inquiry' in key.lower():
                period_to_look_at_started_tmp = vars_periods_to_look_at_thresholds_to_use[vars_periods_to_look_at_thresholds_to_use['variable_base_name']==\
                                                                                 key]['period_to_look_at_started'].iloc[0]
                period_to_look_at_stopped_tmp = vars_periods_to_look_at_thresholds_to_use[vars_periods_to_look_at_thresholds_to_use['variable_base_name']==\
                                                                                 key]['period_to_look_at_stopped'].iloc[0]

                first_var[key] = 'had_'+for_properly_used_inquiries_vars['changed_inquiry_status_to_properly_used'][key]+'_before'
        for key in second_var.keys():
            if 'changed' in key.lower() and 'inquiry' in key.lower():
                period_to_look_at_started_tmp = vars_periods_to_look_at_thresholds_to_use[vars_periods_to_look_at_thresholds_to_use['variable_base_name']==\
                                                                                 key]['period_to_look_at_started'].iloc[0]
                period_to_look_at_stopped_tmp = vars_periods_to_look_at_thresholds_to_use[vars_periods_to_look_at_thresholds_to_use['variable_base_name']==\
                                                                                 key]['period_to_look_at_stopped'].iloc[0]

                second_var[key] = 'had_'+for_properly_used_inquiries_vars['changed_inquiry_status_to_properly_used'][key]+'_'+period_to_look_at_stopped_tmp


    return (first_var, second_var)

def main(without_properly_used_vars, churn_based_on_behaviour_dir, date_dir, exports_dir_path):
    all_vars = bucket.load_csv_from_s3(file_name = churn_based_on_behaviour_dir+'all_variables.csv')
    if without_properly_used_vars == True:
        model_names = \
        bucket.load_csv_from_s3(file_name = churn_based_on_behaviour_dir+'combinations_of_variables_that_are_not_dependent/combinations_and_names_without_properly_used_inquiries_vars/model_names.csv')
        all_vars.drop(all_vars[all_vars['base_var_name'].apply(lambda x: 'changed' in x and 'inquiry' in x)].index, inplace = True)
    else:
        model_names = \
        bucket.load_csv_from_s3(file_name = churn_based_on_behaviour_dir+'combinations_of_variables_that_are_not_dependent/model_names.csv')

    for model_type in ['started_doing_something', 'stopped_doing_something']:
        for file_name in ['significant_variables_sorted_by_variable_name.csv', 'variables_with_p_below_0_2_sorted_by_variable_name.csv']:
            important_variables = pd.DataFrame(columns = ['variable', 'exp(coef) - AVERAGE', 'p value - AVERAGE'])
            for var_ in all_vars['base_var_name'].unique()[0:]:
                if 'changed' in var_.lower() and 'inquiry' in var_.lower():
                    model_number = \
                    get_model_number(var_=for_properly_used_inquiries_vars['changed_inquiry_status_to_properly_used'][var_],\
                                    model_names=model_names)
                else:
                    model_number = get_model_number(var_=var_, model_names=model_names)


                if 'model_'+str(model_number) in os.listdir('data/'+date_dir+'/'+exports_dir_path+'/'+model_type+'/'):
                    # print(model_type)
                    # print(model_number)
                    important_vars_export = \
                    bucket.load_csv_from_s3(file_name = churn_based_on_behaviour_dir + 'data/' + date_dir + '/'+exports_dir_path+'/'+\
                         model_type+'/model_'+str(model_number)+'/'+file_name)


                    if var_ in all_vars[all_vars['original_var_name'].notnull()]['base_var_name'].unique():
                        (first_var, second_var) = get_pairs_of_variables(churn_based_on_behaviour_dir=churn_based_on_behaviour_dir,\
                                                                date_dir=date_dir, model_type=model_type)

                        if first_var[var_] in important_vars_export['variable'].unique():
    #                         print(var_)
    #                         print('1')
                            idx = len(important_variables)
                            important_variables.loc[idx, 'variable'] = first_var[var_]
                            important_variables.loc[idx, 'exp(coef) - AVERAGE'] = \
                            important_vars_export[important_vars_export['variable']==first_var[var_]]['exp(coef) - AVERAGE'].iloc[0]
                            important_variables.loc[idx, 'p value - AVERAGE'] = \
                            important_vars_export[important_vars_export['variable']==first_var[var_]]['p value - AVERAGE'].iloc[0]
                        if second_var[var_] in important_vars_export['variable'].unique():
    #                         print(var_)
    #                         print('2')
                            idx = len(important_variables)
                            important_variables.loc[idx, 'variable'] = second_var[var_]
                            important_variables.loc[idx, 'exp(coef) - AVERAGE'] = \
                            important_vars_export[important_vars_export['variable']==second_var[var_]]['exp(coef) - AVERAGE'].iloc[0]
                            important_variables.loc[idx, 'p value - AVERAGE'] = \
                            important_vars_export[important_vars_export['variable']==second_var[var_]]['p value - AVERAGE'].iloc[0]
                    else:
                        if var_ in important_vars_export['variable'].unique():
                            idx = len(important_variables)
                            important_variables.loc[idx, 'variable'] = var_
                            important_variables.loc[idx, 'exp(coef) - AVERAGE'] = \
                            important_vars_export[important_vars_export['variable']==var_]['exp(coef) - AVERAGE'].iloc[0]
                            important_variables.loc[idx, 'p value - AVERAGE'] = \
                            important_vars_export[important_vars_export['variable']==var_]['p value - AVERAGE'].iloc[0]

            if 'significant_variables' in file_name:
                important_variables.sort_values('p value - AVERAGE').to_csv('data/'+date_dir+'/'+exports_dir_path+'/'+model_type+'/all_significant_variables_sorted_by_p_value.csv',\
                                          index = False)

                bucket.store_csv_to_s3(data_frame = important_variables.sort_values('p value - AVERAGE'), \
                    file_name = 'all_significant_variables_sorted_by_p_value.csv', \
                    dir = '/'+churn_based_on_behaviour_dir+'data/'+date_dir+'/'+exports_dir_path+'/'+model_type+'/')


                important_variables.sort_values('variable').to_csv('data/'+date_dir+'/'+exports_dir_path+'/'+model_type+'/all_significant_variables_sorted_by_variable_name.csv',\
                                          index = False)
                bucket.store_csv_to_s3(data_frame = important_variables.sort_values('variable'), \
                    file_name = 'all_significant_variables_sorted_by_variable_name.csv', \
                    dir = '/'+churn_based_on_behaviour_dir+'data/'+date_dir+'/'+exports_dir_path+'/'+model_type+'/')


            elif 'variables_with_p_below_0_2' in file_name:
                important_variables.sort_values('p value - AVERAGE').to_csv('data/'+date_dir+'/'+exports_dir_path+'/'+model_type+'/all_variables_with_p_below_0_2_sorted_by_p_value.csv',\
                                          index = False)
                bucket.store_csv_to_s3(data_frame = important_variables.sort_values('p value - AVERAGE'), \
                    file_name = 'all_variables_with_p_below_0_2_sorted_by_p_value.csv', \
                    dir = '/'+churn_based_on_behaviour_dir+'data/'+date_dir+'/'+exports_dir_path+'/'+model_type+'/')


                important_variables.sort_values('variable').to_csv('data/'+date_dir+'/'+exports_dir_path+'/'+model_type+'/all_variables_with_p_below_0_2_sorted_by_variable_name.csv',\
                                          index = False)
                bucket.store_csv_to_s3(data_frame = important_variables.sort_values('variable'), \
                    file_name = 'all_variables_with_p_below_0_2_sorted_by_variable_name.csv', \
                    dir = '/'+churn_based_on_behaviour_dir+'data/'+date_dir+'/'+exports_dir_path+'/'+model_type+'/')


        ## export variables that have a p > 0.05 and p < 0.2 ##
        all_significant_vars = \
        bucket.load_csv_from_s3(file_name = churn_based_on_behaviour_dir+'data/'+date_dir+\
                                '/'+exports_dir_path+'/'+model_type+'/all_significant_variables_sorted_by_variable_name.csv')

        all_variables_with_p_below_0_2 = \
        bucket.load_csv_from_s3(file_name = churn_based_on_behaviour_dir+'data/'+date_dir+\
                                '/'+exports_dir_path+'/'+model_type+'/all_variables_with_p_below_0_2_sorted_by_variable_name.csv')


        not_significant_with_p_below_0_2 = all_variables_with_p_below_0_2[(all_variables_with_p_below_0_2['variable'].\
                                                                  isin(all_significant_vars['variable'].unique())==False)]

        ### sorted by p value ###
        not_significant_with_p_below_0_2.sort_values('p value - AVERAGE').\
        to_csv('data/'+date_dir+'/'+exports_dir_path+'/'+model_type+'/not_significant_variables_with_p_below_0_2_sorted_by_p_value.csv',\
              index = False)


        bucket.store_csv_to_s3(data_frame = not_significant_with_p_below_0_2.sort_values('p value - AVERAGE'), \
                file_name = 'not_significant_variables_with_p_below_0_2_sorted_by_p_value.csv', \
                dir = '/'+churn_based_on_behaviour_dir+'data/'+date_dir+'/'+exports_dir_path+'/'+model_type+'/')


        ### sorted by variable name ###
        not_significant_with_p_below_0_2.sort_values('variable').\
        to_csv('data/'+date_dir+'/'+exports_dir_path+'/'+model_type+'/not_significant_variables_with_p_below_0_2_sorted_by_variable_name.csv',\
              index = False)

        bucket.store_csv_to_s3(data_frame = not_significant_with_p_below_0_2.sort_values('variable'), \
                file_name = 'not_significant_variables_with_p_below_0_2_sorted_by_variable_name.csv', \
                dir = '/'+churn_based_on_behaviour_dir+'data/'+date_dir+'/'+exports_dir_path+'/'+model_type+'/')

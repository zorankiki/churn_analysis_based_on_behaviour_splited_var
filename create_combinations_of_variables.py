import pandas as pd
import numpy as np
import get_all_vars
import read_vars_for_separate_models_for_dependent_vars
import ast
from functools import reduce
import main_and_related_vars
import yaml

import os
import sys
sys.path.insert(0, os.path.abspath('../'))
from helpers.s3_bucket_utils import S3BucketUtils
from helpers import settings
from helpers import dummy_utils

bucket = S3BucketUtils()

def get_key(val, dict_):
    for key, value in dict_.items():
        if val == value:
            return key


def drop_duplicates_from_a_list_of_lists(list_of_lists):
    for i in range(0, len(list_of_lists)):
        list_of_lists[i] = sorted(list_of_lists[i])

    list_of_lists = \
    [ast.literal_eval(x) for x in list(dict.fromkeys([str(x) for x in list_of_lists]))]

    return list_of_lists


year_become_customer_cols = dummy_utils.get_dummies_col_names_as_intersection_of_datasets('year_become_customer')
for i in year_become_customer_cols:
    main_and_related_vars.main_and_related_vars_dict[i] = []

(behavioural_vars_original, behavioural_vars_base_names) = get_all_vars.main()
all_vars = \
list(bucket.load_csv_from_s3(file_name='churn_analysis_based_on_behaviour/all_variables.csv')['base_var_name'].unique())

new_main_and_related_vars_dict = dict.fromkeys([behavioural_vars_base_names[x] for x in main_and_related_vars.main_and_related_vars_dict.keys() if x in behavioural_vars_base_names.keys()]+\
[x for x in main_and_related_vars.main_and_related_vars_dict.keys() if x not in behavioural_vars_base_names.keys()])
for key in new_main_and_related_vars_dict:
    new_main_and_related_vars_dict[key] = []

for key in main_and_related_vars.main_and_related_vars_dict.keys():
    for var_ in main_and_related_vars.main_and_related_vars_dict[key]:
        if var_ in behavioural_vars_base_names.keys():
            if key in behavioural_vars_base_names.keys():
                new_main_and_related_vars_dict[behavioural_vars_base_names[key]].append(behavioural_vars_base_names[var_])
            else:
                new_main_and_related_vars_dict[key].append(behavioural_vars_base_names[var_])
        else:
            if key in behavioural_vars_base_names.keys():
                new_main_and_related_vars_dict[behavioural_vars_base_names[key]].append(var_)
            else:
                new_main_and_related_vars_dict[key].append(var_)

main_and_related_vars_dict = new_main_and_related_vars_dict

def main():
    all_possible_variables_combinations = []
    all_main_variables = []
    main_vars_and_their_combinations = dict.fromkeys(main_and_related_vars_dict.keys())
    for var_ in main_and_related_vars_dict.keys():
        #possible_variables_combinations = []

        all_possible_variables_combinations.append(sorted([x for x in all_vars if x not in main_and_related_vars_dict[var_]]))
        all_main_variables.append(var_)
        main_vars_and_their_combinations[var_] = \
        str(sorted([x for x in all_vars if x not in main_and_related_vars_dict[var_]]))
    print('before dropping duplicates')
    print('len(all_possible_variables_combinations): '+str(len(all_possible_variables_combinations)))

    variables_with_a_same_combination = dict.fromkeys(main_and_related_vars_dict.keys())
    for var_ in variables_with_a_same_combination.keys():
        variables_with_a_same_combination[var_] = \
        sorted([k for k,v in main_vars_and_their_combinations.items() if v == main_vars_and_their_combinations[var_]])
    variables_with_a_same_combination = \
    [ast.literal_eval(x) for x in list(dict.fromkeys([str(x) for x in variables_with_a_same_combination.values() if len(x)>1]).keys())]
    for var_ in main_vars_and_their_combinations.keys():
        main_vars_and_their_combinations[var_] = ast.literal_eval(main_vars_and_their_combinations[var_])

    model_names = dict.fromkeys(main_vars_and_their_combinations.keys())
    for var_ in main_vars_and_their_combinations.keys():
        model_names[var_] = var_
    for i in range(0, len(variables_with_a_same_combination)):
        first_var = variables_with_a_same_combination[i][0]
        model_names[first_var] = str(variables_with_a_same_combination[i]).split('[')[1].split(']')[0]
        for var_ in variables_with_a_same_combination[i][1:]:
            del model_names[var_]
            del main_vars_and_their_combinations[var_]
    model_names_list = []
    for var_ in main_vars_and_their_combinations.keys():
        model_names_list.append(model_names[var_])
    model_names = pd.DataFrame({'model_number': list(range(0, len(model_names)+1)),
         'model_name': ['all_variables'] + model_names_list
        })

    print('after dropping duplicates')
    print('len(main_vars_and_their_combinations): '+str(len(main_vars_and_their_combinations)))

    df_all_vars = pd.DataFrame({'variable_name':all_vars, 'model_0':all_vars})

    ### create model names ###
    possible_variables_combinations_dfs = []
    possible_variables_combinations_dfs.append(df_all_vars)
    i = 0
    for var_ in main_vars_and_their_combinations.keys():
        possible_variables_combinations_dfs.append(pd.DataFrame({'variable_name':main_vars_and_their_combinations[var_],\
                                                                 'model_'+str(i+1):main_vars_and_their_combinations[var_]}))
        i = i + 1

    ### merge all combinations of variables ####
    df_all_combinations_of_variables = \
    reduce(lambda  left, right: pd.merge(left, right,on=['variable_name'],
                                                how='outer'), possible_variables_combinations_dfs)

    with open(r'./parameters/for_properly_used_inquiries_vars.yaml') as file:
        for_properly_used_inquiries_vars = yaml.load(file, Loader=yaml.FullLoader)

    for key in for_properly_used_inquiries_vars['changed_inquiry_status_to_properly_used'].keys():
        model_names.loc[(model_names['model_name']==key), 'model_name'] = \
        for_properly_used_inquiries_vars['changed_inquiry_status_to_properly_used'][key]

    ## save all combinations and model names to s3 ##
    bucket.store_csv_to_s3(data_frame = df_all_combinations_of_variables, \
                           file_name = 'combinations_of_variables.csv', \
                           dir = '/churn_analysis_based_on_behaviour/combinations_of_variables_that_are_not_dependent/')

    if not os.path.exists('combinations_of_variables_that_are_not_dependent/'):
        os.makedirs('combinations_of_variables_that_are_not_dependent/')

    df_all_combinations_of_variables.to_csv('combinations_of_variables_that_are_not_dependent/combinations_of_variables.csv',\
                                           index = False)



    bucket.store_csv_to_s3(data_frame = model_names, \
                           file_name = 'model_names.csv', \
                           dir = '/churn_analysis_based_on_behaviour/combinations_of_variables_that_are_not_dependent/')

    model_names.to_csv('combinations_of_variables_that_are_not_dependent/model_names.csv', index = False)

    ### few check-ups ###
    df_all_combinations_of_variables.set_index('variable_name', inplace = True)

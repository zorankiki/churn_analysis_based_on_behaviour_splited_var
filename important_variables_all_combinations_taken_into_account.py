import pandas as pd
from functools import reduce
import numpy as np
import read_a_combination_of_variables

#### Helpers ####
import os
import sys
sys.path.insert(0, os.path.abspath('../'))
from helpers.s3_bucket_utils import S3BucketUtils
from helpers import db_utils
from helpers import settings

bucket = S3BucketUtils()
################


def export_important_variables(date_dir, data_dir, model_type, filename, coef_col = 'exp(coef)', p_value_col = 'p value'):
    if 'significant_variables' in filename:
        export_name = 'all_significant_variables'
    elif 'variables_with_p_below_0_2' in filename:
        export_name = 'all_variables_with_p_below_0_2'

    model_numbers = \
    read_a_combination_of_variables.get_a_list_of_model_numbers(dir_name='combinations_of_variables_that_are_not_dependent/')

    if type(coef_col)!= list and type(p_value_col)!=list:
        average_columns = [coef_col + ' - AVERAGE', p_value_col + ' - AVERAGE']
    else:
        average_columns = [x + ' - AVERAGE' for x in coef_col] + [x + ' - AVERAGE' for x in p_value_col]

    df_all_important_variables = []

    for model_number in model_numbers[1:]:
        important_variables = \
        bucket.load_csv_from_s3(file_name = data_dir+'data/'+date_dir+\
            '/exports/'+model_type+'/'+'model_'+str(model_number)+'/'+filename)
        important_variables.drop([x for x in important_variables.columns if x not in ['variable']+average_columns],\
                                axis = 1, inplace = True)
        important_variables.drop(important_variables[important_variables['variable'].isnull()].index, inplace = True)

        for avg_col in average_columns:
            important_variables.rename(columns = {avg_col:avg_col + ' - model_'+str(model_number)}, inplace = True)

        df_all_important_variables.append(important_variables)

    df_final = reduce(lambda  left,right: pd.merge(left, right, on = ['variable'],
                                            how = 'outer'), df_all_important_variables)

    df_final[coef_col + ' - AVERAGE'] = df_final[[x for x in df_final.columns if coef_col in x]].mean(axis = 1)
    df_final[p_value_col + ' - AVERAGE'] = df_final[[x for x in df_final.columns if p_value_col in x]].mean(axis = 1)

    df_final[coef_col + ' - AVERAGE'] = df_final[coef_col + ' - AVERAGE'].apply(lambda x: round(x, 3))
    df_final[p_value_col + ' - AVERAGE'] = df_final[p_value_col + ' - AVERAGE'].apply(lambda x: round(x, 3))

    ## sorted by p value ##
    bucket.store_csv_to_s3(data_frame = df_final[['variable'] + [x + ' - AVERAGE' for x in [coef_col, p_value_col]]].sort_values(p_value_col + ' - AVERAGE'), \
        file_name = export_name + '_sorted_by_p_value.csv', \
        dir = '/'+data_dir+'data/'+date_dir+'/exports/'+model_type+'/')
    df_final[['variable'] + [x + ' - AVERAGE' for x in [coef_col, p_value_col]]].sort_values(p_value_col + ' - AVERAGE').\
    to_csv('data/'+date_dir+'/exports/'+model_type+'/'+export_name + '_sorted_by_p_value.csv', index = False)

    ## sorted by variable name ##
    bucket.store_csv_to_s3(data_frame = df_final[['variable'] + [x + ' - AVERAGE' for x in [coef_col, p_value_col]]].sort_values('variable'), \
        file_name = export_name + '_sorted_by_variable_name.csv', \
        dir = '/'+data_dir+'data/'+date_dir+'/exports/'+model_type+'/')
    df_final[['variable'] + [x + ' - AVERAGE' for x in [coef_col, p_value_col]]].sort_values('variable').\
    to_csv('data/'+date_dir+'/exports/'+model_type+'/'+export_name + '_sorted_by_variable_name.csv', index = False)

    return df_final

import pandas as pd
from functools import reduce
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


def get_one_export(data_dir, date_dir, exports_dir_path, filename, model_type, model_number, model_name):

    coef_and_pvalues = \
    bucket.load_csv_from_s3(file_name = data_dir+'data/'+date_dir+\
    '/'+exports_dir_path+'/'+model_type+'/'+'model_'+str(model_number)+'/'+filename)


    coef_and_pvalues['exp(coef)'] = coef_and_pvalues['exp(coef)'].apply(lambda x: round(x, 2))
    coef_and_pvalues['p'] = coef_and_pvalues['p'].apply(lambda x: round(x, 3))
    export = pd.DataFrame(columns = ['variable', 'models name', 'exp(coef)', 'p value'])
    export['variable'] = coef_and_pvalues['covariate']
    export['models name'] = model_name
    export['exp(coef)'] = coef_and_pvalues['exp(coef)']
    export['p value'] = coef_and_pvalues['p']

    values = ['exp(coef)', 'p value']
    export = pd.pivot_table(export.set_index('models name'), index = ['variable'], columns = ['models name'],\
              values = values)

    return export


def get_all_exports(data_dir, date_dir, exports_dir_path, model_type, model_number, p_limit):
    exports = []
    spots_sets = ['ALL_spots_with_CB_cancellation_requested',\
    'CAN_CANCEL_spots_wo_CB_cancellation_requested']

    # for spots_set in ['ALL', 'CAN_CANCEL']:
    #     for with_wo_CB in ['with_CB', 'wo_CB']:
    #         for event_date_type in ['cancellation_requested', 'cancellation_confirmed']:
    #             filename = 'coef_and_pvalues'+'_'+spots_set+'_spots_'+with_wo_CB+'_'+event_date_type+'_p_below_'+p_limit+'.csv'
    #             model_name = spots_set+'_spots_'+with_wo_CB+'_'+event_date_type+'_as_event'
    #
    #             export = get_one_export(data_dir=data_dir, date_dir=date_dir, exports_dir_path=exports_dir_path, \
    #             filename=filename, model_type=model_type, model_number=model_number, model_name=model_name)
    #
    #             export.reset_index(inplace = True)
    #             exports.append(export)

    for spots_set in spots_sets:
        filename = 'coef_and_pvalues'+'_'+spots_set+'_p_below_'+p_limit+'.csv'
        model_name = spots_set+'_as_event'
        export = get_one_export(data_dir=data_dir, date_dir=date_dir, exports_dir_path=exports_dir_path, \
        filename=filename, model_type=model_type, model_number=model_number, model_name=model_name)

        export.reset_index(inplace = True)
        exports.append(export)


    final_export = \
    reduce(lambda  left,right: pd.merge(left, right, on = ['variable'],
                                            how = 'outer'), exports)

    final_export.set_index('variable', inplace = True)

    return final_export

def get_significant_variables(data_dir, date_dir, exports_dir_path_from, exports_dir_path_to,  model_type, model_number, p_limit, sort_by = 'variable_name/p_value'):

    export = get_all_exports(data_dir=data_dir, date_dir=date_dir, exports_dir_path=exports_dir_path_from,\
                         model_type=model_type, model_number=model_number, p_limit=p_limit)
    variables = pd.DataFrame(list(export.index), columns = ['variable'])
    export.reset_index(inplace = True)

    vars_to_drop = variables[(variables['variable'].apply(lambda x: 'spot_category_' in x or 'metro_area_' in x))&\
                 (variables['variable'].isin(export['variable']))]
    export.drop(export[(export['variable'].isin(vars_to_drop['variable'].unique()))].index, inplace = True)

    ### leave only variables whose p <= 0.05 ###
    export = export[export['p value'].apply(lambda x: np.any(np.array(x)<=0.05), axis = 1)]
    export.reset_index(drop = True, inplace = True)



    export['exp(coef) - AVERAGE'] = export['exp(coef)'].mean(axis = 1)
    export['exp(coef) - AVERAGE'] = export['exp(coef) - AVERAGE'].apply(lambda x: round(x, 2))

    export['p value - AVERAGE'] = export['p value'].mean(axis = 1)
    export['p value - AVERAGE'] = export['p value - AVERAGE'].apply(lambda x: round(x, 3))

    if sort_by == 'p_value':
        export.sort_values('p value - AVERAGE', ascending = True, inplace = True)
    elif sort_by == 'variable_name':
        export.sort_values('variable', inplace = True)

    export.reset_index(drop = True, inplace = True)

    if not os.path.exists('data/'+date_dir+'/'+exports_dir_path_to+'/'+model_type+'/'+'model_'+str(model_number)+'/'):
        os.makedirs('data/'+date_dir+'/'+exports_dir_path_to+'/'+model_type+'/'+'/'+'model_'+str(model_number)+'/')

    if sort_by == 'p_value':
        bucket.store_csv_to_s3(data_frame = export, \
        file_name = 'significant_variables_sorted_by_p_value.csv', \
        dir = '/'+data_dir+'data/'+date_dir+'/'+exports_dir_path_to+'/'+model_type+'/'+\
        'model_'+str(model_number)+'/')

        export.to_csv('data/'+date_dir+'/'+exports_dir_path_to+'/'+model_type+\
        '/'+'model_'+str(model_number)+'/significant_variables_sorted_by_p_value.csv', index = False)


    elif sort_by == 'variable_name':
        bucket.store_csv_to_s3(data_frame = export, \
        file_name = 'significant_variables_sorted_by_variable_name.csv', \
        dir = '/'+data_dir+'data/'+date_dir+'/'+exports_dir_path_to+'/'+model_type+'/'+\
        'model_'+str(model_number)+'/')

        export.to_csv('data/'+date_dir+'/'+exports_dir_path_to+'/'+model_type+\
        '/'+'model_'+str(model_number)+'/significant_variables_sorted_by_variable_name.csv', index = False)


    return export

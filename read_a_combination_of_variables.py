#### Helpers ####
import os
import sys
sys.path.insert(0, os.path.abspath('../'))
from helpers.s3_bucket_utils import S3BucketUtils
from helpers import settings

bucket = S3BucketUtils()
################

churn_based_on_behaviour_dir = 'churn_analysis_based_on_behaviour/'
#base_file_name = 'combinations_of_variables_that_are_not_dependent'

def get_properly_used_inquiries_model_names_and_numbers(dir_name):
    model_names = bucket.\
    load_csv_from_s3(file_name = churn_based_on_behaviour_dir+dir_name+\
    'model_names.csv')

    return model_names[model_names['model_name'].apply(lambda x: 'properly_used' in x)]

def get_model_names(model_number, dir_name):
    model_names = bucket.\
    load_csv_from_s3(file_name = churn_based_on_behaviour_dir+dir_name+\
    'model_names.csv')

    return model_names.loc[model_number, 'model_name']

def get_a_list_of_model_numbers(dir_name):
    model_names = bucket.\
    load_csv_from_s3(file_name = churn_based_on_behaviour_dir+dir_name+\
    'model_names.csv')

    return list(model_names.index)

def main(model_number, dir_name):

    vars_to_use = bucket.\
    load_csv_from_s3(file_name = churn_based_on_behaviour_dir+dir_name+\
    'combinations_of_variables.csv')

    return list(vars_to_use[vars_to_use['model_'+str(model_number)].notnull()]['model_'+str(model_number)].unique())

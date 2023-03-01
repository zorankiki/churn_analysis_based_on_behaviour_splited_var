#### Helpers ####
import os
import sys
sys.path.insert(0, os.path.abspath('../'))
from helpers.s3_bucket_utils import S3BucketUtils
from helpers import settings

bucket = S3BucketUtils()
################

churn_based_on_behaviour_dir = 'churn_analysis_based_on_behaviour/'
base_file_name = 'separate_models_for_dependent_variables'

def main(model_number, dir_name):

    vars_to_use = bucket.\
    load_csv_from_s3(file_name = churn_based_on_behaviour_dir+dir_name+\
    base_file_name+' - model_'+str(model_number)+'.csv')

    return list(vars_to_use['variable_name'].unique())

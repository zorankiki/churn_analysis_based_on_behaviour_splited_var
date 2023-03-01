from lifelines import CoxTimeVaryingFitter
import pandas as pd
import numpy as np
import os
import get_all_vars
import get_started_doing_something_variables
import get_stopped_doing_something_variables
import read_a_combination_of_variables
from IPython.display import display, Markdown

#### Helpers ####
import os
import sys
sys.path.insert(0, os.path.abspath('../'))
from helpers.s3_bucket_utils import S3BucketUtils
from helpers import db_utils
from helpers import settings

bucket = S3BucketUtils()
################


def is_var_numerical(variable, df_timeline):
    if df_timeline[variable].nunique()==2 and 1 in df_timeline[variable].unique() and 0 in df_timeline[variable].unique():
        return False
    else:
        return True

def get_key(val, dict_):
    for key, value in dict_.items():
        if val == value:
            return key


def fit_the_model(df_timeline, penalizer):
    ctv = CoxTimeVaryingFitter(penalizer=penalizer)
    ctv.fit(df_timeline, id_col="spot_id", event_col="event", start_col="start", stop_col="stop", show_progress=False)

    return ctv

def fit_with_sorted_p_values(df_timeline, base_df, ctv, penalizer, skip_vars = []):
    vars_ = list(ctv.summary.sort_values('p', ascending = True).index) + list(base_df.columns)
    if len(skip_vars)>0:
        for i in range(0, len(skip_vars)):
            if skip_vars[i] in vars_:
                vars_.remove(skip_vars[i])
        ctv  = fit_the_model(df_timeline[vars_], penalizer=penalizer)
    else:
        ctv  = fit_the_model(df_timeline[vars_], penalizer=penalizer)
    return ctv

def fit_with_sorted_variable_names(df_timeline, base_df, ctv, penalizer, skip_vars = []):
    vars_ = list(ctv.summary.sort_index().index) + list(base_df.columns)
    if len(skip_vars)>0:
        for i in range(0, len(skip_vars)):
            if skip_vars[i] in vars_:
                vars_.remove(skip_vars[i])
        ctv  = fit_the_model(df_timeline[vars_], penalizer=penalizer)
    else:
        ctv  = fit_the_model(df_timeline[vars_], penalizer=penalizer)
    return ctv

def fit_the_models(df_timeline, base_df, date_of_analysis, model_type, variables_to_use_for_the_model, coefficient_limit_for_numerical_vars, coefficient_limit_for_cat_vars, p_limit, additional_higher_p_limit, additional_lower_p_limit, skip_vars, penalizer):
    ### results ###
    ctv  = fit_the_model(df_timeline=df_timeline.drop(skip_vars, axis = 1), penalizer=penalizer)
    ctv = \
    fit_with_sorted_variable_names(df_timeline=df_timeline, base_df=base_df, ctv=ctv, penalizer=penalizer,\
                                               skip_vars=skip_vars)


    drop_vars = get_unsignificant_vars(date_of_analysis=date_of_analysis, model_type=model_type, variables_to_use_for_the_model=variables_to_use_for_the_model,\
                                                         df_timeline = df_timeline, ctv = ctv, coef_limit_for_numerical = coefficient_limit_for_numerical_vars,\
                                       coef_limit = coefficient_limit_for_cat_vars, \
                                       p_limit = p_limit, skip_vars = skip_vars, \
                                       penalizer=penalizer)

    ctv = fit_the_model(df_timeline=df_timeline.drop(drop_vars, axis = 1), penalizer=penalizer)
    ctv = \
    fit_with_sorted_variable_names(df_timeline=df_timeline, base_df=base_df, ctv=ctv, \
    penalizer=penalizer, skip_vars = drop_vars)

    drop_vars = get_unsignificant_vars(date_of_analysis=date_of_analysis, model_type=model_type, variables_to_use_for_the_model=variables_to_use_for_the_model,\
                                                         df_timeline = df_timeline, ctv = ctv, coef_limit_for_numerical = coefficient_limit_for_numerical_vars,\
                                       coef_limit = coefficient_limit_for_cat_vars, p_limit = p_limit, \
                                       skip_vars = drop_vars, penalizer = penalizer, \
                                       additional_p_limit = additional_higher_p_limit)

    ctv = fit_the_model(df_timeline=df_timeline.drop(drop_vars, axis = 1), penalizer=penalizer)
    ctv = fit_with_sorted_variable_names(df_timeline=df_timeline, \
                                                     base_df=base_df, ctv=ctv, penalizer=penalizer, skip_vars = drop_vars)

    drop_vars = get_unsignificant_vars(date_of_analysis=date_of_analysis, model_type=model_type, variables_to_use_for_the_model=variables_to_use_for_the_model,\
                                                         df_timeline = df_timeline, ctv = ctv, \
                                                         coef_limit_for_numerical = coefficient_limit_for_numerical_vars,\
                                       coef_limit = coefficient_limit_for_cat_vars, p_limit = p_limit, \
                                                         skip_vars = drop_vars, penalizer = penalizer,\
                                                        additional_p_limit = additional_lower_p_limit)

    ctv = fit_the_model(df_timeline=df_timeline.drop(drop_vars, axis = 1), penalizer=penalizer)
    ctv = fit_with_sorted_variable_names(df_timeline=df_timeline, \
                                                     base_df=base_df, ctv=ctv, penalizer=penalizer, skip_vars = drop_vars)

    return ctv

def fit_the_models_and_print_summaries(df_timeline, base_df, date_of_analysis, model_type, variables_to_use_for_the_model, coefficient_limit_for_numerical_vars, coefficient_limit_for_cat_vars, p_limit, additional_higher_p_limit, additional_lower_p_limit, skip_vars, penalizer):
    ### results ###
    # display(Markdown("### sorted by p value"))
    ctv  = \
    fit_the_model(df_timeline=df_timeline.drop(skip_vars, axis = 1), penalizer=penalizer)
    ctv = \
    fit_with_sorted_p_values(df_timeline=df_timeline, base_df=base_df, ctv=ctv, penalizer=penalizer,\
                                               skip_vars=skip_vars)
    # ctv.print_summary()

    display(Markdown("### sorted by variable name"))
    ctv = \
    fit_with_sorted_variable_names(df_timeline=df_timeline, base_df=base_df, ctv=ctv, penalizer=penalizer,\
                                               skip_vars=skip_vars)
    ctv.print_summary()


    display(Markdown("## without categorical variables with |coeff| < 0.2 and p value > 0.2"))
    display(Markdown("## without numerical variables with |coeff| < 0.01 and p value > 0.2"))
    drop_vars = get_unsignificant_vars(date_of_analysis=date_of_analysis, model_type=model_type, variables_to_use_for_the_model=variables_to_use_for_the_model,\
                                                         df_timeline = df_timeline, ctv = ctv, coef_limit_for_numerical = coefficient_limit_for_numerical_vars,\
                                       coef_limit = coefficient_limit_for_cat_vars, \
                                       p_limit = p_limit, skip_vars = skip_vars, \
                                       penalizer=penalizer)

    # display(Markdown("### sorted by p value"))
    ctv = fit_the_model(df_timeline=df_timeline.drop(drop_vars, axis = 1), penalizer=penalizer)
    ctv = fit_with_sorted_p_values(df_timeline=df_timeline, \
                                                     base_df=base_df, ctv=ctv, penalizer=penalizer, skip_vars = drop_vars)

    # ctv.print_summary()

    display(Markdown("### sorted by variable name"))
    ctv = fit_with_sorted_variable_names(df_timeline=df_timeline, \
                                                     base_df=base_df, ctv=ctv, penalizer=penalizer, skip_vars = drop_vars)
    ctv.print_summary()

    # print(str(len(ctv.summary)))


    display(Markdown("## without variables with p >= 0.5"))
    drop_vars = get_unsignificant_vars(date_of_analysis=date_of_analysis, model_type=model_type, variables_to_use_for_the_model=variables_to_use_for_the_model,\
                                                         df_timeline = df_timeline, ctv = ctv, coef_limit_for_numerical = coefficient_limit_for_numerical_vars,\
                                       coef_limit = coefficient_limit_for_cat_vars, p_limit = p_limit, \
                                       skip_vars = drop_vars, penalizer = penalizer, \
                                       additional_p_limit = additional_higher_p_limit)

    # display(Markdown("### sorted by p value"))
    ctv = fit_the_model(df_timeline=df_timeline.drop(drop_vars, axis = 1), penalizer=penalizer)
    ctv = fit_with_sorted_p_values(df_timeline=df_timeline, \
                                                     base_df=base_df, ctv=ctv, penalizer=penalizer, skip_vars = drop_vars)
    # ctv.print_summary()

    display(Markdown("### sorted by variable name"))
    ctv = fit_with_sorted_variable_names(df_timeline=df_timeline, \
                                                     base_df=base_df, ctv=ctv, penalizer=penalizer, skip_vars = drop_vars)
    ctv.print_summary()

    # print(str(len(ctv.summary)))

    display(Markdown("## without variables with p >= 0.2"))
    drop_vars = get_unsignificant_vars(date_of_analysis=date_of_analysis, model_type=model_type, variables_to_use_for_the_model=variables_to_use_for_the_model,\
                                                         df_timeline = df_timeline, ctv = ctv, \
                                                         coef_limit_for_numerical = coefficient_limit_for_numerical_vars,\
                                       coef_limit = coefficient_limit_for_cat_vars, p_limit = p_limit, \
                                                         skip_vars = drop_vars, penalizer = penalizer,\
                                                        additional_p_limit = additional_lower_p_limit)

    # display(Markdown("### sorted by p value"))
    ctv = fit_the_model(df_timeline=df_timeline.drop(drop_vars, axis = 1), penalizer=penalizer)
    ctv = fit_with_sorted_p_values(df_timeline=df_timeline, \
                                                     base_df=base_df, ctv=ctv, penalizer=penalizer, skip_vars = drop_vars)
    # ctv.print_summary()

    display(Markdown("### sorted by variable name"))
    ctv = fit_with_sorted_variable_names(df_timeline=df_timeline, \
                                                     base_df=base_df, ctv=ctv, penalizer=penalizer, skip_vars = drop_vars)
    ctv.print_summary()

    # print(str(len(ctv.summary)))
    return ctv

def fit_the_models_and_print_summaries_kiza(df_timeline, base_df, date_of_analysis, model_type, variables_to_use_for_the_model, coefficient_limit_for_numerical_vars, coefficient_limit_for_cat_vars, p_limit, additional_higher_p_limit, additional_lower_p_limit, skip_vars, penalizer):
    ### results ###
    # display(Markdown("### sorted by p value"))
    ctv_  = \
    fit_the_model(df_timeline=df_timeline.drop(skip_vars, axis = 1), penalizer=penalizer)
    ctv_ = \
    fit_with_sorted_p_values(df_timeline=df_timeline, base_df=base_df, ctv=ctv_, penalizer=penalizer,\
                                               skip_vars=skip_vars)
    # ctv.print_summary()

    display(Markdown("### sorted by variable name"))
    ctv_ = \
    fit_with_sorted_variable_names(df_timeline=df_timeline, base_df=base_df, ctv=ctv_, penalizer=penalizer,\
                                               skip_vars=skip_vars)
    # ctv.print_summary()


    # display(Markdown("## without categorical variables with |coeff| < 0.2 and p value > 0.2"))
    # display(Markdown("## without numerical variables with |coeff| < 0.01 and p value > 0.2"))
    drop_vars = get_unsignificant_vars(date_of_analysis=date_of_analysis, model_type=model_type, variables_to_use_for_the_model=variables_to_use_for_the_model,\
                                                         df_timeline = df_timeline, ctv = ctv_, coef_limit_for_numerical = coefficient_limit_for_numerical_vars,\
                                       coef_limit = coefficient_limit_for_cat_vars, \
                                       p_limit = p_limit, skip_vars = skip_vars, \
                                       penalizer=penalizer)

    # display(Markdown("### sorted by p value"))

    ctv = fit_the_model(df_timeline=df_timeline.drop(drop_vars, axis = 1), penalizer=penalizer)
    ctv = fit_with_sorted_p_values(df_timeline=df_timeline, \
                                                     base_df=base_df, ctv=ctv, penalizer=penalizer, skip_vars = drop_vars)

    # # ctv.print_summary()

    # display(Markdown("### sorted by variable name"))
    ctv = fit_with_sorted_variable_names(df_timeline=df_timeline, \
                                                     base_df=base_df, ctv=ctv, penalizer=penalizer, skip_vars = drop_vars)
    # ctv.print_summary()

    # # print(str(len(ctv.summary)))


    # display(Markdown("## without variables with p >= 0.5"))
    drop_vars = get_unsignificant_vars(date_of_analysis=date_of_analysis, model_type=model_type, variables_to_use_for_the_model=variables_to_use_for_the_model,\
                                                         df_timeline = df_timeline, ctv = ctv, coef_limit_for_numerical = coefficient_limit_for_numerical_vars,\
                                       coef_limit = coefficient_limit_for_cat_vars, p_limit = p_limit, \
                                       skip_vars = drop_vars, penalizer = penalizer, \
                                       additional_p_limit = additional_higher_p_limit)

    # # display(Markdown("### sorted by p value"))
    ctv = fit_the_model(df_timeline=df_timeline.drop(drop_vars, axis = 1), penalizer=penalizer)
    # ctv = fit_with_sorted_p_values(df_timeline=df_timeline, \
    #                                                  base_df=base_df, ctv=ctv, penalizer=penalizer, skip_vars = drop_vars)
    # # ctv.print_summary()

    # display(Markdown("### sorted by variable name"))
    # ctv = fit_with_sorted_variable_names(df_timeline=df_timeline, \
    #                                                  base_df=base_df, ctv=ctv, penalizer=penalizer, skip_vars = drop_vars)
    # ctv.print_summary()

    # # print(str(len(ctv.summary)))

    display(Markdown("## without variables with p >= 0.2"))
    drop_vars = get_unsignificant_vars(date_of_analysis=date_of_analysis, model_type=model_type, variables_to_use_for_the_model=variables_to_use_for_the_model,\
                                                         df_timeline = df_timeline, ctv = ctv, \
                                                         coef_limit_for_numerical = coefficient_limit_for_numerical_vars,\
                                       coef_limit = coefficient_limit_for_cat_vars, p_limit = p_limit, \
                                                         skip_vars = drop_vars, penalizer = penalizer,\
                                                        additional_p_limit = additional_lower_p_limit)

    # display(Markdown("### sorted by p value"))
    ctv = fit_the_model(df_timeline=df_timeline.drop(drop_vars, axis = 1), penalizer=penalizer)
    ctv = fit_with_sorted_p_values(df_timeline=df_timeline, \
                                                     base_df=base_df, ctv=ctv, penalizer=penalizer, skip_vars = drop_vars)
    # ctv.print_summary()

    display(Markdown("### sorted by variable name"))
    ctv_02 = fit_with_sorted_variable_names(df_timeline=df_timeline, \
                                                     base_df=base_df, ctv=ctv, penalizer=penalizer, skip_vars = drop_vars)
    # ctv_02.print_summary()

    # print(str(len(ctv.summary)))
    return (ctv_, ctv_02)

def fit_the_models_and_print_summaries_kiza_sec(df_timeline, base_df, date_of_analysis, model_type, variables_to_use_for_the_model, coefficient_limit_for_numerical_vars, coefficient_limit_for_cat_vars, p_limit, additional_higher_p_limit, additional_lower_p_limit, skip_vars, penalizer):
    ### results ###
    # display(Markdown("### sorted by p value"))
    ctv_1  = \
    fit_the_model(df_timeline=df_timeline.drop(skip_vars, axis = 1), penalizer=penalizer)
    ctv_2 = \
    fit_with_sorted_p_values(df_timeline=df_timeline, base_df=base_df, ctv=ctv_1, penalizer=penalizer,\
                                               skip_vars=skip_vars)
    # ctv.print_summary()

    display(Markdown("### sorted by variable name"))
    ctv_3 = \
    fit_with_sorted_variable_names(df_timeline=df_timeline, base_df=base_df, ctv=ctv_2, penalizer=penalizer,\
                                               skip_vars=skip_vars)
    # ctv.print_summary()


    display(Markdown("## without categorical variables with |coeff| < 0.2 and p value > 0.2"))
    display(Markdown("## without numerical variables with |coeff| < 0.01 and p value > 0.2"))
    drop_vars = get_unsignificant_vars(date_of_analysis=date_of_analysis, model_type=model_type, variables_to_use_for_the_model=variables_to_use_for_the_model,\
                                                         df_timeline = df_timeline, ctv = ctv_3, coef_limit_for_numerical = coefficient_limit_for_numerical_vars,\
                                       coef_limit = coefficient_limit_for_cat_vars, \
                                       p_limit = p_limit, skip_vars = skip_vars, \
                                       penalizer=penalizer)

    # display(Markdown("### sorted by p value"))
    ctv_4 = fit_the_model(df_timeline=df_timeline.drop(drop_vars, axis = 1), penalizer=penalizer)
    ctv_5 = fit_with_sorted_p_values(df_timeline=df_timeline, \
                                                     base_df=base_df, ctv=ctv_4, penalizer=penalizer, skip_vars = drop_vars)

    # ctv.print_summary()

    display(Markdown("### sorted by variable name"))
    ctv_6 = fit_with_sorted_variable_names(df_timeline=df_timeline, \
                                                     base_df=base_df, ctv=ctv_5, penalizer=penalizer, skip_vars = drop_vars)
    # ctv.print_summary()

    # print(str(len(ctv.summary)))


    display(Markdown("## without variables with p >= 0.5"))
    drop_vars = get_unsignificant_vars(date_of_analysis=date_of_analysis, model_type=model_type, variables_to_use_for_the_model=variables_to_use_for_the_model,\
                                                         df_timeline = df_timeline, ctv = ctv_6, coef_limit_for_numerical = coefficient_limit_for_numerical_vars,\
                                       coef_limit = coefficient_limit_for_cat_vars, p_limit = p_limit, \
                                       skip_vars = drop_vars, penalizer = penalizer, \
                                       additional_p_limit = additional_higher_p_limit)

    # display(Markdown("### sorted by p value"))
    ctv_7 = fit_the_model(df_timeline=df_timeline.drop(drop_vars, axis = 1), penalizer=penalizer)
    ctv_8 = fit_with_sorted_p_values(df_timeline=df_timeline, \
                                                     base_df=base_df, ctv=ctv_7, penalizer=penalizer, skip_vars = drop_vars)
    # ctv.print_summary()

    display(Markdown("### sorted by variable name"))
    ctv_9 = fit_with_sorted_variable_names(df_timeline=df_timeline, \
                                                     base_df=base_df, ctv=ctv_8, penalizer=penalizer, skip_vars = drop_vars)
    ## ctv.print_summary()

    # print(str(len(ctv.summary)))

    display(Markdown("## without variables with p >= 0.2"))
    drop_vars = get_unsignificant_vars(date_of_analysis=date_of_analysis, model_type=model_type, variables_to_use_for_the_model=variables_to_use_for_the_model,\
                                                         df_timeline = df_timeline, ctv = ctv_9, \
                                                         coef_limit_for_numerical = coefficient_limit_for_numerical_vars,\
                                       coef_limit = coefficient_limit_for_cat_vars, p_limit = p_limit, \
                                                         skip_vars = drop_vars, penalizer = penalizer,\
                                                        additional_p_limit = additional_lower_p_limit)

    # display(Markdown("### sorted by p value"))
    ctv_10 = fit_the_model(df_timeline=df_timeline.drop(drop_vars, axis = 1), penalizer=penalizer)
    ctv_11 = fit_with_sorted_p_values(df_timeline=df_timeline, \
                                                     base_df=base_df, ctv=ctv_10, penalizer=penalizer, skip_vars = drop_vars)
    # ctv.print_summary()

    display(Markdown("### sorted by variable name"))
    ctv_12 = fit_with_sorted_variable_names(df_timeline=df_timeline, \
                                                     base_df=base_df, ctv=ctv_11, penalizer=penalizer, skip_vars = drop_vars)
    # ctv.print_summary()

    # print(str(len(ctv.summary)))
    return (ctv_1, ctv_2, ctv_3, ctv_4, ctv_5, ctv_6, ctv_7, ctv_8, ctv_9, ctv_10, ctv_11, ctv_12) 

def fit_the_models_and_return_all_intermediate_results(df_timeline, base_df, date_of_analysis, model_type, variables_to_use_for_the_model, coefficient_limit_for_numerical_vars, coefficient_limit_for_cat_vars, p_limit, additional_higher_p_limit, additional_lower_p_limit, skip_vars, penalizer):
    ### results ###
    all_summaries = {'no_condition':None,\
                    'first_condition':None,\
                    'second_condition':None,\
                    'third_condition':None}

    conditions_described = {'no_condition':'with all initial variables',\
                            'first_condition':'without categorical variables with |coeff| < 0.2 and p value > 0.2; without numerical variables with |coeff| < 0.01 and p value > 0.2',\
                            'second_condition':'without variables p value >= 0.5',\
                            'third_condition':'without variables p value >= 0.2'}

    ctv  = \
    fit_the_model(df_timeline=df_timeline.drop(skip_vars, axis = 1), penalizer=penalizer)
    ctv = \
    fit_with_sorted_variable_names(df_timeline=df_timeline, base_df=base_df, ctv=ctv, penalizer=penalizer,\
                                               skip_vars=skip_vars)

    all_summaries['no_condition'] = ctv.summary


    drop_vars = get_unsignificant_vars(date_of_analysis=date_of_analysis, model_type=model_type, variables_to_use_for_the_model=variables_to_use_for_the_model,\
                                                         df_timeline = df_timeline, ctv = ctv, coef_limit_for_numerical = coefficient_limit_for_numerical_vars,\
                                       coef_limit = coefficient_limit_for_cat_vars, \
                                       p_limit = p_limit, skip_vars = skip_vars, \
                                       penalizer=penalizer)

    ctv = fit_the_model(df_timeline=df_timeline.drop(drop_vars, axis = 1), penalizer=penalizer)
    ctv = fit_with_sorted_variable_names(df_timeline=df_timeline, \
                                                     base_df=base_df, ctv=ctv, penalizer=penalizer, skip_vars = drop_vars)

    all_summaries['first_condition'] = ctv.summary


    drop_vars = get_unsignificant_vars(date_of_analysis=date_of_analysis, model_type=model_type, variables_to_use_for_the_model=variables_to_use_for_the_model,\
                                                         df_timeline = df_timeline, ctv = ctv, coef_limit_for_numerical = coefficient_limit_for_numerical_vars,\
                                       coef_limit = coefficient_limit_for_cat_vars, p_limit = p_limit, \
                                       skip_vars = drop_vars, penalizer = penalizer, \
                                       additional_p_limit = additional_higher_p_limit)

    ctv = fit_the_model(df_timeline=df_timeline.drop(drop_vars, axis = 1), penalizer=penalizer)
    ctv = fit_with_sorted_variable_names(df_timeline=df_timeline, \
                                                     base_df=base_df, ctv=ctv, penalizer=penalizer, skip_vars = drop_vars)

    all_summaries['second_condition'] = ctv.summary

    drop_vars = get_unsignificant_vars(date_of_analysis=date_of_analysis, model_type=model_type, variables_to_use_for_the_model=variables_to_use_for_the_model,\
                                                         df_timeline = df_timeline, ctv = ctv, \
                                                         coef_limit_for_numerical = coefficient_limit_for_numerical_vars,\
                                       coef_limit = coefficient_limit_for_cat_vars, p_limit = p_limit, \
                                                         skip_vars = drop_vars, penalizer = penalizer,\
                                                        additional_p_limit = additional_lower_p_limit)

    ctv = fit_the_model(df_timeline=df_timeline.drop(drop_vars, axis = 1), penalizer=penalizer)
    ctv = fit_with_sorted_variable_names(df_timeline=df_timeline, \
                                                     base_df=base_df, ctv=ctv, penalizer=penalizer, skip_vars = drop_vars)

    all_summaries['third_condition'] = ctv.summary

    return (ctv, all_summaries, conditions_described)


def fit_all_models_and_get_all_summaries(number_of_models, model_numbers, date_of_analysis, churn_based_on_behaviour_dir, spots_set, with_wo_CB, event_date_full_name, df_timeline_all_vars, base_cols, base_df, model_type, coefficient_limit_for_numerical_vars, coefficient_limit_for_cat_vars, p_limit, additional_higher_p_limit, additional_lower_p_limit, penalizer):
    all_models_summaries = {'no_condition':[],\
                                     'first_condition':[],\
                                     'second_condition':[],\
                                     'third_condition':[]}
    all_models_names = {'no_condition':[],\
                                         'first_condition':[],\
                                         'second_condition':[],\
                                         'third_condition':[]}

    for model_number in model_numbers[1:number_of_models+1]:

        cols_to_use = read_a_combination_of_variables.\
        main(model_number=model_number, dir_name='combinations_of_variables_that_are_not_dependent/')

        model_name = \
        read_a_combination_of_variables.get_model_names(model_number=model_number, \
                                                        dir_name='combinations_of_variables_that_are_not_dependent/')

        #### get behavioural variables ####
        if model_type == 'started_doing_something':
            (variables_to_use_for_the_model, did_something_last_month_vars, did_something_before_and_didnt_last_month_vars) = \
            get_started_doing_something_variables.main(date_of_analysis=date_of_analysis, variables_to_use_for_the_model=cols_to_use)
            cols = variables_to_use_for_the_model + \
            did_something_before_and_didnt_last_month_vars + did_something_last_month_vars
        elif model_type == 'stopped_doing_something':
            (variables_to_use_for_the_model, did_something_before_vars, did_something_last_month_vars) = \
            get_stopped_doing_something_variables.main(date_of_analysis=date_of_analysis, variables_to_use_for_the_model=cols_to_use)
            cols = base_cols + variables_to_use_for_the_model + \
            did_something_before_vars + did_something_last_month_vars

        #### data for the model ###
        df_timeline = df_timeline_all_vars.copy()
        vars_that_stay = ['spot_id', 'start', 'stop', 'event']+\
        [x for x in cols if x not in base_cols]+\
        [x for x in df_timeline.columns if 'spot_category_' in x or 'metro_area_' in x]
        df_timeline.drop([x for x in df_timeline.columns if x not in vars_that_stay], axis = 1, inplace = True)

        ### variables to skip ###
        df_timeline.isnull().sum().sum() #OK
        skip_vars = list((df_timeline!=0).sum()[(df_timeline!=0).sum()==0].index)
        skip_vars

        (ctv, all_summaries, conditions_described) = \
        fit_the_models_and_return_all_intermediate_results(df_timeline=df_timeline, base_df=base_df, \
                                                             date_of_analysis=date_of_analysis, model_type=model_type, \
                                                             variables_to_use_for_the_model=cols_to_use,\
                                                             coefficient_limit_for_numerical_vars=coefficient_limit_for_numerical_vars, \
                                                             coefficient_limit_for_cat_vars=coefficient_limit_for_cat_vars, p_limit=p_limit, \
                                                             additional_higher_p_limit=additional_higher_p_limit, \
                                                             additional_lower_p_limit=additional_lower_p_limit,\
                                                             skip_vars=skip_vars, penalizer=penalizer)
        for key in all_summaries:
            all_models_summaries[key].append(all_summaries[key])
            all_models_names[key].append(model_name)

        ### save coefs and p values for p < 0.2 ###
        save_results(df = all_models_summaries['third_condition'][model_number-1].reset_index(), date_of_analysis = date_of_analysis, \
                                       data_dir = churn_based_on_behaviour_dir, dir_name='exports/coefficients_and_pvalues',\
                     results_name = 'coef_and_pvalues', spots_set = spots_set,\
                     with_wo_CB = with_wo_CB, event_date_type = event_date_full_name, p_limit=0.2, model_number=model_number,\
                                      model_type = model_type)

    return (all_models_summaries, all_models_names, conditions_described)


def dont_drop_vars_wo_a_pair(vars_for_dropping, vars_first_set, vars_second_set):
    vars_of_interest = list(vars_first_set.keys())+list(vars_second_set.keys())
    vars_for_dropping_orig  = vars_for_dropping.copy()
    for var_ in [x for x in vars_for_dropping if x in vars_of_interest]:
        if var_ in vars_first_set.keys():
            var_pair = get_key(vars_first_set[var_], vars_second_set)
        elif var_ in vars_second_set.keys():
            var_pair = get_key(vars_second_set[var_], vars_first_set)

        if var_pair not in vars_for_dropping_orig:
            vars_for_dropping.remove(var_)

    return vars_for_dropping

def only_vars_wo_pairs_left(vars_for_dropping, vars_first_set, vars_second_set):
    vars_of_interest = list(vars_first_set.keys())+list(vars_second_set.keys())
    if len([x for x in vars_for_dropping if x not in vars_of_interest])>0:
        return False
    else:
        vars_for_dropping_orig  = vars_for_dropping.copy()
        for var_ in [x for x in vars_for_dropping if x in vars_of_interest]:
            if var_ in vars_first_set.keys():
                var_pair = get_key(vars_first_set[var_], vars_second_set)
            elif var_ in vars_second_set.keys():
                var_pair = get_key(vars_second_set[var_], vars_first_set)

            if var_pair not in vars_for_dropping_orig:
                vars_for_dropping.remove(var_)
        if len(vars_for_dropping)==0:
            return True
        else:
            return False


def get_unsignificant_vars(date_of_analysis, model_type, variables_to_use_for_the_model, df_timeline, ctv, coef_limit_for_numerical, coef_limit, p_limit, skip_vars, penalizer, additional_p_limit=0):
    (all_vars, all_vars_base_names) = get_all_vars.main()
    if model_type == 'started_doing_something':
        (variables_to_use_for_the_model, vars_first_set, vars_second_set) = get_started_doing_something_variables.\
        main(date_of_analysis=date_of_analysis, variables_to_use_for_the_model=variables_to_use_for_the_model)
    elif model_type == 'stopped_doing_something':
        (variables_to_use_for_the_model, vars_first_set, vars_second_set) = get_stopped_doing_something_variables.\
        main(date_of_analysis=date_of_analysis, variables_to_use_for_the_model=variables_to_use_for_the_model)

    vars_first_set = dict(zip(vars_first_set, all_vars_base_names.values()))
    vars_second_set = dict(zip(vars_second_set, all_vars_base_names.values()))

    numerical_vars = [x for x in list(ctv.summary.index) if is_var_numerical(variable=x, df_timeline=df_timeline)]
    categorical_vars = [x for x in list(ctv.summary.index) if is_var_numerical(variable=x, df_timeline=df_timeline)==False]

    if additional_p_limit > 0:
        not_significant_cat_vars = ctv.summary[(ctv.summary.index.isin(categorical_vars))&\
               (((ctv.summary['coef'].apply(lambda x: abs(x)<coef_limit))&\
           (ctv.summary['p']>p_limit))|(ctv.summary['p']>=additional_p_limit))]
        not_significant_num_vars = ctv.summary[(ctv.summary.index.isin(numerical_vars))&\
               (((ctv.summary['coef'].apply(lambda x: abs(x)<coef_limit_for_numerical))&\
           (ctv.summary['p']>p_limit))|(ctv.summary['p']>=additional_p_limit))]
    else:
        not_significant_cat_vars = ctv.summary[(ctv.summary.index.isin(categorical_vars))&\
               (ctv.summary['coef'].apply(lambda x: abs(x)<coef_limit))&\
           (ctv.summary['p']>p_limit)]
        not_significant_num_vars = ctv.summary[(ctv.summary.index.isin(numerical_vars))&\
               (ctv.summary['coef'].apply(lambda x: abs(x)<coef_limit_for_numerical))&\
           (ctv.summary['p']>p_limit)]
    not_significant_vars = list(not_significant_cat_vars.index)+list(not_significant_num_vars.index)
    not_significant_vars = list(dont_drop_vars_wo_a_pair(vars_for_dropping=not_significant_vars, vars_first_set=vars_first_set,\
                                            vars_second_set=vars_second_set))
    vars_for_dropping = skip_vars + not_significant_vars


    while len(not_significant_vars)>0 and only_vars_wo_pairs_left(vars_for_dropping, vars_first_set, vars_second_set)==False:
        ctv  = fit_the_model(df_timeline.drop(vars_for_dropping, axis = 1), penalizer=penalizer)
        if additional_p_limit > 0:
            not_significant_cat_vars = ctv.summary[(ctv.summary.index.isin(categorical_vars))&\
                   (((ctv.summary['coef'].apply(lambda x: abs(x)<coef_limit))&\
               (ctv.summary['p']>p_limit))|(ctv.summary['p']>=additional_p_limit))]
            not_significant_num_vars = ctv.summary[(ctv.summary.index.isin(numerical_vars))&\
                   (((ctv.summary['coef'].apply(lambda x: abs(x)<coef_limit_for_numerical))&\
               (ctv.summary['p']>p_limit))|(ctv.summary['p']>=additional_p_limit))]
        else:
            not_significant_cat_vars = ctv.summary[(ctv.summary.index.isin(categorical_vars))&\
                   (ctv.summary['coef'].apply(lambda x: abs(x)<coef_limit))&\
               (ctv.summary['p']>p_limit)]
            not_significant_num_vars = ctv.summary[(ctv.summary.index.isin(numerical_vars))&\
                   (ctv.summary['coef'].apply(lambda x: abs(x)<coef_limit_for_numerical))&\
               (ctv.summary['p']>p_limit)]
        not_significant_vars = list(not_significant_cat_vars.index)+list(not_significant_num_vars.index)
        not_significant_vars = list(dont_drop_vars_wo_a_pair(vars_for_dropping=not_significant_vars, vars_first_set=vars_first_set,\
                                            vars_second_set=vars_second_set))
        vars_for_dropping = vars_for_dropping + not_significant_vars

    return vars_for_dropping


def save_results(df, date_of_analysis, data_dir, dir_name, results_name, spots_set, with_wo_CB, event_date_type, p_limit, model_number, model_type = 'stopped_doing_something/started_doing_something'):
    date_dir = date_of_analysis.replace('-', '_')

    if not os.path.exists('data/'+date_dir+'/'+dir_name +'/'+model_type+'/'+'model_'+str(model_number)+'/'):
        os.makedirs('data/'+date_dir+'/'+dir_name + '/'+model_type + '/'+'model_'+str(model_number)+'/')
    p_limit = str(p_limit).split('.')[0]+'_'+str(p_limit).split('.')[1]

    df.to_csv('data/'+date_dir+'/'+dir_name+'/'+model_type+'/'+'model_'+str(model_number)+'/'+results_name+'_'+spots_set+'_spots_'+with_wo_CB+'_'+event_date_type+'_p_below_'+p_limit+'.csv', index = False)

    bucket.store_csv_to_s3(data_frame = df, \
    file_name = results_name+'_'+spots_set+'_spots_'+with_wo_CB+'_'+event_date_type+'_p_below_'+p_limit+'.csv', \
    dir = '/'+data_dir+'data/'+date_dir+'/'+dir_name +'/'+model_type+'/'+'model_'+str(model_number)+'/')


def save_results_for_a_specific_variable(df, condition, var_of_interest, date_of_analysis, data_dir, dir_name, results_name, spots_set, with_wo_CB, event_date_type, model_type):
    date_dir = date_of_analysis.replace('-', '_')

    if not os.path.exists('data/'+date_dir+'/exports/coefficients_and_pvalues/'+model_type+'/'+var_of_interest+'/'+condition+'/'):
        os.makedirs('data/'+date_dir+'/exports/coefficients_and_pvalues/'+model_type+'/'+var_of_interest+'/'+condition+'/')

    df.to_csv('data/'+date_dir+'/exports/coefficients_and_pvalues/'+model_type+'/'+var_of_interest+'/'+condition+'/'+results_name+'_'+spots_set+'_spots_'+with_wo_CB+'_'+event_date_type+'.csv', index = False)

    bucket.store_csv_to_s3(data_frame = df, \
    file_name = results_name+'_'+spots_set+'_spots_'+with_wo_CB+'_'+event_date_type+'.csv', \
    dir = '/'+data_dir+'data/'+date_dir+'/exports/coefficients_and_pvalues/'+model_type+'/'+var_of_interest+'/'+condition+'/')

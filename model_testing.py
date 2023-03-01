import pandas as pd
import numpy as np
from scipy import stats
import math
import importlib
from dateutil.relativedelta import relativedelta
import fit_tv_cox_models
import churn_risk_calculation_test


#### Helpers ####
import os
import sys
sys.path.insert(0, os.path.abspath('../'))
from helpers.s3_bucket_utils import S3BucketUtils
from helpers import settings

bucket = S3BucketUtils()
################

churn_based_on_behaviour_dir = 'churn_analysis_based_on_behaviour/'

def add_month(date, m):
    ddd = pd.to_datetime(date, format='%Y-%m-%d')
    ddd2 = ddd + relativedelta(months=m)
    return (str(ddd2))[0:10]

def get_concordance(cr_test):

    true_ones = cr_test[cr_test['y_true']==1]
    true_zeros = cr_test[cr_test['y_true']==0]


    concordance = 0
    discordance = 0
    for i in range(0, len(true_ones)):
        for j in range(0, len(true_zeros)):
            if true_ones['churn_prediction'].iloc[i] > true_zeros['churn_prediction'].iloc[j]:
                concordance = concordance + 1
            else:
                discordance = discordance + 1
    concordance_score = round((concordance/(len(true_ones)*len(true_zeros)))*100, 3)
    discordance_score = round((discordance/(len(true_ones)*len(true_zeros)))*100, 3)

    return (concordance_score, discordance_score)


def get_testing_results(date_of_analysis, df_timeline, base_df, data_all_spots, hs_list_path, model_type, model_number, with_add_vars, p_limit, event_date_type, prediction_month, ctv, spots_set, with_wo_CB, penalizer, test_with_req_canc_before=True):
    drop_left_limit_from = add_month(prediction_month, -1)
    additional_fields = ['limits', 'left_limit', 'right_limit', 'premium_service_hs', 'end']
    df_timeline = df_timeline.merge(data_all_spots[['spot_id', 'time']+additional_fields].rename(columns = {'time':'stop'}),\
                               on = ['spot_id', 'stop'])
    df_timeline_test = df_timeline.drop(df_timeline[df_timeline['left_limit']>=drop_left_limit_from].index)
    df_timeline_test.drop(additional_fields, axis = 1, inplace = True)

    skip_vars = list((df_timeline_test!=0).sum()[(df_timeline_test!=0).sum()==0].index)


    ctv_test = fit_tv_cox_models.\
    fit_with_sorted_p_values(df_timeline=df_timeline_test, base_df=base_df, ctv=ctv, \
                             penalizer=penalizer, skip_vars=skip_vars)

    (cr_test, spots_survived_vars_) = churn_risk_calculation_test.calculate_churn_risk(date_of_analysis = date_of_analysis, df_timeline = df_timeline_test, \
                                            vars_ = list(ctv.summary.index), data_all_spots = data_all_spots, with_add_vars=with_add_vars, p_limit = p_limit, event_date_type = event_date_type, ctv = ctv_test, model_type = model_type, model_number = model_number, prediction_month=prediction_month, spots_set=spots_set, \
                                            with_wo_CB = with_wo_CB)

    cr_test = cr_test.\
    merge(data_all_spots[(data_all_spots['right_limit']==prediction_month)][['spot_id', 'event', 'cancellation_requested']], on = 'spot_id')

    ## spots that requested to cancel during our prediction month ##
    cr_test['y_true'] = 0
    cr_test.loc[(cr_test['cancellation_requested']==prediction_month), 'y_true'] = 1

    ## drop maybe problematic spots ##
    cr_test.drop(cr_test[(cr_test['maybe_problematic']==1)].index, inplace = True)

    ## drop delinquent spots ##
    cr_test.drop(cr_test[(cr_test['Delinquent '].notnull())].index, inplace = True)


    ## drop Fake Sale spots ##
    hs_spots = bucket.load_csv_from_s3(file_name = hs_list_path)
    hs_spots.drop(hs_spots[hs_spots['Reason for Cancelling Survey'].isnull()].index, inplace = True)
    fake_sale_spots = hs_spots[hs_spots['Reason for Cancelling Survey'].apply(lambda x: 'Fake Sale' in x)]['Spot ID'].unique()
    cr_test.drop(cr_test[cr_test['spot_id'].isin(fake_sale_spots)].index, inplace = True)

    ## drop Non-payment spots ##
    non_payment_spots = hs_spots[hs_spots['Reason for Cancelling Survey'].apply(lambda x: 'Non-payment' in x)]['Spot ID'].unique()
    cr_test.drop(cr_test[(cr_test['spot_id'].isin(non_payment_spots))].index, inplace = True)

    cr_test.reset_index(drop = True, inplace = True)

    total_number_of_spots = cr_test['spot_id'].nunique()
    epsilon = 1e-10
    first_20_perc = cr_test.iloc[0:round(cr_test['spot_id'].nunique()*0.2)]

    requested_cancellation_all = cr_test[cr_test['y_true']==1]['spot_id'].nunique()
    rank_of_requested_cancellation_spots_all = tuple(cr_test[cr_test['y_true']==1].index)
    churn_probability_values_all = tuple(cr_test[cr_test['y_true']==1]['churn_prediction'].\
                                        apply(lambda x: round(x, 4)).unique())
    #first option - correlation between prediction and reality
    pearsons_corr_coef_and_p_value_all = stats.pearsonr(cr_test['churn_prediction'], cr_test['y_true'])
    p_corr = round(pearsons_corr_coef_and_p_value_all[0], 3)
    p_pvalue = round(pearsons_corr_coef_and_p_value_all[1], 3)
    pearsons_corr_coef_and_p_value_all = (p_corr, p_pvalue)
    spearmans_corr_coef_and_p_value_all = stats.spearmanr(cr_test['churn_prediction'], cr_test['y_true'])
    s_corr = round(spearmans_corr_coef_and_p_value_all.correlation, 3)
    s_pvalue = round(spearmans_corr_coef_and_p_value_all.pvalue, 3)
    spearmans_corr_coef_and_p_value_all = (s_corr, s_pvalue)

    #second option - log loss
    log_loss_all = -(cr_test[['churn_prediction', 'y_true']].apply(lambda x: x[1]*math.log(x[0]+epsilon) + (1-x[1])*math.log((1-x[0])+epsilon), axis = 1))
    log_loss_all = log_loss_all.mean()

    ##concordance
    (concordance_all, discordance_all) = get_concordance(cr_test)

    requested_cancellation_in_the_first_20_perc_all = first_20_perc[first_20_perc['y_true']==1]['spot_id'].nunique()
    perc_of_requested_cancellation_in_the_first_20_perc_all = (requested_cancellation_in_the_first_20_perc_all/requested_cancellation_all)*100

    testing_results_all = pd.DataFrame(data = [total_number_of_spots, requested_cancellation_all, prediction_month, \
                                               pearsons_corr_coef_and_p_value_all, spearmans_corr_coef_and_p_value_all, \
                                               log_loss_all,\
                                               concordance_all, discordance_all,\
                                               requested_cancellation_in_the_first_20_perc_all, \
                                               perc_of_requested_cancellation_in_the_first_20_perc_all,\
                                               rank_of_requested_cancellation_spots_all, churn_probability_values_all],\
                index = ['total_number_of_spots', 'requested_cancellation', 'prediction_month',\
                         'pearsons_corr_coef_and_p_value', 'spearmans_corr_coef_and_p_value',\
                          'log_loss',\
                         'concordance', 'discordance',\
                         'requested_cancellation_in_the_first_20_%_of_all_spots', \
                          'perc_of_requested_cancellation_spots_in_the_first_20_%_of_all_spots',\
                         'rank_of_requested_cancellation_spots',\
                        'churn_probability_values'],\
                columns = ['all_spots_that_requested_cancellation'])


    ##without closed business

    closed_sold_business_spots = hs_spots[hs_spots['Reason for Cancelling Survey'].apply(lambda x: 'Sold Business' in x or\
                                                                                        'Closed Business' in x or 'Sold/Closed Business' in x or\
                                                                                        'Closed/Sold Business' in x)]['Spot ID'].unique()

    cr_test.drop(cr_test[(cr_test['spot_id'].isin(closed_sold_business_spots))].index, inplace = True)
    cr_test.reset_index(drop = True, inplace = True)

    total_number_of_spots = cr_test['spot_id'].nunique()
    epsilon = 1e-10
    first_20_perc = cr_test.iloc[0:round(cr_test['spot_id'].nunique()*0.2)]

    requested_cancellation_all = cr_test[cr_test['y_true']==1]['spot_id'].nunique()
    rank_of_requested_cancellation_spots_all = tuple(cr_test[cr_test['y_true']==1].index)
    churn_probability_values_all = tuple(cr_test[cr_test['y_true']==1]['churn_prediction'].\
                                        apply(lambda x: round(x, 4)).unique())
    #first option - correlation between prediction and reality
    pearsons_corr_coef_and_p_value_all = stats.pearsonr(cr_test['churn_prediction'], cr_test['y_true'])
    p_corr = round(pearsons_corr_coef_and_p_value_all[0], 3)
    p_pvalue = round(pearsons_corr_coef_and_p_value_all[1], 3)
    pearsons_corr_coef_and_p_value_all = (p_corr, p_pvalue)
    spearmans_corr_coef_and_p_value_all = stats.spearmanr(cr_test['churn_prediction'], cr_test['y_true'])
    s_corr = round(spearmans_corr_coef_and_p_value_all.correlation, 3)
    s_pvalue = round(spearmans_corr_coef_and_p_value_all.pvalue, 3)
    spearmans_corr_coef_and_p_value_all = (s_corr, s_pvalue)

    #second option - log loss
    log_loss_all = -(cr_test[['churn_prediction', 'y_true']].apply(lambda x: x[1]*math.log(x[0]+epsilon) + (1-x[1])*math.log((1-x[0])+epsilon), axis = 1))
    log_loss_all = log_loss_all.mean()

    ##concordance
    (concordance_all, discordance_all) = get_concordance(cr_test)

    requested_cancellation_in_the_first_20_perc_all = first_20_perc[first_20_perc['y_true']==1]['spot_id'].nunique()
    perc_of_requested_cancellation_in_the_first_20_perc_all = (requested_cancellation_in_the_first_20_perc_all/requested_cancellation_all)*100

    testing_results_all_wo_CB_and_problematic = pd.DataFrame(data = [total_number_of_spots, requested_cancellation_all, prediction_month, \
                                               pearsons_corr_coef_and_p_value_all, spearmans_corr_coef_and_p_value_all, \
                                               log_loss_all,\
                                               concordance_all, discordance_all,\
                                               requested_cancellation_in_the_first_20_perc_all, \
                                               perc_of_requested_cancellation_in_the_first_20_perc_all,\
                                               rank_of_requested_cancellation_spots_all, churn_probability_values_all],\
                index = ['total_number_of_spots', 'requested_cancellation', 'prediction_month',\
                         'pearsons_corr_coef_and_p_value', 'spearmans_corr_coef_and_p_value',\
                          'log_loss',\
                         'concordance', 'discordance',\
                         'requested_cancellation_in_the_first_20_%_of_all_spots', \
                          'perc_of_requested_cancellation_spots_in_the_first_20_%_of_all_spots',\
                         'rank_of_requested_cancellation_spots',\
                        'churn_probability_values'],\
                columns = ['all_spots_that_requested_cancellation_wo_Closed_Sold_Business'])




    testing_results = pd.concat([testing_results_all, testing_results_all_wo_CB_and_problematic], axis = 1)


    return (testing_results, df_timeline_test, ctv_test, cr_test)


def save_results(date_of_analysis, df, results_name, model_type, model_number, spots_set, with_wo_CB, event_date_type, with_add_vars, p_limit, prediction_month):
    date_dir = date_of_analysis.replace('-', '_')

    coefs_and_pvalues_path = 'data/'+date_dir+'/exports/coefficients_and_pvalues/'+model_type+'/model_'+str(model_number)+'/'
    coefs_and_pvalues_with_add_important_vars_path = \
    'data/'+date_dir+'/exports/coefficients_and_pvalues_with_add_important_vars/'+model_type +'/model_'+str(model_number)+'/'

    prediction_month_ = prediction_month.replace('-', '_')
    p_limit = str(p_limit).split('.')[0]+'_'+str(p_limit).split('.')[1]
    df.reset_index(inplace = True)
    if with_add_vars == False:
        df.to_csv(coefs_and_pvalues_path+results_name+'_'+prediction_month_+'_'+spots_set+'_spots_'+with_wo_CB+'_'+event_date_type+'_p_below_'+p_limit+'.csv', index = False)
        bucket.store_csv_to_s3(data_frame = df, \
        file_name = results_name+'_'+prediction_month_+'_'+spots_set+'_spots_'+with_wo_CB+'_'+event_date_type+'_p_below_'+p_limit+'.csv', \
        dir = '/'+churn_based_on_behaviour_dir + coefs_and_pvalues_path)
    else:
        df.to_csv(coefs_and_pvalues_with_add_important_vars_path+results_name+'_'+prediction_month_+'_'+spots_set+'_spots_'+with_wo_CB+'_'+event_date_type)
        bucket.store_csv_to_s3(data_frame = df, \
        file_name = results_name+'_'+prediction_month_+'_'+spots_set+'_spots_'+with_wo_CB+'_'+event_date_type+'_p_below_'+p_limit+'.csv', \
        dir = '/'+churn_based_on_behaviour_dir + coefs_and_pvalues_with_add_important_vars_path)

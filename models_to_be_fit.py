import os

def check_churn_prediction_exports(churn_risk_prediction_exports, data_set_name, model_number, num_of_p_values, num_of_prediction_months):
    complete = True
    if 'model_'+str(model_number) not in os.listdir(churn_risk_prediction_exports):
        complete = False
    else:
        if len([x for x in os.listdir(churn_risk_prediction_exports+'/model_'+str(model_number)) if data_set_name in x])!=num_of_p_values:
            complete = False
        else:
            if ('testing' not in os.listdir(churn_risk_prediction_exports+'/model_'+str(model_number)+'/')):
                complete = False
            else:
                if len([x for x in os.listdir(churn_risk_prediction_exports+'/model_'+str(model_number)+'/testing/') if data_set_name in x])!=num_of_p_values*num_of_prediction_months:
                    complete = False
    return complete

def check_coefficients_and_pvalues_exports(coefficients_and_pvalues_exports, data_set_name, model_number, num_of_p_values, num_of_prediction_months):
    complete = True
    if 'model_'+str(model_number) not in os.listdir(coefficients_and_pvalues_exports):
        complete = False
    else:
        if len([x for x in os.listdir(coefficients_and_pvalues_exports+'/model_'+str(model_number)) if data_set_name in x and 'testing_' not in x])!=num_of_p_values:
            complete = False
        else:
            if len([x for x in os.listdir(coefficients_and_pvalues_exports+'/model_'+str(model_number)) if data_set_name in x and 'testing_' in x])!=num_of_p_values*num_of_prediction_months:
                complete = False

    return complete


def main(model_numbers, model_type, date_dir, churn_risk_prediction_exports, coefficients_and_pvalues_exports, data_set_name, num_of_p_values, num_of_prediction_months):
    already_fit = []
    yet_to_be_fit = []

    for model_number in model_numbers[1:]:
        if 'churn_risk_prediction' not in os.listdir('data/'+date_dir+'/exports/') or 'coefficients_and_pvalues' not in os.listdir('data/'+date_dir+'/exports/'):
            yet_to_be_fit.append(model_number)
        elif model_type not in os.listdir('data/'+date_dir+'/exports/churn_risk_prediction/') or model_type not in os.listdir('data/'+date_dir+'/exports/coefficients_and_pvalues/'):
            yet_to_be_fit.append(model_number)
        else:
            churn_prediction_exports_complete = check_churn_prediction_exports(churn_risk_prediction_exports=churn_risk_prediction_exports,\
                                      data_set_name=data_set_name,\
                                      model_number=model_number,\
                                      num_of_p_values=num_of_p_values,\
                                      num_of_prediction_months=num_of_prediction_months)
            coefficients_and_pvalues_exports_complete = check_coefficients_and_pvalues_exports(coefficients_and_pvalues_exports=coefficients_and_pvalues_exports,\
                                      data_set_name=data_set_name,\
                                      model_number=model_number,\
                                      num_of_p_values=num_of_p_values,\
                                      num_of_prediction_months=num_of_prediction_months)
            if churn_prediction_exports_complete and coefficients_and_pvalues_exports_complete:
                already_fit.append(model_number)
            else:
                yet_to_be_fit.append(model_number)

    return (already_fit, yet_to_be_fit)

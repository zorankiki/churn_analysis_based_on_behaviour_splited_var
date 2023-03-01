import pandas as pd

def extract_variables(df, date_dir, model_type, wo_spot_cat_and_metro_area):
    if wo_spot_cat_and_metro_area == True:
        vars_ = [x for x in df.columns if 'spot_category_' not in x and 'metro_area_' not in x\
                 and x not in ['start', 'stop','spot_id', 'event']]
        export = pd.DataFrame(vars_, columns = ['variable_name'])
        export.to_csv('data/'+date_dir+'/exports/'+model_type+'/all_variables_wo_spot_cat_and_metro_areas.csv', index = False)
    else:
        vars_ = [x for x in df.columns if x not in ['start', 'stop', 'spot_id', 'event']]
        export = pd.DataFrame(vars_, columns = ['variable_name'])
        export.to_csv('data/'+date_dir+'/exports/'+model_type+'/all_variables.csv', index = False)

import numpy as np
import pandas as pd
import seaborn as sns
from imblearn.over_sampling import SMOTE
from catboost import Pool, CatBoostClassifier
import catboost
import time
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split

# First is drop all useless columns
def reds_feature_selection(dataframe, ids = 'Yes', names = 'Yes', currency = 'Yes', misc = 'Yes', keep_float = 'No'):
    if names == 'Yes':
        dataframe = dataframe[dataframe.columns.drop(list(dataframe.filter(regex='NAME')))]
    if ids == 'Yes':
        dataframe = dataframe[dataframe.columns.drop(list(dataframe.filter(regex='ID')))]
    if currency == 'Yes':
        dataframe = dataframe[dataframe.columns.drop(list(dataframe.filter(regex='CURRENCY')))]
    if misc == 'Yes':
        dataframe = dataframe.drop(['LOG_ENTRY_TIME', 'USER_AGENT', 'IP_ADDRESS', 'MEDIA_COST_IN_BUCKS', \
                       'DATA_USAGE_TOTAL_COST', \
                       'ADS_TXT_SELLER_TYPE', 'ADS_TXT_SELLER_TYPE', 'MATCHED_GENRE', 'CHANNEL_TYPE', 'MEDIA_TYPE',\
                        'FEE_FEATURES_COST', 'VOLUME_CONTROL_PRIORITY', 'FREQUENCY', 'AUCTION_TYPE', \
                                   'LATITUDE', 'LONGITUDE', 'WEEK_OF_YEAR'], 1)
    if keep_float == 'No':
        dataframe = dataframe.drop(['TEMPERATURE_IN_CELSIUS'], 1)
    return dataframe

# First is drop all useless columns
def reds_feature_selection(dataframe, ids = 'Yes', names = 'Yes', currency = 'Yes', misc = 'Yes', keep_float = 'No'):
    if names == 'Yes':
        dataframe = dataframe[dataframe.columns.drop(list(dataframe.filter(regex='NAME')))]
    if ids == 'Yes':
        dataframe = dataframe[dataframe.columns.drop(list(dataframe.filter(regex='ID')))]
    if currency == 'Yes':
        dataframe = dataframe[dataframe.columns.drop(list(dataframe.filter(regex='CURRENCY')))]
    if misc == 'Yes':
        dataframe = dataframe.drop(['LOG_ENTRY_TIME', 'REFERRER_CATEGORIES', 'USER_AGENT', 'IP_ADDRESS', 'MEDIA_COST_IN_BUCKS', \
                       'DATA_USAGE_TOTAL_COST', 'TEMPERATURE_BUCKET_END_IN_CELSIUS', 'TEMPERATURE_BUCKET_START_IN_CELSIUS', \
                       'ADS_TXT_SELLER_TYPE', 'ADS_TXT_SELLER_TYPE', 'MATCHED_GENRE', 'CHANNEL_TYPE', 'MEDIA_TYPE',\
                        'FEE_FEATURES_COST', 'VOLUME_CONTROL_PRIORITY', 'FREQUENCY', 'AUCTION_TYPE', \
                                   'LATITUDE', 'LONGITUDE', 'LIVE_STREAM', 'CONTENT_DURATION', 'WEEK_OF_YEAR'], 1)
    if keep_float == 'No':
        dataframe = dataframe.drop(['TEMPERATURE_IN_CELSIUS'], 1)
    return dataframe

# filling nulls and fixing types to be able to run in catboost
def fix_nulls_and_types(dataframe, nulls = 'Yes', types = 'Yes', val = 0, threshold = .25):
    if nulls == "Yes":
        dataframe = dataframe.loc[:, dataframe.isnull().sum() < threshold * dataframe.shape[0]]
        dataframe = dataframe.fillna(value = val)
    if types == 'Yes':
        for col in dataframe.columns:
            if dataframe[col].dtype == 'float64' or dataframe[col].dtype == '<M8[ns]':
                dataframe[col] = dataframe[col].astype(str)
    return dataframe


# Splitting data functions
def split_data_all(dataframe, y_val = 'CONVERTED', prop = .2):
    X = dataframe.drop([y_val], 1)
    Y = dataframe[y_val]
    X_train, X_val, Y_train, Y_val = train_test_split(X, Y, test_size= prop, random_state=0, stratify = Y)
    return X_train, X_val, Y_train, Y_val

# splitting data when keeping all conversions, but only part of the other data
def split_data_part(dataframe, non_conversions, y_val = 'CONVERTED', prop = .2):
    import random
    

    random.seed(15)
    clicks_df = dataframe[dataframe[y_val] == 1]
    new_df = dataframe[dataframe[y_val] == 0].sample(non_conversions)
    combined_df = pd.concat([clicks_df, new_df], axis = 0)
    X = combined_df.drop([y_val], 1)
    Y = combined_df[y_val]
    X_train, X_val, Y_train, Y_val = train_test_split(X, Y, test_size= prop, random_state=0, stratify = Y)
    return X_train, X_val, Y_train, Y_val
    

# running catboost, returning the top 10 most common features in graph and name
def run_catboost(X_train, X_val, Y_train, Y_val, iterations, bootstrap, num_features):

    categorical_features_indices = np.where(X_train.dtypes != np.float)[0]
    print(categorical_features_indices)

    clf = CatBoostClassifier(
        iterations=iterations,
        bootstrap_type = bootstrap,
        custom_metric=['F1', 'Accuracy', 'Precision', 'Recall'],
        learning_rate=.18, 
        loss_function='Logloss',
        eval_metric='Logloss'
    )

    clf.fit(X_train, Y_train, 
            cat_features=categorical_features_indices, 
            eval_set=(X_val, Y_val),
            use_best_model=True,
            plot = True, 
            verbose=False
    )

    print('CatBoost model is fitted: ' + str(clf.is_fitted()))
    print('CatBoost model parameters:')
    print(clf.get_params())
    

    print('Best Score:')
    print(clf.get_best_score())
    score = clf.get_best_score()

    print('Best Iteration:')
    print(clf.get_best_iteration())


    #Shap features and feature importance
    import shap
    shap_values = clf.get_feature_importance(Pool(X_val, label=Y_val,cat_features=categorical_features_indices), 
                                                                         type="ShapValues")
    expected_value = shap_values[0,-1]
    shap_values = shap_values[:,:-1]

    shap.initjs()
    shap.force_plot(expected_value, shap_values[3,:], X_val.iloc[3,:])

    shap.summary_plot(shap_values, X_val)
    
    shap.summary_plot(shap_values, features=X_val, feature_names=X_val.columns, plot_type='bar')
    
    # returning top X features
    explainer = shap.TreeExplainer(clf)
    shap_values = explainer.shap_values(X_train)
    new_s = np.average(np.abs(shap_values), axis = 0)
    cols = X_train.columns
    
    r = pd.DataFrame({'Variable': cols, 'Shap': new_s}, columns = ['Variable', 'Shap']).sort_values(by='Shap', ascending=False)
    top_vars = r['Variable'][0:num_features].to_numpy().tolist()
    
    my_top_vars =(','.join(top_vars))
    df2 = [[my_top_vars, 'CONVERTED']]
    

    return df2, clf

#def select_geo(geo_var, dataframe):
#    if geo_var == 'REGION':
#        dataframe == dataframe.drop(['CITY', 'ZIP_CODE_POSTAL'],1)
#        elif geo_var == 'CITY':
#            dataframe == dataframe.drop(['REGION', 'ZIP_CODE_POSTAL'],1)
#            elif geo_var == 'ZIP_CODE_POSTAL':
#                dataframe == dataframe.drop(['CITY', 'REGION'],1)
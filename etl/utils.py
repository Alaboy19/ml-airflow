import pandas as pd 


def outliers(data: pd.Dataframe)-> pd.DataFrame:
    num_cols = data.select_dtypes(['float']).columns
    threshold = 1.5
    potential_outliers = pd.DataFrame()

    for col in num_cols:
        Q1 = data[col].quantile(0.25)# Ваш код здесь #
        Q3 = data[col].quantile(0.75)# Ваш код здесь #
        IQR = Q3 - Q1# Ваш код здесь #
        margin = threshold * IQR# Ваш код здесь #
        lower = Q1 - margin# Ваш Код здесь #
        upper = Q3 + margin#Ваш Код здесь #
        potential_outliers[col] = ~data[col].between(lower, upper)

    outliers = potential_outliers.any(axis=1)
    #print(outliers)
    return data[~outliers]


def fill_missing_values(data: pd.Dataframe)-> pd.DataFrame:
    cols_with_nans = data.isnull().sum()
    cols_with_nans = cols_with_nans[cols_with_nans > 0].index.drop('end_date')
    for col in cols_with_nans:
        if data[col].dtype in [float, int]:
            fill_value = data[col].mean()
        elif data[col].dtype == 'object':
            fill_value = data[col].mode().iloc[0]
        data[col] = data[col].fillna(fill_value)
    return data


def remove_duplicates(data: pd.Dataframe)-> pd.DataFrame:
    feature_cols = data.columns.drop('customer_id').tolist()
    is_duplicated_features = data.duplicated(subset=feature_cols, keep=False)
    data = data[~is_duplicated_features].reset_index(drop=True)
    return data 
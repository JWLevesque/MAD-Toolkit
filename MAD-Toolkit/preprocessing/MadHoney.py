# -*- coding: utf-8 -*-
"""
Created on Tue Mar  4 10:28:03 2025

@author: jwlev
"""
import pandas as pd
import json
from sklearn.base import BaseEstimator, TransformerMixin, _fit_context

class MadHoney(TransformerMixin, BaseEstimator):
    """Performs preprocessing appropriate for the BETH dataset.

    Parameters
    ----------

    Attributes
    ----------
    n_features_in_ : int
        Number of features seen during :term:`fit`.

    feature_names_in_ : ndarray of shape (`n_features_in_`,)
        Names of features seen during :term:`fit`. Defined only when `X`
        has feature names that are all strings.
    """

    # This is a dictionary allowing to define the type of parameters.
    # It used to validate parameter within the `_fit_context` decorator.
    _parameter_constraints = {
        
    }

    def __init__(self):
        pass

    @_fit_context(prefer_skip_nested_validation=True)
    def fit(self, X, y=None):
        """The pipeline API requires this method despite this Transformer being
                stateless.

        Parameters
        ----------
        X : {array-like, sparse matrix}, shape (n_samples, n_features)
            The training input samples.

        y : None
            There is no need of a target in a transformer, yet the pipeline API
            requires this parameter.

        Returns
        -------
        self : object
            Returns self.
        """
        X = self._validate_data(X, accept_sparse=True)

        # Return the transformer
        return self

    def transform(self, X):
        """Transforms a BETH-style dataset.

        Parameters
        ----------
        X : {array-like, sparse-matrix}, shape (n_samples, n_features)
            The input samples.

        Returns
        -------
        X_transformed : array, shape (n_samples, n_features)
            The array containing the transformed values
            in ``X``.
        """
        # Since this is a stateless transformer, we should not call `check_is_fitted`.
        # Common test will check for this particularly.

        # Input validation
        # We need to set reset=False because we don't want to overwrite `n_features_in_`
        # `feature_names_in_` but only check that the shape is consistent.
        X = self._validate_data(X, accept_sparse=True, reset=False)
        
        def _process_args_row(row):
            """
            Takes a single value from the 'args' column
            and returns a processed dataframe row
            
            Args:
                row: A single 'args' value/row
                
            Returns:
                final_df: The processed dataframe row
            """
            
            row = row.split('},')
            row = [string.replace("[", "").replace("]", "").replace("{", "").replace("'", "").replace("}", "").lstrip(" ") for string in row]
            row = [item.split(',', maxsplit = 2) for item in row]
            
            processed_row = []
            for lst in row:
                for key_value in lst:
                    key, value = key_value.split(': ', 1)
                    if not processed_row or key in processed_row[-1]:
                        processed_row.append({})
                    processed_row[-1][key] = value
            
            json_row = json.dumps(processed_row)
            row_df = pd.json_normalize(json.loads(json_row))
            
            final_df = row_df.unstack().to_frame().T.sort_index(axis=1)#1,1)
            final_df.columns = final_df.columns.map('{0[0]}_{0[1]}'.format)
            
            return final_df

        def _process_args_dataframe(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
            """
            Processes the `args` column within the dataset
            """
            
            processed_dataframes = []
            data = df[column_name].tolist()
            
            # Debug counter
            counter = 0
            
            for row in data:
                if row == '[]': # If there are no args
                    pass
                else:
                    try:
                        ret = _process_args_row(row)
                        processed_dataframes.append(ret)
                    except:
                        print(f'Error Encounter: Row {counter} - {row}')

                    counter+=1
                
            processed = pd.concat(processed_dataframes).reset_index(drop=True)
            processed.columns = processed.columns.str.lstrip()
            
            df = pd.concat([df, processed], axis=1)
            
            return df

        def _prepare_dataset(df: pd.DataFrame, process_args=True) -> pd.DataFrame:
            """
            Prepare the dataset by completing the standard feature engineering tasks
            """
            prepped_df = df.copy(deep = True)
            
            prepped_df["processId"] = df["processId"].map(lambda x: 0 if x in [0, 1, 2] else 1)  # Map to OS/not OS
            prepped_df["parentProcessId"] = df["parentProcessId"].map(lambda x: 0 if x in [0, 1, 2] else 1)  # Map to OS/not OS
            prepped_df["userId"] = df["userId"].map(lambda x: 0 if x < 1000 else 1)  # Map to OS/not OS
            prepped_df["mountNamespace"] = df["mountNamespace"].map(lambda x: 0 if x == 4026531840 else 1)  # Map to mount access to mnt/ (all non-OS users) /elsewhere
            prepped_df["eventId"] = df["eventId"]  # Keep eventId values (requires knowing max value)
            prepped_df["returnValue"] = df["returnValue"].map(lambda x: 0 if x == 0 else (1 if x > 0 else 2))  # Map to success/success with value/error
            
            if process_args is True:
                df = _process_args_dataframe(df, 'args')
            
            # Keep : df[["processId", "parentProcessId", "userId", "mountNamespace", "eventId", "argsNum", "returnValue"]]
            # + the expanded process_args columns if the process_args argument is passed as True
            features = df.drop(['timestamp', 'threadId', 'processName', 'hostName', 'eventName', 'stackAddresses','args', 'sus','evil'], axis = 1, errors = 'ignore')
                
            return features
        
        features = _prepare_dataset(X)
        
        return features

    def _more_tags(self):
        # This is a quick example to show the tags API:\
        # https://scikit-learn.org/dev/developers/develop.html#estimator-tags
        # Here, our transformer does not do any operation in `fit` and only validate
        # the parameters. Thus, it is stateless.
        return {"stateless": True}

# def _process_args_row(row):
#     """
#     Takes a single value from the 'args' column
#     and returns a processed dataframe row
    
#     Args:
#         row: A single 'args' value/row
        
#     Returns:
#         final_df: The processed dataframe row
#     """
    
#     row = row.split('},')
#     row = [string.replace("[", "").replace("]", "").replace("{", "").replace("'", "").replace("}", "").lstrip(" ") for string in row]
#     row = [item.split(',', maxsplit = 2) for item in row]
    
#     processed_row = []
#     for lst in row:
#         for key_value in lst:
#             key, value = key_value.split(': ', 1)
#             if not processed_row or key in processed_row[-1]:
#                 processed_row.append({})
#             processed_row[-1][key] = value
    
#     json_row = json.dumps(processed_row)
#     row_df = pd.json_normalize(json.loads(json_row))
    
#     final_df = row_df.unstack().to_frame().T.sort_index(axis=1)#1,1)
#     final_df.columns = final_df.columns.map('{0[0]}_{0[1]}'.format)
    
#     return final_df

# def _process_args_dataframe(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
#     """
#     Processes the `args` column within the dataset
#     """
    
#     processed_dataframes = []
#     data = df[column_name].tolist()
    
#     # Debug counter
#     counter = 0
    
#     for row in data:
#         if row == '[]': # If there are no args
#             pass
#         else:
#             try:
#                 ret = _process_args_row(row)
#                 processed_dataframes.append(ret)
#             except:
#                 print(f'Error Encounter: Row {counter} - {row}')

#             counter+=1
        
#     processed = pd.concat(processed_dataframes).reset_index(drop=True)
#     processed.columns = processed.columns.str.lstrip()
    
#     df = pd.concat([df, processed], axis=1)
    
#     return df

# def prepare_dataset(self, df: pd.DataFrame, process_args=False) -> pd.DataFrame:
#     """
#     Prepare the dataset by completing the standard feature engineering tasks
#     """
#     prepped_df = df.copy(deep = True)
    
#     prepped_df["processId"] = df["processId"].map(lambda x: 0 if x in [0, 1, 2] else 1)  # Map to OS/not OS
#     prepped_df["parentProcessId"] = df["parentProcessId"].map(lambda x: 0 if x in [0, 1, 2] else 1)  # Map to OS/not OS
#     prepped_df["userId"] = df["userId"].map(lambda x: 0 if x < 1000 else 1)  # Map to OS/not OS
#     prepped_df["mountNamespace"] = df["mountNamespace"].map(lambda x: 0 if x == 4026531840 else 1)  # Map to mount access to mnt/ (all non-OS users) /elsewhere
#     prepped_df["eventId"] = df["eventId"]  # Keep eventId values (requires knowing max value)
#     prepped_df["returnValue"] = df["returnValue"].map(lambda x: 0 if x == 0 else (1 if x > 0 else 2))  # Map to success/success with value/error
    
#     if process_args is True:
#         df = _process_args_dataframe(df, 'args')
    
#     if(self.is_labelled):
#         labels = df['sus']
#     else:
#         labels = [None] * df.shape[0]
        
#     # Keep : df[["processId", "parentProcessId", "userId", "mountNamespace", "eventId", "argsNum", "returnValue"]]
#     # + the expanded process_args columns if the process_args argument is passed as True
#     features = df.drop(['timestamp', 'threadId', 'processName', 'hostName', 'eventName', 'stackAddresses','args', 'sus','evil'], axis = 1)
        
#     return features, labels
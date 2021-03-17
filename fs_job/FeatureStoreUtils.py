"""
Utility function for running experimental feature store pipeline
"""
from sagemaker.feature_store.feature_group import FeatureGroup
import pandas as pd


def recreate_feature_store_from_dataframe(df, role, fs_session, sm_client, fg_name, fg_descr, record_col, event_col, s3_uri):
    """
    Create/Recreate Sagemaker Feature Group from a Pandas DataFrame.
    This function helps you iterate on experimental feature groups.
    Create and recreate them at will gradually building up the design of the
    set of features.
    """
    fg = get_clean_feature_group(fg_name, fs_session)
    feature_defs = df_to_defs(df)
    offcfg = {'S3StorageConfig': {'S3Uri': s3_uri}}   

    sm_client.create_feature_group(
        FeatureGroupName = fg_name,
        RecordIdentifierFeatureName = record_col,
        EventTimeFeatureName = event_col,
        FeatureDefinitions = feature_defs,
        Description = fg_descr,
        OnlineStoreConfig = {'EnableOnlineStore': True},
        RoleArn = role,
        OfflineStoreConfig=offcfg)
    
    return fg

    
def get_clean_feature_group(fg_name, fs_session):
    """
    Get a Sagemaker Feature Group Object, deleting previous entry
    if it exists
    
    :param fg_name: Name of the Feature Group
    
    :param fs_session: A Sagemaker Feature Store Session Object
    
    :return: Sagemaker Feature Group
    """
    fg = FeatureGroup(name=fg_name, sagemaker_session=fs_session)
    try:
        fg.delete()
    except:
        print("Feature Store did not exist")
        
    return fg


def df_to_defs(df):
    """
    Given a Pandas DataFrame return an array of feature definitions
    to be used in the creation of a Sagemaker FeatureStore Feature Group.
    
    :param df: Pandas DataFrame
    
    :return: Array( Dictionary('FeatureName','FeatureType') )
    """
    feature_definitions = []
    for col, dt in zip(df.columns, df.dtypes):
        feature = {'FeatureName': col}
        if dt=='float64':
            feature['FeatureType'] = 'Fractional'
        elif dt=='int64':
            feature['FeatureType'] = 'Integral'
        else:
            feature['FeatureType'] = 'String'    
        feature_definitions.append(feature)
    return feature_definitions



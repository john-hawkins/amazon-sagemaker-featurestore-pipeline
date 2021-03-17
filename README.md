# amazon-sagemaker-featurestore-pipeline

The goal of these scripts is to create a Sagemaker Pipeline
that will process a dataset to create features and then store those
features into the Feature Store as a single feature group.

The design is such that all features are provided as functions inside
the file [Features.py](fs_job/Features.py) and added to the dataset
using the function name as the feature name.

The  feature store load will be done assuming that we should overwrite
any feature group of the same name. The data types are inferred from them
columns of the dataframe.

## USAGE

Follow the steps in the [FeatureStore Pipeline Notebook](FeatureStore_Pipeline.ipynb)

You will need to modify the bucket and file details as required.

Once the job is completed you can use the [View Feature Store Notebook](View_FeatureStore.ipynb)
to inspect the new feature group.


### TODO

Convert this into a reory that will work with the CI/CD component of Sagemaker Pipelines.

Create a version that uses a pyspark processing job and feature definition file



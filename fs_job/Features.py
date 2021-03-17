
"""
This file defines your custom features.
It will be used to populate a custom feature group.
The contents of this file will be inspected to determine
the number of features (number of functions).

These functions need to be aware of the column names in the dataframe
"""
import pandas as pd

def InjuryToVehicleClaimRatio(df):
    return df["injury_claim"]/df["vehicle_claim"]

def ReportIsStrange(df):
    regex = "weird|strange|inconsistent|unusual|suspicious"
    return df["report"].str.contains(regex, regex=True, case=False, na=False).map(int)


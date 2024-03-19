import glob
import os.path
import csv
import pandas as pd
from datetime import date, datetime, timedelta
import pyodbc
import logging
import argparse
import variables



# General df parse
def general_parse(dataframe, file_instance):

    logging.info(f"Parsing file: {file_instance.name}")

    file = file_instance.path
    csv_true = file_instance.csv_true
    name = file_instance.name
    nickname = file_instance.nickname
    file_type = file_instance.fileType

    df = pd.DataFrame()

    try:
        if 'Status' in dataframe.columns:
            dataframe = dataframe.loc[dataframe['Status'] == 4]


        # ActivityCode
        if 'Activity Code' in dataframe.columns:
            df['ActivityCode'] = dataframe['Activity Code']
        else:
            df['ActivityCode'] = dataframe['ActivityCode']


        # CompletionDate
        if 'Completion Date' in dataframe.columns:
            df['CompletionDate'] = pd.to_datetime(dataframe['Completion Date'])
            df.loc[:, 'CompletionDate'] = df['CompletionDate'].dt.date
        elif 'CompletionDate' in dataframe.columns:
            df['CompletionDate'] = pd.to_datetime(dataframe['CompletionDate'])
            df.loc[:, 'CompletionDate'] = df['CompletionDate'].dt.date

        
        # Score
        if 'Score' in dataframe.columns:
            df['Score'] = dataframe['Score'].fillna(0)
            df.loc[:, 'Score'] = df['Score'].astype(int)


        # Email
        if 'Student ID' in dataframe.columns:
            df['Email'] = dataframe['Student ID'].str.lower()
        elif 'Student Email' in dataframe.columns:
            df['Email'] = dataframe['Student Email'].str.lower()
            df.loc[:, 'Email'] = df['Email'].str.strip()
            df.loc[:, 'Email'] = df['Email'].fillna('BLANK')


        # EmpID
        if 'Student Username' in dataframe.columns:
            df['EmpID'] = dataframe['Student Username']
            df.loc[:, 'EmpID'] = df['EmpID'].astype(int)



        if file_type == 'csv':
            # Filter for CompletionDate >= 6 months ago
            now = datetime.now()
            cutoff_date = now - timedelta(days= 21)
            logging.info(f"Cutoff date = {cutoff_date}")

            df = df.loc[df['CompletionDate'] >= cutoff_date]
            
            # Reset index
            df = df.reset_index(drop=True)

        # Log Number of Rows in DataFrame
        dfLength = str(len(df.index))
        logging.debug("Number of rows in DataFrame: " + dfLength)



    except Exception as e:
        logging.critical(f"Error occurred: {e}")
        logging.critical(f"FAIL: general_parse({file_instance.name})")

    return df

    
# Merge for active activities
def dfs_merge(df_left, df_right):

    df = pd.DataFrame()

    try:
        df = pd.merge(df_left, df_right, on='ActivityCode', how='inner')
        logging.info("SUCCESS: dfs_merge()")

    except Exception as e:
        logging.critical(f"Error occurred: {e}")
        logging.critical("FAIL: dfs_merge()")

    return df



# Final df parse - required static values
def final_parse(dataframe):
    logging.info("Performing final_parse()")

    df = pd.DataFrame()

    try:
        df['EmployeeNumber'] = dataframe['Email']
        df['ActivityCode'] = dataframe['ActivityCode']
        df['ClassStartDate'] = None
        df['RegistrationDate'] = None

        df['CompletionDate'] = pd.to_datetime(dataframe['CompletionDate'])
        df.loc[:, 'CompletionDate'] = df['CompletionDate'].dt.strftime('%m/%d/%Y') + ' 09:00'

        df['FirstLaunchDate'] = None
        df['Score'] = dataframe['Score']
        df['Passed'] = 1
        df['CancellationDate'] = None
        df['PaymentTerm'] = None
        df['Cost'] = None
        df['Currency'] = None
        df['Timezone'] = 'America/Phoenix'
        df['Status'] = 4
        df['Notes'] = None
        df['SubscriptionSourceActivityCode'] = None
        df['SubscriptionSourceActivityStartDate'] = None
        df['ElapsedTime'] = None
        df['CompletionStatus'] = 1
        df['Location_Name'] = None
        df['Slotstart_Date'] = None
        df['Slotend_Date'] = None
        df['EmpID'] = dataframe['EmpID']

        logging.info("SUCCESS: final_parse()\n")

    except Exception as e:
        logging.critical(f"Error occurred: {e}")
        logging.critical("FAIL: final_parse()\n")
    
    return df
import glob
import os.path
import csv
import pandas as pd
from datetime import date, datetime, timedelta
import pyodbc
import logging
import argparse
import variables



# Connect to SQL Servers
def connect_sql_server(server_instance):

    conn = None
    cursor = None
    
    driver = server_instance.driver
    server = server_instance.server
    database = server_instance.database
    user = server_instance.user

    logging.info(f"Connecting to SQL Server: {server}...")
    logging.debug("SQL Server Driver: " + driver)
    logging.debug("SQL Server Server: " + server)
    logging.debug("SQL Server Database: " + database)
    logging.debug("SQL Server Connection String: " + 'DRIVER='+driver+';SERVER='+server+';DATABASE=' +database+';UID='+user+';Trusted_Connection=yes;TrustServerCertificate=yes')
    logging.debug("Current User: " + os.getlogin())
    logging.info("Current User: " + os.getlogin())

    try:
        conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE=' +
                            database+';UID='+user+';Trusted_Connection=yes;TrustServerCertificate=yes')
        cursor = conn.cursor()

        logging.info(f"SUCCESS: connect_sql_server({ server_instance.server })")

    except Exception as e:
        logging.critical(e)
        logging.critical(f"FAIL: connect_sql_servers({ server_instance.server })")

    return conn, cursor




# Insert dfs into database
def insert_query(conn, dataframe, table):

    tableName = table.name
    tablePath = table.path

    logging.info(f"Inserting df into table: {tablePath}")

    try:
        # Log Number of Rows in DataFrame
        dfLength = str(len(dataframe.index))
        logging.info(f"Number of rows in {tableName}: " + dfLength)

        # Write to log every 5% of rows
        logInt = int(dfLength) / 20
        logInt = round(logInt)
        logging.info("Log interval: " + str(logInt))

        counter = 0

        # Create a cursor
        cursor = conn.cursor()

        # Truncate the table
        cursor.execute(f"TRUNCATE TABLE {tablePath};")
        conn.commit()

        # Iterate over rows in the DataFrame
        for index, row in dataframe.iterrows():

            if counter % logInt == 0:
                logging.info(f"{tablePath}: Inserting row... " + str(counter) + "/" + dfLength)

            # Get values from the DataFrame dynamically
            values = tuple(row[col] for col in dataframe.columns)

            # Define the SQL query dynamically with placeholders for parameterized values
            columns = ','.join(dataframe.columns)
            placeholders = ','.join(['?' for _ in dataframe.columns])
            query = f"""
            INSERT INTO {tablePath}
                ({columns})
                VALUES
                ({placeholders})
            """

            # Execute the query with parameterized values
            cursor.execute(query, values)
            conn.commit()

            counter += 1


        # Close the cursor and connection
        cursor.close()

        logging.info(f"SUCCESS: insert_query({tablePath})")

    except Exception as e:
        logging.critical(f"Error occurred: {e}")
        conn.rollback()  # Roll back changes if an error occurs
        logging.critical(f"FAIL: insert_query({tableName})")



# Adotmaster - join for correct email address
def correct_email(conn):

    logging.info("Querying for correct email")

    try:
        cursor = conn.cursor()

        null_check_query = """
        SELECT COUNT(*)
        FROM [eric].[dbo].[tmp_Tracorp_Daily]
        WHERE Email_adotmaster IS NULL
        """

        cursor.execute(null_check_query)
        null_count = cursor.fetchone()[0]

        if null_count > 0:

            query = """ 
            UPDATE tc 
            SET tc.Email_adotmaster = am.EmployeeEmailAddress
            FROM [eric].[dbo].[tmp_Tracorp_Daily] AS tc
            JOIN [adotmaster].[dbo].[VW_EmployeeRoster] AS am ON tc.EmpID = am.EIN;
            """

            cursor.execute(query)
            conn.commit()
            logging.info("Emails updated in tmp_Tracorp_Daily")


        cursor.close()
        logging.info("SUCCESS: correct_email()")


    except Exception as e:
        logging.critical("FAIL: correct_email()")
        logging.critical(f"Error occurred: {e}")
        conn.rollback()  # Roll back changes if an error occurs


# Remove duplicates
def remove_duplicates(conn):

    logging.info("Removing duplicates from tracorp [eric].[dbo].[tmp_Tracorp_Daily]")

    try:
        query = """ 
        SELECT 
            tc.ActivityCode,
            tc.Email_adotmaster AS Email,
            tc.EmpID,
            tc.CompletionDate,
            tc.Score
        FROM 
            [eric].[dbo].[tmp_Tracorp_Daily] AS tc
        LEFT JOIN 
            [eric].[dbo].[tmp_Successful_Tracorp_Completions_xlsx] AS comp 
        ON 
            tc.ActivityCode = comp.ActivityCode
            AND tc.Email_adotmaster = comp.Email
            AND tc.CompletionDate = comp.CompletionDate
        WHERE 
            comp.ActivityCode IS NULL
            AND tc.Email_adotmaster IS NOT NULL;
        """

        df = pd.read_sql(query, conn)
        df.loc[:, 'Email'] = df['Email'].fillna("BLANK")

        df = df.loc[df['Email'] != "BLANK"]

        dfLength = str(len(df.index))
        
        logging.info("SUCCESS: remove_duplicates()")
        logging.info(f"df_tracorp_final contains: {dfLength} rows!")


    except Exception as e:
        logging.critical("FAIL: remove_duplicates()")
        logging.critical(f"Error occurred: {e}")
        conn.rollback()  # Roll back changes if an error occurs

    return df


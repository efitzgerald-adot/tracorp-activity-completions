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

    logging.info("Connecting to SQL Server...")
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

        logging.info(f"SUCCESS: connect_sql_server({ server_instance.server })n")

    except Exception as e:
        logging.critical(e)
        logging.critical(f"FAIL: connect_sql_servers({ server_instance.server })\n")

    return conn, cursor




# Insert dfs into database
def insert_query(conn, dataframe, table):

    tableName = table.name
    tablePath = table.path
    tableType = table.table_type

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
        if tableType == 'tmp':
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

        logging.info(f"SUCCESS: insert_query({tablePath}) \n")

    except Exception as e:
        logging.critical(f"Error occurred: {e}")
        conn.rollback()  # Roll back changes if an error occurs
        logging.critical(f"FAIL: insert_query({tableName}) \n")



# Adotmaster - join for correct email address
def correct_email(conn):

    logging.info("Querying for correct email")

    try:
        dfLength = str(len(dataframe.index))
        logging.debug("Number of rows in DataFrame: " + dfLength)
        cursor = conn.cursor()

        for index, row in dataframe.iterrows():
            logging.debug("Index: " + str(index) + "/" + dfLength)
            logging.debug("Row: " + str(row))

            user_id = row['EmpID']

            # Pad EIN with leading zeros
            user_id = str(user_id).zfill(9)

            query = f"""
            SELECT {table_instance.key_email} AS Email
            FROM {table_instance.path}
            WHERE {table_instance.key_empId} = {user_id}
            """

            cursor.execute(query)

            email = cursor.fetchone()
                

            if email is None:
                email = row['Email']
                logging.debug("Email not returned in query for user: " + str(user_id))
                logging.debug("Email: " + email)
            else: 
                logging.debug("Email returned in query for user:" + str(user_id))
                logging.debug("Results: " + str(email))
                email = str(email[0])
                logging.debug("Email: " + email)
            
            dataframe.at[index, 'Email'] = email

        cursor.close()
        logging.info("SUCCESS: correct_email()\n")


    except Exception as e:
        logging.critical("FAIL: correct_email()")
        logging.critical(f"Error occurred: {e}\n")
        conn.rollback()  # Roll back changes if an error occurs

    return dataframe




# General remove duplicates
def general_distinct_query(conn, table_left, table_right):

    tableLeft_name = table_left.name
    tableLeft_database = table_left.path
    tableLeft_path = table_left.path
    tableLeft_type = table_left.table_type
    tableLeft_email = table_left.key_email
    tableLeft_activity = table_left.key_activity
    tableLeft_date = table_left.key_date

    tableRight_name = table_right.name
    tableRight_database = table_right.database
    tableRight_path= table_right.path
    tableRight_type = table_right.table_type
    tableRight_email = table_right.key_email
    tableRight_activity = table_right.key_activity
    tableRight_date = table_right.key_date

    logging.info(f"Removing duplicates from {tableLeft_path}")

    df = pd.DataFrame()

    print(tableLeft_path)
    print(tableRight_path)

    try:
        query = f""" 
        SELECT 
            l.{tableLeft_activity},
            l.{tableLeft_email},
            l.EmpID,
            l.{tableLeft_date},
            l.Score
        FROM 
            {tableLeft_path} AS l
        LEFT JOIN 
            {tableRight_path} AS r 
        ON 
            l.{tableLeft_activity} = r.{tableRight_activity}
            AND l.{tableLeft_email} = r.{tableRight_email}
            AND l.{tableLeft_date} = r.{tableRight_date}
        WHERE 
            r.{tableRight_activity} IS NULL
            AND l.{tableLeft_email} IS NOT NULL;
        """

        df = pd.read_sql(query, conn)
        df.loc[:, 'Email'] = df['Email'].fillna("BLANK")

        df = df.loc[df['Email'] != "BLANK"]


        # Print length of new df

        

        logging.info(f"SUCCESS: general_distinct_query({tableLeft_path}) \n")

    except Exception as e:
        logging.critical(f"FAIL: general_distinct_query({tableLeft_path})")
        logging.critical(f"Error occurred: {e} \n")
        conn.rollback()  # Roll back changes if an error occurs

    return df


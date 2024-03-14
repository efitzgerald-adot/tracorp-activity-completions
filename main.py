import glob
import os.path
import csv
import pandas as pd
from datetime import date, datetime, timedelta
import pyodbc
import logging
import argparse
import configparser

# Custom modules
import variables
from functions_sql import *
from functions_in_out import *
from functions_parse import *


# Parse Arguments
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Configuration file path", default="config.ini")
    parser.add_argument("-p", "--path", help="Working directory", default=".")
    parser.add_argument("-n", "--no-sql", help="Execute without connecting to SQL Server", action="store_true")
    parser.add_argument("-d", "--debug", help="Debug mode", action="store_true")
    parser.add_argument("-v", "--verbose", help="Enable verbose console", action="store_true")
    args = parser.parse_args()

    return args


# Setup logging
def setup_logging(path, debug):
    # Get current datetime in YYYYMMDDHHMM format
    now = datetime.now().strftime("%Y%m%d%H%M")
    log_path = os.path.join(path, "logs")
    log_file = os.path.join(log_path, "reformatTracorp_" + now + ".log")
    # Create log directory if it doesn't exist
    if not os.path.exists(log_path):
        logging.debug("Log directory does not exist. Creating...")
        os.makedirs(log_path)

    if debug:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
    logging.basicConfig(filename=log_file, level=log_level,
                        format='%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    console = logging.StreamHandler()
    if args.verbose:
        console.setLevel(logging.DEBUG)
    else:
        console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)
    logging.info("Logging started")
    logging.debug("Log file: " + log_file)
    
    return log_file


# Read config file
def read_config(configFilePath):
    logging.info("Reading configuration file...")
    logging.debug("Configuration file path: " + configFilePath)

    try:
        config = configparser.ConfigParser()
        config.read(configFilePath)
        
        logging.info("Configuration file read successfully")

        return config
    except Exception as e:
        logging.critical("Error reading configuration file: " + str(e))


# Main
def main(log_file):

    # # # Classes
    class sql_server:
        def __init__(self, driver, server, database, user, encrypt, auth):
            self.driver = driver
            self.server = server
            self.database = database
            self.user = user
            self.encrypt = encrypt
            self.auth = auth

    class sql_tables:
        def __init__(self, name, database, path):
            self.name = name
            self.database = database
            self.path = path

    class raw_file:
        def __init__(self, path, name, csv_true, nickname, delimiter, fileType):
            self.path = path
            self.name = name
            self.csv_true = csv_true
            self.nickname = nickname
            self.delimiter = delimiter
            self.fileType = fileType

    class sftp_server:
        def __init__(self, url, username, file_path, key_path):
            self.url = url
            self.username = username
            self.file_path = file_path
            self.key_path = key_path

    class smtp_server:
        def __init__(self, smtpserver, port, email_from, email_to):
            self.smtpserver = smtpserver
            self.port = port
            self.email_from = email_from
            self.email_to = email_to


    # # # Instances
    # SQL Servers
    server_sql11worke = sql_server(driver=config['sql_server_sql11']['driver'],
                                    server=config['sql_server_sql11']['server'],
                                    database=config['sql_server_sql11']['database'],
                                    user=config['sql_server_sql11']['user'],
                                    encrypt=config['sql_server_sql11']['encrypt'],
                                    auth=config['sql_server_sql11']['auth'])

    server_aidwsql = sql_server(driver=config['sql_server_aidwsqld98001']['driver'],
                                server=config['sql_server_aidwsqld98001']['server'],
                                database=config['sql_server_aidwsqld98001']['database'],
                                user=config['sql_server_aidwsqld98001']['user'],
                                encrypt=config['sql_server_aidwsqld98001']['encrypt'],
                                auth=config['sql_server_aidwsqld98001']['auth'])

    # SQL Tables
    table_vw_emp_roster = sql_tables(name=config['sql_table_VW_EmployeeRoster']['name'],
                                    database=config['sql_table_VW_EmployeeRoster']['database'],
                                    path=config['sql_table_VW_EmployeeRoster']['path'])

    table_tmp_xlsx= sql_tables(name=config['sql_table_tmp_xlsx']['name'],
                                database=config['sql_table_tmp_xlsx']['database'],
                                path=config['sql_table_tmp_xlsx']['path'])

    table_tmp_tracorp = sql_tables(name=config['sql_table_tmp_tracorp']['name'],
                                    database=config['sql_table_tmp_tracorp']['database'],
                                    path=config['sql_table_tmp_tracorp']['path'])

    # SFTP Servers
    sftp_in_tracorp = sftp_server(url=config['SFTPSettingsTC']['sftptcurl'],
                                    username=config['SFTPSettingsTC']['username'],
                                    key_path=config['SFTPSettingsTC']['key'],
                                    file_path=config['SFTPSettingsTC']['file'])

    sftp_out_sumtotal = sftp_server(url=config['SFTPSettingsST']['sftptcurl'],
                                    username=config['SFTPSettingsST']['username'],
                                    key_path=config['SFTPSettingsST']['key'],
                                    file_path=config['SFTPSettingsST']['file'])

    # SMTP Server
    smtp_server_settings = smtp_server(smtpserver=config['EmailSettings']['smtpserver'],
                                        port=config['EmailSettings']['port'],
                                        email_from=config['EmailSettings']['from'],
                                        email_to=config['EmailSettings']['to'])

    # Input Files
    file_in_xlsx = raw_file(path=config['file_in_xlsx']['path'],
                            name=config['file_in_xlsx']['name'],
                            csv_true=config['file_in_xlsx']['IsCsv'],
                            nickname=config['file_in_xlsx']['nickname'],
                            delimiter=config['file_in_xlsx']['delimiter'],
                            fileType=config['file_in_xlsx']['type'])

    file_in_tracorp = raw_file(path=config['file_in_tracorp']['path'],
                                name=config['file_in_tracorp']['name'],
                                csv_true=config['file_in_tracorp']['IsCsv'],
                                nickname=config['file_in_tracorp']['nickname'],
                                delimiter=config['file_in_tracorp']['delimiter'],
                                fileType=config['file_in_tracorp']['type'])

    # Output Files
    file_out_csv = raw_file(path=config['file_out_csv_main']['path'],
                            name=config['file_out_csv_main']['name'],
                            csv_true=config['file_out_csv_main']['IsCsv'],
                            nickname=config['file_out_csv_main']['nickname'],
                            delimiter=config['file_out_csv_main']['delimiter'],
                            fileType=config['file_out_csv_main']['type'])

    file_out_tmp = raw_file(path=config['file_out_csv_tmp']['path'],
                            name=config['file_out_csv_tmp']['name'],
                            csv_true=config['file_out_csv_tmp']['IsCsv'],
                            nickname=config['file_out_csv_tmp']['nickname'],
                            delimiter=config['file_out_csv_tmp']['delimiter'],
                            fileType=config['file_out_csv_tmp']['type'])

    file_out_txt = raw_file(path=config['file_out_txt']['path'],
                            name=config['file_out_txt']['name'],
                            csv_true=config['file_out_txt']['IsCsv'],
                            nickname=config['file_out_txt']['nickname'],
                            delimiter=config['file_out_txt']['delimiter'],
                            fileType=config['file_out_txt']['type'])


    logging.debug("Email From: " + smtp_server_settings.email_from)
    logging.debug("Email To: " + smtp_server_settings.email_to)
    logging.debug("Email Server: " + smtp_server_settings.smtpserver)
    logging.debug("Email Port: " + smtp_server_settings.port)

    temp_path = os.path.join(args.path, 'temp') 
    logging.debug("temp path: " + temp_path)

    # Create temp directory if it doesn't exist
    if not os.path.exists(temp_path):
        logging.debug("Temp directory does not exist. Creating...")
        os.makedirs(temp_path)

    # Change working directory to temp directory
    os.chdir(temp_path)

    # Connect to SQL Servers
    if(args.no_sql):
        logging.info("Skipping SQL Server connection")
    else:
        logging.info("Connecting to SQL Server...")
        try:
            conn_sql11worke, cur_sql11worke = connect_sql_server(server_sql11worke, args)
            conn_aidwsql, cur_aidwsql = connect_sql_server(server_aidwsql, args)
        except Exception as e:
            logging.critical("Error connecting to SQL Server")
            logging.critical(e)


    # # # Functions

    # Get files from SFTP (TraCorp)
    tracorp_file = download_file(sftp_in_tracorp.url, sftp_in_tracorp.username, 
                                sftp_in_tracorp.file_path, sftp_in_tracorp.key_path, temp_path)
    xlsx_file = download_file(file_in_xlsx.url, file_in_xlsx.username, 
                                file_in_xlsx.file_path, file_in_xlsx.key_path, temp_path)
    


    # Import Excel (all adoa completions report)
    df_xlsx_report = import_files(xlsx_file)

    # Parse XLSX Report (file_instance is for logging)
    df_xlsx_parsed = general_parse(df_xlsx_report, file_in_xlsx)

    # Query Insert XLSX
    insert_query(conn_aidwsql, df_xlsx_parsed, table_tmp_xlsx)



    # Import Tracorp
    df_tracorp_file = import_files(tracorp_file)

    # Parse Tracorp
    df_tracorp_parsed = general_parse(df_tracorp_file, file_in_tracorp)

    # Parse filter out inactive activities
    df_tracorp_active_activities = dfs_merge(df_tracorp_parsed, variables.df_active_activities)



    # Query Insert Tracorp
    insert_query(conn_aidwsql, df_tracorp_active_activities, table_tmp_tracorp)

    # Query correct email
    correct_email(conn_sql11worke)

    # Query to df remove duplicates with tmp_xlsx
    df_tracorp_no_duplicates = remove_duplicates(conn_aidwsql)

    # Parse final df with required, static values
    df_tracorp_final = final_parse(df_tracorp_no_duplicates)



    # Export to main csv
    modified_main_csv = export_csv(df_tracorp_final, file_out_csv)

    # Export to tmp csv
    modified_pipe_csv = export_csv(df_tracorp_final, file_out_tmp)

    # Export to txt
    sumtotal_file = export_txt(modified_pipe_csv, file_out_txt)

    # Upload file to SFTP (SumTotal)
    upload_file(sftp_out_sumtotal.url, sftp_out_sumtotal.username,
                sftp_out_sumtotal.file_path, sftp_out_sumtotal.key_path, sumtotal_file)

    # Archive files
    archive_files(tracorp_file, sumtotal_file, modified_main_csv)

    # Send Email
    email_log_and_files(smtp_server_settings.email_from, smtp_server_settings.email_to, 
                        smtp_server_settings.smtpserver, smtp_server_settings.port,
                        log_file, tracorp_file, sumtotal_file, modified_main_csv)


    # Clear temp directory
    for filename in os.listdir(temp_path):
        file_path = os.path.join(temp_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            logging.critical('Failed to delete %s. Reason: %s' % (file_path, e))
        else:
            logging.debug("Deleted " + file_path)  


# Run main
if __name__ == "__main__":

    # Parse arguments
    args = parse_args() 

    # Setup logging
    log_file = setup_logging(args.path, args.debug)

    # Log arguments
    logging.debug("Working directory: " + args.path)
    logging.debug("Debug mode: " + str(args.debug))
    logging.debug("No SQL mode: " + str(args.no_sql))
    logging.debug("Configuration file path: " + args.config)

    # Read configuration file
    config = read_config(args.config)



    # Run main
    main(log_file)

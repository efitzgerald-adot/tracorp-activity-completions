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
from variables import *
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






def download_file(host_url, host_username, file_path, key_path, temp_path):
    logging.info("Downloading file from TraCorp SFTP...")
    logging.debug("SFTP URL: " + host_url)
    logging.debug("SFTP Username: " + host_username)
    logging.debug("SFTP File Path: " + file_path)
    logging.debug("SFTP Key Path: " + key_path)

    try:
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        with pysftp.Connection(host=host_url, username=host_username, private_key=key_path, cnopts=cnopts) as sftp:
            sftp.get(file_path)
            sftp.close()
    except Exception as e:
        logging.critical("Error downloading file from SFTP")
        logging.critical(e)
    else:
        file_name = os.path.basename(file_path)
        file = os.path.join(temp_path, file_name)
        logging.info("File downloaded successfully.")
        logging.debug("File path: " + file)
        return file






# Upload file to SFTP
def upload_file(host_url, host_username, file_path, key_path, file):
    logging.info("Uploading file to SumTotal SFTP...")
    logging.debug("SFTP URL: " + host_url)
    logging.debug("SFTP Username: " + host_username)
    logging.debug("SFTP File Path: " + file_path)
    logging.debug("SFTP Key Path: " + key_path)
    logging.debug("File path: " + file)

    try:
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        with pysftp.Connection(host=host_url, username=host_username, private_key=key_path, cnopts=cnopts) as sftp:
            sftp.chdir(os.path.dirname(file_path))
            sftp.put(file)
            sftp.close()
    except Exception as e:
        logging.critical("Error uploading file to SFTP")
        logging.critical(e)
    
    else:
        logging.info("File uploaded successfully.")



# Email Log and Files
def email_log_and_files(email_from, email_to, email_server, email_port, log_file, tracorp_file, sumtotal_file, modified_file):
    # Email log file
    logging.info("Emailing log and files...")

    try:
        msg = MIMEMultipart()
        msg['From'] = email_from
        msg['To'] = email_to
        msg['Subject'] = "SumTotalLMS_TraCorp Transform and Upload Log"
        email_body = "Please see attached log file and files for SumTotalLMS_TraCorp Transform and Upload."
        

        # Attach log file
        # Test if log file exists
        if os.path.exists(log_file): 
            attachment = open(log_file, "rb")
            part = MIMEBase('application', 'octet-stream')
            part.set_payload((attachment).read())
            attachment.close()
            part.add_header(
                'Content-Disposition', "attachment; filename= %s" % log_file)
            msg.attach(part)
        else: 
            email_body = email_body + "\n\nLog file not found."

        # Attach tracorp file
        # Test if tracorp file exists
        if os.path.exists(tracorp_file):
            attachment = open(tracorp_file, "rb")
            part = MIMEBase('application', 'octet-stream')
            part.set_payload((attachment).read())
            attachment.close()
            part.add_header('Content-Disposition',
                            "attachment; filename= %s" % tracorp_file)
            msg.attach(part)
        else:
            email_body = email_body + "\n\nTraCorp file not found."

        # Attach sumtotal file
        # Test if sumtotal file exists
        if os.path.exists(sumtotal_file):
            attachment = open(sumtotal_file, "rb")
            part = MIMEBase('application', 'octet-stream')
            part.set_payload((attachment).read())
            attachment.close()
            part.add_header('Content-Disposition',
                            "attachment; filename= %s" % sumtotal_file)
            msg.attach(part)
        else:
            email_body = email_body + "\n\nSumTotal file not found."

        # Attach modified file
        # Test if modified file exists
        if os.path.exists(modified_file):
            attachment = open(modified_file, "rb")
            part = MIMEBase('application', 'octet-stream')
            part.set_payload((attachment).read())
            attachment.close()
            part.add_header('Content-Disposition',
                            "attachment; filename= %s" % modified_file)
            msg.attach(part)
        else:
            email_body = email_body + "\n\nModified file not found."

        # Attach email body
        msg.attach(MIMEText(email_body, 'plain'))

        # Send email
        server = smtplib.SMTP(email_server, email_port)
        server.starttls()
        text = msg.as_string()
        server.sendmail(email_from, email_to, text)
        server.quit()
    except Exception as e:
        logging.critical("Error emailing log and files")
        logging.critical(e)
        
    else:
        logging.info("Log and files emailed successfully")



# Archive Files
def archive_files(tracorp_file, sumtotal_file, modified_file):
    logging.info("Archiving files...")
    # Setup File Archive Paths
    archive_root = os.path.join(args.path, 'archive')
    archive_date = datetime.now().strftime("%Y%m%d%H%M")
    archive_path = os.path.join(archive_root, archive_date)
    logging.debug("Archive path: " + archive_path)

    # Create Archive Root Directory if it doesn't exist
    if not os.path.exists(archive_root):
        logging.debug("Archive root directory does not exist. Creating...")
        os.makedirs(archive_root)

    # Create archive directory if it doesn't exist
    if not os.path.exists(archive_path):
        logging.debug("Archive directory does not exist. Creating...")
        os.makedirs(archive_path)

    # New File Paths
    tracorp_archive = os.path.join(
        archive_path, os.path.basename(tracorp_file))
    sumtotal_archive = os.path.join(
        archive_path, os.path.basename(sumtotal_file))
    modified_archive = os.path.join(
        archive_path, os.path.basename(modified_file))

    logging.debug("TraCorp archive path: " + tracorp_archive)
    logging.debug("SumTotal archive path: " + sumtotal_archive)
    logging.debug("Modified archive path: " + modified_archive)

    # Copy files to archive directory
    logging.debug("Copying files to archive directory...")

    try:
        logging.debug("Copying " + tracorp_file + " to " + tracorp_archive)
        shutil.copy2(tracorp_file, tracorp_archive)
    except Exception as e:
        logging.critical("Error copying " + tracorp_file + " to " + tracorp_archive)
        logging.critical(e)
    else:
        logging.debug("TraCorp file copied successfully")
    try:
        logging.debug("Copying " + sumtotal_file + " to " + sumtotal_archive)
        shutil.copy2(sumtotal_file, sumtotal_archive)
    except Exception as e:
        logging.critical("Error copying " + sumtotal_file + " to " + sumtotal_archive)
        logging.critical(e)
    else:
        logging.debug("SumTotal file copied successfully")
    try:
        logging.debug("Copying " + modified_file + " to " + modified_archive)
        shutil.copy2(modified_file, modified_archive)
    except Exception as e:
        logging.critical("Error copying " + modified_file + " to " + modified_archive)
        logging.critical(e)
    else:
        logging.debug("Modified file copied successfully")






# Main
def main(log_file, config):

    # # Class Instances
    # SQL Servers
    servers = server_instance(config)
    for index, server in enumerate(servers, start=1):
        server_sql11worke = server_instance(config)[0]
        server_aidwsql = server_instance(config)[1]


    # SQL Tables
    tables = table_instance(config)
    for index, table in enumerate(tables, start=1):
        table_vw_emp_roster = table_instance(config)[0]
        table_tmp_xlsx = table_instance(config)[1]
        table_tmp_tracorp = table_instance(config)[2]
        table_mastercompletions = table_instance(config)[3]


    # In/out Files
    in_out_files = io_file_instance(config)
    for index, table in enumerate(in_out_files, start=1):
        file_in_xlsx = io_file_instance(config)[0]
        file_in_tracorp = io_file_instance(config)[1]
        file_out_csv = io_file_instance(config)[2]
        file_out_tmp = io_file_instance(config)[3]
        file_out_txt = io_file_instance(config)[4]


    # SFTP Settings
    sftp_servers = sftp_instance(config)
    for index, sftp_server in enumerate(sftp_servers, start=1):
        sftp_tc = sftp_instance(config)[0]
        sftp_st = sftp_instance(config)[1]


    # SMTP Settings
    smtp_servers = smtp_instance(config)
    for index, smtp_server in enumerate(smtp_servers, start=1):
        smtp_connect = smtp_instance(config)[0]





    temp_path = os.path.join(args.path, 'temp') 
    logging.debug("temp path: " + temp_path)
    # Create temp directory if it doesn't exist
    if not os.path.exists(temp_path):
        logging.debug("Temp directory does not exist. Creating...")
        os.makedirs(temp_path)

    # Change working directory to temp directory
    os.chdir(temp_path)





    # # # Functions

    # Connect to SQL Servers
    conn_sql11worke, cur_sql11worke = connect_sql_server(server_sql11worke)
    conn_aidwsql, cur_aidwsql = connect_sql_server(server_aidwsql)


    # Get file from SFTP (TraCorp) - Excel (all adoa completions report)
    df_xlsx_report = download_file(sftp_tc.sftpurl, sftp_tc.username, sftp_tc.file, sftp_tc.key, temp_path)

    # Parse XLSX Report (file_instance is for logging)
    df_xlsx_parsed = general_parse(df_xlsx_report, file_in_xlsx)

    # Query Insert XLSX
    insert_query(conn_aidwsql, df_xlsx_parsed, table_tmp_xlsx)


    # Query compare with mastercompletions & remove duplicates to df
    df_xlsx_unique = general_distinct_query(conn_aidwsql, table_tmp_xlsx, table_mastercompletions)

    # Update mastercompletions with daily xlsx unique df
    insert_query(conn_aidwsql, df_xlsx_unique, table_mastercompletions)




    # Import Tracorp
    df_tracorp_file = download_file(sftp_st.sftpurl, sftp_st.username, sftp_st.file, sftp_st.key, temp_path)

    # Parse Tracorp
    df_tracorp_parsed = general_parse(df_tracorp_file, file_in_tracorp)

    # Parse filter out inactive activities
    df_tracorp_active_activities = dfs_merge(df_tracorp_parsed, df_active_activities)

    # Query correct email
    df_correct_emails = correct_email(conn_sql11worke, df_tracorp_active_activities, table_vw_emp_roster)


    # Query Insert Tracorp
    insert_query(conn_aidwsql, df_correct_emails, table_tmp_tracorp)


    # Query to df remove duplicates with mastercompletions
    df_tracorp_no_duplicates = general_distinct_query(conn_aidwsql, table_tmp_tracorp, table_mastercompletions)



    # Insert df_tracorp_no_duplicates into mastercompletions
    insert_query(conn_aidwsql, df_tracorp_no_duplicates, table_mastercompletions)


    # Parse final df with required, static values
    df_tracorp_final = final_parse(df_tracorp_no_duplicates)


    # Export to main csv
    export_csv(df_tracorp_final, file_out_csv)

    # Export to tmp csv
    export_csv(df_tracorp_final, file_out_tmp)

    # Export to txt
    export_txt(file_out_tmp, file_out_txt)

    # Upload file to SFTP (SumTotal)
    upload_file(sftp_st.sftpurl, sftp_st.username, sftp_st.file, sftp_st.key, export_txt)


    # Archive files
    archive_files(df_tracorp_file, export_txt, export_csv)


    # Send Email
    email_log_and_files(smtp_connect.addressFrom, smtp_connect.addressTo, smtp_connect.server, smtp_connect.port,
                        log_file, df_tracorp_file, export_txt, export_csv)


    # Clear temp directory
    # Delete all files in temp directory
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
    main(log_file, config)

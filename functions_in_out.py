import glob
import os.path
import csv
import pandas as pd
from datetime import date, datetime, timedelta
import pyodbc
import logging
import argparse
import variables




# Import files
def import_files(file_instance):

    df = pd.DataFrame()

    file = file_instance.path
    csv_true = file_instance.csv_true
    name = file_instance.name
    nickname = file_instance.nickname
    file_type = file_instance.fileType

    logging.info(f"Importing file: {name}")
    logging.debug(f"File path: " + file)

    try:
        if file_type == 'csv':
            read_file = pd.read_csv(file)

        else:
            read_file = pd.read_excel(file, index_col=None)
        
        df = pd.DataFrame(read_file)

        logging.info(f"SUCCESS: import_files({name})")

    except Exception as e:
        logging.critical(f"Error occurred: {e}")
        logging.critical(f"FAIL: import_files({name})")
        logging.critical(f"{name}.csv_true = {csv_true}\n")

    return df


# Export csv
def export_csv(dataframe, file_instance):

    filePath = file_instance.path
    filedelimiter = file_instance.delimiter
    fileName = file_instance.name
    is_csv = file_instance.csv_true

    logging.info(f"Exporting file: {filePath}")
    logging.debug("File path: " + filePath)

    try:
        dataframe.to_csv(filePath, sep=filedelimiter, index=False)
        logging.info(f"SUCCESS: export_file({filePath})")

    except Exception as e:
        logging.critical(f"Error occurred: {e}")
        logging.critical(f"FAIL: export_file({filePath})")



# Export txt file
def export_txt(input_csv, output_txt):

    inputCsvPath = input_csv.path
    inputCsvName = input_csv.name

    outputTxtPath = output_txt.path
    outputTxtName = output_txt.name

    logging.info("Converting CSV to TXT...")
    logging.debug("TXT file path: " + outputTxtPath)

    try:
        with open(outputTxtPath, "w") as my_output_file:
            with open(inputCsvPath, "r") as my_input_file:
                [my_output_file.write(" ".join(row)+'\n')
                 for row in csv.reader(my_input_file)]
            my_output_file.close()

        logging.info(f"Created {outputTxtName}")

        os.remove(inputCsvPath)

        logging.info(f"deleted {inputCsvName}")
        logging.info(f"SUCCESS: export_txt({outputTxtName})")


    except Exception as e:
        logging.critical(f"Error occurred: {e}")
        logging.critical(f"FAIL: export_txt({outputTxtName})")




# # # Original functions:
# Download file from SFTP
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


# Email Logs
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



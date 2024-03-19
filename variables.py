import glob
import os.path
import csv
import pandas as pd
from datetime import date, datetime, timedelta
import pyodbc
import logging
import argparse
import configparser


today = date.today()
now = datetime.now().strftime("%Y%m%d%H%M")

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
    def __init__(self, name, database, path, table_type, key_email, key_activity, key_date, key_empId):
        self.name = name
        self.database = database
        self.path = path
        self.table_type = table_type
        self.key_email = key_email
        self.key_activity = key_activity
        self.key_date = key_date
        self.key_empId = key_empId

class raw_file:
    def __init__(self, path, name, csv_true, nickname, delimiter, fileType):
        self.path = path
        self.name = name
        self.csv_true = csv_true
        self.nickname = nickname
        self.delimiter = delimiter
        self.fileType = fileType


class sftp_settings:
    def __init__(self, sftpurl, username, key, file):
        self.sftpurl = sftpurl
        self.username = username
        self.key = key
        self.file = file


class smtp_settings:
    def __init__(self, server, port, addressFrom , addressTo):
        self.server = server
        self.port = port
        self.addressFrom = addressFrom
        self.addressTo = addressTo



# # # Class Instances
# # SQL Servers
def server_instance(config):
    servers = []
    for key in config.sections():
        if key.startswith('sql_server_'):
            server_config = config[key]
            server = sql_server(driver= server_config['driver'],
                                server= server_config['server'],
                                database= server_config['database'],
                                user= server_config['user'],
                                encrypt= server_config['encrypt'],
                                auth= server_config['auth'])
            servers.append(server)
    return servers


# # Tables
def table_instance(config):
    tables = []
    for key in config.sections():
        if key.startswith('sql_table_'):
            table_config = config[key]
            table = sql_tables(name= table_config['name'],
                                database= table_config['database'],
                                path= table_config['path'],
                                table_type= table_config['table_type'],
                                key_email= table_config['key_email'],
                                key_activity= table_config['key_activity'],
                                key_date= table_config['key_date'],
                                key_empId=table_config['key_empId'])
            tables.append(table)
    return tables


# # IO Files
def io_file_instance(config):
    io_files = []
    for key in config.sections():
        if key.startswith('file_'):
            file_config = config[key]
            io_file = raw_file(path= file_config['path'],
                                name= file_config['name'],
                                csv_true= file_config['IsCsv'],
                                nickname= file_config['nickname'],
                                delimiter= file_config['delimiter'],
                                fileType= file_config['type'])
            io_files.append(io_file)
    return io_files



# # SFTP Settings
def sftp_instance(config):
    sftp_infos = []
    for key in config.sections():
        if key.startswith('SFTP'):
            sftp_config = config[key]
            sftp_info = sftp_settings(sftpurl= sftp_config['sftpurl'],
                                    username= sftp_config['username'],
                                    key= sftp_config['key'],
                                    file= sftp_config['file'])
            sftp_infos.append(sftp_info)
    return sftp_infos



# # SMTP Settings
def smtp_instance(config):
    smtp_infos = []
    for key in config.sections():
        if key.startswith('Email'):
            smtp_config = config[key]
            smtp_info = smtp_settings(server=smtp_config['smtpserver'],
                                        port=smtp_config['port'],
                                        addressFrom=smtp_config['from'],
                                        addressTo=smtp_config['to'])
            smtp_infos.append(smtp_info)
    return smtp_infos










# Activities
active_activities = {
    "ActivityCode": ["ADAPPAB100W","ADAPPRQ200W","ADAPPRQ210W","ADAPPRQ220W",
                    "ADAPPRQ230W","ADAPPRQ270","ADAPPRQ300W","ADAPPSC300",
                    "ADAPPSC310W","ADASETDS01","ADASETDS02","ADASETDS03",
                    "ADASETDS04","ADASETDS05","ADASETDSINTRO","ADBEN102",
                    "ADORI100","ADRISKSTF","AZPPLATFORM-EES",
                    "AZPPLATFORM-MGRS","CIS001","HRIS0064","HRIS0065",
                    "LAW1000","LAW1002","LAW1003","LAW1004","LAW1005",
                    "LAW1006","LAW1006EMP","LAW1007","LDR3000","LDR3001",
                    "MGT1000","MGT1001","MGT1002","MGT1003","MGT1004",
                    "MGT1005","MGT1006","MGT1007","PCI0002","PCI0003",
                    "PCI0004","PCI0005","RM29","SPSORI100","SPSPERFAPP",
                    "TRP1001","TRP1002","TRP1003","TRP1004","TRVPOL"]}


df_active_activities = pd.DataFrame(active_activities)
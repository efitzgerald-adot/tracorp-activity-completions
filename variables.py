import glob
import os.path
import pandas as pd
from datetime import date, datetime, timedelta
import pyodbc


today = date.today()
now = datetime.now().strftime("%Y%m%d%H%M")

# Activities
active_activities = {
    "ActivityCode": [
        "ADAPPAB100W",
        "ADAPPRQ200W",
        "ADAPPRQ210W",
        "ADAPPRQ220W",
        "ADAPPRQ230W",
        "ADAPPRQ270",
        "ADAPPRQ300W",
        "ADAPPSC300",
        "ADAPPSC310W",
        "ADASETDS01",
        "ADASETDS02",
        "ADASETDS03",
        "ADASETDS04",
        "ADASETDS05",
        "ADASETDSINTRO",
        "ADBEN102",
        "ADORI100",
        "ADRISKSTF",
        "AZPPLATFORM-EES",
        "AZPPLATFORM-MGRS",
        "CIS001",
        "HRIS0064",
        "HRIS0065",
        "LAW1000",
        "LAW1002",
        "LAW1003",
        "LAW1004",
        "LAW1005",
        "LAW1006",
        "LAW1006EMP",
        "LAW1007",
        "LDR3000",
        "LDR3001",
        "MGT1000",
        "MGT1001",
        "MGT1002",
        "MGT1003",
        "MGT1004",
        "MGT1005",
        "MGT1006",
        "MGT1007",
        "PCI0002",
        "PCI0003",
        "PCI0004",
        "PCI0005",
        "RM29",
        "SPSORI100",
        "SPSPERFAPP",
        "TRP1001",
        "TRP1002",
        "TRP1003",
        "TRP1004",
        "TRVPOL"
    ]
}


df_active_activities = pd.DataFrame(active_activities)
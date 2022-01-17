# allow connection to Google BigQuery
from pandas.io import gbq
import pandas as pd
import numpy as np
import datetime

#get date to add to filename in output
def _getToday():
    return datetime.date.today().strftime("%Y%m%d")

# static information
# do not change
proj_id = project_id = "panoply-455-b68e860f73ff"

#change filename for output
filename = "%s_%s.%s" % ("sfdc_leads_analysis_v2", _getToday(), "csv")
#change output directory on your machine
outpath = r"/Users/jamescollier/Downloads/" 

# Fields needed for export
fields = "id,first_name,last_name,email,mobile_phone,street,city,state,zip,country,company_name,employee_count,industry,domain"
table = "leads_testing.sfdc_cleaned"
conditions = "mobile_phone <> '' and \
            company_name <> '' and \
            industry <> '' and \
            domain <> '' and \
            state <> '' and \
            employee_count>0" #have to build a process to group and stratify the employee count#

# SQL to get the raw data
df = gbq.read_gbq(
    f"SELECT {fields} " f"FROM {table} " f"WHERE {conditions};", project_id=proj_id
)

print(df)
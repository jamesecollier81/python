# allow connection to Google BigQuery
from pandas.io import gbq
import pandas as pd
import numpy as np
from datetime import datetime
import re

#get date to add to filename in output
def _getToday():
    return datetime.now().strftime("%Y%m%d %H%M")


# static information
# do not change
proj_id = project_id = "panoply-455-b68e860f73ff"
filename = "%s_%s.%s" % ("leads_download", _getToday(), "csv")
#change output directory on your machine
outpath = r"/Users/jamescollier/Downloads/" 

# Fields needed for export
fields = "company_name,industry,first_name,last_name,email,mobile_phone,street,city,state,zip,country,domain,employee_count,customer_crm,company_revenue,purchasing_dept"
table = "leads_db.combined_leads"
#conditions = ""
#            company_name <> '' and \
#            industry <> '' and \
#            domain <> '' and \
#            state <> '' and \
#            employee_count>0"

# SQL to get the raw data
df = gbq.read_gbq(
    f"SELECT {fields} " f"FROM {table} ;", project_id=proj_id
)

# clean up the mobile phone number and remove characters
df["mobile_phone"].replace(regex=True, inplace=True, to_replace=r"\D", value=r"")
#print out the dataframe for viewing
print(df['company_name'])

##################
# export results
df.to_csv(outpath+filename, index = False)
##################

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
proj_id = project_id = "panoply-###-#########"
filename = "%s_%s.%s" % ("sfdc_leads_analysis", _getToday(), "csv")

#change output directory on your machine
outpath = r"/Users/jamescollier/Downloads/" 

# Fields needed for export
fields = "id,first_name,last_name,email,mobile_phone,street,city,state,zip,country,company_name,employee_count,industry,domain"
table = "leads_testing.sfdc_cleaned"
#conditions = "mobile_phone <> '' and \
#              company_name <> '' and \
#              industry <> '' and \
#              domain <> '' and \
#              state <> '' and \
#              employee_count>0"
#   f"SELECT {fields} " f"FROM {table} " f"WHERE {conditions};", project_id=proj_id

# SQL to get the raw data
df = gbq.read_gbq(
    f"SELECT {fields} " f"FROM {table} ;" , project_id=proj_id
)

# clean up the mobile phone number and remove characters
df["mobile_phone"].replace(regex=True, inplace=True, to_replace=r"\D", value=r"")
print(df)


df2=df.copy(deep=True)
#drop any unwanted fields
#df3=df2.drop(['street','city','state','zip','country'],axis=1)

df2['first_name'] = np.where(df2['first_name'].isnull(), '0', '1')
df2['last_name'] = np.where(df2['last_name'].isnull(), '0', '1')
df2['email'] = np.where(df2['email'].isnull(), '0', '1')
df2['mobile_phone'] = np.where(df2['mobile_phone'].isnull(), '0', '1')
df2['street'] = np.where(df2['street'].isnull(), '0', '1')
df2['city'] = np.where(df2['city'].isnull(), '0', '1')
df2['state'] = np.where(df2['state'].isnull(), '0', '1')
df2['zip'] = np.where(df2['zip'].isnull(), '0', '1')
df2['country'] = np.where(df2['country'].isnull(), '0', '1')
df2['company_name'] = np.where(df2['company_name'].isnull(), '0', '1')
df2['employee_count'] = np.where(df2['employee_count'].isnull(), '0', '1')
df2['industry'] = np.where(df2['industry'].isnull(), '0', '1')
df2['domain'] = np.where(df2['domain'].isnull(), '0', '1')

grouped=df2.groupby(['first_name','last_name','email','mobile_phone','street','city','state','zip','country','company_name','employee_count','industry','domain'])['id'].count().reset_index()
print(grouped)

###################
# export results
grouped.to_csv(outpath+filename, index = False)
###################

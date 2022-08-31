# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.7.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# + [markdown] papermill={"duration": 0.015088, "end_time": "2022-08-30T20:31:19.583817", "exception": false, "start_time": "2022-08-30T20:31:19.568729", "status": "completed"} tags=[]
# # QA queries on new CDR_deid household AND state generalization

# + papermill={"duration": 0.019286, "end_time": "2022-08-30T20:31:19.613945", "exception": false, "start_time": "2022-08-30T20:31:19.594659", "status": "completed"} tags=["parameters"]
project_id = ""
rt_cdr_deid=""
combine = ""
pid_threshold=""

# + papermill={"duration": 0.022464, "end_time": "2022-08-30T20:31:19.647295", "exception": false, "start_time": "2022-08-30T20:31:19.624831", "status": "completed"} tags=["injected-parameters"]
# Parameters
project_id = "aou-res-curation-prod"
combine = "2021q3r2_combined"
rt_cdr_deid = "R2021q3r2_deid"
pid_threshold = "200"


# + papermill={"duration": 1.896856, "end_time": "2022-08-30T20:31:21.554124", "exception": false, "start_time": "2022-08-30T20:31:19.657268", "status": "completed"} tags=[]
import pandas as pd

from analytics.cdr_ops.notebook_utils import execute
from common import JINJA_ENV
from utils import auth
from utils.bq import get_client

client = get_client(project_id)

pd.options.display.max_rows = 120
pd.set_option('display.max_colwidth', -1) # show all content

# + papermill={"duration": 0.023322, "end_time": "2022-08-30T20:31:21.588284", "exception": false, "start_time": "2022-08-30T20:31:21.564962", "status": "completed"} tags=[]
# df will have a summary in the end
df = pd.DataFrame(columns = ['query', 'result']) 

# +
#project_id='aou-res-curation-prod'
#rt_cdr_deid='R2021q3r2_deid'
#table='pdr_participant'

query = f"""

SELECT 
*
FROM `{project_id}.{rt_cdr_deid}.person` 
limit 5
"""
df1 = pd.read_gbq(query, dialect='standard')
df1.shape

# +
#project_id='aou-res-curation-prod'
#rt_cdr_deid='R2021q3r2_deid'
#table='pdr_participant'

query = JINJA_ENV.from_string("""

SELECT 
*
FROM `{{project_id}}.{{rt_cdr_deid}}.person` 
limit 5
""")
q = query.render(project_id=project_id,rt_cdr_deid=rt_cdr_deid)

df1=execute(client, q)
df1.shape

# + papermill={"duration": 2.427138, "end_time": "2022-08-30T20:31:24.048182", "exception": false, "start_time": "2022-08-30T20:31:21.621044", "status": "completed"} tags=[]
query=JINJA_ENV.from_string("""

SELECT COUNT (*) AS n_row_not_pass
FROM `{{project_id}}.{{rt_cdr_deid}}.observation`
WHERE
  observation_source_concept_id = 1585890
  AND value_as_concept_id NOT IN (2000000012,2000000010,903096)
""")
q = query.render(project_id=project_id,rt_cdr_deid=rt_cdr_deid)
df1=execute(client, q)

if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query1 observation_source_concept_id 1585890', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query1 observation_source_concept_id 1585890', 'result' : 'Failure'},  
                ignore_index = True) 
df1


# + [markdown] papermill={"duration": 0.01686, "end_time": "2022-08-30T20:31:37.153798", "exception": false, "start_time": "2022-08-30T20:31:37.136938", "status": "completed"} tags=[]
# # Summary_deid_household AND state generalization

# + papermill={"duration": 0.479086, "end_time": "2022-08-30T20:31:37.650676", "exception": false, "start_time": "2022-08-30T20:31:37.171590", "status": "completed"} tags=[]
def highlight_cells(val):
    color = 'red' if 'Failure' in val else 'white'
    return f'background-color: {color}' 

df.style.applymap(highlight_cells).set_properties(**{'text-align': 'left'})
# -

# !gcloud auth list

# !printenv PYTHONPATH

# !printenv GOOGLE_APPLICATION_CREDENTIALS



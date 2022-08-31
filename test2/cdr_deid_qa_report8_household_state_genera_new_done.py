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

# # QA queries on new CDR_deid household AND state generalization

# + tags=["parameters"]
project_id = ""
rt_cdr_deid=""
combine = ""
pid_threshold=""

# +
import pandas as pd

from analytics.cdr_ops.notebook_utils import execute
from common import JINJA_ENV
from utils import auth
from utils.bq import get_client

client = get_client(project_id)

pd.options.display.max_rows = 120
pd.set_option('display.max_colwidth', -1) # show all content
# -

# df will have a summary in the end
df = pd.DataFrame(columns = ['query', 'result']) 

# # Query1 Verify that if the observation_source_concept_id  field in OBSERVATION table populates: 1585890, the value_as_concept_id field in de-id table should populate : 2000000012
#
# DC-1049
#
# Expected result:
#
# Null is the value poplulated in the value_as_number fields 
#
# AND 2000000012, 2000000010 AND 903096 are the values that are populated in value_as_concept_id field  in the deid table.
#
# Per Francis, the other two values are valid. so it is pass. 

# +
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
# -

# # Query2 Verify that if the observation_source_concept_id  field in OBSERVATION table populates: 1333023 , the value_as_concept_id field in de-id table should populate : 2000000012
#
# expected results:
#
# Null is the value poplulated in the value_as_number fields
#
# AND 2000000012, 2000000010 AND 903096 are the values that are populated in value_as_concept_id field in the deid table.
#
# ## one row had error in new cdr

# +
query=JINJA_ENV.from_string("""

SELECT COUNT (*) AS n_row_not_pass
FROM `{{project_id}}.{{rt_cdr_deid}}.observation`
WHERE
  observation_source_concept_id = 1333023
  AND value_as_concept_id NOT IN (2000000012,2000000010,903096)
""")
q = query.render(project_id=project_id,rt_cdr_deid=rt_cdr_deid)
df1=execute(client, q)

if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query2 observation_source_concept_id 1333023', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query2 observation_source_concept_id 1333023', 'result' : 'Failure'},  
                ignore_index = True) 
df1
# -

query=JINJA_ENV.from_string("""

SELECT *
FROM `{{project_id}}.{{rt_cdr_deid}}.observation`
WHERE
  observation_source_concept_id = 1333023
  AND value_as_concept_id NOT IN (2000000012,2000000010,903096)
""")
q = query.render(project_id=project_id,rt_cdr_deid=rt_cdr_deid)
df1=execute(client, q)
df1

# # Query3 Verify that if the observation_source_concept_id  field in OBSERVATION table populates: 1585889,  the value_as_concept_id field in de-id table should populate : 2000000013
#
# DC-1059
#
# expected results:
#
# Null is the value poplulated in the value_as_number fields 
#
# AND 2000000013, 2000000010 AND 903096 are the values that are populated in value_as_concept_id field in the deid table.

# +
query=JINJA_ENV.from_string("""
SELECT COUNT (*) AS n_row_not_pass
FROM  `{{project_id}}.{{rt_cdr_deid}}.observation`
WHERE
  observation_source_concept_id = 1585889
  AND value_as_concept_id NOT IN (2000000013,2000000010,903096)
""")
q = query.render(project_id=project_id,rt_cdr_deid=rt_cdr_deid)
df1=execute(client, q)

if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query3 observation_source_concept_id 1585889', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query3 observation_source_concept_id 1585889', 'result' : 'Failure'},  
                ignore_index = True) 
df1
# -

# # Query4 Verify that if the observation_source_concept_id  field in OBSERVATION table populates: 1333015,  the value_as_concept_id field in de-id table should populate : 2000000013
#
# Generalization Rules for reference 
#
# Living Situation: COPE survey Generalize household size >10
#
# expected results:
#
# Null is the value poplulated in the value_as_number fields 
#
# AND 2000000013, 2000000010 AND 903096 are the values that are populated in value_as_concept_id field in the deid table.

# +
query=JINJA_ENV.from_string("""

SELECT COUNT (*) AS n_row_not_pass
FROM  `{{project_id}}.{{rt_cdr_deid}}.observation`
WHERE
  observation_source_concept_id = 1333015
  AND value_as_concept_id NOT IN (2000000013,2000000010,903096)
""")
q = query.render(project_id=project_id,rt_cdr_deid=rt_cdr_deid)
df1=execute(client, q)

if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query4 observation_source_concept_id 1333015', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query4 observation_source_concept_id 1333015', 'result' : 'Failure'},  
                ignore_index = True) 
df1
# -

# # state generalizaion

# ## Query5 Verify that in Observation table where observation_source_concept_id = 1585249, value_source_concept_id that populates are none of the ones listed in the J column AND one of the value_source_concept_id popluates as 2000000011.
#
# DC-1045
#
# For rows in the pre_deid_com_cdr OBSERVATION table where observation_source_concept_id = 1585249 (StreetAddress_PIIState)  where the value_source_concept_id is one of the values listed in the query,  (generalize value_source_concept_id to XXX (Unspecified state))
#
# step1:
#
# 1. query with the condition 
# observation_source_concept_id = 1585249
# 2. verify that none value_source_concept_id listed in J show up in the results.  
#
# expected results:
#
# 1. value_source_concept_id of the States that are not generalized show up AND 
#
# 2.only one row displays the generalized value_source_concept_id :  2000000011. 
#  
# step2 
#
# 1. query using the listed value_source_concept_id as condition
#
# 2. these are the states that are generalized. 
#
# expected results: returns no results 

# +
query=JINJA_ENV.from_string(""" 
WITH df1 AS (
SELECT distinct deid.value_source_concept_id
FROM
  `{{project_id}}.{{combine}}.observation` com
  JOIN `{{project_id}}.{{rt_cdr_deid}}.observation` deid 
  ON com.observation_id=deid.observation_id
  
WHERE
  com.observation_source_value LIKE 'StreetAddress_PIIState'
  AND com.observation_source_concept_id = 1585249
  AND ( com.value_source_concept_id =         1585299
OR com.value_source_concept_id =        1585304
OR com.value_source_concept_id =        1585284
OR com.value_source_concept_id =        1585315
OR com.value_source_concept_id =        1585271
OR com.value_source_concept_id =        1585263
OR com.value_source_concept_id =        1585306
OR com.value_source_concept_id =        1585274
OR com.value_source_concept_id =        1585270
OR com.value_source_concept_id =        1585411
OR com.value_source_concept_id =        1585313
OR com.value_source_concept_id =        1585409
OR com.value_source_concept_id =        1585262
OR com.value_source_concept_id =        1585309
OR com.value_source_concept_id =        1585307
OR com.value_source_concept_id =        1585275)
)

SELECT COUNT (*) AS n_row_not_pass FROM df1
WHERE value_source_concept_id !=2000000011
""")
q = query.render(project_id=project_id,rt_cdr_deid=rt_cdr_deid,combine=combine)
df1=execute(client, q)

if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query5_state_generalization', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query5_state_generalization', 'result' : 'Failure'},  
                ignore_index = True) 
df1
# -

query=JINJA_ENV.from_string(""" 
WITH df1 AS (
SELECT distinct deid.value_source_concept_id
FROM
  `{{project_id}}.{{combine}}.observation` com
  JOIN `{{project_id}}.{{rt_cdr_deid}}.observation` deid 
  ON com.observation_id=deid.observation_id
  
WHERE
  com.observation_source_value LIKE 'StreetAddress_PIIState'
  AND com.observation_source_concept_id = 1585249
  AND ( com.value_source_concept_id =         1585299
OR com.value_source_concept_id =        1585304
OR com.value_source_concept_id =        1585284
OR com.value_source_concept_id =        1585315
OR com.value_source_concept_id =        1585271
OR com.value_source_concept_id =        1585263
OR com.value_source_concept_id =        1585306
OR com.value_source_concept_id =        1585274
OR com.value_source_concept_id =        1585270
OR com.value_source_concept_id =        1585411
OR com.value_source_concept_id =        1585313
OR com.value_source_concept_id =        1585409
OR com.value_source_concept_id =        1585262
OR com.value_source_concept_id =        1585309
OR com.value_source_concept_id =        1585307
OR com.value_source_concept_id =        1585275)
)

SELECT * FROM df1
WHERE value_source_concept_id !=2000000011
""")
q = query.render(project_id=project_id,rt_cdr_deid=rt_cdr_deid,combine=combine)
df1=execute(client, q)
df1

# ## Query6 update to verifie that value_as_concept_id and value_source_concept_id are set to 2000000011 for states with less than 200 participants.
#
# Set the value_source_concept_id = 2000000011 and value_as_concept_id =2000000011 
#
# DC-2377 and DC-1614
#

# +
query=JINJA_ENV.from_string(""" 

with df1 as (
select value_source_concept_id, value_as_concept_id ,count(*) as participant_count
from `{{project_id}}.{{rt_cdr_deid}}.observation`
where observation_source_concept_id = 1585249
group by 1,2

)

SELECT COUNT (*) AS n_row_not_pass FROM df1
WHERE (value_source_concept_id !=2000000011 or value_as_concept_id !=2000000011) and 
participant_count < {{pid_threshold}}
""")
q = query.render(project_id=project_id,rt_cdr_deid=rt_cdr_deid,pid_threshold=pid_threshold)
df1=execute(client, q)

if df1.loc[0].sum()==0:
 df = df.append({'query' : 'Query6 state generalization if counts <200', 'result' : 'PASS'},  
                ignore_index = True) 
else:
 df = df.append({'query' : 'Query6 state generalization if counts <200', 'result' : 'Failure'},  
                ignore_index = True) 
df1


# -

# # Summary_deid_household AND state generalization

# +
def highlight_cells(val):
    color = 'red' if 'Failure' in val else 'white'
    return f'background-color: {color}' 

df.style.applymap(highlight_cells).set_properties(**{'text-align': 'left'})

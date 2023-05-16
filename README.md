

# MAO_Report

#mao report

import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization

import os import pandas as pd

with open("rsa_key.p8", "rb") as key: p_key=
serialization.load_pem_private_key( key.read(), password=None,
backend=default_backend())

pkb = p_key.private_bytes( encoding=serialization.Encoding.DER,
format=serialization.PrivateFormat.PKCS8,
encryption_algorithm=serialization.NoEncryption())

ctx = snowflake.connector.connect(
[user='502275\@VNSHEALTH.ORG](mailto:user='502275@VNSHEALTH.ORG){.email}',
account='vnsnyprd.us-east-1.privatelink', private_key=pkb, warehouse =
'DAS_WH_PRD', role = 'SNOWFLAKE_CHOICE_PRD' )

q="select current_account(), current_user(), current_client(),
current_role();"

df=pd.read_sql(q, con=ctx)

print(df) query= "with base as (
select * 
from DLAKE.CHOICE.MLTC_ADMISSION
where to_date(SYS_UPD_TS) = to_date(sysdate()) -- today
--where to_date(SYS_UPD_TS) = to_date(sysdate())-1 --yesterday

)
--select * from base;
,
add_detail as (
SELECT DISTINCT 
  base.*,
  REPORTING_MONTH, 
  ADD_INFO.LAST_NAME, 
  ADD_INFO.FIRST_NAME, 
  DOB, CIN_num, 
  SUBSCRIBER_ID, 
  CM_NAME,
  CONCAT(CONCAT(CSD2.FIRST_NAME, ' '),CSD2.LAST_NAME) AS CM_MANAGER

 FROM base
  LEFT JOIN(
     SELECT
         FMM.REPORTING_MONTH,
         RANK() OVER(PARTITION BY FMM.MEDICAID ORDER BY FMM.REPORTING_MONTH desc)rank_month,   
         FMM.LAST_NAME,
         FMM.FIRST_NAME,
         FMM.DOB,
         fmm.medicaid AS CIN_num,
         FMM.SUBSCRIBER_ID,
         CONCAT(CONCAT(FMM.CARE_MANAGER_FIRST_NAME, ' '),FMM.CARE_MANAGER_LAST_NAME) AS CM_NAME,
           CARE_MANAGER_ID
     FROM DLAKE.CHOICE.FACT_MEMBER_MONTH FMM
     ) add_info ON(add_info.CIN_num = base.MBM_CIN)
 LEFT JOIN NEXUS.CMGC.CARE_STAFF_DETAILS CSD ON(CSD.MEMBER_ID=ADD_INFO.CARE_MANAGER_ID)
 LEFT JOIN NEXUS.CMGC.CARE_STAFF_DETAILS CSD2 ON(CSD.ASSIGNED_TO=CSD2.MEMBER_ID)

WHERE add_info.rank_month =1
)
--select * from add_detail
,
add_npi as(
select distinct
  add_detail.*,
  npi.PRACTICE_OFFICE_NAME
from add_detail
left join DLAKE.CHOICE.DIM_PROVIDER npi ON(add_detail.ADMIT_NPI = npi.NPI)
  --SELECT *  FROM DLAKE.CHOICE.DIM_PROVIDER WHERE SRC_SYS = 'TMG' AND DL_ACTIVE_REC_IND = 'Y'
WHERE SRC_SYS = 'TMG' 
AND DL_ACTIVE_REC_IND = 'Y'
and CATEGORY IN ('HSP', 'SNF')
and PROVIDER_TERM_DT >= sysdate()
)

-- Organize for CM
SELECT distinct
    MBM_LAST_NAME,
    MBM_FIRST_NAME,
      SUBSCRIBER_ID, 
      CM_NAME,
      CM_MANAGER,         
    MBM_DOB,
    MBM_MBI,
    MBM_CIN,
    ADMIT_DATE_TIME,
    ADMIT_TYPE,
    ADMIT_NPI,
        PRACTICE_OFFICE_NAME as Admitting_facility,
    ADMIT_DX,
    PCP_NPI,
    PLAN_HCODE
    CONTRACT,
    MEDICAID_PAYOR_NAME,
    SYS_UPD_TS as file_date   
    
FROM add_npi
ORDER BY MBM_LAST_NAME, MBM_FIRST_NAME, ADMIT_TYPE, ADMIT_DATE_TIME;




with base as (
select * 
from DLAKE.CHOICE.MLTC_ADMISSION
where to_date(SYS_UPD_TS) = to_date(sysdate()) 

)

,
add_detail as (
SELECT DISTINCT 
  base.*,
  REPORTING_MONTH, 
  ADD_INFO.LAST_NAME, 
  ADD_INFO.FIRST_NAME, 
  DOB, CIN_num, 
  SUBSCRIBER_ID, 
  CM_NAME,
  CONCAT(CONCAT(CSD2.FIRST_NAME, ' '),CSD2.LAST_NAME) AS CM_MANAGER

 FROM base
  LEFT JOIN(
     SELECT
         FMM.REPORTING_MONTH,
         RANK() OVER(PARTITION BY FMM.MEDICAID ORDER BY FMM.REPORTING_MONTH desc)rank_month,   
         FMM.LAST_NAME,
         FMM.FIRST_NAME,
         FMM.DOB,
         fmm.medicaid AS CIN_num,
         FMM.SUBSCRIBER_ID,
         CONCAT(CONCAT(FMM.CARE_MANAGER_FIRST_NAME, ' '),FMM.CARE_MANAGER_LAST_NAME) AS CM_NAME,
           CARE_MANAGER_ID
     FROM DLAKE.CHOICE.FACT_MEMBER_MONTH FMM
     ) add_info ON(add_info.CIN_num = base.MBM_CIN)
 LEFT JOIN NEXUS.CMGC.CARE_STAFF_DETAILS CSD ON(CSD.MEMBER_ID=ADD_INFO.CARE_MANAGER_ID)
 LEFT JOIN NEXUS.CMGC.CARE_STAFF_DETAILS CSD2 ON(CSD.ASSIGNED_TO=CSD2.MEMBER_ID)

WHERE add_info.rank_month =1
)
,
add_npi as(
select distinct
  add_detail.*,
  npi.PRACTICE_OFFICE_NAME
from add_detail
left join DLAKE.CHOICE.DIM_PROVIDER npi ON(add_detail.ADMIT_NPI = npi.NPI)
WHERE SRC_SYS = 'TMG' 
AND DL_ACTIVE_REC_IND = 'Y'
and CATEGORY IN ('HSP', 'SNF')
and PROVIDER_TERM_DT >= sysdate()
)

SELECT distinct
    MBM_LAST_NAME,
    MBM_FIRST_NAME,
      SUBSCRIBER_ID, 
      CM_NAME,
      CM_MANAGER,         
    MBM_DOB,
    MBM_MBI,
    MBM_CIN,
    ADMIT_DATE_TIME,
    ADMIT_TYPE,
    ADMIT_NPI,
        PRACTICE_OFFICE_NAME as Admitting_facility,
    ADMIT_DX,
    PCP_NPI,
    PLAN_HCODE
    CONTRACT,
    MEDICAID_PAYOR_NAME,
    SYS_UPD_TS as file_date   
    
FROM add_npi
ORDER BY MBM_LAST_NAME, MBM_FIRST_NAME, ADMIT_TYPE, ADMIT_DATE_TIME" test = pd.read_sql_query(query, con= ctx ) 
print(test) ctx.close()


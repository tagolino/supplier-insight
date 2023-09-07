import pandas as pd

from google.cloud import bigquery

client = bigquery.Client()

p2p_pr_header_query = """
SELECT
  Client_Id AS ClientID,
  PR_Number AS PR_Number,
  Supplier AS Supplier_Name_Original,
  '' AS Supplier_Name_Normalized,
  Region,
  '' AS Country,
  CAST(PR_Total_Amount_In_Usd AS FLOAT64) AS Value_PR_USD,
  PR_DELIVERY_DATE AS PR_Completion_Date
  -- Supplier_Id
FROM
  `lisle-pbps-analytics-platform.lisle_pbps_prm.PRM_HEADER`
"""
p2p_pr_header_df = client.query(p2p_pr_header_query).to_dataframe()

p2p_pr_details_query = """
SELECT
  CAST(CUSTOMER_PARTY_ID AS STRING) AS ClientID,
  REQ_NUM AS PR_Number,
  REQ_HDR_SUP_NAME AS Supplier_Name_Original,
  '' AS Supplier_Name_Normalized,
  '' AS Region,
  '' AS Country,
  CAST(REQ_AMT_TOTAL_USD AS FLOAT64) AS Value_PR_USD,
  REQ_DTTM_CLOSED AS PR_Completion_Date
  -- Supplier_Id
FROM
  `lisle-pbps-analytics-platform.lisle_pbps_ptp.REQ_RECORD_DETAILS_Views`
"""
p2p_pr_details_df = client.query(p2p_pr_details_query).to_dataframe()

supplier_name_global_table_query = """
SELECT
  ORG_NAME,
  ORG_NAME_VARIATION
FROM 
  `lisle-pbps-analytics-platform.lisle_pbps_gdm_discovery.DN_LU_ORG_NAME_VARIATION`
WHERE
  ORG_NAME IS NOT NULL AND
  ORG_NAME_VARIATION IS NOT NULL
ORDER BY ORG_NAME_VARIATION
"""
supplier_name_global_table_df = client.query(supplier_name_global_table_query).to_dataframe()

p2p_pr_df = pd.concat([p2p_pr_header_df, p2p_pr_details_df], ignore_index=True)
    
identifier_column_name = 'Supplier_Name_Original'
    
pr_counts_df = p2p_pr_df.groupby(identifier_column_name).size().reset_index(name='total_pr')
grand_total = pr_counts_df['total_pr'].sum()
pr_counts_df['pr_grand_total'] = grand_total
pr_counts_df = pr_counts_df.sort_values(by='total_pr', ascending=False)
pr_counts_df['total_pr_percentage'] = round((pr_counts_df['total_pr'] / pr_counts_df['pr_grand_total']) * 100, 4)
pr_counts_df['cumulative_sum'] = pr_counts_df['total_pr_percentage'].cumsum()

threshold_value = 90.0
total_percent_condition = pr_counts_df['total_pr_percentage'] > 0
cumulative_sum_condition = pr_counts_df['cumulative_sum'] <= threshold_value
pr_counts_df = pr_counts_df[total_percent_condition & cumulative_sum_condition]  # get top records only by threshold value

df_to_insert = p2p_pr_df[p2p_pr_df[identifier_column_name].isin(pr_counts_df[identifier_column_name])]
df_to_insert = df_to_insert.merge(pr_counts_df[[identifier_column_name, 'total_pr_percentage']], on=identifier_column_name, how='left')
df_to_insert.rename(columns={'total_pr_percentage': 'PR_Count_Percentage'}, inplace=True)
df_to_insert = df_to_insert.sort_values(by='PR_Count_Percentage', ascending=False)
df_to_insert = pd.merge(df_to_insert, supplier_name_global_table_df, left_on='Supplier_Name_Original', right_on='ORG_NAME_VARIATION', how='left')
df_to_insert['Supplier_Name_Normalized'] = df_to_insert['ORG_NAME'].fillna(df_to_insert['Supplier_Name_Original'])
df_to_insert = df_to_insert.drop(columns=['ORG_NAME', 'ORG_NAME_VARIATION'])

table_id = 'lisle-pbps-analytics-platform.lisle_pbps_supplier_factset_discovery.P2PPR_Supplier_Factset_Table'
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField('ClientID', 'STRING'),
        bigquery.SchemaField('PR_Number', 'STRING'),
        bigquery.SchemaField('Supplier_Name_Original', 'STRING'),
        bigquery.SchemaField('Supplier_Name_Normalized', 'STRING'),
        bigquery.SchemaField('Region', 'STRING'),
        bigquery.SchemaField('Country', 'STRING'),
        bigquery.SchemaField('Value_PR_USD', 'FLOAT'),
        bigquery.SchemaField('PR_Completion_Date', 'DATETIME'),
        bigquery.SchemaField('PR_Count_Percentage', 'FLOAT'),
    ],
    write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
)
job = client.load_table_from_dataframe(
    df_to_insert, table_id, job_config=job_config
)
job.result()  # Wait for the job to complete

client.close()

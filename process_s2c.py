import pandas as pd

from google.cloud import bigquery

client = bigquery.Client()

project_supplier_detail_query = ""  # Query here
project_supplier_detail_df = client.query(project_supplier_detail_query).to_dataframe()

project_summary_refined_query = ""  # Query here
project_summary_refined_df = client.query(project_summary_refined_query).to_dataframe()

project_supplier_contact_query = ""  # Query here
project_supplier_contact_df = client.query(project_supplier_contact_query).to_dataframe()

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

project_supplier_detail_df['ProjectID'] = project_supplier_detail_df['ProjectID'].astype('string')
project_supplier_detail_df['ORIG_Supplier_Name'] = project_supplier_detail_df['ORIG_Supplier_Name'].astype('string')
project_summary_refined_df['ProjectID'] = project_summary_refined_df['ProjectID'].astype('string')
project_supplier_contact_df['ProjectID'] = project_supplier_contact_df['ProjectID'].astype('string')
supplier_name_global_table_df['ORG_NAME_VARIATION'] = supplier_name_global_table_df['ORG_NAME_VARIATION'].astype('string')

suppliers_df = pd.merge(project_supplier_detail_df, project_summary_refined_df, on='ProjectID', how='left')
suppliers_df = pd.merge(suppliers_df, project_supplier_contact_df, on='ProjectID', how='left')
suppliers_df = pd.merge(suppliers_df, supplier_name_global_table_df, left_on='ORIG_Supplier_Name', right_on='ORG_NAME_VARIATION', how='left')  # can be optimized, should do first before other merges
suppliers_df['Supplier_Name'] = suppliers_df['ORG_NAME'].fillna(suppliers_df['ORIG_Supplier_Name'])

field_mappings = {
    'ClientID': 'string',
    'Invited_Reason': 'string',
    'Participated_Reason': 'string',
    'Award_Reason': 'string',
    'Project_status': 'string',
    'Project_type': 'string',
    'Region': 'string',
    'Country': 'string',
    'Practice': 'string',
    'Category': 'string',
    'Sub_Category': 'string',
    'Supplier_Contact_Name': 'string',
    'Supplier_Contact_Email': 'string',
    'Supplier_Contact_Phone': 'string',
    'Supplier_Contact_Location': 'string'
}

suppliers_df.loc[suppliers_df['Region'].str.contains(';'), 'Region'] = 'Global'
suppliers_df.loc[suppliers_df['Country'].str.contains(';'), 'Country'] = 'Multiple'

suppliers_df = suppliers_df.drop(columns=['ORIG_Supplier_Name', 'ORG_NAME', 'ORG_NAME_VARIATION'])

table_id = ''
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField('ProjectID', 'STRING'),
        bigquery.SchemaField('ClientID', 'STRING'),
        bigquery.SchemaField('Project_status', 'STRING'),
        bigquery.SchemaField('Project_type', 'STRING'),
        bigquery.SchemaField('Region', 'STRING'),
        bigquery.SchemaField('Country', 'STRING'),
        bigquery.SchemaField('Practice', 'STRING'),
        bigquery.SchemaField('Category', 'STRING'),
        bigquery.SchemaField('Sub_Category', 'STRING'),
        bigquery.SchemaField('Total_Spend_Project_USD', 'NUMERIC'),
        bigquery.SchemaField('Addressable_Spend_USD', 'NUMERIC'),
        bigquery.SchemaField('Supplier_Name', 'STRING'),
        bigquery.SchemaField('Incumbent_status', 'INTEGER'),
        bigquery.SchemaField('Preferred_status', 'INTEGER'),
        bigquery.SchemaField('RFP_Invite_status', 'INTEGER'),
        bigquery.SchemaField('Invited_Reason', 'STRING'),
        bigquery.SchemaField('Participated_status', 'INTEGER'),
        bigquery.SchemaField('Participated_Reason', 'STRING'),
        bigquery.SchemaField('Award_status', 'INTEGER'),
        bigquery.SchemaField('Award_Reason', 'STRING'),
        bigquery.SchemaField('Total_awarded_spend_USD', 'NUMERIC'),
        bigquery.SchemaField('Project_savings_USD', 'BIGNUMERIC'),
        bigquery.SchemaField('Supplier_Contact_Name', 'STRING'),
        bigquery.SchemaField('Supplier_Contact_Email', 'STRING'),
        bigquery.SchemaField('Supplier_Contact_Phone', 'STRING'),
        bigquery.SchemaField('Supplier_Contact_Location', 'STRING'),
    ],
    write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
)
job = client.load_table_from_dataframe(
    suppliers_df, table_id, job_config=job_config
)
job.result()  # Wait for the job to complete

client.close()

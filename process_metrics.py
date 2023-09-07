import pandas as pd
import numpy as np

from google.cloud import bigquery

client = bigquery.Client()

GROUP_BY_FIELDS = ['Supplier_Name_Normalized', 'Practice', 'Category', 'Subcategory', 'Region', 'Country']

supplier_name_global_table_query = ""  # Query here
supplier_name_global_table_df = client.query(supplier_name_global_table_query).to_dataframe()

# get df for supplier practice, category and subcategory
master_supplier_query = ""  # Query here
master_supplier_df = client.query(master_supplier_query).to_dataframe()

# get supplier list summary

# s2c
supplier_summary_s2c_query = ""  # Query here
# df_to_insert[GROUP_BY_FIELDS[1:]] = df_to_insert[GROUP_BY_FIELDS[1:]].fillna("UNAVAILABLE").replace("", "UNAVAILABLE")
supplier_summary_s2c_df = client.query(supplier_summary_s2c_query).to_dataframe()
supplier_summary_s2c_df[GROUP_BY_FIELDS[1:]] = supplier_summary_s2c_df[GROUP_BY_FIELDS[1:]].fillna("UNAVAILABLE").replace("", "UNAVAILABLE")

# spend
supplier_summary_spend_query = ""  # Query here
supplier_summary_spend_df = client.query(supplier_summary_spend_query).to_dataframe()
supplier_summary_spend_df[GROUP_BY_FIELDS[1:]] = supplier_summary_spend_df[GROUP_BY_FIELDS[1:]].fillna("UNAVAILABLE").replace("", "UNAVAILABLE")

# p2p po
supplier_summary_p2p_po_query = ""  # Query here
supplier_summary_p2p_po_df = client.query(supplier_summary_p2p_po_query).to_dataframe()
supplier_summary_p2p_po_df[GROUP_BY_FIELDS[1:]] = supplier_summary_p2p_po_df[GROUP_BY_FIELDS[1:]].fillna("UNAVAILABLE").replace("", "UNAVAILABLE")

# p2p pr
supplier_summary_p2p_pr_query = ""  # Query here
supplier_summary_p2p_pr_df = client.query(supplier_summary_p2p_pr_query).to_dataframe()
supplier_summary_p2p_pr_df[GROUP_BY_FIELDS[1:]] = supplier_summary_p2p_pr_df[GROUP_BY_FIELDS[1:]].fillna("UNAVAILABLE").replace("", "UNAVAILABLE")

frames = [supplier_summary_s2c_df, supplier_summary_spend_df, supplier_summary_p2p_po_df, supplier_summary_p2p_pr_df]
supplier_name_summary_df = pd.concat(frames)
supplier_name_summary_df = supplier_name_summary_df.drop_duplicates(subset=['Supplier_Name', 'Practice', 'Category', 'Subcategory', 'Region', 'Country'])
supplier_name_summary_df = pd.merge(supplier_name_summary_df, supplier_name_global_table_df, left_on='Supplier_Name', right_on='ORG_NAME_VARIATION', how='left')
supplier_name_summary_df['Supplier_Name_Normalized'] = supplier_name_summary_df['ORG_NAME'].fillna(supplier_name_summary_df['Supplier_Name'])

supplier_name_summary_df = supplier_name_summary_df.drop(
    columns=['ORG_NAME', 'ORG_NAME_VARIATION', 'Supplier_Name']
)

supplier_name_summary_df = supplier_name_summary_df.drop_duplicates(subset=['Supplier_Name_Normalized', 'Practice', 'Category', 'Subcategory', 'Region', 'Country'])

supplier_spend_query = ""  # Query here
supplier_spend_df = client.query(supplier_spend_query).to_dataframe()
supplier_spend_df[GROUP_BY_FIELDS[1:]] = supplier_spend_df[GROUP_BY_FIELDS[1:]].fillna("UNAVAILABLE").replace("", "UNAVAILABLE")

s2c_query = ""  # Query here
s2c_df = client.query(s2c_query).to_dataframe()
s2c_df[GROUP_BY_FIELDS[1:]] = s2c_df[GROUP_BY_FIELDS[1:]].fillna("UNAVAILABLE").replace("", "UNAVAILABLE")

p2p_po_query = ""  # Query here
p2p_po_df = client.query(p2p_po_query).to_dataframe()
p2p_po_df['Value_PO'] = p2p_po_df['Value_PO_converted'].fillna(p2p_po_df['Value_PO_original'])
p2p_po_df = p2p_po_df.drop(columns=['Value_PO_original', 'Value_PO_converted'])
p2p_po_df[GROUP_BY_FIELDS[1:]] = p2p_po_df[GROUP_BY_FIELDS[1:]].fillna("UNAVAILABLE").replace("", "UNAVAILABLE")
p2p_po_df = p2p_po_df.drop_duplicates()

p2p_pr_query = ""  # Query here
p2p_pr_df = client.query(p2p_pr_query).to_dataframe()
p2p_pr_df[GROUP_BY_FIELDS[1:]] = p2p_pr_df[GROUP_BY_FIELDS[1:]].fillna("UNAVAILABLE").replace("", "UNAVAILABLE")
p2p_pr_df = p2p_pr_df.drop_duplicates()

""" SPEND """
# Get Total lifetime spend we see in spend data
supplier_total_spend_df = supplier_spend_df.groupby(GROUP_BY_FIELDS)['Spend'].sum().reset_index()
supplier_total_spend_df.rename(columns={'Spend': 'Total_Spend'}, inplace=True)

# Get Total lifetime number of unique client IDs in spend data
supplier_client_count_df = supplier_spend_df.groupby(GROUP_BY_FIELDS)['Client_ID'].nunique().reset_index()
supplier_client_count_df.rename(columns={'Client_ID': 'Spend_Clients_Num'}, inplace=True)

supplier_invoice_count_df = supplier_spend_df.groupby(GROUP_BY_FIELDS).size().reset_index(name='Spend_Invoice_Num')

""" S2C """
supplier_projects_count = s2c_df.groupby(GROUP_BY_FIELDS)['ProjectID'].nunique().reset_index()
supplier_projects_count.rename(columns={'ProjectID': 'Number_of_projects_S2C'}, inplace=True)

supplier_client_projects_count = s2c_df.groupby(GROUP_BY_FIELDS)['ClientID'].nunique().reset_index()
supplier_client_projects_count.rename(columns={'ClientID': 'Number_of_clients_projects_exec_S2C'}, inplace=True)

s2c_total_projects_spend = s2c_df.groupby(GROUP_BY_FIELDS)['Total_Spend_Project_USD'].sum().reset_index()
s2c_total_projects_spend.rename(columns={'Total_Spend_Project_USD': 'Total_Spend_S2C'}, inplace=True)
s2c_total_projects_spend['Total_Spend_S2C'] = s2c_total_projects_spend['Total_Spend_S2C'].apply(float)

s2c_total_addressable_spend = s2c_df.groupby(GROUP_BY_FIELDS)['Addressable_Spend_USD'].sum().reset_index()
s2c_total_addressable_spend.rename(columns={'Addressable_Spend_USD': 'Total_Addressable_Spend_S2C'}, inplace=True)
s2c_total_addressable_spend['Total_Addressable_Spend_S2C'] = s2c_total_addressable_spend['Total_Addressable_Spend_S2C'].apply(float)

s2c_total_savings = s2c_df.groupby(GROUP_BY_FIELDS)['Project_savings_USD'].sum().reset_index()
s2c_total_savings.rename(columns={'Project_savings_USD': 'Total_Savings_S2C'}, inplace=True)
s2c_total_savings['Total_Savings_S2C'] = s2c_total_savings['Total_Savings_S2C'].apply(float)

s2c_incumbent_projects_count = s2c_df[s2c_df['Incumbent_status'] == 1].groupby(GROUP_BY_FIELDS)['ProjectID'].nunique().reset_index()
s2c_incumbent_projects_count.rename(columns={'ProjectID': 'Incumbent_Projects_Num_S2C'}, inplace=True)

filter_condition = (s2c_df['Incumbent_status'] == 1) & (s2c_df['Award_status'] == 1)
s2c_incumbent_awarded_projects_count = s2c_df[filter_condition].groupby(GROUP_BY_FIELDS)['ProjectID'].nunique().reset_index()
s2c_incumbent_awarded_projects_count.rename(columns={'ProjectID': 'Incumbent_Awarded_Projects_Num_S2C'}, inplace=True)

filter_condition = (s2c_df['Incumbent_status'] == 1) & (s2c_df['Award_status'] == 0)
s2c_incumbent_unawarded_projects_count = s2c_df[filter_condition].groupby(GROUP_BY_FIELDS)['ProjectID'].nunique().reset_index()
s2c_incumbent_unawarded_projects_count.rename(columns={'ProjectID': 'Incumbent_Unawarded_Projects_Num_S2C'}, inplace=True)

filter_condition = (s2c_df['Incumbent_status'] == 0) & (s2c_df['RFP_Invite_status'] == 1)
s2c_unincumbent_invited_projects_count = s2c_df[filter_condition].groupby(GROUP_BY_FIELDS)['ProjectID'].nunique().reset_index()
s2c_unincumbent_invited_projects_count.rename(columns={'ProjectID': 'Unincumbent_Invited_Projects_Num_S2C'}, inplace=True)

s2c_unincumbent_projects_count = s2c_df[s2c_df['Incumbent_status'] == 0].groupby(GROUP_BY_FIELDS)['ProjectID'].nunique().reset_index()
s2c_unincumbent_projects_count.rename(columns={'ProjectID': 'Unincumbent_Projects_Num_S2C'}, inplace=True)

filter_condition = (s2c_df['Incumbent_status'] == 0) & (s2c_df['Award_status'] == 1)
s2c_unincumbent_awarded_projects_count = s2c_df[filter_condition].groupby(GROUP_BY_FIELDS)['ProjectID'].nunique().reset_index()
s2c_unincumbent_awarded_projects_count.rename(columns={'ProjectID': 'Unincumbent_Awarded_Projects_Num_S2C'}, inplace=True)

filter_condition = (s2c_df['Incumbent_status'] == 0) & (s2c_df['Award_status'] == 0)
s2c_unincumbent_unawarded_projects_count = s2c_df[filter_condition].groupby(GROUP_BY_FIELDS)['ProjectID'].nunique().reset_index()
s2c_unincumbent_unawarded_projects_count.rename(columns={'ProjectID': 'Unincumbent_Unwarded_Projects_Num_S2C'}, inplace=True)

temp_df = s2c_df[s2c_df['Incumbent_status'] == 1].groupby(GROUP_BY_FIELDS)[['Project_savings_USD', 'Addressable_Spend_USD']].sum().reset_index()
incumbent_suppliers_outlier_df = temp_df.copy()
incumbent_suppliers_outlier_df['is_Incumbent_Outlier'] = incumbent_suppliers_outlier_df['Project_savings_USD'] > incumbent_suppliers_outlier_df['Addressable_Spend_USD']
incumbent_suppliers_outlier_df = incumbent_suppliers_outlier_df.drop(columns=['Project_savings_USD', 'Addressable_Spend_USD'])
temp_df['Incumbent_Supplier_Spend_Percentage_S2C'] = (temp_df['Project_savings_USD'] / temp_df['Addressable_Spend_USD'].replace(0, np.nan)).astype(float)
incumbent_suppliers_savings_addressable_spend_average = temp_df.drop(columns=['Project_savings_USD', 'Addressable_Spend_USD'])

filter_condition = (s2c_df['Incumbent_status'] == 1) & (s2c_df['Award_status'] == 1)
temp_df = s2c_df[filter_condition].groupby(GROUP_BY_FIELDS)[['Project_savings_USD', 'Addressable_Spend_USD']].sum().reset_index()
incumbent_awarded_suppliers_outlier_df = temp_df.copy()
incumbent_awarded_suppliers_outlier_df['is_Incumbent_Awarded_Outlier'] = incumbent_awarded_suppliers_outlier_df['Project_savings_USD'] > incumbent_awarded_suppliers_outlier_df['Addressable_Spend_USD']
incumbent_awarded_suppliers_outlier_df = incumbent_awarded_suppliers_outlier_df.drop(columns=['Project_savings_USD', 'Addressable_Spend_USD'])
temp_df['Incumbent_Awarded_Spend_Percentage_S2C'] = (temp_df['Project_savings_USD'] / temp_df['Addressable_Spend_USD'].replace(0, np.nan)).astype(float)
incumbent_awarded_suppliers_savings_addressable_spend_average = temp_df.drop(columns=['Project_savings_USD', 'Addressable_Spend_USD'])

filter_condition = (s2c_df['Incumbent_status'] == 1) & (s2c_df['Award_status'] == 0)
temp_df = s2c_df[filter_condition].groupby(GROUP_BY_FIELDS)[['Project_savings_USD', 'Addressable_Spend_USD']].sum().reset_index()
incumbent_unawarded_suppliers_outlier_df = temp_df.copy()
incumbent_unawarded_suppliers_outlier_df['is_Incumbent_Unawarded_Outlier'] = incumbent_unawarded_suppliers_outlier_df['Project_savings_USD'] > incumbent_unawarded_suppliers_outlier_df['Addressable_Spend_USD']
incumbent_unawarded_suppliers_outlier_df = incumbent_unawarded_suppliers_outlier_df.drop(columns=['Project_savings_USD', 'Addressable_Spend_USD'])
temp_df['Incumbent_Unawarded_Spend_Percentage_S2C'] = (temp_df['Project_savings_USD'] / temp_df['Addressable_Spend_USD'].replace(0, np.nan)).astype(float)
incumbent_unawarded_suppliers_savings_addressable_spend_average = temp_df.drop(columns=['Project_savings_USD', 'Addressable_Spend_USD'])

temp_df = s2c_df[s2c_df['Incumbent_status'] == 0].groupby(GROUP_BY_FIELDS)[['Project_savings_USD', 'Addressable_Spend_USD']].sum().reset_index()
unincumbent_suppliers_outlier_df = temp_df.copy()
unincumbent_suppliers_outlier_df['is_Unincumbent_Outlier'] = unincumbent_suppliers_outlier_df['Project_savings_USD'] > unincumbent_suppliers_outlier_df['Addressable_Spend_USD']
unincumbent_suppliers_outlier_df = unincumbent_suppliers_outlier_df.drop(columns=['Project_savings_USD', 'Addressable_Spend_USD'])
temp_df['Unincumbent_Supplier_Spend_Percentage_S2C'] = (temp_df['Project_savings_USD'] / temp_df['Addressable_Spend_USD'].replace(0, np.nan)).astype(float)
unincumbent_suppliers_savings_addressable_spend_average = temp_df.drop(columns=['Project_savings_USD', 'Addressable_Spend_USD'])

filter_condition = (s2c_df['Incumbent_status'] == 0) & (s2c_df['Award_status'] == 1)
temp_df = s2c_df[filter_condition].groupby(GROUP_BY_FIELDS)[['Project_savings_USD', 'Addressable_Spend_USD']].sum().reset_index()
unincumbent_awarded_suppliers_outlier_df = temp_df.copy()
unincumbent_awarded_suppliers_outlier_df['is_Unincumbent_Awarded_Outlier'] = unincumbent_awarded_suppliers_outlier_df['Project_savings_USD'] > unincumbent_awarded_suppliers_outlier_df['Addressable_Spend_USD']
unincumbent_awarded_suppliers_outlier_df = unincumbent_awarded_suppliers_outlier_df.drop(columns=['Project_savings_USD', 'Addressable_Spend_USD'])
temp_df['Unincumbent_Awarded_Spend_Percentage_S2C'] = (temp_df['Project_savings_USD'] / temp_df['Addressable_Spend_USD'].replace(0, np.nan)).astype(float)
unincumbent_awarded_suppliers_savings_addressable_spend_average = temp_df.drop(columns=['Project_savings_USD', 'Addressable_Spend_USD'])

filter_condition = (s2c_df['Incumbent_status'] == 0) & (s2c_df['Award_status'] == 0)
temp_df = s2c_df[filter_condition].groupby(GROUP_BY_FIELDS)[['Project_savings_USD', 'Addressable_Spend_USD']].sum().reset_index()
unincumbent_unawarded_suppliers_outlier_df = temp_df.copy()
unincumbent_unawarded_suppliers_outlier_df['is_Unincumbent_Unawarded_Outlier'] = unincumbent_unawarded_suppliers_outlier_df['Project_savings_USD'] > unincumbent_unawarded_suppliers_outlier_df['Addressable_Spend_USD']
unincumbent_unawarded_suppliers_outlier_df = unincumbent_unawarded_suppliers_outlier_df.drop(columns=['Project_savings_USD', 'Addressable_Spend_USD'])
temp_df['Unincumbent_Unawarded_Spend_Percentage_S2C'] = (temp_df['Project_savings_USD'] / temp_df['Addressable_Spend_USD'].replace(0, np.nan)).astype(float)
unincumbent_unawarded_suppliers_savings_addressable_spend_average = temp_df.drop(columns=['Project_savings_USD', 'Addressable_Spend_USD'])

s2c_awarded_projects_count = s2c_df[s2c_df['Award_status'] == 1].groupby(['Supplier_Name_Normalized', 'Subcategory'])['ProjectID'].nunique().reset_index()
s2c_awarded_projects_count.rename(columns={'ProjectID': 'Awarded_Projects_Num_S2C'}, inplace=True)
s2c_invited_projects_count = s2c_df[s2c_df['RFP_Invite_status'] == 1].groupby(['Supplier_Name_Normalized', 'Subcategory'])['ProjectID'].nunique().reset_index()
s2c_invited_projects_count.rename(columns={'ProjectID': 'Invited_Projects_Num_S2C'}, inplace=True)
s2c_awarded_invited_df = pd.merge(s2c_awarded_projects_count, s2c_invited_projects_count, on=['Supplier_Name_Normalized', 'Subcategory'], how='left')
s2c_awarded_invited_df['Awarded_Invited_Supplier_Percentage_S2C'] = (s2c_awarded_invited_df['Awarded_Projects_Num_S2C'] / s2c_awarded_invited_df['Invited_Projects_Num_S2C'])

""" P2P PO """
supplier_po_count_df = p2p_po_df.groupby(GROUP_BY_FIELDS)['PO_Number'].nunique().reset_index()
supplier_po_count_df.rename(columns={'PO_Number': 'Total_PO_Num_P2P'}, inplace=True)

supplier_po_client_count_df = p2p_po_df.groupby(GROUP_BY_FIELDS)['ClientID'].nunique().reset_index()
supplier_po_client_count_df.rename(columns={'ClientID': 'Total_PO_Client_Num_P2P'}, inplace=True)

supplier_po_total_value_df = p2p_po_df.groupby(GROUP_BY_FIELDS)['Value_PO'].sum().reset_index()
supplier_po_total_value_df.rename(columns={'Value_PO': 'Total_PO_Value_Num_P2P'}, inplace=True)
supplier_po_total_value_df['Total_PO_Value_Num_P2P'] = supplier_po_total_value_df['Total_PO_Value_Num_P2P'].apply(float)

""" P2P PR """
supplier_pr_count_df = p2p_pr_df.groupby(GROUP_BY_FIELDS)['PR_Number'].nunique().reset_index()
supplier_pr_count_df.rename(columns={'PR_Number': 'Total_PR_Num_P2P'}, inplace=True)

supplier_pr_client_count_df = p2p_pr_df.groupby(GROUP_BY_FIELDS)['ClientID'].nunique().reset_index()
supplier_pr_client_count_df.rename(columns={'ClientID': 'Total_PR_Client_Num_P2P'}, inplace=True)

supplier_pr_total_value_df = p2p_pr_df.groupby(GROUP_BY_FIELDS)['Value_PR_USD'].sum().reset_index()
supplier_pr_total_value_df.rename(columns={'Value_PR_USD': 'Total_PR_Value_Num_P2P'}, inplace=True)
supplier_pr_total_value_df['Total_PR_Value_Num_P2P'] = supplier_pr_total_value_df['Total_PR_Value_Num_P2P'].apply(float)

""" Generating data for metrics table """

def safe_division(x, y):
    if y == 0:
        return np.nan  # or any other value you prefer for this case
    else:
        return float(x) / float(y)

df_to_insert = supplier_name_summary_df.reindex(columns=GROUP_BY_FIELDS)

df_to_insert['is_normalized'] = df_to_insert['Supplier_Name_Normalized'].isin(supplier_name_global_table_df['ORG_NAME'])

""" SPEND """
df_to_insert = df_to_insert.merge(supplier_total_spend_df, on=GROUP_BY_FIELDS, how='left')  # Total_Spend
df_to_insert = df_to_insert.merge(supplier_client_count_df, on=GROUP_BY_FIELDS, how='left')  # Spend_Clients_Num
df_to_insert['Average_Spend_per_Client_Num'] = df_to_insert['Total_Spend'] / df_to_insert['Spend_Clients_Num']
df_to_insert = df_to_insert.merge(supplier_invoice_count_df, on=GROUP_BY_FIELDS, how='left')

""" S2C """
df_to_insert = df_to_insert.merge(supplier_projects_count, on=GROUP_BY_FIELDS, how='left')  # Number_of_projects_S2C
df_to_insert = df_to_insert.merge(supplier_client_projects_count, on=GROUP_BY_FIELDS, how='left')  # Number_of_clients_projects_exec_S2C
df_to_insert = df_to_insert.merge(s2c_total_projects_spend, on=GROUP_BY_FIELDS, how='left')  # Total_Spend_S2C
df_to_insert = df_to_insert.merge(s2c_total_addressable_spend, on=GROUP_BY_FIELDS, how='left')  # Total_Addressable_Spend_S2C
df_to_insert = df_to_insert.merge(s2c_total_savings, on=GROUP_BY_FIELDS, how='left')  # Total_Savings_S2C
df_to_insert['Percentage_Savings_S2C'] = df_to_insert.apply(lambda row: safe_division(row['Total_Savings_S2C'], row['Total_Addressable_Spend_S2C']), axis=1)
df_to_insert['Average_Spend_Per_Project_S2C'] = df_to_insert.apply(lambda row: safe_division(row['Total_Spend_S2C'], row['Number_of_projects_S2C']), axis=1)
df_to_insert['Average_Addressable_Spend_Per_Project_S2C'] = df_to_insert.apply(lambda row: safe_division(row['Total_Addressable_Spend_S2C'], row['Number_of_projects_S2C']), axis=1)
df_to_insert = df_to_insert.merge(s2c_incumbent_projects_count, on=GROUP_BY_FIELDS, how='left')  # Incumbent_Projects_Num_S2C
df_to_insert = df_to_insert.merge(s2c_incumbent_awarded_projects_count, on=GROUP_BY_FIELDS, how='left')  # Incumbent_Awarded_Projects_Num_S2C
df_to_insert = df_to_insert.merge(s2c_incumbent_unawarded_projects_count, on=GROUP_BY_FIELDS, how='left')  # Incumbent_Unawarded_Projects_Num_S2C
df_to_insert = df_to_insert.merge(s2c_unincumbent_invited_projects_count, on=GROUP_BY_FIELDS, how='left')  # Unincumbent_Invited_Projects_Num_S2C
df_to_insert = df_to_insert.merge(s2c_unincumbent_projects_count, on=GROUP_BY_FIELDS, how='left')  # Unincumbent_Projects_Num_S2C
df_to_insert = df_to_insert.merge(s2c_unincumbent_awarded_projects_count, on=GROUP_BY_FIELDS, how='left')  # Unincumbent_Awarded_Projects_Num_S2C
df_to_insert = df_to_insert.merge(s2c_unincumbent_unawarded_projects_count, on=GROUP_BY_FIELDS, how='left')  # Unincumbent_Unwarded_Projects_Num_S2C
df_to_insert = df_to_insert.merge(incumbent_suppliers_savings_addressable_spend_average, on=GROUP_BY_FIELDS, how='left')  # Incumbent suppliers total savings and total addressable spend average
df_to_insert = df_to_insert.merge(incumbent_awarded_suppliers_savings_addressable_spend_average, on=GROUP_BY_FIELDS, how='left')  # Incumbent awarded suppliers total savings and total addressable spend average
df_to_insert = df_to_insert.merge(incumbent_unawarded_suppliers_savings_addressable_spend_average, on=GROUP_BY_FIELDS, how='left')  # Incumbent unawarded suppliers total savings and total addressable spend average
df_to_insert = df_to_insert.merge(unincumbent_suppliers_savings_addressable_spend_average, on=GROUP_BY_FIELDS, how='left')  # Incumbent suppliers total savings and total addressable spend average
df_to_insert = df_to_insert.merge(unincumbent_awarded_suppliers_savings_addressable_spend_average, on=GROUP_BY_FIELDS, how='left')  # Incumbent awarded suppliers total savings and total addressable spend average
df_to_insert = df_to_insert.merge(unincumbent_unawarded_suppliers_savings_addressable_spend_average, on=GROUP_BY_FIELDS, how='left')  # Incumbent unawarded suppliers total savings and total addressable spend average
df_to_insert = df_to_insert.merge(s2c_awarded_invited_df, on=['Supplier_Name_Normalized', 'Subcategory'], how='left')  # Supplier total awarded / total invited percentage
df_to_insert = df_to_insert.merge(incumbent_suppliers_outlier_df, on=GROUP_BY_FIELDS, how='left')  # Outliers tagging for 6 savings/addressable spend metrics
df_to_insert = df_to_insert.merge(incumbent_awarded_suppliers_outlier_df, on=GROUP_BY_FIELDS, how='left')
df_to_insert = df_to_insert.merge(incumbent_unawarded_suppliers_outlier_df, on=GROUP_BY_FIELDS, how='left')
df_to_insert = df_to_insert.merge(unincumbent_suppliers_outlier_df, on=GROUP_BY_FIELDS, how='left')
df_to_insert = df_to_insert.merge(unincumbent_awarded_suppliers_outlier_df, on=GROUP_BY_FIELDS, how='left')
df_to_insert = df_to_insert.merge(unincumbent_unawarded_suppliers_outlier_df, on=GROUP_BY_FIELDS, how='left')

""" P2P PO """
df_to_insert = df_to_insert.merge(supplier_po_count_df, on=GROUP_BY_FIELDS, how='left')
df_to_insert = df_to_insert.merge(supplier_po_client_count_df, on=GROUP_BY_FIELDS, how='left')
df_to_insert['Average_PO_per_Client_Num_P2P'] = df_to_insert['Total_PO_Num_P2P'] / df_to_insert['Total_PO_Client_Num_P2P']
df_to_insert = df_to_insert.merge(supplier_po_total_value_df, on=GROUP_BY_FIELDS, how='left')

""" P2P PR """
df_to_insert = df_to_insert.merge(supplier_pr_count_df, on=GROUP_BY_FIELDS, how='left')
df_to_insert = df_to_insert.merge(supplier_pr_client_count_df, on=GROUP_BY_FIELDS, how='left')
df_to_insert['Average_PR_per_Client_Num_P2P'] = df_to_insert['Total_PR_Num_P2P'] / df_to_insert['Total_PR_Client_Num_P2P']
df_to_insert = df_to_insert.merge(supplier_pr_total_value_df, on=GROUP_BY_FIELDS, how='left')

""" Supplier Source Availability """

supplier_s2c_global_df = pd.merge(supplier_summary_s2c_df, supplier_name_global_table_df, left_on='Supplier_Name', right_on='ORG_NAME_VARIATION', how='left')
supplier_s2c_global_df['Supplier_Name_Normalized'] = supplier_s2c_global_df['ORG_NAME'].fillna(supplier_s2c_global_df['Supplier_Name'])
df_to_insert['is_in_S2C'] = df_to_insert['Supplier_Name_Normalized'].isin(supplier_s2c_global_df['Supplier_Name_Normalized'])

supplier_spend_global_df = pd.merge(supplier_summary_spend_df, supplier_name_global_table_df, left_on='Supplier_Name', right_on='ORG_NAME_VARIATION', how='left')
supplier_spend_global_df['Supplier_Name_Normalized'] = supplier_spend_global_df['ORG_NAME'].fillna(supplier_spend_global_df['Supplier_Name'])
df_to_insert['is_in_Spend'] = df_to_insert['Supplier_Name_Normalized'].isin(supplier_spend_global_df['Supplier_Name_Normalized'])

supplier_p2ppo_global_df = pd.merge(supplier_summary_p2p_po_df, supplier_name_global_table_df, left_on='Supplier_Name', right_on='ORG_NAME_VARIATION', how='left')
supplier_p2ppo_global_df['Supplier_Name_Normalized'] = supplier_p2ppo_global_df['ORG_NAME'].fillna(supplier_p2ppo_global_df['Supplier_Name'])
df_to_insert['is_in_P2PPO'] = df_to_insert['Supplier_Name_Normalized'].isin(supplier_p2ppo_global_df['Supplier_Name_Normalized'])

supplier_p2ppr_global_df = pd.merge(supplier_summary_p2p_pr_df, supplier_name_global_table_df, left_on='Supplier_Name', right_on='ORG_NAME_VARIATION', how='left')
supplier_p2ppr_global_df['Supplier_Name_Normalized'] = supplier_p2ppr_global_df['ORG_NAME'].fillna(supplier_p2ppr_global_df['Supplier_Name'])
df_to_insert['is_in_P2PPR'] = df_to_insert['Supplier_Name_Normalized'].isin(supplier_p2ppr_global_df['Supplier_Name_Normalized'])

df_to_insert['is_Outlier'] = df_to_insert['Total_Savings_S2C'] > df_to_insert['Total_Addressable_Spend_S2C']

df_to_insert['Cumulative_Spend'] = df_to_insert['Total_Spend'].fillna(0.0) + df_to_insert['Total_Spend_S2C'].fillna(0.0) + df_to_insert['Total_PO_Value_Num_P2P'].fillna(0.0) + df_to_insert['Total_PR_Value_Num_P2P'].fillna(0.0)

df_to_insert[GROUP_BY_FIELDS[1:]] = df_to_insert[GROUP_BY_FIELDS[1:]].fillna("UNAVAILABLE").replace("", "UNAVAILABLE")
df_to_insert['Supplier_Name_Normalized'] = df_to_insert["Supplier_Name_Normalized"].replace("", "UNKNOWN")

""" Cleaning data for removing invalid suppliers """
exclude_suppliers = ["training supplier", "Generic Supplier", "test supplier", "testing supplier", "Various Supplier", "Consulting Supplier", "Dummy"]
numeric_pattern = r'^\d+$'
non_english_pattern = r'[^a-zA-Z\s]'
condition = '|'.join(exclude_suppliers) + '|' + numeric_pattern + '|' + non_english_pattern
df_to_insert = df_to_insert[~df_to_insert['Supplier_Name_Normalized'].str.contains(condition, case=False, regex=True)]

table_id = 'lisle-pbps-analytics-platform.lisle_pbps_supplier_factset_discovery.Supplier_Factset_Metrics'
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField('Supplier_Name_Normalized', 'STRING'),
        bigquery.SchemaField('Practice', 'STRING'),
        bigquery.SchemaField('Category', 'STRING'),
        bigquery.SchemaField('Subcategory', 'STRING'),
        bigquery.SchemaField('Region', 'STRING'),
        bigquery.SchemaField('Country', 'STRING'),
        bigquery.SchemaField('is_normalized', 'BOOLEAN'),
        bigquery.SchemaField('Total_Spend', 'Float'),
        bigquery.SchemaField('Spend_Clients_Num', 'Float'),
        bigquery.SchemaField('Average_Spend_per_Client_Num', 'Float'),
        bigquery.SchemaField('Spend_Invoice_Num', 'Float'),
        bigquery.SchemaField('Number_of_projects_S2C', 'Float'),
        bigquery.SchemaField('Number_of_clients_projects_exec_S2C', 'Float'),
        bigquery.SchemaField('Total_Spend_S2C', 'Float'),
        bigquery.SchemaField('Total_Addressable_Spend_S2C', 'Float'),
        bigquery.SchemaField('Total_Savings_S2C', 'Float'),
        bigquery.SchemaField('Percentage_Savings_S2C', 'Float'),
        bigquery.SchemaField('Average_Spend_Per_Project_S2C', 'Float'),
        bigquery.SchemaField('Average_Addressable_Spend_Per_Project_S2C', 'Float'),
        bigquery.SchemaField('Incumbent_Projects_Num_S2C', 'Float'),
        bigquery.SchemaField('Incumbent_Awarded_Projects_Num_S2C', 'Float'),
        bigquery.SchemaField('Incumbent_Unawarded_Projects_Num_S2C', 'Float'),
        bigquery.SchemaField('Unincumbent_Invited_Projects_Num_S2C', 'Float'),
        bigquery.SchemaField('Unincumbent_Projects_Num_S2C', 'Float'),
        bigquery.SchemaField('Unincumbent_Awarded_Projects_Num_S2C', 'Float'),
        bigquery.SchemaField('Unincumbent_Unwarded_Projects_Num_S2C', 'Float'),
        bigquery.SchemaField('Incumbent_Supplier_Spend_Percentage_S2C', 'Float'),
        bigquery.SchemaField('Incumbent_Awarded_Spend_Percentage_S2C', 'Float'),
        bigquery.SchemaField('Incumbent_Unawarded_Spend_Percentage_S2C', 'Float'),
        bigquery.SchemaField('Unincumbent_Supplier_Spend_Percentage_S2C', 'Float'),
        bigquery.SchemaField('Unincumbent_Awarded_Spend_Percentage_S2C', 'Float'),
        bigquery.SchemaField('Unincumbent_Unawarded_Spend_Percentage_S2C', 'Float'),
        bigquery.SchemaField('Awarded_Invited_Supplier_Percentage_S2C', 'Float'),
        bigquery.SchemaField('Total_PO_Num_P2P', 'Float'),
        bigquery.SchemaField('Total_PO_Client_Num_P2P', 'Float'),
        bigquery.SchemaField('Average_PO_per_Client_Num_P2P', 'Float'),
        bigquery.SchemaField('Total_PO_Value_Num_P2P', 'Float'),
        bigquery.SchemaField('Total_PR_Num_P2P', 'Float'),
        bigquery.SchemaField('Total_PR_Client_Num_P2P', 'Float'),
        bigquery.SchemaField('Average_PR_per_Client_Num_P2P', 'Float'),
        bigquery.SchemaField('Total_PR_Value_Num_P2P', 'Float'),
        bigquery.SchemaField('is_in_S2C', 'BOOLEAN'),
        bigquery.SchemaField('is_in_Spend', 'BOOLEAN'),
        bigquery.SchemaField('is_in_P2PPO', 'BOOLEAN'),
        bigquery.SchemaField('is_in_P2PPR', 'BOOLEAN'),
        bigquery.SchemaField('is_Outlier', 'BOOLEAN'),
        bigquery.SchemaField('Cumulative_Spend', 'Float'),
        bigquery.SchemaField('is_Incumbent_Outlier', 'BOOLEAN'),
        bigquery.SchemaField('is_Incumbent_Awarded_Outlier', 'BOOLEAN'),
        bigquery.SchemaField('is_Incumbent_Unawarded_Outlier', 'BOOLEAN'),
        bigquery.SchemaField('is_Unincumbent_Outlier', 'BOOLEAN'),
        bigquery.SchemaField('is_Unincumbent_Awarded_Outlier', 'BOOLEAN'),
        bigquery.SchemaField('is_Unincumbent_Unawarded_Outlier', 'BOOLEAN'),
    ],
    write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
)
job = client.load_table_from_dataframe(
    df_to_insert, table_id, job_config=job_config
)
job.result()

client.close()

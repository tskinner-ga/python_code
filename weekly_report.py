from pyhive import hive
from TCLIService.ttypes import TOperationState
import argparse
import pandas as pd
import numpy as np 

host = "hs2prod.corp.adobe.com"
port = 10000
database = "tskinner"
user = "tskinner"
password = "OfficeTest634!"

conn = hive.connect(host=host, port=port, username=user,password=password, database=database,auth="LDAP" )

iso_map = pd.read_csv("~/Documents/Target Docs/Mapping Files/iso_code_reference_csv.csv")

date_map = pd.read_csv("~/Documents/Target Docs/Mapping Files/date_mapping.csv", dtype = {'week':object,'fiscal_qtr':object,'week_num_qtr':object})

marketing_map = pd.read_csv("~/Documents/Target Docs/Mapping Files/subtype_marketing_channel.csv")

channel_map = pd.read_csv("~/Documents/Target Docs/Mapping Files/aa_marketing_channel_mapping.csv")

edc = pd.read_excel("~/Documents/Target Docs/edc_data.xlsx", dtype = {'Fiscal_Week':object,'Cam_Subtype':object,'Cam_Name':object,'Market_Area':object,'Response':float,'MQLs':float,'Disposition':float,'Accepted':float,'SALs':float,'SQLs':float,'Gross_ASV':float} )

aa_targets_q3fy19 = pd.read_excel("~/Documents/Target Docs/Q3FY19_Web_Targets.xlsx")

aa_pipeline_targets_q3fy19 = pd.read_excel("~/Documents/Target Docs/aa_pipeline_targets_q3fy19.xlsx")

full_metrics_table = pd.read_sql("SELECT * from tskinner.full_metrics_table WHERE last_touch_marketing_channel IS NOT NULL",conn)

fmt_device = pd.merge(full_metrics_table,
                      iso_map,
                      left_on = 'geo_country',
                      right_on = 'iso_code',
                      how = 'left')

fmt_device = pd.merge(fmt_device,
                      channel_map,
                   left_on = 'last_touch_marketing_channel',
                   right_on = 'aa_ltc',
                   how = 'left')

fmt_device = pd.DataFrame(data=fmt_device, columns =['fiscal_yr_and_qtr','fiscal_wk_in_qtr','new_mc','market_area','mobile_web','visits','visitors','repeat_visitors','engaged_visitors','faas_inquiries'])

fmt_device = pd.DataFrame(fmt_device.groupby(['fiscal_yr_and_qtr','fiscal_wk_in_qtr','new_mc','market_area','mobile_web']).sum())

fmt_device = fmt_device.reset_index(level=['fiscal_yr_and_qtr','fiscal_wk_in_qtr','new_mc','market_area','mobile_web'])


edc = pd.merge(edc,
               date_map,
               left_on = 'Fiscal_Week',
               right_on = 'week',
               how='left')

edc = pd.merge(edc,
               marketing_map,
               left_on = 'Cam_Subtype',
               right_on = 'edc_cam_subtype',
               how='left')

edc_cam = pd.DataFrame(data = edc, columns = ['fiscal_qtr','week_num_qtr','marketing_channel','Market_Area','Cam_Name','Response','MQLs','Disposition','Accepted','SALs','SQLs','Gross_ASV'])

edc = pd.DataFrame(data=edc, columns =['fiscal_qtr','week_num_qtr','marketing_channel','Market_Area','Response','MQLs','Disposition','Accepted','SALs','SQLs','Gross_ASV'])

edc = pd.DataFrame(edc.groupby(['fiscal_qtr','week_num_qtr','marketing_channel','Market_Area']).sum())

edc = edc.reset_index(level=['fiscal_qtr','week_num_qtr','marketing_channel','Market_Area'])

edc['week_num_qtr'] = edc['week_num_qtr'].apply(lambda x: '{0:0>2}'.format(x))

fmt_iso = pd.merge(full_metrics_table,
                   iso_map,
                   left_on = 'geo_country',
                   right_on = 'iso_code',
                   how="left")

fmt_iso = pd.merge(fmt_iso,
                   channel_map,
                   left_on = 'last_touch_marketing_channel',
                   right_on = 'aa_ltc',
                   how = 'left')


fmt_iso = pd.DataFrame(fmt_iso.groupby(['fiscal_yr_and_qtr','fiscal_wk_in_qtr','new_mc','market_area']).sum())

fmt_iso = fmt_iso.reset_index(level=['fiscal_yr_and_qtr','fiscal_wk_in_qtr','new_mc','market_area'])

fmt_iso_edc = pd.merge(fmt_iso,
                   edc,
                   left_on = ['fiscal_yr_and_qtr','fiscal_wk_in_qtr','new_mc','market_area'],
                   right_on = ['fiscal_qtr','week_num_qtr','marketing_channel','Market_Area'],
                   how="left")

fmt_iso_edc = pd.DataFrame(data=fmt_iso_edc, columns =['fiscal_yr_and_qtr','fiscal_wk_in_qtr','new_mc','market_area','visits','visitors','repeat_visitors','engaged_visitors','faas_inquiries','Response','MQLs','Disposition','Accepted','SALs','SQLs','Gross_ASV'])

fmt_iso_edc = pd.DataFrame.fillna(fmt_iso_edc,'0')

aa_targets_q3fy19['fiscal_yr_qtr'] = aa_targets_q3fy19.fiscal_yr_qtr.astype('str')

aa_targets_q3fy19['fiscal_week_qtr'] = aa_targets_q3fy19.fiscal_week_qtr.astype('str')

aa_targets_q3fy19['fiscal_week_qtr'] = aa_targets_q3fy19['fiscal_week_qtr'].apply(lambda x: '{0:0>2}'.format(x))

aa_pipeline_targets_q3fy19['week'] = aa_pipeline_targets_q3fy19.week.astype('str')

aa_pipeline_targets_q3fy19['week'] = aa_pipeline_targets_q3fy19['week'].apply(lambda x: '{0:0>2}'.format(x))

fmt_q3fy19 = full_metrics_table[full_metrics_table.fiscal_yr_and_qtr == '20193']

fmt_q3fy19 = pd.DataFrame(fmt_q3fy19.groupby(['fiscal_wk_in_qtr']).sum())

edc_summary = edc[edc.fiscal_qtr == '20193']

edc_summary = pd.DataFrame(edc_summary.groupby(['week_num_qtr']).sum())

edc_summary = edc_summary.reset_index(level=['week_num_qtr'])

fmt_q3fy19 = fmt_q3fy19.reset_index(level=['fiscal_wk_in_qtr'])

fmt_q3fy19 = pd.merge(fmt_q3fy19,
                    aa_pipeline_targets_q3fy19,
                    left_on = 'fiscal_wk_in_qtr',
                    right_on = 'week',
                    how = 'left')

fmt_q3fy19 = pd.merge(fmt_q3fy19,
                      edc_summary,
                      left_on = 'fiscal_wk_in_qtr',
                      right_on = 'week_num_qtr')

fmt_channels = pd.merge(full_metrics_table,
                   channel_map,
                   left_on = 'last_touch_marketing_channel',
                   right_on = 'aa_ltc',
                   how = 'left')

fmt_channels = pd.DataFrame(fmt_channels.groupby(['fiscal_yr_and_qtr','fiscal_wk_in_qtr','new_mc']).sum())

fmt_channels = fmt_channels.reset_index(level=['fiscal_yr_and_qtr','fiscal_wk_in_qtr','new_mc'])

fmt_channels_targets = pd.merge(fmt_channels,
                   aa_targets_q3fy19,
                   left_on = ['fiscal_yr_and_qtr','fiscal_wk_in_qtr','new_mc'],
                   right_on = ['fiscal_yr_qtr','fiscal_week_qtr','marketing_channel'],
                   how = 'left')

pd.DataFrame.to_csv(fmt_iso_edc,"~/Documents/Target Docs/edc_fmt_pythonoutput.csv")
pd.DataFrame.to_csv(fmt_device,"~/Documents/Target Docs/fmt_device_pythonoutput.csv")
pd.DataFrame.to_csv(fmt_channels_targets,"~/Documents/Target Docs/fmt_fmt_channels_targets.csv")
pd.DataFrame.to_csv(fmt_q3fy19,"~/Documents/Target Docs/fmt_pipe_q3fy19_targets.csv")
pd.DataFrame.to_csv(edc_cam,"~/Documents/Target Docs/edc_cam.csv")


dnjkqdnkjdq

nxbkax bjca
dnjnsdk

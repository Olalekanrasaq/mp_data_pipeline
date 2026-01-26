import pandas as pd
import os
from scripts.extractions import get_final_report
from datetime import datetime, timedelta
from scripts.data_quality import get_number_brms
from scripts.get_cluster_TA import get_cms_ta

# report_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

def get_reports(day, month, year, report_date, yesterday_date):
    sc_dirs = os.listdir("/opt/airflow/dags/DataFiles/")

    for dir in sc_dirs:
        if dir == "Regional":
            continue
        sc_dir = f"/opt/airflow/dags/DataFiles/{dir}"
        day_dir = f"{sc_dir}/{report_date}"
        cms = f'{sc_dir}/cms.csv'
        cms_ta = f'{sc_dir}/cms_ta.csv'

        business = f"{day_dir}/{day}-report.pdf"
        if not os.path.exists(business):
            business = f"{sc_dir}/{yesterday_date}/{int(day)-1}-report.pdf"
            print(f"{day_dir}/{day}-report.pdf not found. Using yesterday's instead.")
        loan = f"{day_dir}/{day}-loan.pdf"
        if not os.path.exists(loan):
            loan = f"{sc_dir}/{yesterday_date}/{int(day)-1}-loan.pdf"
            print(f"{day_dir}/{day}-loan.pdf not found. Using yesterday's instead.")
        moniebook = f"{day_dir}/{day}-moniebook.pdf"
        mb_day = day
        if not os.path.exists(moniebook):
            moniebook = f"{sc_dir}/{yesterday_date}/{int(day)-1}-moniebook.pdf"
            mb_day = int(day) - 1
            print(f"{day_dir}/{day}-moniebook.pdf not found. Using yesterday's instead.")
        if os.path.exists(f'{day_dir}/sc_report.csv'):
            print(f"Report for {dir} on {report_date} already exists. Skipping extraction.")
            continue
        df = get_final_report(business, loan, moniebook, day, month, year, cms, mb_day)

        new_col = ['BRM Name', 'day', 'month', 'total_terminals', 'assigned_terminals', 'non_transc_terminals', 
                'terminals_activity', 'payment_value', 'payment_vol', 'bo_retention', 'top_bo', 'retained_bo', 
                'Cards Sold MTD', 'referrals', 'value_disbursed', 'loans_disbursed', 
                'MTD Moniebook Onboarded', 'Active Business', 'MTD Active Moniebook']
        reports = df.reindex(columns=new_col)

        no_brms = get_number_brms(business)
        if no_brms != len(reports):
            raise ValueError(f"Data quality check failed: Expected {no_brms} BRMs, but got {len(reports)}")
        else:
            print(f"Data quality check passed: {no_brms} BRMs found.")
            reports.to_csv(f'{day_dir}/sc_report.csv', index=False)

        cluster_ta = get_cms_ta(cms_ta, business)
        cluster_ta.to_csv(f'{day_dir}/cluster_ta.csv', index=False)

        
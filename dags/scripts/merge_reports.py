from datetime import datetime, timedelta
import os
import pandas as pd

date_now = datetime.now().strftime("%Y-%m-%d")
no_backfills = 2

def merge_backfill_reports(**context):

    logical_date = context["logical_date"].date().strftime("%Y-%m-%d")  
    if logical_date != date_now:
        return "Not a latest run. No merging needed."
    
    base_dir = "/opt/airflow/dags/DataFiles"
    
    for sc in os.listdir(base_dir):
        if sc == "Regional":
            continue

        all_dfs = []

        n = no_backfills
        while n > 0:
            report_date = (datetime.now() - timedelta(days=n)).strftime("%Y-%m-%d")
            csv_path = f"{base_dir}/{sc}/{report_date}/sc_report.csv"
            if os.path.exists(csv_path):
                all_dfs.append(pd.read_csv(csv_path))

            n -= 1

        combined = pd.concat(all_dfs, ignore_index=True)
        combined.to_csv(
            f"{base_dir}/{sc}/{report_date}/combined_report.csv",
            index=False
        )
        print(f"Merging completed successfully for {sc}.")

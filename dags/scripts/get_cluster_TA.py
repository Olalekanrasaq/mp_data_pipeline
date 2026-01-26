import pandas as pd
import tabula
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

def get_best_match(name, choices, threshold=60):
    match, score = process.extractOne(name, choices, scorer=fuzz.token_set_ratio)
    return match if score >= threshold else None

def get_cms_ta(csv_path, pdf_path):
    df2 = pd.read_csv(csv_path)
    dfs = tabula.read_pdf(pdf_path, pages='7-15')
    target_dfs = [df for df in dfs if len(df.columns)==12 and df.columns[1]=="CM" and df.columns[-1]=="Payment Volume"]
    cms_df = pd.concat(target_dfs, ignore_index=True)
    cms_df = cms_df[['CM', 'Top BO Retention\rRate', 'Terminal Activity\rRate']]
    cms_df['CM'] = cms_df['CM'].str.replace('\r', ' ')
    cms_df = cms_df[~cms_df['CM'].isin(["Oluwaseun Ogunsola", "Abiodun Oyetunde", "Akintoye Adetola"])]
    cms_df["CM Name"] = cms_df['CM'].apply(lambda x: get_best_match(x, df2['CM Name'].tolist()))
    cms_ta_df = pd.merge(df2, cms_df, how="left", on="CM Name")
    cms_ta_df = cms_ta_df[['Team', 'CM Name', 'Top BO Retention\rRate', 'Terminal Activity\rRate']]
    cms_ta_df = cms_ta_df[~cms_ta_df['CM Name'].isin(["Oluwaseun Ogunsola", "Abiodun Oyetunde", "Akintoye Adetola"])]
    return cms_ta_df
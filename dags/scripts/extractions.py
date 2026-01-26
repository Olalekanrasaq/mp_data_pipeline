import pandas as pd
import numpy as np
import fitz
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

def extract_cards(pdf_path):
    doc = fitz.open(pdf_path)
    text = ""
    for page in doc[100:]:
        text += page.get_text("text") + "\n"
        
    lines = text.split("\n")
    idx = lines.index('BRM Summary - Card Sales/Referrals')
    data_list = lines[idx+30:-4]

    brms = []
    i = 0
    while i+5 < len(data_list):
        if 'Page' in data_list[i]:
            i += 32
        name = data_list[i].strip()
        if i + 1 < len(data_list) and not data_list[i + 1].isdigit() and not data_list[i + 2].isdigit():
            name += " " + data_list[i + 1].strip() + " " + data_list[i + 2].strip()
            i += 2
        elif i + 1 < len(data_list) and not data_list[i + 1].isdigit():
            name += " " + data_list[i + 1].strip()
            i += 1
        brm = {
            'BRM_Name': name,
            'Cards Sold MTD': data_list[i + 5],
            'referrals': data_list[i + 3]
        }
        brms.append(brm)
        i += 15

    df = pd.DataFrame(brms)
    df.loc[df["BRM_Name"] == "isichei emeka", "BRM_Name"] = "isichei emeka _"
    df.loc[df["BRM_Name"] == "Adeleye Kayode", "BRM_Name"] = "Adeleye Kayode IC"
    df.loc[df["BRM_Name"] == "Musa Abubakar", "BRM_Name"] = "Musa Abubakar -"
    df["BRM_name_adjust"] = df["BRM_Name"].str.replace(" ", "")
    return df

def extract_business(pdf_path, day, month, cms):
    cm_df = pd.read_csv(cms)
    cm_list = cm_df['CM'].tolist()
    doc = fitz.open(pdf_path)
    text = ""
    for page in doc[60:200]:
        text += page.get_text("text") + "\n"
        
    lines = text.split("\n")
    idx = lines.index('BRM Summary - Business Banking')
    end_idx = lines.index("Top 200 Declined BOs")
    data_list = lines[idx+21: end_idx]

    brms = []
    i = 0
    
    while i < len(data_list):
        name_parts = []
        start = i
    
        while start < len(data_list):
            if 'Page' in data_list[start]:
                start += 23
            try:
                is_name = data_list[start].strip()
            except IndexError:
                pass
            if is_name.isdigit():
                break
            name_parts.append(is_name)
            start += 1
        i = start-1
    
        name = " ".join(name_parts)
        try:
            brm = {
                'BRM_Name': name,
                'day': day,
                'month': month,
                'bo_retention': data_list[i+2],
                'top_bo': data_list[i+4],
                'retained_bo': data_list[i+8],
                'terminals_activity': data_list[i+6],
                'total_terminals': data_list[i+9],
                "assigned_terminals": data_list[i+5],
                "non_transc_terminals": data_list[i+10],
                "payment_value": data_list[i+11],
                "payment_vol": data_list[i+3]
            }
        
            brms.append(brm)
        except IndexError:
            pass
    
        i += 12

    for brm in brms:
        name = brm['BRM_Name']
        name_part = name.split()
        for cm in cm_list:
            pattern = f'*Self- Managed(Aux) {cm}'
            if pattern in name:
                continue
            elif cm in name and len(name_part) > 3 and (cm == ' '.join(name_part[:2]) or cm == ' '.join(name_part[:3])):
                name = name.replace(cm, '')
                break
                
        brm['BRM_Name'] = ' '.join(name.split())
        parts = brm['BRM_Name'].split()
        
        new_parts = []
        i = 0
        
        while i < len(parts):
            part = parts[i]
            
            # If this part should merge with previous word
            if len(part) <= 2 and part != '-' and i > 0:
                # merge with previous element in new_parts
                new_parts[-1] = new_parts[-1] + part
            else:
                new_parts.append(part)
            
            i += 1
        
        brm['BRM_Name'] = ' '.join(new_parts)

    df = pd.DataFrame(brms)
    df.loc[df["BRM_Name"] == "isichei emeka", "BRM_Name"] = "isichei emeka _"
    df.loc[df["BRM_Name"] == "ABDULMAJEE D ABDULRASHE", "BRM_Name"] = "ABDULMAJEED ABDULRASHEED"
    df.loc[df["BRM_Name"] == "Adeleye Kayode", "BRM_Name"] = "Adeleye Kayode IC"
    df.loc[df["BRM_Name"] == "ABDULHAMME D KHOLEELRAH", "BRM_Name"] = "ABDULHAMMED KHOLEELRAHMON AYOBI"
    df.loc[df["BRM_Name"] == "Musa Abubakar", "BRM_Name"] = "Musa Abubakar -"

    return df

# Function to find best match
def get_best_match(name, choices, threshold=60):
    name_tokens = set(name.lower().split())
    best_match, best_score, best_overlap = None, 0, 0
    
    for choice in choices:
        choice_tokens = set(choice.lower().split())
        
        # Count how many tokens overlap
        overlap = len(name_tokens & choice_tokens)
        
        # Fuzzy score as secondary measure
        score = fuzz.token_set_ratio(name, choice)
        
        # Prioritize by overlap count, then by fuzzy score
        if (overlap > best_overlap) or (overlap == best_overlap and score > best_score):
            best_match, best_score, best_overlap = choice, score, overlap
    
    # Require at least 2-token overlap if possible
    if best_overlap >= 2 and best_score >= threshold:
        return best_match
    elif best_overlap == 1 and best_score >= threshold:
        # fallback to single overlap if no 2+ match exists
        return best_match
    else:
        return None

def get_final_business(pdf_path, day, month, cms):
    cards = extract_cards(pdf_path)
    business = extract_business(pdf_path, day, month, cms)
    business['BRM Name'] = business['BRM_Name'].apply(lambda x: get_best_match(x, cards['BRM_Name'].tolist()))
    business = business.drop("BRM_Name", axis=1)
    business = business[[business.columns[-1]] + list(business.columns[:-1])]
    business["BRM_name_adjust"] = business["BRM Name"].str.replace(" ", "")
    cards_df = cards.drop(columns=["BRM_Name"])
    joint_df = pd.merge(business, cards_df, how="left", on="BRM_name_adjust")
    joint_df = joint_df.drop(columns=["BRM_name_adjust"])
    return joint_df

def extract_loan(pdf_path):
    doc = fitz.open(pdf_path)
    text = ""
    for page in doc[1:26]:
        text += page.get_text("text") + "\n"
    
    lines = text.split("\n")
    idx = lines.index('BRM Growth Performance')
    end_idx = lines.index("BRM NPL Performance")
    data_list = lines[idx+20: end_idx]

    brms = []
    i = 0
    
    while i+4 < len(data_list):
        if 'Prepared' in data_list[i]:
            i += 21
        name_parts = []
        start = i
    
        while start < len(data_list):
            name = data_list[start].strip()
            if name.isdigit():
                break
            name_parts.append(name)
            start += 1
    
        i = start
    
        if len(name_parts) < 2:
            brm_name_ = " ".join(name_parts)
        elif len(name_parts) == 2 and ((len(name_parts[0].split())==2 and len(name_parts[1].split())==1) or (len(name_parts[0].split())==1 and len(name_parts[1].split())==2)):
            brm_name_ = " ".join(name_parts)
        elif len(name_parts) == 2 and (len(name_parts[0].split()) >= 2 and len(name_parts[1].split()) >= 2):
            brm_name_ = " ".join(name_parts[1:])
        elif len(name_parts) == 2 and len(name_parts[0].split()) >= 4:
            brm_name_ = " ".join(name_parts)
        elif len(name_parts) == 3 and len(name_parts[0].split()) == 3:
            brm_name_ = " ".join(name_parts[1:])
        elif len(name_parts) == 3 and (len(name_parts[0].split()) == 2 and len(name_parts[1].split()) == 1 and len(name_parts[2].split()) >= 2):
            brm_name_ = " ".join(name_parts[2:])
        elif len(name_parts) == 3 and (len(name_parts[0].split()) >= 2 and len(name_parts[1].split()) == 1 and len(name_parts[2].split()) == 1):
            brm_name_ = " ".join(name_parts[1:])
        elif len(name_parts) == 3 and (len(name_parts[0].split()) == 2 and len(name_parts[1].split()) >= 2 and len(name_parts[2].split()) == 1):
            brm_name_ = " ".join(name_parts[1:])
        elif len(name_parts) == 4:
            brm_name_ = " ".join(name_parts[2:])
            
        if len(brm_name_.split()) == 4 and (brm_name_.split()[-1] != brm_name_.split()[-2]):
            brm_name_ = " ".join(brm_name_.split()[2:])
        elif len(brm_name_.split()) > 4:
            brm_name_ = " ".join(brm_name_.split()[-3:])
        
        loan_vol = data_list[i+3]
        
        if data_list[i+4].endswith('.') or '.' not in data_list[i+4] or data_list[i+5]=='0':
            loan_value = data_list[i+4] + data_list[i+5]
            i += 1
        else:
            loan_value = data_list[i+4]
            
        if data_list[i+6].endswith('.') or '.' not in data_list[i+6] or data_list[i+7]=='0':
            i += 1
        try:
            if data_list[i+8].endswith('.') or '.' not in data_list[i+8] or data_list[i+9]=='0':
                i += 1
        except IndexError:
            pass
            
        try:
            brm = {
                "BRM": brm_name_,
                "value_disbursed": loan_value,
                "loans_disbursed": loan_vol
            }
        except IndexError:
            pass
    
        brms.append(brm)
    
        i += 10

    df = pd.DataFrame(brms)
    df = df.drop_duplicates(subset=["BRM"])
    df.loc[df["BRM"] == "isichei emeka", "BRM"] = "isichei emeka _"
    df.loc[df["BRM"] == "Adeleye Kayode", "BRM"] = "Adeleye Kayode IC"
    df.loc[df["BRM"] == "Musa Abubakar", "BRM"] = "Musa Abubakar -"

    return df

def extract_moniebook(pdf_path, month, mb_day, year):
    # Open the PDF
    doc = fitz.open(pdf_path)
    
    # Extract text from all pages
    text = ""
    for page in doc:
        text += page.get_text("text") + "\n"

    # Extract required fields using string search
    lines = text.split("\n")
    idx_start = lines.index('Performing BRMs')
    idx_end = lines.index('Non Performing CMs')
    data_list = lines[idx_start+23:idx_end]

    # Initialize an empty list to store the dictionaries
    brms = []
    i = 0
    while i < len(data_list):
        if data_list[i+1] == f"{month} {mb_day}, {year}":
            i += 25
        # Combine name parts (if necessary)
        try:
            name = data_list[i]
            if i + 1 < len(data_list) and not data_list[i + 1].isdigit():
                name += " " + data_list[i + 1]
                
                i += 1  # Skip the next item as it's part of the name
            
            # Create the brm dictionary
            brm = {
                        'BRM': name,
                        'MTD Moniebook Onboarded': data_list[i + 1],
                        'Active Business': data_list[i + 4],
                        'MTD Active Moniebook': data_list[i + 6]
                    }
            
            brms.append(brm)
                
        except IndexError:
            pass
        i += 10  # Move to the next record
    
    df = pd.DataFrame(brms)
    try:
        df.loc[df.loc[df["BRM"].str.lower() == "adeleye kayode"].iloc[0].name, "BRM"] = "Adeleye Kayode IC"
    except IndexError:
        pass
    if len(brms) == 0:
        df = pd.DataFrame(columns=["BRM", "MTD Moniebook Onboarded", "Active Business", "MTD Active Moniebook"])
               
    return df

def get_final_report(business, loan, moniebook, day, month, year, cms, mb_day):
    business = get_final_business(business, day, month, cms)
    business = business.dropna(subset=['BRM Name'])
    loan = extract_loan(loan)
    try:
         loan['BRM Name'] = loan['BRM'].apply(lambda x: get_best_match(x, business['BRM Name'].tolist()))
    except:
        pass
    final_df = pd.merge(business, loan, how="left", on="BRM Name")
    final_df['value_disbursed'] = final_df['value_disbursed'].fillna(0)
    final_df['loans_disbursed'] = final_df['loans_disbursed'].fillna(0)
    final_df = final_df.drop(columns=['BRM'])
    mbook = extract_moniebook(moniebook, month, int(mb_day)+1, year)
    try:
        mbook['BRM Name'] = mbook['BRM'].apply(lambda x: get_best_match(x, final_df['BRM Name'].tolist()))
    except:
        pass
    final_df2 = pd.merge(final_df, mbook, how="left", on="BRM Name")
    final_df2['MTD Moniebook Onboarded'] = final_df2['MTD Moniebook Onboarded'].fillna(0)
    final_df2['Active Business'] = final_df2['Active Business'].fillna(0)
    final_df2['MTD Active Moniebook'] = final_df2['MTD Active Moniebook'].fillna(0)
    final_df2 = final_df2.drop(columns=['BRM'])
    final_df2 = final_df2.drop_duplicates(subset=["BRM Name"])
    return final_df2
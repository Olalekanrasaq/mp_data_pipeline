import fitz

def get_number_brms(pdf_path):
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
                'day': 9,
                'month': 'November',
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

    return len(brms)

            
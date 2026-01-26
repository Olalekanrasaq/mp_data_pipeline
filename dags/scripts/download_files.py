import imaplib
import email
import os
import re
from datetime import datetime, timedelta

# report_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
# day = (datetime.now() - timedelta(days=1)).day

def download_files(day, report_date):
    EMAIL = 'olalekanrasaq1331@gmail.com'
    PASSWORD = 'gakc mgme dcia qsfc'
    IMAP_SERVER = 'imap.gmail.com'
    MAILBOX = 'INBOX'
    data_dir = '/opt/airflow/dags/DataFiles/'
    os.makedirs(data_dir, exist_ok=True)

    # Connect to the server
    imapSession = imaplib.IMAP4_SSL(IMAP_SERVER)
    typ, _ = imapSession.login(EMAIL, PASSWORD)

    if typ != 'OK':
        raise Exception('Login failed')

    imapSession.select(MAILBOX)

    VALID_SUBJECTS = [
    f'Moniebook Regional Manager Business Report for {report_date}',
    f'Moniebook State Coordinator Business Report for {report_date}',
    f'Moniepoint Regional Manager Report for {report_date}',
    f'Moniepoint State Coordinator Report for {report_date}',
    f'Moniepoint Regional Manager Loans Report for {report_date}',
    f'Moniepoint State Coordinator Loans Report for {report_date}'
    ]
    
    for sub in VALID_SUBJECTS:
        typ, data = imapSession.search(None, f'(HEADER Subject "{sub}")')
        # typ, data = imapSession.search(None, '(HEADER Subject "Moniepoint State Coordinator Loans Report for 2026-01-11")')
        
        if typ != 'OK':
            raise Exception('IMAP search failed')
            
        # ---------------- PROCESS EMAILS ----------------
        for msgId in data[0].split():
            typ, messageParts = imapSession.fetch(msgId, '(RFC822)')
            if typ != 'OK':
                continue
        
            msg = email.message_from_bytes(messageParts[0][1])
            subject = msg.get('Subject', '').strip()
            
            # Extract message body (text only)
            body_text = ''
            for part in msg.walk():
                if part.get_content_type() == 'text/html':
                    body_text = part.get_payload(decode=True).decode(errors='ignore')
                    break
                    
            # Extract name
            match = re.search(r'Hello\s+([A-Za-z\s]+),', body_text)
            if match:
                person_name = match.group(1).strip()
            if not match and "Regional" not in subject:
                continue
            elif "Regional" in subject:
                person_name = "Regional"
                
            if person_name in ["Lovemax", "Lawal akeem", "Ayomide Eniolorunda"]:
                continue
            
            person_dir = os.path.join(data_dir, person_name)
            os.makedirs(person_dir, exist_ok=True)

            date_dir = os.path.join(person_dir, report_date)
            os.makedirs(date_dir, exist_ok=True)
        
            # 🔸 Download only PDF attachments
            for part in msg.walk():
                if part.get_content_maintype() == 'multipart':
                    continue
                if part.get('Content-Disposition') is None:
                    continue
                
                filename = part.get_filename()
                if not filename:
                    continue
        
                if not filename.lower().endswith('.pdf'):
                    continue
        
                if 'State Coordinator Business Report' in subject:
                    if not "moniebook" in filename:
                        continue
                    file_prefix = f'{day}-moniebook.pdf'
                elif 'State Coordinator Report' in subject:
                    file_prefix = f'{day}-report.pdf'
                elif 'State Coordinator Loans Report' in subject:
                    if not "daily_loans" in filename:
                        continue
                    file_prefix = f'{day}-loan.pdf'
                elif 'Regional Manager Loans Report' in subject:
                    if not "daily_loans" in filename:
                        continue
                    file_prefix = f'regional-{day}-loan.pdf'
                elif 'Regional Manager Business Report' in subject:
                    file_prefix = f'regional-{day}-moniebook.pdf'
                elif 'Regional Manager Report' in subject:
                    file_prefix = f'regional-{day}.pdf'
                
                
                file_path = os.path.join(date_dir, file_prefix)
        
                with open(file_path, 'wb') as f:
                    f.write(part.get_payload(decode=True))
                print(f'Downloaded: {person_name}/{report_date}/{file_prefix}')

    imapSession.close()
    imapSession.logout()

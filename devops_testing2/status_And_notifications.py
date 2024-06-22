import configparser
import smtplib
import imaplib
import email
from email.mime.text import MIMEText
import threading
import time
import snowflake.connector
import subprocess
import csv

# Email configuration
smtp_server = 'smtp.gmail.com'
smtp_port = 587
imap_server = 'imap.gmail.com'
imap_port = 993
sender_email = 'kasmosnowflake@gmail.com'
sender_password = 'clnxwbblzitvytge'

def run_git_command(command, cwd=None):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, cwd=cwd)
    stdout, stderr = process.communicate()
    return stdout.decode('utf-8'), stderr.decode('utf-8'), process.returncode

def sync_branches(source_branch, target_branch, repo_path):
    # Fetch changes from remote
    fetch_command = 'git fetch origin'
    stdout, stderr, returncode = run_git_command(fetch_command, cwd=repo_path)
    if returncode != 0:
        print(f"Error fetching changes: {stderr}")
        return

    # Checkout dev branch
    checkout_command = f'git checkout {source_branch}'
    stdout, stderr, returncode = run_git_command(checkout_command, cwd=repo_path)
    if returncode != 0:
        print(f"Error checking out dev branch: {stderr}")
        return

    # Pull changes from dev branch
    pull_command = f'git pull origin {source_branch}'
    stdout, stderr, returncode = run_git_command(pull_command, cwd=repo_path)
    if returncode != 0:
        print(f"Error pulling changes from dev branch: {stderr}")
        return

    # Checkout test branch and merge changes from dev branch
    checkout_merge_command = f'git checkout {target_branch} && git merge {source_branch}'
    stdout, stderr, returncode = run_git_command(checkout_merge_command, cwd=repo_path)
    if returncode != 0:
        print(f"Error checking out or merging test branch: {stderr}")
        return

    # Push changes to test branch
    push_command = f'git push origin {target_branch}'
    stdout, stderr, returncode = run_git_command(push_command, cwd=repo_path)
    if returncode != 0:
        print(f"Error pushing changes: {stderr}")
        return

    print("Branches synchronized successfully.")


approve_keywords = ["Approve", "approve", "APPROVE", "APPROVED", "approved", "Approved"]

class EmailListener(threading.Thread):
    def __init__(self, subject,source_branch, target_branch,repo_path):
        threading.Thread.__init__(self)
        self.subject = subject
        self.source_branch = source_branch
        self.target_branch = target_branch
        self.repo_path=repo_path
        self.reply_received = threading.Event()
        self.reply_subject = None
        self.reply_text = None

    def run(self):
        while not self.reply_received.is_set():
            self.check_for_reply(self.subject,self.source_branch, self.target_branch,self.repo_path)
            time.sleep(5)


    def check_for_reply(self, subject,source_branch, target_branch, repo_path):
        with imaplib.IMAP4_SSL(imap_server, imap_port) as server:
            server.login(sender_email, sender_password)
            server.select('inbox')

            search_criteria = f'SUBJECT "{subject}" UNSEEN'
            _, data = server.search(None, search_criteria)
            email_ids = data[0].split()

            if email_ids:
                latest_email_id = email_ids[-1]
                _, msg_data = server.fetch(latest_email_id, '(RFC822)')
                msg = email.message_from_bytes(msg_data[0][1])

                reply_subject = msg['Subject']
                reply_text = ""

                for part in msg.walk():
                    if part.get_content_type() == "text/plain":
                        reply_text = part.get_payload(decode=True).decode()

                if reply_subject and reply_text:
                    print("Reply Subject:", reply_subject)
                    print("Reply Text:", reply_text)
                    if any(keyword in reply_text for keyword in approve_keywords):
                        print(f"Specific word found in reply text.")
                        # Execute the synchronization process
                        sync_branches(source_branch, target_branch, repo_path)
                    self.reply_received.set()


def send_email(subject, message, to_email):
    msg = MIMEText(message)
    msg['From'] = sender_email
    msg['To'] = to_email
    msg['Subject'] = subject

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, to_email, msg.as_string())

def check_failures(csv_file_path, status_column_index):
    try:
        with open(csv_file_path, 'r', encoding='latin-1') as csv_file:
            csv_reader = csv.reader(csv_file)

            # Get the header row
            header = next(csv_reader, None)

            # Initialize variables to track failures and error rows
            failures_exist = False
            error_rows = []

            # Iterate through the rows and check the specified status column
            for row in csv_reader:
                if status_column_index < len(row):
                    status_value = row[status_column_index].strip().upper()
                    if status_value == "FAIL":
                        failures_exist = True
                        # Capture the entire row
                        error_rows.append(row)
                    elif status_value != "SUCCESS":
                        print(f"Invalid status value: {status_value}")
                        return False, [], header  # Return header along with failures and an empty list
                else:
                    print(f"Invalid status column index {status_column_index} provided.")
                    return False, [], header  # Return header along with failures and an empty list

            return failures_exist, error_rows, header
    except Exception as e:
        print(f"An error occurred: {e}")
        return False, [], []


def notify(csv_file_path,source_branch,target_branch, execution_status, path,sf_params):
    # Specify the index of the column for status ("Success" or "Fail")
    status_column_index = 3  # Replace with the correct index of the status column

    # Specify the index of the error column

    # Call the function to check for failures
    failures_exist, error_rows, header = check_failures(csv_file_path, status_column_index)

    conn = snowflake.connector.connect(**sf_params)

    cursor = conn.cursor()

    # Define your criteria to fetch email configuration data based on success or failure
    if failures_exist:
        execution_status = False

        # Fetch email configuration for failure
        cursor.execute("SELECT subject, mail_body,email1  FROM DEVOPS_PROJECT_SCHEMA.APPROVER_MAIL_METADATA WHERE ENVIRONMENT='Dev'")
        row = cursor.fetchone()

        # Check if a matching record was found in EmailConfig
        if row:
            subject, message, to_email = row
        else:
            # Handle the case where no matching record was found
            print("No matching record found in EmailConfig for failures.")
            subject = "Default Subject for Failures"
            message = "Default Message for Failures"
            to_email = 'failure@example.com'

        if failures_exist:
            # Generate error messages with column names and values
            error_messages = []
            for error_row in error_rows:
                error_message = "\n".join([f"{header[i]} = {error_row[i]}" for i in range(len(header))])
                error_messages.append(error_message)

            error_message = "\n\n".join(error_messages)
            print(f"Error Rows:\n{error_message}")
            # Include error_message in the email message if there are failures
            message += error_message

            # Send the email immediately
            send_email(subject, message, to_email)
            print(f"Immediate email sent to {to_email} for failures.")

    else:
        execution_status=True
        if "uat" in target_branch.lower():
            # Check if there are any successful rows
            cursor.execute("SELECT subject, mail_body,email1  FROM DEVOPS_PROJECT_SCHEMA.APPROVER_MAIL_METADATA WHERE ENVIRONMENT='UAT'"        )
            row = cursor.fetchone()
        if "prod" in target_branch.lower():
            cursor.execute(
                "SELECT subject, mail_body,email1  FROM DEVOPS_PROJECT_SCHEMA.APPROVER_MAIL_METADATA WHERE ENVIRONMENT='PROD'")
            row = cursor.fetchone()


        # If all rows are successful, set up the email listener
        if row:
            subject, message, to_email = row

            # Send the email using the retrieved or default values
            send_email(subject, message, to_email)
            print(f"Email sent to {to_email} based on retrieved data.")
            
            email_listener = EmailListener(subject,source_branch,target_branch,path)
            email_listener.start()
            email_listener.join()

            if email_listener.reply_received.is_set():
                print("Reply Subject:", email_listener.reply_subject)
                print("Reply Text:", email_listener.reply_text)
            else:
                print("No reply received.")


    cursor.close()
    conn.close()

    print("Done.")
    return execution_status

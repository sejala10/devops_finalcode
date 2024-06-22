import configparser
import os
from git import Repo
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import snowflake.connector as sf
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import subprocess
import time


# def capture(source_branch, target_branch,repo_path):
def capture(email_subject, email_body, source_branch, target_branch, repo_path, sender_email, receiver_email, password, sf_params):

    commit_message = 'Automated file commit'  # Modify the commit message

    # Check if the repository already exists, if not, initialize it
    if not os.path.exists(os.path.join(repo_path, '.git')):
        Repo.init(repo_path)

    # Initialize the repository
    repo = Repo(repo_path)

    # Email sending code (same as before)
    # def send_email(subject, body):
    def send_email(subject, body, sender_email, receiver_email, password):
        message = MIMEMultipart()
        message['From'] = sender_email
        message['To'] = receiver_email
        message['Subject'] = subject
        message.attach(MIMEText(body, 'plain'))

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message.as_string())
        server.quit()
        print('Email sent to TEST-MANAGER')

    # Define a custom event handler for file system events
    class FileEventHandler(FileSystemEventHandler):
        def process(self, event):
            if event.is_directory or event.src_path.startswith(repo_path + os.sep + '.git'):
                return
            # print(f"Detected change in file: {event.src_path}")
            self.commit_and_push(event.src_path)

        def on_created(self, event):
            self.process(event)

        def on_modified(self, event):
            self.process(event)

        def commit_and_push(self, file_path):
            try:
                # Stage the file
                # Stage all modified files
                repo.git.add('--all')

                # Commit all staged changes
                repo.index.commit(commit_message)

                # Push changes to the remote repository
                origin = repo.remote(name='origin')  # Assuming 'origin' is the remote's name
                origin.push()

                print("Pushed changes to the remote repository.")
            except Exception as e:
                print(f"Error while processing {file_path}: {e}")

    # Git synchronization code
    def run_git_command(command):
        # Enclose the repo_path in double quotes to handle spaces
        command = command.replace(repo_path, f'"{repo_path}"')
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = process.communicate()
        return stdout.decode('utf-8'), stderr.decode('utf-8'), process.returncode

    def sync_branches(dev_branch, test_branch, repo_path):
        # Fetch changes from remote
        fetch_command = f'git -C {repo_path} fetch origin'
        stdout, stderr, returncode = run_git_command(fetch_command)
        if returncode != 0:
            print(f"Error fetching changes: {stderr}")
            return
        # Checkout dev branch and pull changes
        checkout_pull_command = f'git -C {repo_path} checkout {dev_branch} && git -C {repo_path} pull origin {dev_branch}'
        stdout, stderr, returncode = run_git_command(checkout_pull_command)
        if returncode != 0:
            print(f"Error checking out or pulling dev branch: {stderr}")
            return
        # Checkout test branch and merge changes from dev branch
        checkout_merge_command = f'git -C {repo_path} checkout {test_branch} && git -C {repo_path} merge {dev_branch}'
        stdout, stderr, returncode = run_git_command(checkout_merge_command)
        if returncode != 0:
            print(f"Error checking out or merging test branch: {stderr}")
            return
        # Push changes to test branch
        push_command = f'git -C {repo_path} push origin {test_branch}'
        stdout, stderr, returncode = run_git_command(push_command)
        if returncode != 0:
            print(f"Error pushing changes: {stderr}")
            return

        print("Branches synchronized successfully.")

    # Initialize a watchdog observer
    event_handler = FileEventHandler()
    observer = Observer()
    observer.schedule(event_handler, path=repo_path, recursive=True)  # Recursive=True to traverse folders
    observer.start()

    connection=sf.connect(**sf_params)

    cursor = connection.cursor()

    def execute_and_save_query(query, filename):
        try:
            # cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetchone()

            with open(filename, 'w', encoding='utf-8') as file:
                if result and isinstance(result[0], str):
                    file.write(result[0])

            print(f'Results saved to {filename}')
        except Exception as e:
            print(f'Error executing query: {e}')

    drop_output_folder = f"{repo_path}\\drop_scripts"

    def warehouse():
        query = "SHOW WAREHOUSES;"
        cursor.execute(query)
        results = cursor.fetchall()

        # Create a folder to save the SQL files if it doesn't exist
        create_output_folder = f"{repo_path}\\create_scripts\\warehouses"
       

        if not os.path.exists(create_output_folder):
            os.makedirs(create_output_folder)

        if not os.path.exists(drop_output_folder):
            os.makedirs(drop_output_folder)

        exclude_databases = ['COMPUTE_WH']

        # Generate SQL files
        for row in results:
            warehouse_name = row[0]

            if warehouse_name not in exclude_databases:
                create_sql_file_name = f"{create_output_folder}/warehouse_{warehouse_name}.sql"

                # Write the SQL statement to the file
                with open(create_sql_file_name, 'w') as create_sql_file:
                    create_sql_file.write(f"CREATE WAREHOUSE {warehouse_name};")

                drop_sql_file_name = f"{drop_output_folder}/rollback_warehouse_{warehouse_name}.sql"

                # Write the SQL statement to the file
                with open(drop_sql_file_name, 'w') as drop_sql_file:
                    drop_sql_file.write(f"drop WAREHOUSE {warehouse_name};")

        print(f"{len(results)} SQL files generated and saved in the '{create_output_folder}' folder.")

    def user():
        query = "select * from snowflake.ACCOUNT_USAGE.USERS WHERE  default_warehouse = 'COMPUTE_WH';"
        cursor.execute(query)
        results = cursor.fetchall()

        # Create a folder to save the SQL files if it doesn't exist
        create_output_folder = f"{repo_path}\\create_scripts\\users"

        if not os.path.exists(create_output_folder):
            os.makedirs(create_output_folder)

        if not os.path.exists(drop_output_folder):
            os.makedirs(drop_output_folder)

        # Generate SQL files
        for row in results:
            user_name = row[1]

            create_sql_file_name = f"{create_output_folder}/user_{user_name}.sql"

            # Write the SQL statement to the file
            with open(create_sql_file_name, 'w') as create_sql_file:
                create_sql_file.write(f"CREATE OR REPLACE USER {user_name};")

            drop_sql_file_name = f"{drop_output_folder}/rollback_user_{user_name}.sql"

            # Write the SQL statement to the file
            with open(drop_sql_file_name, 'w') as drop_sql_file:
                drop_sql_file.write(f"drop USER {user_name};")

        print(f"{len(results)} SQL files generated and saved in the '{create_output_folder}' folder.")

    def role():
        query = "select * from snowflake.ACCOUNT_USAGE.roles WHERE  OWNER is not null;"
        cursor.execute(query)
        results = cursor.fetchall()

        # Create a folder to save the SQL files if it doesn't exist
        create_output_folder = f"{repo_path}\\create_scripts\\roles"

        if not os.path.exists(create_output_folder):
            os.makedirs(create_output_folder)

        if not os.path.exists(drop_output_folder):
            os.makedirs(drop_output_folder)

        # Generate SQL files
        for row in results:
            role_name = row[3]
            create_sql_file_name = f"{create_output_folder}/role_{role_name}.sql"

            # Write the SQL statement to the file
            with open(create_sql_file_name, 'w') as create_sql_file:
                create_sql_file.write(f"CREATE OR REPLACE ROLE {role_name};")

            drop_sql_file_name = f"{drop_output_folder}/rollback_role_{role_name}.sql"

            # Write the SQL statement to the file
            with open(drop_sql_file_name, 'w') as drop_sql_file:
                drop_sql_file.write(f"DROP ROLE {role_name};")

        print(f"{len(results)} SQL files generated and saved in the '{create_output_folder}' folder.")

    def database():
        query = "SHOW DATABASES;"
        cursor.execute(query)
        results = cursor.fetchall()

        # Create a folder to save the SQL files if it doesn't exist
        create_output_folder = f"{repo_path}\\create_scripts\\database"

        if not os.path.exists(create_output_folder):
            os.makedirs(create_output_folder)

        if not os.path.exists(drop_output_folder):
            os.makedirs(drop_output_folder)

        # Define databases to exclude from execution
        exclude_databases = ['SNOWFLAKE', 'SNOWFLAKE_SAMPLE_DATA']

        # Initialize a counter for processed databases
        processed_count = 0

        # Generate SQL files
        for row in results:
            database_name = row[1]

            # Check if the database_name is not in the exclude list
            if database_name not in exclude_databases:
                create_sql_file_name = f"{create_output_folder}/database_{database_name}.sql"

                # Write the SQL statement to the file
                with open(create_sql_file_name, 'w') as create_sql_file:
                    create_sql_file.write(f"CREATE OR REPLACE DATABASE {database_name};")

                drop_sql_file_name = f"{drop_output_folder}/rollback_database_{database_name}.sql"

                # Write the SQL statement to the file
                with open(drop_sql_file_name, 'w') as drop_sql_file:
                    drop_sql_file.write(f"DROP DATABASE {database_name};")

                processed_count += 1

        print(f"{processed_count} SQL files generated and saved in the '{create_output_folder}' folder.")

    def schema():
        query = "show schemas;"
        cursor.execute(query)
        results = cursor.fetchall()

        # Create a folder to save the SQL files if it doesn't exist
        create_output_folder = f"{repo_path}\\create_scripts\\schema"

        if not os.path.exists(create_output_folder):
            os.makedirs(create_output_folder)

        if not os.path.exists(drop_output_folder):
            os.makedirs(drop_output_folder)

        exclude_schema = ['INFORMATION_SCHEMA', 'PUBLIC']
        processed_count = 0

        # Generate SQL files
        for row in results:
            SCHEMA_name = row[1]
            DATABASE_NAME = row[4]
            if SCHEMA_name not in exclude_schema:
                create_sql_file_name = f"{create_output_folder}/schema_{SCHEMA_name}.sql"

                # Write the SQL statement to the file
                with open(create_sql_file_name, 'w') as create_sql_file:
                    create_sql_file.write(f"CREATE OR REPLACE SCHEMA {DATABASE_NAME}.{SCHEMA_name};")

                drop_sql_file_name = f"{drop_output_folder}/rollback_schema_{SCHEMA_name}.sql"

                # Write the SQL statement to the file
                with open(drop_sql_file_name, 'w') as drop_sql_file:
                    drop_sql_file.write(f"DROP SCHEMA {SCHEMA_name};")

                processed_count += 1

        print(f"{processed_count} SQL files generated and saved in the '{create_output_folder}' folder.")

    def stage():
        query = "SHOW SCHEMAS;"
        cursor = connection.cursor()
        cursor.execute(query)
        schemas = [row[1] for row in cursor.fetchall()]

        create_output_folder = f"{repo_path}\\create_scripts\\stages"

        if not os.path.exists(create_output_folder):
            os.makedirs(create_output_folder)

        if not os.path.exists(drop_output_folder):
            os.makedirs(drop_output_folder)

        # Iterate through schemas and generate SQL files for stages in each schema
        for schema in schemas:
            # Execute the SHOW STAGES query for the current schema
            query = f"SHOW STAGES IN SCHEMA {schema};"
            cursor = connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()

            for row in results:
                stage_name = row[1]
                url = row[4]  # Assuming the URL column is at index 4 in the query results

                create_sql_file_name = os.path.join(create_output_folder, f"stage_{stage_name}.sql")

                # Write the SQL statement to the file
                with open(create_sql_file_name, 'w') as create_sql_file:
                    create_sql_file.write(
                        f"CREATE OR REPLACE STAGE {schema}.{stage_name}\nURL='{url}'\nCREDENTIALS=(AZURE_SAS_TOKEN='?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-10-07T14:55:13Z&st=2023-09-19T06:55:13Z&spr=https&sig=aMttQM57uqT0aTSK30ibgfpvYvpAzn5ACImPCUfUkF8%3D');")

                drop_sql_file_name = os.path.join(drop_output_folder, f"rollback_stage_{stage_name}.sql")

                # Write the SQL statement to the file
                with open(drop_sql_file_name, 'w') as drop_sql_file:
                    drop_sql_file.write(
                        f"DROP STAGE {schema}.{stage_name};")

            print(
                f"{len(results)} SQL files generated and saved in the '{create_output_folder}' folder for schema {schema}.")

    def procedure():
        # Create a directory to store the output files
        create_output_folder = f"{repo_path}\\create_scripts\\procedures"

        if not os.path.exists(create_output_folder):
            os.makedirs(create_output_folder)
        if not os.path.exists(drop_output_folder):
            os.makedirs(drop_output_folder)

        try:
            # Get the list of schemas within the database
            cursor.execute("SHOW SCHEMAS")
            schemas_result = cursor.fetchall()

            for schema_info in schemas_result:
                schema_name = schema_info[1]  # Assuming the schema name is in the second column
                print(f"Processing Schema: {schema_name}")

                try:
                    # Get the list of procedures in the current schema
                    cursor.execute(f"SHOW PROCEDURES IN SCHEMA {schema_name}")
                    procedures_result = cursor.fetchall()

                    for procedure_info in procedures_result:
                        schema_name = procedure_info[2]  # Assuming the schema name is in the second column

                        procedure_name = procedure_info[1]  # Assuming the name is in the first column
                        arguments = procedure_info[8]  # Assuming the arguments are in the ninth column

                        print(f"Processing PROCEDURE: {schema_name}.{procedure_name}")

                        # Check if schema_name is not null and not empty before processing
                        if schema_name and schema_name.strip():
                            try:
                                # Find the position of the "RETURN" keyword
                                return_position = arguments.find('RETURN')

                                # Extract the part of arguments before the "RETURN" keyword
                                if return_position != -1:
                                    arguments_before_return = arguments[:return_position]
                                else:
                                    arguments_before_return = arguments

                                ddl_query = f"SELECT GET_DDL('PROCEDURE', '{schema_name}.{arguments_before_return}')"
                                execute_and_save_query(ddl_query, os.path.join(create_output_folder,
                                                                               f"procedure_{procedure_name}.sql"))

                                drop_sql_file_name = os.path.join(drop_output_folder,
                                                                  f"rollback_procedure_{procedure_name}.sql")

                                # Write the SQL statement to the file
                                with open(drop_sql_file_name, 'w') as drop_sql_file:
                                    drop_sql_file.write(
                                        f"DROP PROCEDURE {schema_name}.{arguments_before_return};")

                            except sf.errors.ProgrammingError as e:
                                print(f'Error processing PROCEDURE: {schema_name}.{procedure_name}: {e}')
                            except Exception as e:
                                print(f'Unexpected error processing PROCEDURE: {schema_name}.{procedure_name}: {e}')
                        else:
                            print(f"Skipping PROCEDURE {procedure_name} due to null or empty schema.")


                except sf.errors.ProgrammingError as e:
                    print(f'Error processing Schema: {schema_name}: {e}')
                except Exception as e:
                    print(f'Unexpected error processing Schema: {schema_name}: {e}')

        except Exception as e:
            print(f'Error executing query: {e}')

    def table():
        create_output_folder = f"{repo_path}\\create_scripts\\tables"

        if not os.path.exists(create_output_folder):
            os.makedirs(create_output_folder)

        if not os.path.exists(drop_output_folder):
            os.makedirs(drop_output_folder)

        try:
            # Get the list of schemas within the database
            cursor.execute("SHOW SCHEMAS")
            schemas_result = cursor.fetchall()

            for schema_info in schemas_result:
                schema_name = schema_info[1]  # Assuming the schema name is in the second column
                print(f"Processing Schema: {schema_name}")

                try:
                    # Get the list of tables in the current schema
                    cursor.execute(f"SHOW TABLES IN SCHEMA {schema_name}")
                    tables_result = cursor.fetchall()

                    for table_info in tables_result:
                        table_name = table_info[1]  # Assuming the table name is in the second column
                        print(f"Processing Table: {schema_name}.{table_name}")

                        ddl_query = f"SELECT GET_DDL('table', '{schema_name}.{table_name}')"
                        execute_and_save_query(ddl_query, os.path.join(create_output_folder, f"table_{table_name}.sql"))

                        drop_sql_file_name = os.path.join(drop_output_folder,
                                                          f"rollback_table_{table_name}.sql")

                        # Write the SQL statement to the file
                        with open(drop_sql_file_name, 'w') as drop_sql_file:
                            drop_sql_file.write(
                                f"DROP TABLE {schema_name}.{table_name};")

                except sf.errors.ProgrammingError as e:
                    print(f'Error processing Schema: {schema_name}: {e}')
                except Exception as e:
                    print(f'Unexpected error processing Schema: {schema_name}: {e}')

        except Exception as e:
            print(f'Error executing query: {e}')

    def view():
        create_output_folder = f"{repo_path}\\create_scripts\\views"

        if not os.path.exists(create_output_folder):
            os.makedirs(create_output_folder)
        if not os.path.exists(drop_output_folder):
            os.makedirs(drop_output_folder)

        exclude_databases = ['INFORMATION_SCHEMA', 'PUBLIC']

        try:
            # Get the list of schemas within the database
            cursor.execute("SHOW SCHEMAS")
            schemas_result = cursor.fetchall()

            for schema_info in schemas_result:
                schema_name = schema_info[1]  # Assuming the schema name is in the second column
                print(f"Processing Schema: {schema_name}")
                if schema_name not in exclude_databases:

                    try:
                        # Get the list of views in the current schema
                        cursor.execute(f"SHOW VIEWS IN SCHEMA {schema_name}")
                        views_result = cursor.fetchall()

                        for view_info in views_result:
                            view_name = view_info[1]  # Assuming the view name is in the second column
                            print(f"Processing VIEW: {schema_name}.{view_name}")

                            ddl_query = f"SELECT GET_DDL('VIEW', '{schema_name}.{view_name}')"
                            execute_and_save_query(ddl_query,
                                                   os.path.join(create_output_folder, f"view_{view_name}.sql"))

                            drop_sql_file_name = os.path.join(drop_output_folder,
                                                              f"rollback_view_{view_name}.sql")

                            # Write the SQL statement to the file
                            with open(drop_sql_file_name, 'w') as drop_sql_file:
                                drop_sql_file.write(
                                    f"DROP VIEW {schema_name}.{view_name};")


                    except sf.errors.ProgrammingError as e:
                        print(f'Error processing Schema: {schema_name}: {e}')
                    except Exception as e:
                        print(f'Unexpected error processing Schema: {schema_name}: {e}')

        except Exception as e:
            print(f'Error executing query: {e}')

    def file_format():
        create_output_folder = f"{repo_path}\\create_scripts\\file_formats"

        if not os.path.exists(create_output_folder):
            os.makedirs(create_output_folder)

        if not os.path.exists(drop_output_folder):
            os.makedirs(drop_output_folder)

        try:
            # Get the list of schemas within the database
            cursor.execute("SHOW SCHEMAS")
            schemas_result = cursor.fetchall()

            for schema_info in schemas_result:
                schema_name = schema_info[1]  # Assuming the schema name is in the second column
                print(f"Processing Schema: {schema_name}")

                # Get the list of file formats in the current schema
                cursor.execute(f"SHOW FILE FORMATS IN SCHEMA {schema_name}")
                file_formats_result = cursor.fetchall()

                for file_format_info in file_formats_result:
                    file_format_name = file_format_info[1]  # Assuming the name is in the first column
                    print(f"Processing FILE FORMAT: {schema_name}.{file_format_name}")

                    try:
                        ddl_query = f"SELECT GET_DDL('FILE_FORMAT', '{schema_name}.{file_format_name}')"
                        execute_and_save_query(ddl_query,
                                               os.path.join(create_output_folder,
                                                            f"File format_{file_format_name}.sql"))

                    except sf.errors.ProgrammingError as e:
                        print(f'Error processing FILE FORMAT: {schema_name}.{file_format_name}: {e}')
                    except Exception as e:
                        print(f'Unexpected error processing FILE FORMAT: {schema_name}.{file_format_name}: {e}')

                    drop_sql_file_name = os.path.join(drop_output_folder,
                                                      f"rollback_file_format_{file_format_name}.sql")

                    # Write the SQL statement to the file
                    with open(drop_sql_file_name, 'w') as drop_sql_file:
                        drop_sql_file.write(
                            f"DROP FILE FORMAT {schema_name}.{file_format_name};")

        except Exception as e:
            print(f'Error executing query: {e}')

    def udf():
        create_output_folder = f"{repo_path}\\create_scripts\\udfs"

        if not os.path.exists(create_output_folder):
            os.makedirs(create_output_folder)
        if not os.path.exists(drop_output_folder):
            os.makedirs(drop_output_folder)

        try:
            # Get the list of schemas within the database
            cursor.execute("SHOW SCHEMAS")
            schemas_result = cursor.fetchall()

            for schema_info in schemas_result:
                schema_name = schema_info[1]  # Assuming the schema name is in the second column
                print(f"Processing Schema: {schema_name}")

                try:
                    # Get the list of user-defined functions (UDFs) in the current schema
                    cursor.execute(f"SHOW USER FUNCTIONS IN SCHEMA {schema_name}")
                    udf_result = cursor.fetchall()

                    for udf_info in udf_result:
                        udf_name = udf_info[1]  # Assuming the name is in the first column
                        arguments = udf_info[8]
                        print(f"Processing UDF: {schema_name}.{udf_name}")

                        if schema_name and schema_name.strip():
                            try:
                                # Find the position of the "RETURN" keyword
                                return_position = arguments.find('RETURN')

                                # Extract the part of arguments before the "RETURN" keyword
                                if return_position != -1:
                                    arguments_before_return = arguments[:return_position]
                                else:
                                    arguments_before_return = arguments

                                ddl_query = f"SELECT GET_DDL('FUNCTION', '{schema_name}.{arguments_before_return}')"
                                execute_and_save_query(ddl_query,
                                                       os.path.join(create_output_folder, f"udf_{udf_name}.sql"))
                            except sf.errors.ProgrammingError as e:
                                print(f'Error processing UDF: {schema_name}.{udf_name}: {e}')
                            except Exception as e:
                                print(f'Unexpected error processing UDF: {schema_name}.{udf_name}: {e}')

                            drop_sql_file_name = os.path.join(drop_output_folder,
                                                              f"rollback_udf_{udf_name}.sql")

                            # Write the SQL statement to the file
                            with open(drop_sql_file_name, 'w') as drop_sql_file:
                                drop_sql_file.write(
                                    f"DROP FUNCTION {schema_name}.{udf_name}();")

                        else:
                            print(f"Skipping PROCEDURE {udf_name} due to null or empty schema.")

                except sf.errors.ProgrammingError as e:
                    print(f'Error processing Schema: {schema_name}: {e}')
                except Exception as e:
                    print(f'Unexpected error processing Schema: {schema_name}: {e}')


        except Exception as e:
            print(f'Error executing query: {e}')

    def sequence():
        create_output_folder = f"{repo_path}\\create_scripts\\sequences"

        if not os.path.exists(create_output_folder):
            os.makedirs(create_output_folder)
        if not os.path.exists(drop_output_folder):
            os.makedirs(drop_output_folder)

        try:
            # Get the list of schemas within the database
            cursor.execute("SHOW SCHEMAS")
            schemas_result = cursor.fetchall()

            for schema_info in schemas_result:
                schema_name = schema_info[1]  # Assuming the schema name is in the second column
                print(f"Processing Schema: {schema_name}")

                try:
                    # Get the list of sequences in the current schema
                    cursor.execute(f"SHOW SEQUENCES IN SCHEMA {schema_name}")
                    sequences_result = cursor.fetchall()

                    for sequence_info in sequences_result:
                        sequence_name = sequence_info[0]  # Assuming the name is in the first column
                        print(f"Processing SEQUENCE: {schema_name}.{sequence_name}")

                        try:
                            ddl_query = f"SELECT GET_DDL('SEQUENCE', '{schema_name}.{sequence_name}')"
                            execute_and_save_query(ddl_query,
                                                   os.path.join(create_output_folder, f"sequence_{sequence_name}.sql"))
                        except sf.errors.ProgrammingError as e:
                            print(f'Error processing SEQUENCE: {schema_name}.{sequence_name}: {e}')
                        except Exception as e:
                            print(f'Unexpected error processing SEQUENCE: {schema_name}.{sequence_name}: {e}')

                        drop_sql_file_name = os.path.join(drop_output_folder,
                                                          f"rollback_sequence_{sequence_name}.sql")

                        # Write the SQL statement to the file
                        with open(drop_sql_file_name, 'w') as drop_sql_file:
                            drop_sql_file.write(
                                f"DROP SEQUENCE {schema_name}.{sequence_name};")

                except sf.errors.ProgrammingError as e:
                    print(f'Error processing Schema: {schema_name}: {e}')
                except Exception as e:
                    print(f'Unexpected error processing Schema: {schema_name}: {e}')

        except Exception as e:
            print(f'Error executing query: {e}')

    def stream():
        create_output_folder = f'{repo_path}\\create_scripts\\streams'

        if not os.path.exists(create_output_folder):
            os.makedirs(create_output_folder)
        if not os.path.exists(drop_output_folder):
            os.makedirs(drop_output_folder)

        try:
            # Get the list of schemas within the database
            cursor.execute("SHOW SCHEMAS")
            schemas_result = cursor.fetchall()

            for schema_info in schemas_result:
                schema_name = schema_info[1]  # Assuming the schema name is in the second column
                print(f"Processing Schema: {schema_name}")

                try:
                    # Get the list of streams in the current schema
                    cursor.execute(f"SHOW STREAMS IN SCHEMA {schema_name}")
                    streams_result = cursor.fetchall()

                    for stream_info in streams_result:
                        stream_name = stream_info[1]  # Assuming the name is in the first column
                        print(f"Processing STREAM: {schema_name}.{stream_name}")

                        try:
                            ddl_query = f"SELECT GET_DDL('STREAM', '{schema_name}.{stream_name}')"
                            execute_and_save_query(ddl_query,
                                                   os.path.join(create_output_folder, f"stream_{stream_name}.sql"))
                        except sf.errors.ProgrammingError as e:
                            print(f'Error processing STREAM: {schema_name}.{stream_name}: {e}')
                        except Exception as e:
                            print(f'Unexpected error processing STREAM: {schema_name}.{stream_name}: {e}')

                        drop_sql_file_name = os.path.join(drop_output_folder,
                                                          f"rollback_stream_{stream_name}.sql")

                        # Write the SQL statement to the file
                        with open(drop_sql_file_name, 'w') as drop_sql_file:
                            drop_sql_file.write(
                                f"DROP STREAM {schema_name}.{stream_name};")

                except sf.errors.ProgrammingError as e:
                    print(f'Error processing Schema: {schema_name}: {e}')
                except Exception as e:
                    print(f'Unexpected error processing Schema: {schema_name}: {e}')

        except Exception as e:
            print(f'Error executing query: {e}')

    def pipe():
        create_output_folder = f'{repo_path}\\create_scripts\\pipes'

        if not os.path.exists(create_output_folder):
            os.makedirs(create_output_folder)
        if not os.path.exists(drop_output_folder):
            os.makedirs(drop_output_folder)

        try:
            # Get the list of schemas within the database
            cursor.execute("SHOW SCHEMAS")
            schemas_result = cursor.fetchall()

            for schema_info in schemas_result:
                schema_name = schema_info[1]  # Assuming the schema name is in the second column
                print(f"Processing Schema: {schema_name}")

                try:
                    # Get the list of pipes in the current schema
                    cursor.execute(f"SHOW PIPES IN SCHEMA {schema_name}")
                    pipes_result = cursor.fetchall()

                    for pipe_info in pipes_result:
                        pipe_name = pipe_info[1]  # Assuming the name is in the first column
                        print(f"Processing PIPE: {schema_name}.{pipe_name}")

                        try:
                            ddl_query = f"SELECT GET_DDL('PIPE', '{schema_name}.{pipe_name}')"
                            execute_and_save_query(ddl_query,
                                                   os.path.join(create_output_folder, f"pipe_{pipe_name}.sql"))
                        except sf.errors.ProgrammingError as e:
                            print(f'Error processing PIPE: {schema_name}.{pipe_name}: {e}')
                        except Exception as e:
                            print(f'Unexpected error processing PIPE: {schema_name}.{pipe_name}: {e}')

                        drop_sql_file_name = os.path.join(drop_output_folder,
                                                          f"rollback_pipe_{pipe_name}.sql")

                        # Write the SQL statement to the file
                        with open(drop_sql_file_name, 'w') as drop_sql_file:
                            drop_sql_file.write(
                                f"DROP PIPE {schema_name}.{pipe_name};")

                except sf.errors.ProgrammingError as e:
                    print(f'Error processing Schema: {schema_name}: {e}')
                except Exception as e:
                    print(f'Unexpected error processing Schema: {schema_name}: {e}')

        except Exception as e:
            print(f'Error executing query: {e}')

    def task():
        create_output_folder = f'{repo_path}\\create_scripts\\tasks'

        if not os.path.exists(create_output_folder):
            os.makedirs(create_output_folder)
        if not os.path.exists(drop_output_folder):
            os.makedirs(drop_output_folder)

        try:
            # Get the list of schemas within the database
            cursor.execute("SHOW SCHEMAS")
            schemas_result = cursor.fetchall()

            for schema_info in schemas_result:
                schema_name = schema_info[1]  # Assuming the schema name is in the second column
                print(f"Processing Schema: {schema_name}")

                try:
                    # Get the list of tasks in the current schema
                    cursor.execute(f"SHOW TASKS IN SCHEMA {schema_name}")
                    tasks_result = cursor.fetchall()

                    for task_info in tasks_result:
                        task_name = task_info[1]  # Assuming the name is in the first column
                        print(f"Processing TASK: {schema_name}.{task_name}")

                        try:
                            ddl_query = f"SELECT GET_DDL('TASK', '{schema_name}.{task_name}')"
                            execute_and_save_query(ddl_query,
                                                   os.path.join(create_output_folder, f"Task_{task_name}.sql"))
                        except sf.errors.ProgrammingError as e:
                            print(f'Error processing TASK: {schema_name}.{task_name}: {e}')
                        except Exception as e:
                            print(f'Unexpected error processing TASK: {schema_name}.{task_name}: {e}')

                        drop_sql_file_name = os.path.join(drop_output_folder,
                                                          f"rollback_task_{task_name}.sql")

                        # Write the SQL statement to the file
                        with open(drop_sql_file_name, 'w') as drop_sql_file:
                            drop_sql_file.write(
                                f"DROP TASK {schema_name}.{task_name};")

                except sf.errors.ProgrammingError as e:
                    print(f'Error processing Schema: {schema_name}: {e}')
                except Exception as e:
                    print(f'Unexpected error processing Schema: {schema_name}: {e}')

        except Exception as e:
            print(f'Error executing query: {e}')

    # calling Functions
    warehouse()
    time.sleep(2)
    user()
    time.sleep(2)
    role()
    time.sleep(2)
    database()
    time.sleep(2)
    schema()
    time.sleep(2)
    file_format()
    time.sleep(2)
    stage()
    time.sleep(2)
    table()
    time.sleep(2)
    view()
    time.sleep(2)
    procedure()
    time.sleep(2)
    udf()
    time.sleep(2)
    sequence()
    time.sleep(2)
    stream()
    time.sleep(2)
    pipe()
    time.sleep(2)
    task()
    time.sleep(2)

    # Close the connection
    connection.close()

    observer.stop()
    observer.join()

    # send_email(email_subject, email_body)
    send_email(email_subject, email_body, sender_email, receiver_email, password)

    # Synchronize branches after a commit
    sync_branches(source_branch, target_branch, repo_path)

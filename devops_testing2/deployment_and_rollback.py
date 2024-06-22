import git
from git import Repo
import os
import subprocess
import datetime

def deploy(connection_profile,branch,git_local_path,url):
    # Base log file name
    base_log_file = f"deployment_log_{branch}"

    # Get the current date and time for the log file name
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M_%S")

    # provide repository URL and desired branch name
    git_repo_url = url
    git_repository_local_path = git_local_path
    git_branch = branch  # Specify the branch you want to clone

    print("git cloning....")

    # if os.path.exists(git_repository_local_path):
    #     print("Repository exists. Attempting to pull changes...")
    #     repo = git.Repo(git_repository_local_path)
    #     repo.git.checkout(branch)
    #     repo.git.pull()
    #     print("Pull successful!")
    # else:
    #     print("Repository does not exist. Cloning...")
    #     # Clone the repository
    #     repo = Repo.clone_from(git_repo_url, git_repository_local_path)
    #     print("Clone successful!")
    #     repo.git.checkout(branch)
    #     # Checkout the specific branch
    #     repo.git.checkout(git_branch)
    #     print(f"Checked out branch '{git_branch}'")
    if os.path.exists(git_repository_local_path) and os.path.isdir(os.path.join(git_repository_local_path, '.git')):
            print("Repository exists. Attempting to pull changes...")
            repo = git.Repo(git_repository_local_path)
            repo.git.checkout(branch)
            repo.git.pull()
            print("Pull successful!")
    else:
        print("Repository does not exist. Cloning...")
        # Clone the repository
        repo = git.Repo.clone_from(git_repo_url, git_repository_local_path)
        print("Clone successful!")
        repo.git.checkout(branch)
        print(f"Checked out branch '{git_branch}'")

    print("deployment start")

    # Directory containing .sql files
    sql_files_directory = f"{git_repository_local_path}\create_scripts"

    # Create log file name with timestamp
    log_file = f"{base_log_file}_{timestamp}.txt"

    # Clear log file if it exists
    if os.path.exists(log_file):
        os.remove(log_file)

    # Open the log file for writing
    def log(message, include_timestamp=True):
        with open(log_file, "a") as f:
            if include_timestamp:
                timestamp_message = f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}"
                f.write(timestamp_message + "\n")
            else:
                f.write(message + "\n")

    log(f"Deployment started at {timestamp}" + "\n")

    # List of folders to process in the desired order
    folders_to_process = [
        "warehouses",
        "users",
        "roles",
        "database",
        "schema",
        "file_formats",
        "stages",
        "tables",
        "views",
        "procedures",
        "udfs",
        "sequences",
        "streams",
        "pipes",
        "tasks"
        # Add more folders in the desired order
    ]

    error_detected = False  # Flag to track if an error was detected
    successful_sql_files = []

    for folder_name in folders_to_process:
        folder_path = os.path.join(sql_files_directory, folder_name)

        if not os.path.exists(folder_path):
            log(f"Folder '{folder_name}' does not exist. Skipping...")
            continue

        for root, dirs, files in os.walk(folder_path):
            for sql_file in files:
                if sql_file.endswith(".sql"):
                    deploying_message = f"Deploying_File_Name  \"{sql_file}\""
                    log(deploying_message)

                    cmd = f"snowsql -c {connection_profile} -f \"{os.path.join(root, sql_file)}\""
                    output = subprocess.getoutput(cmd)
                    output_lines = output.split("\n")

                    for line in output_lines:
                        if "error" in line.lower():
                            log(line.strip())  # Log the error message
                            error_detected = True

                        if line.startswith("|") and not line.startswith("|-"):
                            if not line.startswith("| status"):
                                log(line.strip("| "))

                        if line.startswith("Object '") or "cannot" in line.lower() or "invalid" in line.lower() or "not" in line.lower() or 'insufficient' in line.lower():
                            log(line)
                            error_detected = True

                    if error_detected:
                        log("Deployment failed.", include_timestamp=False)
                        log("", include_timestamp=False)  # Add a blank line without a timestamp
                        break
                    else:
                        log("Deployment completed.", include_timestamp=False)
                        successful_sql_files.append(sql_file)

                    log("", include_timestamp=False)  # Add a blank line without a timestamp

            if error_detected:
                break
        if error_detected:
            break

    if error_detected:
        log("Deployment not successful." + "\n" + "\n" + "Initiating rollback..." + "\n", include_timestamp=False)

        # Implement the rollback logic here, using the list of successful_sql_files
        # You should execute the corresponding rollback scripts
        rollback_directory = f"{git_repository_local_path}\drop_scripts"  # Change this to the directory where your rollback scripts are located

        for sql_file in successful_sql_files:
            rollback_script = f"rollback_{sql_file}"  # Assuming your rollback scripts are named like "rollback_script_name.sql"
            rollback_script_path = os.path.join(rollback_directory, rollback_script)

            if os.path.exists(rollback_script_path):
                rollback_cmd = f"snowsql -c {connection_profile} -f \"{rollback_script_path}\""
                rollback_output = subprocess.getoutput(rollback_cmd)

                rollback_output_lines = rollback_output.split("\n")

                log(f"Deploying_Rollback_Script for \"{sql_file}\":")

                for output_line in rollback_output_lines:
                    if "error" in output_line.lower():
                        log(output_line.strip())  # Log the error message

                    if output_line.startswith("|") and not output_line.startswith("|-"):
                        if not output_line.startswith("| status"):
                            log(output_line.strip("| "))

                    if output_line.startswith("Object '") or "cannot" in output_line.lower() or "invalid" in output_line.lower() or "not" in output_line.lower() or 'insufficient' in output_line.lower():
                        log(output_line)


                log("", include_timestamp=False)  # Add a blank line without a timestamp

            else:
                log(f"Rollback script not found for {sql_file}. Manual intervention required.")

        log("Rollback completed.")
    else:
        log("All Deployment successfully completed.")

    print("All deployments completed. Check", log_file, "for details.")

    return log_file

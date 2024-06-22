from snowToGit import capture
from deployment_and_rollback import deploy
from deployment_metadata_To_csv import get_metadata
from status_And_notifications import notify

# SnowSQL connection parameter for dev
sf_params_dev = {
    'user': 'sejal',
    'password': 'Kasmo@123',
    'account': 'jt98113.ap-south-1.aws',
    'warehouse': 'COMPUTE_WH',
    'database': 'DEVOPS_PROJECT_TEST',
    'schema': 'DEVOPS_PROJECT_SCHEMA'
}


# SnowSQL connection profile for test
connection_profile_test= "testcli"

# SnowSQL connection profile for uat
connection_profile_uat = "uatcli"



stats_test="deployment_stats_test"
stats_uat="deployment_stats_uat"

#git parameters
dev_branch="d"
test_branch="t"
uat_branch="u"
prod_branch="prod"
status=True

git_url = "https://github.com/sejala10/test_repo.git"

git_local_repo_path=r"C:\Users\ThinkPad\Downloads\repogit\test_repo"
git_local_path_test = r"C:\Users\ThinkPad\Downloads\repogit\test"
git_local_path_uat = r"C:\Users\ThinkPad\Downloads\gitrepo\uat"

#email parameters
sender_email = 'kasmosnowflake@gmail.com'
receiver_email = 'bhanuprakash.sigireddy1997@gmail.com'
password = 'clnxwbblzitvytge'
email_subject = 'SYNC - CODE IS SYNC FROM DEV BRANCH TO TEST BRANCH '
email_body = 'A new commit has been made to the Dev branch.'

if __name__ == "__main__":
    print("--- Capturing Objects, and Upload into GIT-Test Branch ---")
    # capture(dev_branch,test_branch,git_local_repo_path)
    capture(email_subject, email_body, dev_branch, test_branch, git_local_repo_path, sender_email, receiver_email, password, sf_params_dev)

    print("\n"+f"--- 1st Deployment Start from Git {test_branch} to Snowflake Test ENV. ---")
    log_file_path_test=deploy(connection_profile_test,test_branch,git_local_path_test,git_url)

    print("\n"+"*** DEPLOYMENT COMPLETED IN SNOWFLAKE TEST ENV. ***"+"\n")

    print("\n"+"--- Capturing log into Metadata Statistics for Test ---")
    csv_file_path_test=get_metadata(log_file_path_test,stats_test)

    print("\n"+"*** CAPTURING DONE ***")

    print("\n"+"--- Check Status and Notify ----")
    execution_status_test=notify(csv_file_path_test,test_branch,uat_branch,status,git_local_repo_path, sf_params_dev)



    print(execution_status_test)

    if execution_status_test:
        print("\n" + "***** 1ST DEPLOYMENT COMPLETED dev->test->uat git *****")

        print("\n"+"\n"+"--- 2nd Deployment Starting in Snowflake UAT ENV. ---")

        print("\n"+f"Deployment Start from GIT {uat_branch} to Snowflake UAT ENV")
        log_file_path_uat = deploy(connection_profile_uat, uat_branch, git_local_path_uat,git_url)

        print("\n" + "*** DEPLOYMENT COMPLETED IN SNOWFLAKE UAT ENV. ***" + "\n")

        print("\n"+"--- Capturing log into Metadata Statistics for UAT ---")
        csv_file_path_uat = get_metadata(log_file_path_uat,stats_uat)

        print("\n" + "*** CAPTURING DONE ***")

        print("\n" + "--- Check Status and Notify ----")
        execution_status_uat=notify(csv_file_path_uat, uat_branch, prod_branch,status,git_local_repo_path, sf_params_dev)
        if execution_status_uat:
            print("\n" + "***** 2nd DEPLOYMENT COMPLETED UAT to Prod git *****")
        else:
            print("\n"+"2nd DEPLOYMENT NOT COMPLETED")
    else:
        print("\n" + "***** 1ST DEPLOYMENT FAILED*****")

from request_status_mapping import request_status_mapping,request_type_mapping  # Import the mapping
from logging_utils import log_and_print, normalize_hebrew, logger
from logging_utils import BOLD_YELLOW, BOLD_GREEN, BOLD_RED
import logging
import pyodbc,os
from pymongo.database import Database
from pymongo import MongoClient

def parse_leading_status_by_case_ids(case_ids: list, db: Database) -> list:
    """
    Parse the Requests array for multiple Case IDs and calculate the leading status for each case.

    Args:
        case_ids (list): A list of Case IDs to fetch data for.
        db (Database): The MongoDB database connection.

    Returns:
        list: A list of dictionaries containing Case ID and corresponding leading status.
    """
    leading_statuses_list = []  # List to store the leading status for each case

    for case_id in case_ids:
        try:
            collection = db["Case"]
            document = collection.find_one({"_id": case_id}, {"Requests": 1, "_id": 0})

            if not document:
                log_and_print(f"No document found for Case ID {case_id}.", "info", BOLD_RED, is_hebrew=True)
                continue

            requests = document.get("Requests", [])
            if not isinstance(requests, list):
                log_and_print(f"Invalid 'Requests' field format for Case ID {case_id}.", "info", BOLD_RED, is_hebrew=True)
                continue

            log_and_print(f"******({len(requests)}) סהכ בקשות בתיק *****", "info", BOLD_GREEN, is_hebrew=True)

            case_leading_status = None  # To store the leading status of this case

            for index, request in enumerate(requests, start=1):
                request_id = request.get("RequestId")
                request_type_id = request.get("RequestTypeId")
                des_request_heb = normalize_hebrew(request_type_mapping.get(request_type_id, "Unknown Status"))
                leading_statuses = request.get("RequestLeadingStatuses", [])

                
                #log_and_print(f"{request_id} בקשה", "info", BOLD_GREEN, indent=4, is_hebrew=True)
                log_and_print(f"{des_request_heb}({request_type_id})", "info", BOLD_GREEN, is_hebrew=True, indent=4)

                if leading_statuses and isinstance(leading_statuses, list):
                    statuses_with_null_end_date = []  # Collect statuses where EndDate is None

                    # First pass: Collect statuses with EndDate as None
                    for status_index, status in enumerate(leading_statuses, start=1):
                        request_status_type_id = status.get("RequestStatusTypeId")
                        description_heb = normalize_hebrew(request_status_mapping.get(request_status_type_id, "Unknown Status"))
                        end_date = status.get("EndDate")

                        # Collect statuses where EndDate is None
                        if end_date is None:
                            statuses_with_null_end_date.append((status_index, description_heb))

                    # Determine the main status
                    if statuses_with_null_end_date:
                        # Assuming the first status with null EndDate is the main status
                        main_status_index, main_status = statuses_with_null_end_date[0]
                        log_and_print(f"*****סטטוס בבקשה : {main_status}*****", "info", BOLD_GREEN, is_hebrew=True, indent=4)
                        case_leading_status = main_status  # Set the main status for the case
                    else:
                        log_and_print("No Main Status Identified (EndDate is not null)", "info", BOLD_RED, is_hebrew=True, indent=4)
                        case_leading_status = "No Main Status"  # If no main status, set a placeholder
                else:
                    log_and_print("RequestLeadingStatuses: None or invalid format", "info", BOLD_RED, is_hebrew=True, indent=4)
                    case_leading_status = "Invalid Format"  # Placeholder for invalid format

            # Append the case's leading status to the final list
            if case_leading_status:
                leading_statuses_list.append({
                    "CaseId": case_id,
                    "LeadingStatus": case_leading_status
                })

        except Exception as e:
            log_and_print(f"Error processing case document for Case ID {case_id}: {e}", "error", BOLD_RED, is_hebrew=True)

    return leading_statuses_list

#################  סטטוס תיק בודד ###############################

def parse_leading_status_by_case_id(case_id, db: Database) -> list:
    """
    Parse the Requests array for multiple Case IDs and calculate the leading status for each case.

    Args:
        case_ids (list): A list of Case IDs to fetch data for.
        db (Database): The MongoDB database connection.

    Returns:
        list: A list of dictionaries containing Case ID and corresponding leading status.
    """
       
    try:
        collection = db["Case"]
        document = collection.find_one({"_id": case_id}, {"Requests": 1, "_id": 0})

        if not document:
            log_and_print(f"No document found for Case ID {case_id}.", "info", BOLD_RED, is_hebrew=True)
            

        requests = document.get("Requests", [])
        if not isinstance(requests, list):
            log_and_print(f"Invalid 'Requests' field format for Case ID {case_id}.", "info", BOLD_RED, is_hebrew=True)
            

        
        case_leading_status = None  # To store the leading status of this case

        for index, request in enumerate(requests, start=1):
            request_id = request.get("RequestId")
            request_type_id = request.get("RequestTypeId")
            des_request_heb = normalize_hebrew(request_type_mapping.get(request_type_id, "Unknown Status"))
            leading_statuses = request.get("RequestLeadingStatuses", [])

            
        #    log_and_print(f"{des_request_heb}({request_type_id})", "info", BOLD_GREEN, is_hebrew=True, indent=4)

            if leading_statuses and isinstance(leading_statuses, list):
                statuses_with_null_end_date = []  # Collect statuses where EndDate is None

                # First pass: Collect statuses with EndDate as None
                for status_index, status in enumerate(leading_statuses, start=1):
                    request_status_type_id = status.get("RequestStatusTypeId")
                    description_heb = normalize_hebrew(request_status_mapping.get(request_status_type_id, "Unknown Status"))
                    end_date = status.get("EndDate")

                    # Collect statuses where EndDate is None
                    if end_date is None:
                        statuses_with_null_end_date.append((status_index, description_heb))

                # Determine the main status
                if statuses_with_null_end_date:
                    # Assuming the first status with null EndDate is the main status
                    main_status_index, main_status = statuses_with_null_end_date[0]
                    log_and_print(f"*****סטטוס בבקשה : {main_status}*****", "info", BOLD_GREEN, is_hebrew=True, indent=4)
                    case_leading_status = main_status  # Set the main status for the case
                else:
                    log_and_print("No Main Status Identified (EndDate is not null)", "info", BOLD_RED, is_hebrew=True, indent=4)
                    case_leading_status = "No Main Status"  # If no main status, set a placeholder
            else:
                log_and_print("RequestLeadingStatuses: None or invalid format", "info", BOLD_RED, is_hebrew=True, indent=4)
                case_leading_status = "Invalid Format"  # Placeholder for invalid format

        # Append the case's leading status to the final list
        if case_leading_status:
            leading_statuses ={"CaseId": case_id, "LeadingStatus": case_leading_status}

    except Exception as e:
        log_and_print(f"Error processing case document for Case ID {case_id}: {e}", "error", BOLD_RED, is_hebrew=True)

    return leading_statuses


#######################   תיקים עם בקשת ביניים ####################
def collect_cases_with_mid_request(case_ids: list, db: Database) -> list:
    """
    Parse the Requests array for multiple Case IDs and calculate the leading status for each case.

    Args:
        case_ids (list): A list of Case IDs to fetch data for.
        db (Database): The MongoDB database connection.

    Returns:
        list: A list of dictionaries containing Case ID and corresponding leading status.
    """
    cases_with_mid_requests = []  # List to store the leading status for each case

    for case_id in case_ids:
        try:
            collection = db["Case"]
            document = collection.find_one({"_id": case_id}, {"Requests": 1, "_id": 0})

            if not document:
                log_and_print(f"No document found for Case ID {case_id}.", "info", BOLD_RED, is_hebrew=True)
                continue

            requests = document.get("Requests", [])
            if not isinstance(requests, list):
                log_and_print(f"Invalid 'Requests' field format for Case ID {case_id}.", "info", BOLD_RED, is_hebrew=True)
                continue

            if len(requests) > 1:                
                cases_with_mid_requests.append(case_id)
                #log_and_print(f"תיק {case_id} עם בקשה ביניים חדשה",is_hebrew=True)
            #else:
            #    log_and_print(f"תיק {case_id}   ללא בקשה ביניים חדשה",is_hebrew=True)

        except Exception as e:
            log_and_print(f"Error processing case document for Case ID {case_id}: {e}", "error", BOLD_RED, is_hebrew=True)

    return cases_with_mid_requests



#######################  סטטוס מנורה בתיק ##########################
def fetch_request_status_from_menora(case_id, cursor):
    """Retrieve request status for each appeal ID using an existing cursor and case_id."""
    try:
        # SQL query to fetch the status based on case_id
        sql_query = """
        SELECT r.Description_Heb
            FROM Menora_Conversion.dbo.Appeal a 
            JOIN External_Courts.cnvrt.Case_Status_To_Case_Status_BO cn ON a.Appeal_Status = cn.Case_Status_BO
            JOIN cases_bo.dbo.CT_Case_Status_Types c ON c.Case_Status_Type_Id = cn.Case_Status_Type_Id
            JOIN cases_bo.dbo.CT_Request_Status_Types r ON r.Request_Status_Type_Id = c.Request_Status_Type_Id
            WHERE cn.Court_Id = 11 AND a.Case_Id = 2004890
        """
        #cursor.execute(sql_query, (case_id,))
        cursor.execute(sql_query)
        # Fetch results from the executed query
        menora_status_per_case = cursor.fetchall()

        if menora_status_per_case:
            status_heb = menora_status_per_case[0][0]  # Assuming you're interested in the first description
            log_and_print(f"Menora status fetched: {status_heb}", "info", BOLD_GREEN, is_hebrew=True)
            return status_heb
        else:
            log_and_print(f"No Menora status found for case ID {case_id}", "warning", BOLD_YELLOW, is_hebrew=True)
            return None
    except Exception as e:
        log_and_print(f"Error querying request status for Case ID {case_id}: {e}", "error", BOLD_RED, is_hebrew=True)
        return None

#########################################################################################################
import pandas as pd


def create_output_with_all_status_cases(leading_statuses_list: list, output_file: str) -> None:
    """
    Create an output Excel file with Case IDs and their corresponding leading statuses.
    The Excel file will contain columns: 'מצב אחודה' and 'מספר תיק'.

    Args:
        leading_statuses_list (list): List of dictionaries containing 'CaseId' and 'LeadingStatus'.
        output_file (str): The file path to save the output Excel file.

    Returns:
        None
    """
    # List to store the data for the DataFrame
    case_data_to_pd = []

    for case in leading_statuses_list:
        case_id = case.get("CaseId")
        leading_status = case.get("LeadingStatus")

        # Append the case data to the list
        case_data_to_pd.append({
            "מצב אחודה": leading_status,  # Leading status mapped to "מצב אחודה"
            "מספר תיק": case_id  # Case ID as "מספר תיק"
        })

    # Create a DataFrame from the case data
    df = pd.DataFrame(case_data_to_pd)

    # Save the DataFrame to an Excel file
    df.to_excel(output_file, index=False)

    print(f"Data has been successfully written to {output_file}.")




def connect_to_mongodb():
    """Establish a connection to MongoDB."""
    try:
        mongo_connection = os.getenv("MONGO_CONNECTION_STRING", "")
        if not mongo_connection:
            raise ValueError("MongoDB connection string is not provided.")
        
        log_and_print("Connecting to MongoDB...")
        mongo_client = MongoClient(mongo_connection, minPoolSize=5, serverSelectionTimeoutMS=5000)
        db = mongo_client["CaseManagement"]
        mongo_client.server_info()  # This will throw an exception if the server is not reachable
        log_and_print("Connected to MongoDB successfully.")
        return mongo_client, db
    except Exception as e:
        log_and_print(f"Error connecting to MongoDB: {e}", "error")
        return None, None


def connect_to_sql_server():
    """Establish a connection to SQL Server."""
    try:
        server = os.getenv("DB_SERVER")
        database = os.getenv("DB_NAME")
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASS")

        # Ensure all required environment variables are present
        if not all([server, database, user, password]):
            raise ValueError("One or more required database connection details are missing.")

        connection = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
            f"Trusted_Connection=yes;"
        )
        log_and_print("Connection to SQL Server established successfully.")
        return connection
    except Exception as e:
        log_and_print(f"Error connecting to SQL Server: {e}", "error")
        return None


########################## סטטוס תיק במנורה ##############################

def fetch_request_status_from_menora(case_id,server_name, database_name, user_name, password):
    request_status_id = 16
    """Retrieve request status for each appeal ID and print them."""
    sql_query_2 = """
      SELECT r.Description_Heb
        FROM Menora_Conversion.dbo.Appeal a 
        JOIN External_Courts.cnvrt.Case_Status_To_Case_Status_BO cn ON a.Appeal_Status = cn.Case_Status_BO
        JOIN cases_bo.dbo.CT_Case_Status_Types c ON c.Case_Status_Type_Id = cn.Case_Status_Type_Id
        JOIN cases_bo.dbo.CT_Request_Status_Types r ON r.Request_Status_Type_Id = c.Request_Status_Type_Id
        WHERE cn.Court_Id = 11 AND a.Case_Id = ?
    """
    try:
        
        # Establish the SQL Server connection
        connection = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server_name};"
            f"DATABASE={database_name};"
            f"UID={user_name};"
            f"PWD={password};"
            f"Trusted_Connection=yes;"
        )
        cursor = connection.cursor()
        #log_and_print("Connection to SQL Server established successfully.\n", "info")

        cursor.execute(sql_query_2, case_id)
        menora_status_per_case = cursor.fetchall()
        if menora_status_per_case:
            status_heb = menora_status_per_case[0][0]  # Assuming you're interested in the first description
            return status_heb
        else:
            log_and_print(f"No Menora status found for case ID {case_id}", "warning", BOLD_YELLOW, is_hebrew=True)
            return None
    except Exception as e:
        log_and_print(f"Error querying request status for Case ID {case_id}: {e}", "error", BOLD_RED, is_hebrew=True)
        return None

    finally:
        # Close the cursor and connection to avoid open connections
        if cursor:
            cursor.close()
        if connection:
            connection.close()
############################### תכתובות במנורה  ######################


def fetch_notes_status_from_menora(case_id, server_name, database_name, user_name, password):
    """Retrieve request status for each appeal ID and print them."""
    sql_query = """
        select a.Appeal_Number_Display
        from Menora_Conversion.dbo.Appeal a
        join External_Courts.cnvrt.Case_Status_To_Case_Status_BO cn on a.Appeal_Status= cn.Case_Status_BO
        join cases_bo.dbo.CT_Case_Status_Types c on c.Case_Status_Type_Id = cn.Case_Status_Type_Id
        join cases_bo.dbo.CT_Request_Status_Types r on r.Request_Status_Type_Id = c.Request_Status_Type_Id
        where cn.Court_Id = 11 and a.Case_id= ?   
    """

    sql_query_2 = """
        SELECT n.NoteId, n.NoteBody 'תוכן', n.CreateUserID, n.CreateUser, n.Create_Date, n.NoteStatus, ns.Name 'סטטוס',
        n.NoteSubject, n.ReminderDate 'תאריך תזכורת'
        FROM Menora_Conversion.dbo.AppealNotes n
        join Menora_Conversion.dbo.CT_NoteStatus ns on n.NoteStatus=ns.Code
        join Menora_Conversion.dbo.Appeal a on n.Appeal_Id=a.Appeal_ID
        where a.Appeal_Number_Display= ?
    """

    try:
        # Establish the SQL Server connection
        connection = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server_name};"
            f"DATABASE={database_name};"
            f"UID={user_name};"
            f"PWD={password};"
            f"Trusted_Connection=yes;"
        )
        cursor = connection.cursor()
        
        cursor.execute(sql_query, case_id)
        appeal_id_for_case = cursor.fetchall()

        if appeal_id_for_case:
            # Extract the Appeal Number from the first query's result
            appeal_number = appeal_id_for_case[0][0]

            cursor.execute(sql_query_2, appeal_number)
            notes = cursor.fetchall()

            return notes
        else:
            log_and_print(f"No Menora status found for case ID {case_id}", "warning", BOLD_YELLOW, is_hebrew=True)
            return None
    except Exception as e:
        log_and_print(f"Error querying request status for Case ID {case_id}: {e}", "error", BOLD_RED, is_hebrew=True)
        return None

    finally:
        # Close the cursor and connection to avoid open connections
        if cursor:
            cursor.close()
        if connection:
            connection.close()

############### דיונים במנורה #######################
import pyodbc

def fetch_discussion_status_from_menora(case_id, server_name, database_name, user_name, password):
    """Retrieve request status for each appeal ID and print them."""
    sql_query = """
        select a.Appeal_Number_Display
        from Menora_Conversion.dbo.Appeal a
        join External_Courts.cnvrt.Case_Status_To_Case_Status_BO cn on a.Appeal_Status= cn.Case_Status_BO
        join cases_bo.dbo.CT_Case_Status_Types c on c.Case_Status_Type_Id = cn.Case_Status_Type_Id
        join cases_bo.dbo.CT_Request_Status_Types r on r.Request_Status_Type_Id = c.Request_Status_Type_Id
        where cn.Court_Id = 11 and a.Case_id= ?   
    """

    sql_query_2 = """
        select d.Discussion_Id,d.Discussion_Date, d.Discussion_Strat_Time,d.Discussion_End_Time,
        d.Status, ds.Name 'סטטוס', d.virtualDiscussion, d.discussionLink, a.Appeal_Number_Display,
        d.CancelationReason, d.Moj_ID
        from Menora_Conversion.dbo.Discussion d
        join Menora_Conversion.dbo.Link_Request_Discussion lr on lr.Discussion_Id=d.Discussion_Id
        join Menora_Conversion.dbo.CT_Discussion_Status ds on ds.Code=d.Status
        left join Menora_Conversion.dbo.CT_DiscussionCancelationReason cr on cr.Code=d.CancelationReason
        join Menora_Conversion.dbo.Appeal a on lr.appeal_id=a.Appeal_ID
        where a.Appeal_Number_Display= ?
    """

    try:
        # Establish the SQL Server connection
        connection = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server_name};"
            f"DATABASE={database_name};"
            f"UID={user_name};"
            f"PWD={password};"
            f"Trusted_Connection=yes;"
        )
        cursor = connection.cursor()
        
        cursor.execute(sql_query, case_id)
        appeal_id_for_case = cursor.fetchall()

        if appeal_id_for_case:
            # Extract the Appeal Number from the first query's result
            appeal_number = appeal_id_for_case[0][0]

            cursor.execute(sql_query_2, appeal_number)
            discussions = cursor.fetchall()

            if discussions:
                log_and_print(f"מספר דיונים: {len(discussions)}", is_hebrew=True)
                for discussion in discussions:
                    # Extracting relevant details for logging
                    discussion_date = discussion[1]
                    status = discussion[5]
                    log_and_print(f"דיון בתאריך: {discussion_date}, סטטוס: {status}", is_hebrew=True)
            else:
                log_and_print("אין מידע מבוקש", "info", is_hebrew=True)

            return discussions
        else:
            log_and_print(f"No Menora status found for case ID {case_id}", "warning", BOLD_YELLOW, is_hebrew=True)
            return None
    except Exception as e:
        log_and_print(f"Error querying request status for Case ID {case_id}: {e}", "error", BOLD_RED, is_hebrew=True)
        return None
    finally:
        # Close the cursor and connection to avoid open connections
        if cursor:
            cursor.close()
        if connection:
            connection.close()

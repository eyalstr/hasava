
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
            log_and_print(f"case_id={case_id}")
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

def create_common_output_with_all_status_cases(
    leading_statuses_list,
    case_to_processes,
    bpm_status_map,
    continued_process_map,
    conversion_status_list,
    output_file="output.xlsx"
):
    """
    Creates an Excel file with case ID, leading status, BPM status,
    continued process flag, and conversion flag.

    Args:
        leading_statuses_list (list): List of {"CaseId": ..., "LeadingStatus": ...}
        case_to_processes (dict): {case_id: [process_ids]}
        bpm_status_map (dict): {case_id: "alive"/"dead"/"no_process"/"error"}
        continued_process_map (dict): {case_id: True/False/None}
        conversion_status_list (list): List of {"CaseId": ..., "IsConversion": True/False}
        output_file (str): Output Excel file path
    """

    # Create lookup dictionary for conversion flag
    conversion_map = {
        entry["CaseId"]: entry.get("IsConversion", False)
        for entry in conversion_status_list
    }

    combined_data = []

    for entry in leading_statuses_list:
        case_id = entry.get("CaseId")
        leading_status = entry.get("LeadingStatus")
        process_ids = case_to_processes.get(case_id, [])
        bpm_status = bpm_status_map.get(case_id, "unknown")
        has_cp = continued_process_map.get(case_id, None)
        is_conversion = conversion_map.get(case_id, False)

        combined_data.append({
            "מספר תיק": case_id,
            "מצב אחודה": leading_status,
            "סטטוס BPM": bpm_status,
            "ProcessId": process_ids,
            "המשך טיפול": (
                "✔️ קיים" if has_cp is True else
                "❌ אין" if has_cp is False else
                "✔️ לא לבדיקה"
            ),
            "תיק הסבה": "✔️" if is_conversion else ""
        })

    df = pd.DataFrame(combined_data)
    df.to_excel(output_file, index=False)
    print(f"📁 Excel file created: {output_file}")



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



############### הפצות במנורה #######################

import pyodbc

def fetch_distributions_from_menora(case_id, server_name, database_name, user_name, password):
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
        select d.SendDate,d.SendUser,d.SendFrom,d.SendTo,d.SendSubject,d.SendBody,d.AttachmentsDocMojID,d.Discussion_Id,
        d.SendErrorCode,d.SendErrorDesc,d.Distribution_Status,d.Distribution_Status_Desc,
        d.Distribution_type, dt.Name 'סוג הפצה'
        from Menora.dbo.Log_DistributionService d
        join Menora.dbo.CT_Distribution_Type dt on d.Distribution_type=dt.Code
        join Menora.dbo.Appeal a on d.appeal_id=a.Appeal_ID
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
            distributions = cursor.fetchall()

            if distributions:
                log_and_print(f"מספר הפצות: {len(distributions)}", is_hebrew=True)
                for distribution in distributions:
                    # Extracting relevant details for logging
                    date = distribution[0]  # SendDate is the first item in the tuple
                    subject = distribution[4]  # SendSubject is the fifth item in the tuple
                    log_and_print(f"בתאריך: {date} נושא: {subject}", is_hebrew=True)
            else:
                log_and_print("אין מידע מבוקש", "info", is_hebrew=True)

            return distributions
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

############### החלטות במנורה #######################

import pyodbc

def fetch_decisions_from_menora(case_id, server_name, database_name, user_name, password):
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
       select distinct d.Decision_Date,
        d.Decision_Id, a.Appeal_Number_Display, d.Status,ds.Name,d.Decision_Type, dt.Name,
        d.Is_For_Advertisement
        from Menora_Conversion.dbo.Decision d
        join Menora_Conversion.dbo.Link_Request_Decision ld on ld.Decision_Id=d.Decision_Id
        join Menora_Conversion.dbo.CT_Decision_Status ds on ds.Code=d.Status
        join Menora_Conversion.dbo.CT_Decision_Type dt on dt.Code=d.Decision_Type
        join Menora_Conversion.dbo.Appeal a on ld.appeal_id=a.Appeal_ID
        where  a.Appeal_Number_Display= ?
        order by 1 desc
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
            decisions = cursor.fetchall()

            if decisions:
                log_and_print(f"מספר החלטות: {len(decisions)}", is_hebrew=True)
                for decision in decisions:
                    # Extracting relevant details for logging
                    decision_date = decision[0]  # Decision_Date is the first item in the tuple
                    decision_id = decision[1]  # Decision_Id is the second item
                    appeal_number_display = decision[2]  # Appeal_Number_Display is the third item
                    status = decision[3]  # Status is the fourth item
                    status_name = decision[4]  # Status name (from the join with CT_Decision_Status)
                    decision_type = decision[5]  # Decision_Type
                    decision_type_name = decision[6]  # Decision_Type name (from the join with CT_Decision_Type)
                    is_for_advertisement = decision[7]  # Is_For_Advertisement

                    log_and_print(f"החלטה {decision_id} ({decision_date}) - {status_name} - {decision_type_name} - פרסום: {is_for_advertisement}", is_hebrew=True)
            else:
                log_and_print("אין מידע מבוקש", "info", is_hebrew=True)

            return decisions
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



def parse_conv_status_by_case_ids(case_ids: list, db: Database) -> list:
    """
    Check if each case in the list is marked as a conversion case.

    Args:
        case_ids (list): List of case IDs.
        db (Database): MongoDB connection.

    Returns:
        list: List of dictionaries like {"CaseId": ..., "IsConversion": True/False}.
    """
    conv_statuses_list = []

    for case_id in case_ids:
        try:
            collection = db["Case"]
            log_and_print(f"case_id={case_id}")
            document = collection.find_one({"_id": case_id})

            if not document:
                log_and_print(f"No document found for Case ID {case_id}.", "info", BOLD_RED, is_hebrew=True)
                continue

            is_conversion = bool(document.get("IsConversion", False))
            log_and_print(f"case: {case_id}, isconversion: {is_conversion}")

            conv_statuses_list.append({
                "CaseId": case_id,
                "IsConversion": is_conversion
            })

        except Exception as e:
            log_and_print(f"Error processing case document for Case ID {case_id}: {e}", "error", BOLD_RED, is_hebrew=True)

    return conv_statuses_list

# def check_continued_process_status(case_ids, db):
#     """
#     Checks if any ContinuedProcessId exists (not None) for each case.

#     Args:
#         case_ids (list): List of case IDs to process.
#         db: MongoDB database connection.

#     Returns:
#         dict: {case_id: True/False/None}
#     """
#     collection = db["Case"]
#     results = {}

#     for case_id in case_ids:
#         try:
#             document = collection.find_one(
#                 {"_id": case_id},
#                 {"Decisions": 1, "_id": 0}
#             )

#             if not document:
#                 print(f"❌ Case ID {case_id}: Document not found.")
#                 results[case_id] = None
#                 continue

#             has_cp = False
#             decisions = document.get("Decisions", [])

#             for decision in decisions:
#                 for req in decision.get("DecisionRequests", []):
#                     for sub in req.get("SubDecisions", []):
#                         if sub.get("ContinuedProcessId") is not None:
#                             has_cp = True
#                             break
#                     if has_cp:
#                         break
#                 if has_cp:
#                     break

#             results[case_id] = has_cp

#         except Exception as e:
#             print(f"❌ Error processing Case ID {case_id}: {e}")
#             results[case_id] = None

#     return results



def check_specific_continued_process_status(caseid_table, decisionid_table, subdecision_table, db):
    """
    Checks if ContinuedProcessId is set (not None) for specific (case_id, decision_id, subdecision_id) triples.

    Args:
        caseid_table (list): List of Case IDs.
        decisionid_table (list): List of Decision IDs.
        subdecision_table (list): List of SubDecision IDs.
        db: MongoDB database connection.

    Returns:
        dict: {case_id: True/False/None}
    """
    collection = db["Case"]
    results = {}

    for case_id, decision_id, subdecision_id in zip(caseid_table, decisionid_table, subdecision_table):
        try:
            doc = collection.find_one(
                {"_id": case_id},
                {"Decisions": 1}
            )

            if not doc:
                results[case_id] = None
                continue

            found = False
            for decision in doc.get("Decisions", []):
                if decision.get("DecisionId") != decision_id:
                    continue
                for req in decision.get("DecisionRequests", []):
                    for sub in req.get("SubDecisions", []):
                        if sub.get("SubDecisionId") == subdecision_id:
                            results[case_id] = sub.get("ContinuedProcessId") is not None
                            found = True
                            break
                    if found:
                        break
                if found:
                    break

            if not found and case_id not in results:
                results[case_id] = None

        except Exception:
            results[case_id] = None

    return results

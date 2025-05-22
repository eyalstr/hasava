
import ctypes
from config import load_configuration
from pymongo import MongoClient
from dotenv import load_dotenv
#from process_data_manager import execute_sql_process_queries, fetch_process_ids_by_case_id_sorted,execute_sql_process_tasks,execute_sql_all_processes
#from document_data_manager import fetch_documents_by_case_id
#from decision_data_manager import fetch_decisions_and_documents_by_case_id
#from request_data_manager import parse_requests_by_case_id
from logging_utils import log_and_print, normalize_hebrew, BOLD_YELLOW, BOLD_GREEN, BOLD_RED
from colorama import init, Fore, Style
#from task_module_manager import fetch_decisions_by_case_id,check_assignments_for_decisions,fetch_tasks_by_case
from bpm_utils import (fetch_process_ids_and_request_type_by_case_id_sorted,
                       bpm_collect_all_processes_steps_and_status,
                       print_process_info,
                       filter_process_info_by_waiting_for_task_status,
                       check_process_assignment_is_valid,
                       filter_population_process_status,
                       filter_internal_judge_task_process_status,
                       filter_internal_secretery_task_process_status,fetch_all_process_ids_by_case_ids,check_first_process_alive) 
from menora_utils import (fetch_request_status_from_menora,
                          connect_to_sql_server,connect_to_mongodb,
                          parse_leading_status_by_case_ids,create_common_output_with_all_status_cases,
                          parse_leading_status_by_case_id,collect_cases_with_mid_request,
                          fetch_discussion_status_from_menora,fetch_notes_status_from_menora,fetch_distributions_from_menora,
                          fetch_decisions_from_menora,parse_conv_status_by_case_ids,check_specific_continued_process_status)

from config import cases_list,caseid_table,decisionid_table,subdecision_table,cases_ids
from url_tests import test_url_based_case_id

import os

# Initialize colorama
init(autoreset=True)

# ANSI escape codes replaced with colorama equivalents
BOLD_YELLOW = Fore.YELLOW + Style.BRIGHT
BOLD_GREEN = Fore.GREEN + Style.BRIGHT
BOLD_RED = Fore.RED + Style.BRIGHT
RESET = Style.RESET_ALL


def set_temporary_console_font():
    """
    Temporarily set the console font to 'Courier New' for the current session.
    This change does not persist after the console is closed.
    """
    LF_FACESIZE = 32  # Maximum font face name length
    STD_OUTPUT_HANDLE = -11  # Handle to the standard output

    # Define COORD structure for font size
    class COORD(ctypes.Structure):
        _fields_ = [("X", ctypes.c_short), ("Y", ctypes.c_short)]

    # Define CONSOLE_FONT_INFOEX structure
    class CONSOLE_FONT_INFOEX(ctypes.Structure):
        _fields_ = [
            ("cbSize", ctypes.c_ulong),
            ("nFont", ctypes.c_ulong),
            ("dwFontSize", COORD),
            ("FontFamily", ctypes.c_uint),
            ("FontWeight", ctypes.c_uint),
            ("FaceName", ctypes.c_wchar * LF_FACESIZE),
        ]

    # Set up the font structure
    font = CONSOLE_FONT_INFOEX()
    font.cbSize = ctypes.sizeof(CONSOLE_FONT_INFOEX)
    font.nFont = 0  # Font index (not typically used)
    font.dwFontSize.X = 0  # Width (0 lets Windows decide based on height)
    font.dwFontSize.Y = 18  # Character height in pixels
    font.FontFamily = 54  # FF_MODERN | FIXED_PITCH
    font.FontWeight = 400  # FW_NORMAL (400 for normal, 700 for bold)
    font.FaceName = "Courier New"  # Font name

    # Apply the font settings to the current console
    handle = ctypes.windll.kernel32.GetStdHandle(STD_OUTPUT_HANDLE)
    result = ctypes.windll.kernel32.SetCurrentConsoleFontEx(
        handle, ctypes.c_long(False), ctypes.pointer(font)
    )

    if result == 0:  # Check if the function failed
        raise ctypes.WinError()  # Raise an exception if it failed

    print("Temporary font change applied: Courier New (18px height)")


def connect_to_mongodb(mongo_connection, db_name="CaseManagement"):
    """
    Establish a connection to MongoDB and return the client and database object.
    """
    try:
        log_and_print("Connecting to MongoDB...", ansi_format=BOLD_YELLOW)
        mongo_client = MongoClient(mongo_connection,minPoolSize=5)
        db = mongo_client[db_name]
        log_and_print("Connected to MongoDB successfully.\n", ansi_format=BOLD_GREEN)
        return mongo_client, db
    except Exception as e:
        log_and_print(f"Error connecting to MongoDB: {e}", "error", ansi_format=BOLD_RED)
        return None, None

#from colorama import init, Fore, Style
import os

# Initialize colorama
#init(autoreset=True, strip=True)

# ANSI escape codes replaced with colorama equivalents
#BOLD_YELLOW = Fore.YELLOW + Style.BRIGHT
#BOLD_GREEN = Fore.GREEN + Style.BRIGHT
#BOLD_RED = Fore.RED + Style.BRIGHT
#RESET = Style.RESET_ALL


def set_temporary_console_font():
    """
    Temporarily set the console font to 'Courier New' for the current session.
    This change does not persist after the console is closed.
    """
    LF_FACESIZE = 32  # Maximum font face name length
    STD_OUTPUT_HANDLE = -11  # Handle to the standard output

    # Define COORD structure for font size
    class COORD(ctypes.Structure):
        _fields_ = [("X", ctypes.c_short), ("Y", ctypes.c_short)]

    # Define CONSOLE_FONT_INFOEX structure
    class CONSOLE_FONT_INFOEX(ctypes.Structure):
        _fields_ = [
            ("cbSize", ctypes.c_ulong),
            ("nFont", ctypes.c_ulong),
            ("dwFontSize", COORD),
            ("FontFamily", ctypes.c_uint),
            ("FontWeight", ctypes.c_uint),
            ("FaceName", ctypes.c_wchar * LF_FACESIZE),
        ]

    # Set up the font structure
    font = CONSOLE_FONT_INFOEX()
    font.cbSize = ctypes.sizeof(CONSOLE_FONT_INFOEX)
    font.nFont = 0  # Font index (not typically used)
    font.dwFontSize.X = 0  # Width (0 lets Windows decide based on height)
    font.dwFontSize.Y = 18  # Character height in pixels
    font.FontFamily = 54  # FF_MODERN | FIXED_PITCH
    font.FontWeight = 400  # FW_NORMAL (400 for normal, 700 for bold)
    font.FaceName = "Courier New"  # Font name

    # Apply the font settings to the current console
    handle = ctypes.windll.kernel32.GetStdHandle(STD_OUTPUT_HANDLE)
    result = ctypes.windll.kernel32.SetCurrentConsoleFontEx(
        handle, ctypes.c_long(False), ctypes.pointer(font)
    )

    if result == 0:  # Check if the function failed
        raise ctypes.WinError()  # Raise an exception if it failed

    print("Temporary font change applied: Courier New (18px height)")

def display_menu():
    """
    Display a menu of options for the user and return their choice.
    """
    print(f"\nMenu:")
   # print(f"1. {normalize_hebrew('מציג סטטוס של תיק באחודה')}")    
   # print(f"2. {normalize_hebrew('מציג סטטוס של כל התיקים באחודה')}")   
    print(f"3. {normalize_hebrew('בדיקות מקיפות להסבת נתונים לאחודה')}")   
   # print(f"4. {normalize_hebrew('מציג סטטוס של תיק במנורה')}")
   # print(f"5. {normalize_hebrew('הצגת תכתובות במנורה')}")
   # print(f"6. {normalize_hebrew('הצגת דיונים במנורה')}")
   # print(f"7. {normalize_hebrew('הצגת הפצות במנורה')}")    
   # print(f"8. {normalize_hebrew('הצגת החלטות במנורה')}")
    print(f"9. {normalize_hebrew('יציאה')}")

    try:
        choice = int(input(f"Enter your choice: "))
        return choice
    except ValueError:
        log_and_print("Invalid input. Please enter a number.", "error")
        return None
    
def get_case_id_by_displayed_id(db):
    """
    Prompt the user for a Case Displayed ID and fetch the corresponding Case ID from the database.

    Args:
        db: The MongoDB database connection object.

    Returns:
        str: The Case ID corresponding to the provided Case Displayed ID.

    Raises:
        SystemExit: If an invalid or non-existent Case Displayed ID is provided.
    """
    while True:
        try:
            # Prompt the user for Case Displayed ID or Site Action ID
            user_input = input("Please enter the Case Displayed ID (e.g., 1018/25) or Site Action ID (e.g., 67371): ").strip()

            if not user_input:
                log_and_print("Input cannot be empty. Please try again.", "error")
                continue

            # Initialize the case_id variable
            case_id = None

            # Determine the input type (CaseDisplayId or SiteActionId)
            if "-" in user_input:
                # Handle CaseDisplayId
                #log_and_print(f"Identified input as Case Displayed ID: {user_input}", "info")
                case_id = get_case_id_from_displayed(user_input, db)
            else:
                # Handle SiteActionId
                try:
                    site_action_id = int(user_input)
                    log_and_print(f"Identified input as Site Action ID: {site_action_id}", "info")
                    case_id = get_case_id_from_site_action_id(site_action_id, db)
                except ValueError:
                    log_and_print("Invalid Site Action ID. It must be a numeric value. Please try again.", "error")
                    continue

            # Check if a valid Case ID was retrieved
            if case_id is None:
                log_and_print(f"Could not find Case ID for the provided input: {user_input}", "error")
                continue

            # Log the retrieved Case ID and exit the loop
            log_and_print(f"\n######--({case_id})({user_input}) מספר תיק --######", "info", BOLD_YELLOW, is_hebrew=True)
            return case_id

        except Exception as e:
            log_and_print(f"Unexpected error: {str(e)}", "error")
            exit()



def get_case_id_from_site_action_id(site_action_id, db, collection_name="Case"):
    """
    Query MongoDB to get the _id of a case based on SiteActionId found in any Requests element.

    Args:
        site_action_id (int): The SiteActionId to search for.
        db: The MongoDB database connection object.
        collection_name (str): The name of the collection to query (default: "Case").

    Returns:
        ObjectId or None: The _id of the case if found, otherwise None.
    """
    try:
        if db is None:
            log_and_print("Database connection is not initialized.", "error")
            return None

        if not site_action_id:
            log_and_print("Invalid SiteActionId provided (empty or None)", "error")
            return None

        log_and_print(f"Searching for SiteActionId: {site_action_id} in Requests array within collection: {collection_name}", "info")

        # Step 1: Retrieve the entire document to inspect the Requests array
        collection = db[collection_name]
        document = collection.find_one({"Requests.SiteActionId": site_action_id}, {"Requests": 1, "_id": 1})

        if not document:
            log_and_print(f"No document found for SiteActionId {site_action_id}.", "info", BOLD_RED, is_hebrew=True)
            return None

        # Step 2: Extract the Requests array
        requests = document.get("Requests", [])
        if not requests:
            log_and_print(f"Requests array is empty for the document with SiteActionId {site_action_id}.", "info", BOLD_RED, is_hebrew=True)
            return None

        # Step 3: Search within the Requests array for the matching SiteActionId
        for request in requests:
            if request.get("SiteActionId") == site_action_id:
                log_and_print(f"Found matching SiteActionId in document with _id: {document['_id']}", "info", is_hebrew=True)
                return document["_id"]

        log_and_print(f"No matching SiteActionId found in Requests for document with _id: {document['_id']}.", "info", BOLD_RED, is_hebrew=True)
        return None

    except Exception as e:
        log_and_print(f"Error while querying MongoDB in collection {collection_name}: {e}", "error")
        return None

           
def get_case_id_from_displayed(case_displayed, db, collection_name="Case"):
    """
    Query MongoDB to get the _id based on the case_displayed value.

    Args:
        case_displayed (str): The display ID of the case.
        db: The MongoDB database connection object.
        collection_name (str): The name of the collection to query (default: "Case").

    Returns:
        str or None: The _id of the case if found, otherwise None.
    """
    try:
        if not case_displayed:
            log_and_print("Invalid CaseDisplayId provided (empty or None)", "error")
            return None

        #log_and_print(f"Searching for CaseDisplayId: {case_displayed} in collection: {collection_name}", "info")

        # Search for the document in the specified collection
        case = db[collection_name].find_one({"CaseDisplayId": case_displayed})
        
        if case:
            #log_and_print(f"Found case with _id: {case['_id']} for CaseDisplayId: {case_displayed}", "info")
            return case["_id"]
        else:
            #log_and_print(f"No case found with CaseDisplayId: {case_displayed}", "error")
            return None
    except Exception as e:
        log_and_print(f"Error while querying MongoDB in collection {collection_name}: {e}", "error")
        return None



if __name__ == "__main__":
    try:
        # Apply font settings
        set_temporary_console_font()
        
        # Load environment variables
        load_dotenv()
        load_configuration()

        # MongoDB connection string
        mongo_connection_string = os.getenv("MONGO_CONNECTION_STRING", "")

        # SQL Server connection parameters
        server_name = os.getenv("DB_SERVER")
        database_name = os.getenv("DB_NAME")
        user_name = os.getenv("DB_USER")
        password = os.getenv("DB_PASS")
        bearer = os.getenv("BEARER_TOKEN")
        
        # Connect to MongoDB
        mongo_client, db = connect_to_mongodb(mongo_connection_string)
        if db is None:
            log_and_print("Failed to connect to MongoDB. Exiting.", "error")
            exit()

        case_id = get_case_id_by_displayed_id(db)
        
        while True:
            IsOtherTask = False
            IsJudgeTask = False
           

            choice = display_menu()

            if choice == 1:
                 # Request Case Display ID from the user
                #case_id = get_case_id_by_displayed_id(db)
                log_and_print(f"DB={db}")
                #list_of_leads = parse_leading_status_by_case_id(case_id, db)
                #log_and_print(f"סטטוס מוביל : {list_of_leads}",is_hebrew=True)
                #create_output_with_all_status_cases(list_of_leads,"output_statuses.xlsx")
                
                 # Step 2: Get the first process ID per case from Mongo (no sorting)
                list_of_process_ids = fetch_all_process_ids_by_case_ids(cases_ids, db)

                # Step 3: Get BPM status of first process for each case (from SQL)
                mapping_cases_tbl = check_first_process_alive(
                    list_of_process_ids,
                    server_name,
                    database_name,
                    user_name,
                    password
                )
                
                #parse_conv_status_by_case_ids(cases_list, db)
            
            elif choice == 2:
                 # Step 2: Get the first process ID per case from Mongo (no sorting)
                list_of_process_ids = fetch_all_process_ids_by_case_ids(cases_list, db)

                #list_of_leads = parse_leading_status_by_case_ids(cases_list, db)
               # create_output_with_all_status_cases(list_of_leads,"output_statuses.xlsx")
                # Step 5: Check if ContinuedProcessId exists in any decision for each case
                continued_process_map = check_specific_continued_process_status(
                    caseid_table, decisionid_table, subdecision_table, db,list_of_process_ids)
            elif choice == 3:
                 # Step 1: Get the leading status from Mongo
                list_of_leads = parse_leading_status_by_case_ids(cases_list, db)

                # Step 2: Get the first process ID per case from Mongo (no sorting)
                list_of_process_ids = fetch_all_process_ids_by_case_ids(cases_list, db)

                # Step 3: Get BPM status of first process for each case (from SQL)
                mapping_cases_tbl = check_first_process_alive(
                    list_of_process_ids,
                    server_name,
                    database_name,
                    user_name,
                    password
                )

                # ✅ Step 4: Get conversion status
                conversion_status_list = parse_conv_status_by_case_ids(cases_list, db)

                # Step 5: Check if ContinuedProcessId exists in any decision for each case
                continued_process_map = check_specific_continued_process_status(
                    caseid_table, decisionid_table, subdecision_table, db,list_of_process_ids
                )

                # Step 6: Write combined results to Excel
                create_common_output_with_all_status_cases(
                    list_of_leads,
                    list_of_process_ids,
                    mapping_cases_tbl,
                    continued_process_map,
                    conversion_status_list,  # ✅ pass this new argument
                    output_file="output.xlsx"
                )

                # if list_of_process_ids:
                #     mapping_cases_tbl = check_first_process_alive(list_of_process_ids,server_name, database_name, user_name, password)
                #     if mapping_cases_tbl:
                #         for case_id, status in mapping_cases_tbl.items():
                #             log_and_print(f"🔍 Case {case_id}: Process is {status}")
                # #log_and_print(f"list_of_process_ids = {list_of_process_ids}")


            elif choice == 4:
                #sub_decision_map = extract_continued_process_ids(cases_list[6:10], db)     
             

                continued_process_map = check_specific_continued_process_status(caseid_table, decisionid_table, subdecision_table, db)

                for case_id, has_cp in continued_process_map.items():
                    if has_cp is True:
                        status = "✅ ContinuedProcessId exists"
                    elif has_cp is False:
                        status = "❌ ContinuedProcessId is None"
                    else:
                        status = "⚠️ Not found or error"
                    
                    print(f"{case_id}: {status}")


            # elif choice == 4:
            #      # Request Case Display ID from the user
            #     #case_id = get_case_id_by_displayed_id(db)
            #     men_status = fetch_request_status_from_menora(case_id,server_name, database_name, user_name, password)
            #     log_and_print(f"סטטוס תיק {case_id} במנורה {men_status}",is_hebrew=True)
            #     #create_output_with_all_status_cases(list_of_leads,"output_statuses.xlsx")
            elif choice == 5:
                #case_id = get_case_id_by_displayed_id(db)
                notes = fetch_notes_status_from_menora(case_id,server_name, database_name, user_name, password)
                if notes:
                    for note in notes:
                        log_and_print(f"{notes}")
                else:
                    log_and_print("אין מידע מבוקש", "info", is_hebrew=True)

            elif choice == 6:
                #case_id = get_case_id_by_displayed_id(db)
                discussions = fetch_discussion_status_from_menora(case_id,server_name, database_name, user_name, password)

            elif choice == 7:
                #case_id = get_case_id_by_displayed_id(db)
                discussions = fetch_distributions_from_menora(case_id,server_name, database_name, user_name, password)
            
            elif choice == 8:
                #case_id = get_case_id_by_displayed_id(db)
                discussions = fetch_decisions_from_menora(case_id,server_name, database_name, user_name, password)
                            
            elif choice == 9:
                log_and_print("Exiting application.", "info")
                break

            elif choice == 10:
                test_url_based_case_id()
                break

    except Exception as e:
        log_and_print(f"An unexpected error occurred: {e}", "error")
    finally:
        if mongo_client:
            mongo_client.close()
            log_and_print("MongoDB connection closed.")

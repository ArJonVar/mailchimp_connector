#region imports
import requests
import json
import hashlib
import mailchimp_marketing as Mailchimp_marketing
from mailchimp_marketing.api_client import ApiClientError
from globals import mailchimp_token, bamb_token, smartsheet_token
import smartsheet
from smartsheet_grid import grid
import hashlib
from datetime import datetime
import pandas as pd
from logger import ghetto_logger
#endregion


class Mailchimp_Connector():
    '''This class connects bamboohr and mailchimp. specifically if there is a discrepency in emails from one platform or the other it removes or adds (removes those not in bamboo, adds those no in mailchimp). 
    If there is a discrepency in email, but same name found in both, they are flagged for human to administrate
    Further, the script will not repost to API Action a row if it matches an existing row in email, first anme, last name, action, intended action, and further description'''
    def __init__(self, config):
        self.config = config
        self.smartsheet_token=config.get('smartsheet_token')
        self.bamb_token=config.get('bamb_token')
        self.mailchimp_token=config.get('mailchimp_token')
        self.log=ghetto_logger("mailchimp_connector.py")
        # smartsheet client & grid class
        self.smart = smartsheet.Smartsheet(access_token=self.smartsheet_token)
        self.smart.errors_as_exceptions(True)
        self.logr=ghetto_logger("mailchimp_connector.py")
        self.logr.log('started running...')
        grid.token=smartsheet_token

        # mailchimp client
        self.mailchimp = Mailchimp_marketing.Client()
        self.mailchimp.set_config({
          "api_key": mailchimp_token,
          "server": "us16"
        })

        # smartsheet sheet ids
        self.api_action_ssid='2636970236792708'
        self.exceptions_ssid='7789698336378756'
        self.update_stamp_sum_id = '502638802063236'

        # Get current date and time
        now = datetime.now()

        # Format the date as MM/DD/YYYY
        self.formatted_date = now.strftime("%m/%d/%Y")
        self.iso_str = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    def error_handler(self, func, *args, **kwargs):
        '''logs errors as e per function'''
        try:
            return func(*args, **kwargs)
        except Exception as e:
            self.logr.wrapper_log(f"{func.__name__}", f"Error: {e}")
    #region mailchmp
    def get_db_listid_mc(self):
        '''mailchimp has many audiences, and we need to get the id of the db audience, which is the one we'll be administrating'''
        lists =self.mailchimp.lists.get_all_lists()
        for list in lists.get('lists'):
            if list.get('name') == 'Dowbuilt':
                self.db_list_id=list.get('id')
    def get_all_members_mc(self):
        '''gets all members of 'dowbuilt' audience in mailchimp, which is the 'audience' that we need to administrate'''
        # Define the count (number of items per page) and offset (starting position)
        count = 100
        offset = 0         

        # Get the total number of members in the list
        members_info = self.mailchimp.lists.get_list_members_info(self.db_list_id)
        total_members = members_info['total_items']        

        # Initialize an empty list to store all members
        self.all_members = []           

        # Loop through the pages and retrieve members
        while offset < total_members:
            # Get the current page of members
            current_page_members = self.mailchimp.lists.get_list_members_info(self.db_list_id, count=count, offset=offset)
            # Add the current page of members to the all_members list
            self.all_members.extend(current_page_members['members'])        

            # Update the offset for the next iteration
            offset += count      
    def flag_botched_addition(self, email, message):
        '''if removal is not successful, we want an error message'''
        for usr in self.post_data:
            if usr.get('Email Address') == email:
                usr['Further Description'] = f"Attempted to ADD user to mailchimp but {message} (contact the backend-engineer for further support)"
                usr['Action'] = 'Flagged'
    def add_user_mc(self, email, fname, lname):
        '''adding a user to mailchimp'''
        try:
            self.add = self.mailchimp.lists.add_list_member(
                self.db_list_id,
                {"email_address": email,
                "status": "subscribed",
                "merge_fields": {
                    "FNAME": fname,
                    "LNAME": lname
                }})
            if self.add.get("status") == "subscribed":
                self.logr.log(f"{email} has been added to mailchimp")
            else:
                self.flag_botched_addition(email, f"user got status of {self.add.get('status')} from addition")
        except ApiClientError as error:
            self.logr.log("Error: {}".format(error.text))
            self.flag_botched_addition(email, f"got API error <{json.loads(error.text).get('detail')}>")
    def flag_botched_removal(self, email, message):
        '''if removal is not successful, we want an error message'''
        for usr in self.post_data:
            if usr.get('Email Address') == email:
                usr['Further Description'] = f"Attempted to REMOVE user from mailchimp but {message} (contact the backend-engineer for further support)"
                usr['Action'] = 'Flagged'
    def remove_user_mc(self, email):
        '''removes user from mailchimp, it is an archive not perminent delete nor unsubscribe'''
        try:
            email_hash = hashlib.md5(email.lower().encode('utf-8')).hexdigest()
            self.delete = self.mailchimp.lists.delete_list_member(self.db_list_id, email_hash)
            if self.delete.status_code == 204:
                self.logr.log(f"{email} has been archived in mailchimp")
            else:
                self.flag_botched_removal(email, f'the user delete had a status of {self.delete.status_code}')
        except ApiClientError as error:
            self.logr.log("Error: {}".format(error.text))
            self.flag_botched_removal(email, f"got API error <{json.loads(error.text).get('detail')}>")
    def execute_changes_mc(self):
        '''exectute additions and removals'''
        for action in self.post_data:
            if action.get("Action") == 'Added':
                self.add_user_mc(action.get('Email Address'), action.get('First Name'), action.get('Last Name'))
            if action.get("Action") == "Removed":
                self.remove_user_mc(action.get('Email Address'))

    #endregion
    #region bamboohr
    def get_all_employees_bhr(self):
        '''grabbing all active employees from bamboo hr'''
        url = "https://api.bamboohr.com/api/gateway.php/Dowbuilt/v1/employees/directory"
        headers = {"accept": "application/json"}
        response = requests.get(url, headers=headers, auth=(bamb_token, ''))
        self.all_employees = json.loads(response.content.decode('utf-8'))
    def handle_names_bhr(self, usr):
        ''' takes in a user of the bamboo usr list, handles full name vs preferred full name and first vs preferred first'''
        try:
            bamb_default_name = usr.get('firstName') + " " + usr.get('lastName')
        except TypeError:
            bamb_default_name = ""
        try:
            bamb_preff_name = usr.get('preferredName') + " " + usr.get('lastName')
        except TypeError:
            bamb_preff_name = ""
        if bamb_preff_name == "":
            first_name = usr.get('firstName')
        else:
            first_name = usr.get('preferredName')

        return bamb_default_name, bamb_preff_name, first_name
    #endregion
    #region smartsheet
    def fetch_exceptions_ss(self):
        '''grabbing exceptions from a smartsheet list of emails not to delete even if they are not in bamboohr'''
        sheet = grid(self.exceptions_ssid)
        sheet.fetch_content()
        r_exceptions = sheet.df['DO NOT REMOVE Exception Emails'].to_list()
        a_exceptions = sheet.df['DO NOT ADD Exception Emails'].to_list()
        try:
            self.exceptions= {'dont_remove': [item for item in r_exceptions if item], 'dont_add': [item for item in a_exceptions if item]}
        except:
            self.exceptions = []
    def generate_columnid_dict(self):
        '''makes a dict for each column in the smartsheet log to quickly grab their column_ids'''
        # get sheet ids
        sheet = grid(self.api_action_ssid)
        sheet.fetch_content()
        sheet.column_df

        # Create an empty dictionary to store the key-value pairs
        self.column_id_dict = {}

        # Loop through each row of the DataFrame and add the key-value pairs to the dictionary
        for index, row in sheet.column_df.iterrows():
            self.column_id_dict[row['title']] = row['id']
    def audit_post_duplicates(self):
        '''creates list of posts that should not be posted as they would be exact duplicates (email, fname, lname, description, action)'''
        self.dont_post = []
        self.sheet = grid(self.api_action_ssid)
        self.sheet.fetch_content()
        # does not consider it a duplication if the problem was resolved and it came back. The mechanic is to remove rows where there is a non null resolved date
        filtered_df = self.sheet.df[self.sheet.df['Resolved Flag Date'].isnull()]
        posted = filtered_df.to_dict('records')

        # checks if post [email, fname, lname, description, status] already exists in the smartsheet (with diff date and ignoring other columns)
        for post in self.post_data:
            might_post = post.copy()
            del might_post['Script Date']

            for existing_row in posted:
                if all(item in existing_row.items() for item in might_post.items()):
                    self.dont_post.append(post)
    def gen_blank_row_id_list(self):
        '''delete blank rows after posting to clean sheet up'''
        filtered_df = self.sheet.df[self.sheet.df['Email Address'].isnull()]
        blank_row_ids = filtered_df['id'].tolist()

        return blank_row_ids
    def post_to_ss(self):
        '''posts all to ss'''
        self.audit_post_duplicates()
        self.generate_columnid_dict()
        self.rows = []

        for item in self.post_data:
            row = smartsheet.models.Row()
            row.to_top = True
            for key in self.column_id_dict:
                if item.get(key) != None: 
                    if item not in self.dont_post:    
                        row.cells.append({
                        'column_id': self.column_id_dict[key],
                        'value': item[key]
                        })
            self.rows.append(row)

        self.ss_post = self.smart.Sheets.add_rows(self.api_action_ssid, self.rows)
        try:
            self.ss_delete_blank = self.smart.Sheets.delete_rows(self.api_action_ssid, self.gen_blank_row_id_list())
        except: 
            # will fail there are no blanks
            pass
        self.post_update_stamp()
        self.logr.log('posting complete!')
    def post_update_stamp(self):
        '''posts date to summary column to tell ppl when the last time this script succeeded was'''
        sum = smartsheet.models.SummaryField({
            "id": int(self.update_stamp_sum_id),
            "ObjectValue":self.formatted_date
        })

        self.post_update_stamp = self.smart.Sheets.update_sheet_summary_fields(
            self.api_action_ssid,    # sheet_id
            [sum],
            False    # rename_if_conflict
        )
    #endregion
    #region prep action data
    def create_email_references(self):
        '''creates dict holding lists of emails (organized by source) all in lowercase so its easiest to check if an email is/is not in a certain place by being able to use "if email in list"'''
        bamb_lower= []
        mc_lower=[]
        exceptions_lower = []
        for exception_key in self.exceptions:
            for exception in self.exceptions[exception_key]:
                try:
                    exceptions_lower.append(exception.lower())
                except AttributeError:
                    pass
        for bamb in self.all_employees.get('employees'):
            try: 
                bamb_lower.append(bamb.get("workEmail").lower())
            except AttributeError:
                pass
        for mc in self.all_members:
            try:
                mc_lower.append(mc.get('email_address').lower())
            except AttributeError:
                pass
        self.email_reference_dict = {'bhr':bamb_lower, 'mc':mc_lower, 'exceptions':exceptions_lower}
    def initial_add_remove(self):
        '''creates initial dict of additions and removals'''
        add =[]
        remove=[]     

        for bamb in self.email_reference_dict.get('bhr'):
            if bamb not in self.email_reference_dict.get('mc') and bamb not in self.email_reference_dict.get('exceptions'):
                add.append(bamb)       

        for mc in self.email_reference_dict.get('mc'):
            if mc not in self.email_reference_dict.get('bhr') and mc not in self.email_reference_dict.get('exceptions'):
                remove.append(mc)
        
        self.action_reference = {'add':add, 'remove':remove}
    def gather_delete_info(self):
        '''we need to gather more data on those we want to delete to see if we will flag instead'''
        self.users_tobe_deleted = []
        for mc in self.all_members:
            for usr in self.action_reference.get('remove'):
                if usr == mc.get('email_address').lower() and usr.lower() not in self.email_reference_dict.get('exceptions'):
                    self.users_tobe_deleted.append(mc)
    def handle_flags(self):
        '''we flag users that seem like they should be deleted (because they are no longer in bamboohr according to their email) BUT their name is still in bamboo. This could be a duplicate name (seperate person) OR a need to update employees name'''
        self.error_handler(self.gather_delete_info)
        self.flagged_lower = []

        for usr in self.all_employees.get('employees'):
            bamb_default_name, bamb_preff_name, first_name = self.error_handler(self.handle_names_bhr, usr)
 
            #handling various flag cases
            for mc_usr in self.users_tobe_deleted:
                if mc_usr.get('full_name') == bamb_default_name or mc_usr.get('full_name') == bamb_preff_name:
                    name_modifier = ""
                    self.flagged_lower.append(mc_usr.get('email_address').lower())
                    if mc_usr.get('full_name') == bamb_default_name and mc_usr.get('full_name') == bamb_preff_name:
                        pass
                    elif mc_usr.get('full_name') == bamb_default_name:
                        name_modifier = ' full'
                    elif mc_usr.get('full_name') == bamb_preff_name:
                        name_modifier = ' preferred'
                    flag_reason = f"{mc_usr.get('email_address')} is not in BambooHR, but their{name_modifier} name {mc_usr.get('full_name')} is in BambooHR with the following email address: {usr.get('workEmail')}"
                    self.post_data.append({'Email Address': mc_usr.get('email_address'), 'Further Description': flag_reason, 'First Name': first_name, 'Last Name': usr.get('lastName'), 'Action':'Flagged', 'Script Date':self.iso_str})
    def final_remove_list(self):
        '''refactor list now that flags have been removed'''
        remove_final =[]

        for mc in self.email_reference_dict.get('mc'):
            if mc in self.action_reference.get('remove') and mc not in self.flagged_lower:
                remove_final.append(mc)

        self.action_reference['remove']=remove_final
    def extract_post_data(self):
        '''turning list of actions into post dict for smartsheet logger'''
        for member in self.all_members:
            if member['status'] != 'subscribed':
                email = member['email_address']
                fname, lname = member['full_name'].split(' ')
                status = member['status']
                self.post_data.append({
                    'First Name': fname, 
                    'Last Name': lname, 
                    'Email Address': email, 
                    'Action': 'Flagged', 
                    'Further Description': f"{email} has a status of {status}. They will need a status of 'subscribed' to receive any emails from mailchimp", 
                    'Script Date': self.iso_str, 
                    'Intended Action': "Identify Non-Subscribers"})
                
        for action in self.action_reference:
            if action == "remove":
                for email in self.action_reference.get(action, []):  # Ensure default to empty list if action key doesn't exist
                    first_name, last_name = None, None  # Initialize variables outside the loops
                    for member in self.all_members:
                        member_email = member.get("email_address", "").lower()
                        if email.lower() == member_email:  # Ensuring case-insensitive comparison
                            full_name = member.get("full_name", "").strip()  # Handles leading space in full name
                            name_parts = full_name.split(" ")
                            if len(name_parts) >= 2:  # Ensure there's at least a first name and last name
                                first_name, last_name = name_parts[0], name_parts[1]
                                # Additional logic if there are middle names or suffixes not covered here
                                break  # Assuming only one match is needed, exit the loop once found
                    if first_name and last_name:  # Ensure first_name and last_name were found
                        self.post_data.append({
                            'First Name': first_name, 
                            'Last Name': last_name, 
                            'Email Address': email, 
                            'Action': 'Removed', 'Further Description': f"{email} was found in Mailchimp, but not in BambooHR. Now the user is being archived from Mailchimp", 
                            'Script Date': self.iso_str, 
                            'Intended Action': "Remove"})

            if action == "add":
                for email in self.action_reference.get(action):
                    for employee in self.all_employees.get("employees", []):  # Ensure default to empty list if "employees" key doesn't exist
                        work_email = employee.get("workEmail")
                        if work_email and email.lower() == work_email.lower():  # Check if work_email is not None and then compare
                            try:
                                first_name = employee.get('preferredName', employee.get('firstName'))  # Simplified logic
                                if first_name == None:
                                    first_name = employee.get('firstName')
                            except:
                                first_name = employee.get('firstName')
                            last_name = employee.get("lastName")
                    self.post_data.append({
                        'First Name': first_name, 
                        'Last Name': last_name, 
                        'Email Address': email, 
                        'Action': 'Added', 
                        'Further Description': f"{email} was found in BambooHR but not in MailChimp. User is now being added to Mailchimp", 
                        'Script Date':self.iso_str, 
                        'Intended Action':"Add"})
    #endregion
    def run(self):
        '''runs main script as intended'''
        self.post_data = []
        functions_to_run=[
            self.get_db_listid_mc,
            self.get_all_members_mc,
            self.get_all_employees_bhr,
            self.fetch_exceptions_ss,
            self.create_email_references,
            self.initial_add_remove,
            self.handle_flags,
            self.final_remove_list,
            self.extract_post_data,
            self.audit_post_duplicates,
            self.execute_changes_mc,
            self.post_to_ss
        ]
        for func in functions_to_run:
            self.error_handler(func)
        

if __name__ == "__main__":
    config = {
        'smartsheet_token':smartsheet_token,
        'mailchimp_token': mailchimp_token,
        'bamboohr_token':  bamb_token
    }
    mc = Mailchimp_Connector(config)
    mc.run()
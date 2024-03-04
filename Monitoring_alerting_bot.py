import pandas as pd
import os
import oracledb
import json
import glob
import polars as pl
from datetime import datetime, timedelta
import pymsteams
import pprint
import warnings
import time
import sys
global alert_channel


#Define config locations
config_py = os.path.abspath(os.path.join(os.path.dirname(__file__), 'C:\\Scheduled_job_configs\\feed_monitoring'))
config_directory = 'C:\\Scheduled_job_configs\\feed_monitoring'
table_data_path = os.path.join(config_directory, "table_data.json")
notifications_data_path = os.path.join(config_directory, "notification_timestamps.json")
job_data_path = os.path.join(config_directory, "job_data.json")
secrets_file_path = os.path.join(config_directory, "Secrets.json")
table_space_data_path = os.path.join(config_directory, "table_space_data.json")
last_run_data_path = os.path.join(config_directory, "last_run_config.json")


sys.path.append(config_py)
from config_items import user_lookup, high_prio_channel, med_prio_channel, low_prio_channel

warnings.filterwarnings('ignore')
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

with open(secrets_file_path, 'r') as f:
    config = json.load(f)
    user_name = config['user_name']
    pwd_mcrm = config['pwd_mcrm']
    pwd_gabi = config['pwd_gabi']
    dsn_mcrm = config['dsn2']
    dsn_gabiprd = config['dsn']
    lib_dir = config[r'lib_dir']



# oracledb.init_oracle_client(lib_dir)
# connection_mcrm = oracledb.connect(
#     user= user_name,
#     password= pwd_mcrm,
#     dsn= dsn_mcrm)
# cursor = connection_mcrm.cursor()

# connection_gabiprd = oracledb.connect(
#     user= user_name,
#     password= pwd_gabi,
#     dsn= dsn_gabiprd)
# cursor = connection_gabiprd.cursor()

# today = datetime.today()
# yesterday = today - timedelta(days=1)

#replace with MS SQL stuff

#Should probably try to eliminate repeated functions from within the classes when we get a minute.


class LoadDateChecker:
    def __init__(self, desired_load_date, query, expected_time, connection, message, contact_group, job_name, colour):
        self.desired_load_date = desired_load_date
        self.query = query
        self.expected_time = expected_time
        self.connection = connection
        self.message = message
        self.contact_group = contact_group
        self.job_name = job_name
        self.colour = colour

    def check_load_date(self):
        current_time = datetime.now().time()
        expected_time = datetime.strptime(self.expected_time, "%H:%M:%S.%f").time()
        df = pd.read_sql(self.query, con=self.connection)
        max_date = str(df.iloc[0, 0].date())[:10]
        print(max_date)
        desired_load_date = str(self.desired_load_date)[:10]
        print(desired_load_date)
        expected_load_time_str = expected_time.strftime("%H:%M:%S.%f")[:8]
        expected_load_datetime = f"{desired_load_date} {expected_load_time_str}"
        if max_date >= desired_load_date:
            state = True
        else:
            max_date_time = datetime.strptime(max_date, "%Y-%m-%d")
            max_date_time = datetime.combine(max_date_time.date(), current_time)
            desired_load_datetime = datetime.strptime(expected_load_datetime, "%Y-%m-%d %H:%M:%S")
            print(max_date_time)
            print(desired_load_datetime)

            if current_time > expected_time and max_date_time < desired_load_datetime:
                state = False
            else:
                state = True
        print(state)
        return state




    def get_job_name(self):
        return self.job_name



    def generate_teams_payload(self):

        payload = {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": {
                        "type": "AdaptiveCard",
                        "body": [
                            {
                                "type": "TextBlock",
                                "size": "Large",
                                "weight": "Bolder",
                                "text": f" Alert: {self.message}",
                                "color": f"{self.colour}",
                                "wrap": "true"
                            },
                        ],
                        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                        "version": "1.0",
                        "msteams": {
                            "entities": []
                        }
                    }
                }
            ]
        }


        if self.colour == "default":
            pass
        else:
            if self.contact_group in user_lookup:
                names_emails = user_lookup[self.contact_group]
                for name, email in names_emails.items():
                    payload["attachments"][0]["content"]["body"].append({
                        "type": "TextBlock",
                        "text": f"<at>{name}</at>",
                    })

                    payload["attachments"][0]["content"]["msteams"]["entities"].append({
                        "type": "mention",
                        "text": f"<at>{name}</at>",
                        "mentioned": {
                            "id": email,
                            "name": name
                        }
                    })


        return payload


    def send_message(self, state, payload):

        if self.colour == "attention":
            alert_channel = high_prio_channel
        elif self.colour == "warning":
            alert_channel = med_prio_channel
        else:
            alert_channel = low_prio_channel
        myTeamsMessage = pymsteams.connectorcard(alert_channel)

        myMessageSection = pymsteams.cardsection()

        myMessageSection.title("Pipeline failure alert")

        myMessageSection.activityTitle(f" Alert: {self.message}")
        myTeamsMessage.color("<E44A10>")

        myTeamsMessage.text("There has been a data quality error")

        myTeamsMessage.payload = payload

        myTeamsMessage.send()

class JobStateChecker:
    def __init__(self, query, connection, message, contact_group, job_name, colour):
        self.query = query
        self.connection = connection
        self.message = message
        self.contact_group = contact_group
        self.job_name = job_name
        self.colour = colour

    def check_job_latest_status(self):
        df = pd.read_sql(self.query, con=self.connection)
        print(df)
        try:
            status = str(df.iloc[0, 0])

            if status == 'SUCCEEDED':
                state = True
            else:
                state = False
        except IndexError:
            state = True
            pass
        return state

    def get_job_name(self):
        return self.job_name


    def generate_teams_payload(self):
        payload = {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": {
                        "type": "AdaptiveCard",
                        "body": [
                            {
                                "type": "TextBlock",
                                "size": "Large",
                                "weight": "Bolder",
                                "text": f" Alert: {self.message}",
                                "color": f"{self.colour}",
                                "wrap": "true"
                            },
                        ],
                        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                        "version": "1.0",
                        "msteams": {
                            "entities": []
                        }
                    }
                }
            ]
        }


        if self.colour == "default":
            pass
        else:
            if self.contact_group in user_lookup:
                names_emails = user_lookup[self.contact_group]
                for name, email in names_emails.items():
                    payload["attachments"][0]["content"]["body"].append({
                        "type": "TextBlock",
                        "text": f"<at>{name}</at>",
                    })

                    payload["attachments"][0]["content"]["msteams"]["entities"].append({
                        "type": "mention",
                        "text": f"<at>{name}</at>",
                        "mentioned": {
                            "id": email,
                            "name": name
                        }
                    })
    
        return payload
    
    def send_message(self, state, payload):

        if self.colour == "attention":
            alert_channel = high_prio_channel
        elif self.colour == "warning":
            alert_channel = med_prio_channel
        else:
            alert_channel = low_prio_channel
        myTeamsMessage = pymsteams.connectorcard(alert_channel)

        myMessageSection = pymsteams.cardsection()

        myMessageSection.title("Pipeline failure alert")

        myMessageSection.activityTitle(f" Alert: {self.message}")
        myTeamsMessage.color("<E44A10>")

        myTeamsMessage.text("There has been a data quality error")

        myTeamsMessage.payload = payload

        myTeamsMessage.send()

class LoadDateCheckerIntraday:
    def __init__(self, desired_load_date, query, time_windows, connection, message, contact_group, job_name, colour):
        self.desired_load_date = desired_load_date
        self.query = query
        self.time_windows = time_windows
        self.connection = connection
        self.message = message
        self.contact_group = contact_group
        self.job_name = job_name
        self.colour = colour

    def check_load_date_intraday(self, current_date):
        print("Start of intraday checks")
        current_time = datetime.now().time()
        df = pd.read_sql(self.query, con=self.connection)
        max_date_time = pd.to_datetime(df.iloc[0, 0])

        closest_previous_time_window = None
        for start_time, end_time in self.time_windows:
            if end_time <= current_time:
                closest_previous_time_window = (start_time, end_time)
            else:
                break
        print(closest_previous_time_window)
        print(max_date_time)
        if closest_previous_time_window:
            start_time, end_time = closest_previous_time_window
            current_datetime = datetime.combine(current_date, current_time)
            start_datetime = datetime.combine(current_date, start_time)
            end_datetime = datetime.combine(current_date, end_time)
            if current_datetime > end_datetime and max_date_time < start_datetime:
                return False

        return True


    def get_job_name(self):
        return self.job_name


    def generate_teams_payload(self):
        payload = {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": {
                        "type": "AdaptiveCard",
                        "body": [
                            {
                                "type": "TextBlock",
                                "size": "Large",
                                "weight": "Bolder",
                                "text": f" Alert: {self.message}",
                                "color": f"{self.colour}",
                                "wrap": "true"
                            },
                        ],
                        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                        "version": "1.0",
                        "msteams": {
                            "entities": []
                        }
                    }
                }
            ]
        }



        if self.colour == "default":
            pass
        else:
            if self.contact_group in user_lookup:
                names_emails = user_lookup[self.contact_group]
                for name, email in names_emails.items():
                    payload["attachments"][0]["content"]["body"].append({
                        "type": "TextBlock",
                        "text": f"<at>{name}</at>",
                    })

                    payload["attachments"][0]["content"]["msteams"]["entities"].append({
                        "type": "mention",
                        "text": f"<at>{name}</at>",
                        "mentioned": {
                            "id": email,
                            "name": name
                        }
                    })

        return payload


    def send_message(self, state, payload):

        if self.colour == "attention":
            alert_channel = high_prio_channel
        elif self.colour == "warning":
            alert_channel = med_prio_channel
        else:
            alert_channel = low_prio_channel
        myTeamsMessage = pymsteams.connectorcard(alert_channel)

        myMessageSection = pymsteams.cardsection()

        myMessageSection.title("Pipeline failure alert")

        myMessageSection.activityTitle(f" Alert: {self.message}")
        myTeamsMessage.color("<E44A10>")

        myTeamsMessage.text("There has been a data quality error")

        myTeamsMessage.payload = payload

        myTeamsMessage.send()

class LastTimeChecker:
    def __init__(self, query, connection, message, contact_group, job_name, num_minutes, colour):
        self.query = query
        self.connection = connection
        self.message = message
        self.contact_group = contact_group
        self.job_name = job_name
        self.num_minutes = int(num_minutes)
        self.colour = colour

    def check_last_time(self):
        expected_time = (datetime.now() - timedelta(minutes=self.num_minutes)).time()
        df = pd.read_sql(self.query, con=self.connection)
        max_time_str = str(df.iloc[0, 0])
        max_time = datetime.strptime(max_time_str, "%H:%M:%S").time()

        print(expected_time)
        print(max_time)

        if max_time < expected_time:
            state = False
        else:
            state = True

        return state

    def get_job_name(self):
        return self.job_name


    def generate_teams_payload(self):
        payload = {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": {
                        "type": "AdaptiveCard",
                        "body": [
                            {
                                "type": "TextBlock",
                                "size": "Large",
                                "weight": "Bolder",
                                "text": f" Alert: {self.message}",
                                "color": f"{self.colour}",
                                "wrap": "true"
                            },
                        ],
                        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                        "version": "1.0",
                        "msteams": {
                            "entities": []
                        }
                    }
                }
            ]
        }



        if self.colour == "default":
            pass
        else:
            if self.contact_group in user_lookup:
                names_emails = user_lookup[self.contact_group]
                for name, email in names_emails.items():
                    payload["attachments"][0]["content"]["body"].append({
                        "type": "TextBlock",
                        "text": f"<at>{name}</at>",
                    })

                    payload["attachments"][0]["content"]["msteams"]["entities"].append({
                        "type": "mention",
                        "text": f"<at>{name}</at>",
                        "mentioned": {
                            "id": email,
                            "name": name
                        }
                    })
        return payload

    def send_message(self, state, payload):

        if self.colour == "attention":
            alert_channel = high_prio_channel
        elif self.colour == "warning":
            alert_channel = med_prio_channel
        else:
            alert_channel = low_prio_channel
        myTeamsMessage = pymsteams.connectorcard(alert_channel)

        myMessageSection = pymsteams.cardsection()

        myMessageSection.title("Pipeline failure alert")

        myMessageSection.activityTitle(f" Alert: {self.message}")
        myTeamsMessage.color("<E44A10>")

        myTeamsMessage.text("There has been a data quality error")

        myTeamsMessage.payload = payload

        myTeamsMessage.send()

class TableSpaceChecker:
    def __init__(self, query, connection, message, contact_group, threshold, job_name, colour):
        self.query = query
        self.connection = connection
        self.message = message
        self.contact_group = contact_group
        self.threshold = int(threshold)
        self.job_name = job_name
        self.colour = colour

    def check_pct(self):
        df = pd.read_sql(self.query, con=self.connection)
        pct_remaining = int(df.iloc[0, 0])
        print(f"Job name: {self.job_name}")
        print(f"Percent remaining {pct_remaining}")
        print(f"Threshold for alert: {self.threshold}")
        if pct_remaining < self.threshold:
            state = False
        else:
            state = True
        print(state)
        return state

    def get_job_name(self):
        return self.job_name


    def generate_teams_payload(self):
        payload = {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": {
                        "type": "AdaptiveCard",
                        "body": [
                            {
                                "type": "TextBlock",
                                "size": "Large",
                                "weight": "Bolder",
                                "text": f" Alert: {self.message}",
                                "color": f"{self.colour}",
                                "wrap": "true"
                            },
                        ],
                        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                        "version": "1.0",
                        "msteams": {
                            "entities": []
                        }
                    }
                }
            ]
        }


        if self.colour == "default":
            pass
        else:
            if self.contact_group in user_lookup:
                names_emails = user_lookup[self.contact_group]
                for name, email in names_emails.items():
                    payload["attachments"][0]["content"]["body"].append({
                        "type": "TextBlock",
                        "text": f"<at>{name}</at>",
                    })

                    payload["attachments"][0]["content"]["msteams"]["entities"].append({
                        "type": "mention",
                        "text": f"<at>{name}</at>",
                        "mentioned": {
                            "id": email,
                            "name": name
                        }
                    })

        return payload



    def send_message(self, state, payload):

        if self.colour == "attention":
            alert_channel = high_prio_channel
        elif self.colour == "warning":
            alert_channel = med_prio_channel
        else:
            alert_channel = low_prio_channel
        myTeamsMessage = pymsteams.connectorcard(alert_channel)

        myMessageSection = pymsteams.cardsection()

        myMessageSection.title("Pipeline failure alert")

        myMessageSection.activityTitle(f" Alert: {self.message}")
        myTeamsMessage.color("<E44A10>")

        myTeamsMessage.text("There has been a data quality error")

        myTeamsMessage.payload = payload

        myTeamsMessage.send()


def generate_date_checks(connection_mcrm, connection_gabiprd, table_data_path):
    with open(table_data_path, "r") as file:
        date_checks_config = json.load(file)
    date_checks = []
    for item in date_checks_config:
        if item["active"] == "True":
                fascia = 'null'
                expected_date = item["expected_date"]
                date_field = str(item["date_field"])
                table_name = str(item["table_name"])
                expected_time = item["expected_time"]
                connection = str(item["connection"])
                users = str(item["user_lookup"])
                try:
                    fascia = str(item["fascia"])
                except:
                    pass
                priority = item['priority']
                if priority == 1:
                    colour = 'attention'
                elif priority == 2:
                    colour = 'warning'
                elif priority == 3:
                    colour = 'default'

                if connection == 'MCRM':
                    connection = connection_mcrm
                elif connection == 'GABIPRD':
                    connection = connection_gabiprd

                if expected_date == 'today':
                    expected_date = datetime.today()
                elif expected_date == 'yesterday':
                    expected_date = datetime.today() - timedelta(days=1)
                elif expected_date == 'today-2':
                    expected_date = datetime.today() - timedelta(days=2)

                if fascia == 'null':
                    sql_query = f"select max({date_field}) from {table_name}"
                    input = (f'{expected_date}', f'{sql_query}', f'{expected_time}', connection, f"Data is not up to date within {table_name}. Please investigate", f"{users}", f"{table_name}", f"{colour}")
                else:
                    sql_query = f"select max({date_field}) from {table_name} where fascia = {fascia}"
                    input = (f'{expected_date}', f'{sql_query}', f'{expected_time}', connection, f"The unsubs file for fascia: {fascia} is late or missing in the database. Please investigate", f"{users}", f"{table_name}", f"{colour}")
                print(sql_query)
                date_checks.append(input)


    return date_checks

def get_job_checks(connection_gabiprd):
    job_checks = []
    with open(job_data_path, "r") as file:
        job_data = json.load(file)
    for item in job_data:
        try:
             if item["active"] == "True":
                job_name = item["JOB_NAME"]
                users = item['user_lookup']
                priority = item['priority']
                if priority == 1:
                    colour = 'attention'
                elif priority == 2:
                    colour = 'warning'
                elif priority == 3:
                    colour = 'default'
                input = (f'''select status from DATABASE_HEALTH.MC_TABLE_PROCEDURAL_LOG where JOB_NAME = '{job_name}' and JOB_DATE = trunc(sysdate) order by JOB_DATE desc, JOB_minute desc FETCH FIRST ROW ONLY''', connection_gabiprd, f"P{priority}: "f"The job {job_name} has failed. Please investigate", f"{users}", f"{job_name}", f"{colour}")
                job_checks.append(input)

        except Exception as e:
            print(e)
            print("test is failing here")
            print(job_name)
            continue
    return job_checks

def generate_tablespace_checks(connection_gabiprd):
    space_checks = []
    with open(table_space_data_path, "r") as file:
        space_data = json.load(file)
    for item in space_data:
        try:
            if item["active"] == "True":
                table_name = str(item["table_name"])
                table_space = str(item["table_space"])
                users = str(item["user_lookup"])
                threshold = int(item["threshold"])
                job_name = table_space
                if threshold == 1:
                    colour = 'attention'
                elif threshold == 5:
                    colour = 'warning'
                elif threshold == 10:
                    colour = 'default'
        except Exception:
            pass
    
        sql_query = f"SELECT PCT_FREE FROM {table_name} where TABLE_SPACE = '{table_space}' order by RUNDATE desc, RUNHOUR desc fetch first row only"
        input = (f'{sql_query}', connection_gabiprd, f"{table_space} is below {threshold}% free space remaining", f"{users}", f"{threshold}", f"{job_name}", f'{colour}')
        space_checks.append(input)
    
    return space_checks

def get_last_ran_checks(connection_mcrm, connection_gabiprd):
    last_runs = []
    with open(last_run_data_path, "r") as file:
        run_data = json.load(file)
    for item in run_data:
        try:
            if item["active"] == "True":
                table_name = str(item["table_name"])
                time_field = str(item["time_field"])
                date_field = str(item["date_field"])
                users = str(item["user_lookup"])
                connection = item["connection"]
                num_minutes = int(item["within_last_x_minutes"])
                priority = item['priority']
                job_name = table_name
                if priority == 1:
                    colour = 'attention'
                elif priority == 2:
                    colour = 'warning'
                elif priority == 3:
                    colour = 'default'

                if connection == 'MCRM':
                    connection = connection_mcrm
                elif connection == 'GABIPRD':
                    connection = connection_gabiprd
        except Exception:
            pass

    sql_query = f"SELECT MAX({time_field}) from {table_name} where {date_field} = trunc(sysdate) and {time_field} < TO_CHAR(sysdate, 'HH24:MI:SS')"
    input = (f"{sql_query}", connection, f"Data has not updated in {table_name} for {num_minutes} minutes", f"{users}", f"{job_name}", f"{num_minutes}", f"{colour}")
    last_runs.append(input)

    return last_runs


def run_checks(item_list, check_function, notification_timestamps):
    for item in item_list:
        state = check_function(item, notification_timestamps)

        if state is False:
            job_name = item.get_job_name()
            print(job_name)
            payload = item.generate_teams_payload()

            if job_name in notification_timestamps:
                last_notification_time = datetime.fromisoformat(notification_timestamps[job_name])
                notification_interval = timedelta(hours=24)
                elapsed_time = datetime.now() - last_notification_time

                if elapsed_time < notification_interval:
                    continue

            notification_timestamps[job_name] = datetime.now().isoformat()
            item.send_message(state, payload)
            print("Sending alert")
        else:
            continue

    return notification_timestamps




#Currently Intra_day is in script. Later will try and move to config files like the others. 

intra_day_timewindows_tm1 = [(datetime.strptime("07:00:00", "%H:%M:%S").time(), datetime.strptime("08:25:00", "%H:%M:%S").time()),
                             (datetime.strptime("09:00:00", "%H:%M:%S").time(), datetime.strptime("10:15:00", "%H:%M:%S").time()),
                             (datetime.strptime("13:45:00", "%H:%M:%S").time(), datetime.strptime("15:15:00", "%H:%M:%S").time()),
                             (datetime.strptime("17:00:00", "%H:%M:%S").time(), datetime.strptime("18:15:00", "%H:%M:%S").time())]
intra_day_checks = [
    (today, '''{query}''',
     intra_day_timewindows_tm1, connection_gabiprd, "Data is not up to date within MC_TABLE_TM1_DAILY_CUBE_LIVE. Please investigate", "daily_sales_tracker", "TM1_SALES", "attention"),
    ]

current_date =datetime.today()




def main():
    # Check if run time is out of bounds
    start = time.time()
    today = datetime.today()
    current_time = datetime.now().time()
    current_time = datetime.strptime(str(current_time), "%H:%M:%S.%f").time()

    start_time = "08:00:00.0000"
    start_time = datetime.strptime(start_time, "%H:%M:%S.%f").time()
    end_time = "20:00:00.0000"
    end_time = datetime.strptime(end_time, "%H:%M:%S.%f").time()

    if current_time < start_time or current_time > end_time:
        print("out of hours")
        exit()

    else:
        print("Inside bounds")


    #Open notifications sent timestamp history
    try:
        with open(notifications_data_path, "r") as file:
            notification_timestamps = json.load(file)
    except FileNotFoundError:
        notification_timestamps = {}

    #Generate date_checks and job_checks inputs from config files (These are in the scheduled_jobs_config folder)

    date_checks = generate_date_checks(connection_mcrm, connection_gabiprd, table_data_path)
    job_checks = get_job_checks(connection_gabiprd)
    space_checks = generate_tablespace_checks(connection_gabiprd)
    last_ran_checks = get_last_ran_checks(connection_mcrm, connection_gabiprd)
    print(last_ran_checks)
    #Create lists for checks
    date_checker_list = [LoadDateChecker(*items) for items in date_checks]
    job_checker_list = [JobStateChecker(*items) for items in job_checks]
    intra_day_check_list = [LoadDateCheckerIntraday(*items) for items in intra_day_checks]
    last_time_check_list = [LastTimeChecker(*items) for items in last_ran_checks]
    tablespace_check_list = [TableSpaceChecker(*items) for items in space_checks]
    start = time.time()


    try:
        with open(notifications_data_path, "r") as file:
            notification_timestamps = json.load(file)
    except FileNotFoundError:
        notification_timestamps = {}

    # Call the function for each list of items with the corresponding check function
    print("Checking last time")
    notification_timestamps = run_checks(last_time_check_list, lambda item, timestamps: item.check_last_time(), notification_timestamps)

    print("Checking job latest status")
    notification_timestamps = run_checks(job_checker_list, lambda item, timestamps: item.check_job_latest_status(), notification_timestamps)

    print("checking tablespaces")
    notification_timestamps = run_checks(tablespace_check_list, lambda item, timestamps: item.check_pct(), notification_timestamps)

    print("Checking load date")
    notification_timestamps = run_checks(date_checker_list, lambda item, timestamps: item.check_load_date(), notification_timestamps)

    print("Checking intra day load date")
    notification_timestamps = run_checks(intra_day_check_list, lambda item, timestamps: item.check_load_date_intraday(current_date), notification_timestamps)


    #Save last sent timigns.
    with open(notifications_data_path, "w") as file:
        json.dump(notification_timestamps, file)


    end = time.time()
    duration = end-start
    print(duration)
    print("time to run")

if __name__ == '__main__':
    main()





#TO DO - Create class similar to data date checker for Windows Task scheduler procedures. This will let us check the status of Python scripts also and alert to failure cases. (Scratch this, we want to switch to airflow reporting. Could set up something to read airflow log files?)

#TO DO - Create class similar to above for Source data file locations. EG, whether a source excel data file has been updated/deposited at the correct time. For TM1 this could be as simple as having it Ping if a file enters the failure folder.




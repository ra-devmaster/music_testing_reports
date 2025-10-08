import datetime
import os
from datetime import timedelta

from ra_data_helper import get_latest_calculated_datetime
from ra_datetime_helper import end_of_the_week
from ra_mysql_package import *
from dotenv import load_dotenv

load_dotenv()

override_sql_settings({
    1: {'user_name': os.environ['SQL_USERNAME'], 'password': os.environ['SQL_PASSWORD']},
    2: {'user_name': os.environ['SQL_USERNAME'], 'password': os.environ['SQL_PASSWORD']},
})

API_BASE_URL = os.environ['BASE_URL']
HEADERS = {"API-key": os.environ['API_KEY']}


def get_radio_names(radio_ids):
    try:
        q = 'SELECT radio_id, radio_name FROM radioanalyzer.radios WHERE radio_id in %s'
        with SQLConnection(1) as conn:
            conn.execute(q, (radio_ids,))
            res = conn.fetchall(True)
        radio_names = {}
        for r in res:
            radio_names[r['radio_id']] = r['radio_name']
        return radio_names
    except Exception as e:
        raise Exception(f'Failed to get radio_names: {repr(e)}')

def get_report_details(report_id: int):
    try:
        q = ('SELECT report_id, user_id, radio_ids, weeks_to_check, email_address, min_spins, max_spins, daypart_id, demo_id, hide_invalid, hide_deltas '
             'FROM radioanalyzer.music_testing_reports WHERE '
             'report_id = %s')
        with SQLConnection(1) as conn:
            conn.execute(q, (report_id, ))
            res = conn.fetchone(True)
        return res
    except Exception as e:
        raise Exception(f'Failed to get report details: {repr(e)}')


def create_message_queue_entries(conn):
    from functions import get_demographic
    try:
        q = ('SELECT report_id, radio_ids, demo_id FROM radioanalyzer.music_testing_reports AS mts JOIN radioanalyzer.users AS u USING(user_id) WHERE being_processed != 1 AND ('
             'DATE_ADD('
             'last_date_used, INTERVAL frequency WEEK) < '
             'NOW() OR last_date_used IS Null) AND is_disabled = 0')
        conn.execute(q)
        res = conn.fetchall(True)
        end_dt = end_of_the_week(datetime.now() - timedelta(days=7)) - timedelta(hours=1)
        final_jobs = []
        for r in res:
            try:
                demo = get_demographic(r['demo_id'])
                latest_datas = []
                for radio_id in r['radio_ids'].split(','):
                    # if radio does burn check that instead
                    latest_datas.append(get_latest_datetime(conn, radio_id, demo['data_type'], demo['listener_group']))
                if not any(x < end_dt for x in latest_datas):
                    final_jobs.append({'report_id': r['report_id']})
            except:
                continue
        return final_jobs
    except Exception as e:
        raise Exception(f'Failed to create messages: {repr(e)}')


def set_reports_being_processed(conn1, report_ids,  being_processed, last_data_used = None):
    try:
        last_data_query = ''
        args = ()
        if last_data_used:
            last_data_query = 'last_date_used = %s,'
            args += (last_data_used, )
        args += (being_processed, report_ids)
        q = f'UPDATE radioanalyzer.music_testing_reports SET {last_data_query} being_processed = %s WHERE report_id IN %s'
        conn1.execute(q, args)
    except Exception as e:
        raise Exception(f'Failed to update report: {repr(e)}')


def get_greeting_name(user_id):
    try:
        q = 'SELECT greeting_name FROM radioanalyzer.users WHERE user_id = %s'
        with SQLConnection(1) as conn:
            conn.execute(q, (user_id, ))
            res = conn.fetchone(True)
        return res['greeting_name']
    except Exception as e:
        raise Exception(f'Failed to get greeting name: {repr(e)}')


def get_daypart_name(radio_id, daypart_id):
    try:
        q = 'SELECT daypart_text FROM radioanalyzer.song_deltas_dayparts WHERE radio_id IN (%s, -1) AND daypart_id = %s ORDER BY radio_id DESC LIMIT 1'
        with SQLConnection(1) as conn:
            conn.execute(q, (radio_id, daypart_id))
            res = conn.fetchone(True)
        return res['daypart_text']
    except Exception as e:
        raise Exception(f'Failed to get daypart name: {repr(e)}')


def get_latest_datetime(conn, radio_id, data_type, listener_group):
    q = "SELECT enabled FROM settings.radio_module_settings WHERE radio_id = %s AND module_id = 14"
    with SQLConnection(2) as conn2:
        conn2.execute(q, (radio_id,))
        res=conn.fetchone(True)
    modules_to_check = [{'module_id': 9, 'listener_group': listener_group > 0}]
    if res and res['enabled']:
        modules_to_check.append({'module_id': 14, 'listener_group': False})
    latest_data = []
    for m in modules_to_check:
        latest_data.append(get_latest_calculated_datetime(conn, m['module_id'], radio_id, data_type, m['listener_group']))
    return min(latest_data)
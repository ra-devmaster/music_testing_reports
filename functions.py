import base64
import os
import requests
from dotenv import load_dotenv
import pandas as pd
from queries import get_greeting_name, get_daypart_name

load_dotenv()


API_BASE_URL = os.environ['BASE_URL']
HEADERS = {"API-key": os.environ['API_KEY']}

def generate_attachments(songs, start_dt, end_dt, score_columns, radio_names):
    attachments = []
    radio_names_string = ("-".join(radio_names.values())).replace(' ', '_')
    loc = os.path.dirname(os.path.abspath(__file__))
    file_name = f'{loc}/attachments/{radio_names_string}_({start_dt})_to_({end_dt})'
    song_df = pd.DataFrame.from_dict(songs)
    song_df.to_csv(file_name + '.csv', header=True, index=False)
    attachments.append(file_name + '.csv')
    attachments.append(make_excel_nice(song_df, file_name, 'Music Testing', score_columns))
    html_file = open(file_name + '.html', 'w', encoding='utf-8')
    song_df.to_html(buf=html_file, classes='table table-stripped', index=False, justify='left')
    html_file.close()
    attachments.append(file_name + '.html')
    return attachments


def make_excel_nice(song_table, file_name, radio_names_string, score_columns):
    attachment = file_name + '.xlsx'
    writer = pd.ExcelWriter(attachment, engine='xlsxwriter')
    song_table.to_excel(writer, sheet_name=radio_names_string, index=False)
    workbook = writer.book
    worksheet = writer.sheets[radio_names_string]
    (max_row, max_col) = song_table.shape

    # Formats
    score_worst = workbook.add_format({'bg_color': '#e8304f', 'font_color': '#000'})
    score_worse = workbook.add_format({'bg_color': '#fa8128', 'font_color': '#000'})
    score_middle = workbook.add_format({'bg_color': '#80cbc4', 'font_color': '#000'})
    score_better = workbook.add_format({'bg_color': '#9ccc65', 'font_color': '#000'})
    score_best = workbook.add_format({'bg_color': '#388e3c', 'font_color': '#000'})

    for column in score_columns:
        col_idx = list(song_table.columns).index(column)
        for row_idx, val in enumerate(song_table[column], start=1):
            if pd.isna(val):
                worksheet.write(row_idx, col_idx, None)  # leave unformatted
            elif val < 4:
                worksheet.write(row_idx, col_idx, val, score_worst)
            elif val < 5:
                worksheet.write(row_idx, col_idx, val, score_worse)
            elif val < 6:
                worksheet.write(row_idx, col_idx, val, score_middle)
            elif val < 7:
                worksheet.write(row_idx, col_idx, val, score_better)
            elif val >= 7:
                worksheet.write(row_idx, col_idx, val, score_best)

    # Make the columns wider for clarity.
    worksheet.set_column(0, max_col - 1, 12)
    # Set the autofilter.
    worksheet.autofilter(0, 0, max_row, max_col - 1)
    # Close the Pandas Excel writer and output the Excel file.
    writer.close()
    return attachment


def create_email(job, song_dict, radio_names):
    demo = get_demographic(job.demo_id)
    daypart_name = get_daypart_name(job.radio_ids[0], job.daypart_id)
    radio_names_string = ", ".join(radio_names.values())

    greeting_name = get_greeting_name(job.user_id)

    email = {'subject': f'Music Testing on {radio_names_string} from RadioAnalyzer', 'body': f'Hi {greeting_name},<br><br>'}

    if job.min_spins > 0 and job.max_spins > 0:
        spins_text = f'between {job.min_spins} and {job.max_spins} time(s) '
    elif job.min_spins > 0 and job.max_spins == 0:
        spins_text = f'at least {job.min_spins} time(s) '
    elif job.min_spins == 0 and job.max_spins > 0:
        spins_text = f'at most {job.max_spins} time(s) '
    else:
        spins_text = 'at least 1 time(s) '

    email['body'] += 'There were no results for ' if len(song_dict) < 1 else 'Here is your '
    email['body'] += f'music testing for {job.weeks_to_check} {"week" if job.weeks_to_check == 1 else "weeks"} of data on {radio_names_string}.<br>'
    email['body'] += f'Containing songs played {spins_text}'
    email['body'] += f'for demographic {"Online:" if demo["data_type"] == 1 else "Cover:"} {demo["name"]} '
    email['body'] += f'for perspective {daypart_name}'

    email['body'] += '<br><br>Have a question about this or any other report? Reach out to us at support@radioanalyzer.com'
    email['body'] += '<br>Your RadioAnalyzer Team<br>'

    return email


def get_song_list(job):
    api_url = f'{API_BASE_URL}/music_test/?user_id={job.user_id}'
    data = {
        "radio_ids":job.radio_ids,
        "start_date":job.start_dt,
        "end_date":job.end_dt,
        "daypart_id":job.daypart_id,
        "demo_id":job.demo_id
    }
    resp = requests.post(api_url, headers=HEADERS, json=data)
    return resp.json()


def get_demographic(demo_id):
    resp = requests.get('https://api.radioanalyzerserver.dk/demographics/', headers=HEADERS).json()
    demo = next(item for item in resp if item["demo_id"] == demo_id)
    return demo


def send_api(email, subject, body, attachments):
    attachments_encoded = []
    # return True
    for a in attachments:
        file_name = a.split('/')[-1]
        with open(a, 'rb') as f:
            b64 = base64.b64encode(f.read()).decode()
            attachments_encoded.append({'content': b64, 'filename':file_name})

    data = {
        'recipients': email,
        'subject': subject,
        'body': body,
        'attachments': attachments_encoded
    }
    resp = requests.post(f'{API_BASE_URL}/emails/send_email/', headers=HEADERS, json=data)
    return resp.status_code == 204

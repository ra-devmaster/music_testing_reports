from ra_datetime_helper import start_of_the_week

from functions import *
from ra_service_helper import BackendService
from ra_service_helper.models import filter_dataclass_fields
from models import Job
from queries import *

load_dotenv()

# hardcoded_job = {'report_id': 13}

# For production, uncomment line below
hardcoded_job = None


def init_job(instance: BackendService, job: dict):
    # Crate job object from job dict
    try:
        job = Job(**filter_dataclass_fields(Job, job))
    except Exception as ex:
        ex.add_note(f"Failed to create Job from dict, {ex}")
        raise
    radio_names = ' '.join(f'{v}[{k}]' for k, v in job.radio_names.items())
    job_name = f"Generating report[{job.report_id}] for user [{job.user_id}] for radios {radio_names}"
    instance.set_job_name(job_name)
    instance.update_job_action('Starting')
    instance.check_stop_processing()

    # Return object we want to get in process_job function
    return job


def process_job(instance: BackendService, job: Job):
    dt_now = datetime.now()
    job.start_dt = start_of_the_week(dt_now - timedelta(days=7*job.weeks_to_check)).strftime('%Y-%m-%d')
    job.end_dt_date = end_of_the_week(dt_now - timedelta(days=7))
    job.end_dt = job.end_dt_date.strftime('%Y-%m-%d')

    instance.log_activity('Getting song list')
    songs = get_song_list(job)['data']

    if job.hide_invalid:
        songs = [s for s in songs if s['combined']['valid']]

    if job.max_spins > 0:
        songs = [s for s in songs if (job.max_spins >= s['combined']['spins'] >= job.min_spins)]
    else:
        songs = [s for s in songs if s['combined']['spins'] >= job.min_spins]
    if not songs or len(songs) <= 0:
        instance.log_activity('No songs found. Exiting...')
        return True
    instance.log_activity('Formatting song data')
    report_data = []
    if len(job.radio_ids) == 1:
        score_columns = ['Score']
        for song in songs:
            data = {
                'Artist': song['song']['artist'],
                'Title': song['song']['title'],
                'Release Year': song['song']['release_year'],
                'Spins': song['combined']['spins'],
                'Score': song['combined']['song_score'],
                'Burn': song['combined']['burn'],
                'Familiarity': song['combined']['familiarity_tier'],
                'Impressions': song['combined']['impressions'],
            }
            if not job.hide_deltas:
                data['Delta %'] = song['combined']['weighted_delta']
            report_data.append(data)
            sort_column = "Spins"
    else:
        score_columns = [f'{v} Score' for k, v in job.radio_names.items()]
        score_columns.append('Combined Score')
        for song in songs:
            data = {
                'Artist': song['song']['artist'],
                'Title': song['song']['title'],
                'Release Year': song['song']['release_year'],
                'Combined Spins': song['combined']['spins'],
                'Combined Score': song['combined']['song_score'],
                'Combined Burn': song['combined']['burn']
            }
            for r in job.radio_ids:
                data[f'{job.radio_names[r]} Score'] = next((x['song_score'] for x in song['individual'] if x['radio_id'] == r), None)
            report_data.append(data)
        sort_column = "Combined Spins"

    report_data = sorted(report_data, key=lambda d: d[sort_column], reverse=True)
    instance.log_activity('Generating attachments')
    attachments = generate_attachments(report_data, job.start_dt, job.end_dt, score_columns, job.radio_names)
    instance.log_activity('Creating e-mail')
    email = create_email(job, report_data, job.radio_names)
    instance.log_activity('Sending e-mail')
    email_sent = send_api(job.email_address, email['subject'], email['body'], attachments)
    instance.log_activity('Removing files')
    if email_sent:
        for a in attachments:
            os.remove(a)
    return email_sent


def on_success(instance: BackendService, job: Job):
    with SQLConnection(1) as conn:
        set_reports_being_processed(conn, [job.report_id], 0, job.end_dt)


def on_fail(instance: BackendService, job: Job):
    with SQLConnection(1) as conn:
        set_reports_being_processed(conn, [job['report_id']], -1)


worker = BackendService(hardcoded_job, init_job, process_job, on_success_func=on_success, on_failure_func=on_fail)
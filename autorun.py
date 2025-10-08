from ra_autorun import autorun_enabled, publish_and_create_autorun_jobs

from queries import *
from ra_service_helper import *

load_dotenv()
# Settings for SQL connection
override_sql_settings({
    2: {'user_name': os.environ['AUTORUN_USERNAME'], 'password': os.environ['AUTORUN_PASSWORD']},
    3: {'user_name': os.environ['AUTORUN_USERNAME'], 'password': os.environ['AUTORUN_PASSWORD']},
})


def run():
    if not autorun_enabled():
        log.error("Autorun is disabled.")
        return
    st_time = time.time()

    with SQLConnection(1) as conn1, SQLConnection(2) as conn2:
        jobs = create_message_queue_entries(conn1)
        if len(jobs):
            report_ids = [x['report_id'] for x in jobs]
            set_reports_being_processed(conn1, report_ids, 1)
            publish_and_create_autorun_jobs(jobs)

        log.info(f"Auto run completion time: {str(timedelta(seconds=round(time.time() - st_time)))}")


run()

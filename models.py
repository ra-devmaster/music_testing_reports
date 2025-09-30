from dataclasses import dataclass, asdict
from datetime import datetime
from enum import Enum

from queries import get_report_details, get_radio_names


class MarketType(Enum):
    RADIO = 0
    COMPETITOR = 1
    AIRPLAY = 2
    TAG = 3


@dataclass
class Job:
    report_id: int
    user_id: int = None
    radio_ids: list[int] = None
    weeks_to_check: int = None
    email_address: list[str] = None
    min_spins: int = None
    max_spins: int = None
    daypart_id: int = None
    demo_id: int = None
    hide_invalid: bool = None
    hide_deltas: bool = None
    start_dt: str = None
    end_dt: str = None
    end_dt_date: datetime = None
    radio_names: dict = None


    def __post_init__(self):
            self.report_id= int(self.report_id)
            if not self.user_id:
                report = get_report_details(self.report_id)
                self.report_id = int(report['report_id'])
                self.user_id = int(report['user_id'])
                self.radio_ids = [int(x) for x in str(report['radio_ids']).split(',')]
                self.weeks_to_check = int(report['weeks_to_check'])
                self.email_address = str(report['email_address']).split(',')
                self.min_spins = int(report['min_spins']) if report['min_spins'] else 0
                self.max_spins = int(report['max_spins']) if report['max_spins'] else 0
                self.demo_id = int(report['demo_id'])
                self.daypart_id = int(report['daypart_id'])
                self.hide_invalid = bool(report['hide_invalid']) if report['hide_invalid'] != None else True
                self.hide_deltas = bool(report['hide_deltas']) if report['hide_deltas'] != None else True
                self.radio_names = get_radio_names(self.radio_ids)



    def to_flow_message(self):
        # Convert the dataclass to a dictionary
        flow_message = asdict(self)
        # Return cleaned dictionary
        return flow_message

    to_dict = asdict

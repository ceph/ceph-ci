from .report import Report, ReportTimes, EventGeneric
from .report_last_contact import EventLastContact
from typing import Optional
import time

class ReportLastContactServiceQuery(Report):
    def __init__(self, agent, filter_event_id) -> None:
        super().__init__(agent, 'last_contact')

        self.filter_event_id = filter_event_id

    def compile(self) -> Optional[dict]:
        # We override compile() because this event gets a non standard generate arguments
        report_times = ReportTimes()
        self.set_headers(report_times, self.report_event_id)
        event = EventLastContactServiceQuery(self.agent).generate(report_times, self.filter_event_id)
        self.add_event(event)
        return self.data


class EventLastContactServiceQuery(EventLastContact):
    def generate(self, report_times: ReportTimes, filter_event_id):
        super().generate(report_times)

        self.data["body"]["enable_response_detail_filter"] = [filter_event_id]
        return self



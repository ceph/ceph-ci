from .report import Report, ReportTimes

class ReportURError(Report):
    def __init__(self, agent, report_event_id):
        super().__init__(agent, 'status', [])  # We assume that we dont need to send any event when reporting an error.
        self.report_event_id = report_event_id

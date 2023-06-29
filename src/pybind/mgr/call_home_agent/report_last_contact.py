from .report import Report, ReportTimes, EventGeneric
import time

class ReportLastContact(Report):
    def __init__(self, agent) -> None:
        super().__init__(agent, 'last_contact', [EventLastContact])

class EventLastContact(EventGeneric):
    def gather(self) -> dict:
        return {'last_contact': format(int(time.time()))}

    def generate(self, report_times: ReportTimes):
        super().generate('last_contact', 'ceph_last_contact', 'Last contact timestamps with Ceph cluster', report_times)

        # self.data["body"]["event_transaction_id"] = f"IBM_event_RedHatMarine_ceph_{self.agent.ceph_cluster_id}_{report_times.time_ms}_last_contact_event"  # TODO check
        self.data["body"]["context"]["messagetype"] = 1
        self.data["body"]["enable_response_detail"] = True
        self.data["body"]["enable_response_detail_filter"] = ["Unsolicited_Storage_Insights_RedHatMarine_ceph_Request"]

        self.set_content(self.gather())
        return self



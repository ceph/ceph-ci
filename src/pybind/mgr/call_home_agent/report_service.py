from .report import Report, ReportTimes, EventGeneric
from typing import Optional
import time
import json

class ReportService(Report):
    def __init__(self, agent, alerts: list) -> None:
        super().__init__(agent, 'service')
        self.alerts = alerts

    def compile(self) -> Optional[dict]:
        # We override compile() because this event gets a non standard generate() arguments
        report_times = ReportTimes()
        self.set_headers(report_times, self.report_event_id)
        event = EventService(self.agent).generate(report_times, self.alerts)
        self.add_event(event)
        self.agent.log.debug("Generated service event:")
        self.agent.log.debug(json.dumps(self.agent._filter_report(self.data)))
        return self.data

class EventService(EventGeneric):
    def generate(self, report_times: ReportTimes, alerts: list):
        super().generate('service', 'ibm_redhat_ceph_service_manager', 'Ceph service request', report_times)

        r, outb, outs = self.agent.mon_command({
            'prefix': 'status',
            'format': 'text'
        })
        note = outb
        '''
        # in case we add the new alerts to the 'notes' field:
        note += "\n\n"
        note += "Alerts:\n"
        # print(f"------------------------- [{alerts}] ---------------------")
        note += "\n".join(a.get('labels', {}).get('alertname', "Unknown alert") for a in alerts) + "\n"
        '''

        r, outb, outs = self.agent.mon_command({
            'prefix': 'versions',
            'format': 'json'
        })
        versions = json.loads(outb)

        # We use the first alert as a base for the code (subject) text of the ticket.
        # We do that by stable sorting the list of alerts.
        alerts_sorted = sorted(alerts, key = lambda d: json.dumps(d, sort_keys = True))
        first_alert = alerts_sorted[0]
        alert_name = first_alert['labels']['alertname']  # this value must exist since we sorted the original alert list by it
        alert_instance = first_alert['labels'].get('instance')  # this value might not exist
        alert_subject = alert_name + ((':' + alert_instance) if alert_instance else '')
        alert_subject = alert_subject[:140]  # Call home 'code' field is limited to 140 characters.

        # Use this in case of having body.description in addition to
        # body.payload.description as the former has a 10K characters limitation.
        # description = json.dumps(alerts_sorted, sort_keys = True, indent=4)
        # description = description[:10000]

        now = time.time()

        self.data['body'].update( {
            "customer": self.agent.icn,
            "country": self.agent.customer_country_code,
            "error_type": "software",
            "error_software_type": "distributed",
            "routing_identifier": "5900AVA00",
            "product_code_identifier": "SCSTZ",
            "record_type": 1,
            "test": False,
            "code": alert_subject,  # the title of the error that we're seeing; max 140 characters
            "note": note,  # currently the "note" field contains the output of `ceph -s`
            "context": {
                "origin": 2,
                "timestamp": int(now),  # time in seconds
                "transid": int(now * 1000)  # time in milliseconds
            },
            "object_instance_virtual_id": self.agent.ceph_cluster_id,
            "object_instance": self.agent.ceph_cluster_id,
            "object_type": "ceph",
            "object_category": "RedHat",
            "object_group": "Storage",
            "object_version": "612",  # a static value for now since it's not used
            "object_logical_name": "01t3p00000TKZPjAAP",  # hardcoded for now; can be used in the future to describe a subsystem
            "submitter_instance_virtual_id": "ceph_storage_00001",  # hardcoded for now; can be used in case of a proxy submitter, like fusion
            "submitter_instance": "infrastructure",
            "submitter_type": "ceph",
            "submitter_category": "RedHat",
            "submitter_group": "Storage",
            # for now these fields are not required:
            # "contact_email": "admin@company.com",
            # "contact_name": "John Doe",
            # "contact_organization": "Company",
            # "contact_phone": "123-456-6789",
            # "contact_address": "123 Main Street, City, CA 12345, US",
            ###############
            "payload": {
                "ceph_versions": versions,
                "description": alerts_sorted, # semi described, 10K character limit
                "error_code": alert_subject,  # the same as body.code above
                "software": {
                    "diagnostic_provided": True,
                    "ibm_ceph_version": "9.9.0.0" if self.agent.target_space == "prod" else "8.0.0"
                }
            }
        } )

        return self


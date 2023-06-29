from .report import Report, ReportTimes, EventGeneric
from .prometheus import Prometheus
from .workflow_service_events import WorkFlowServiceEvents
import time
import json
import requests
from datetime import datetime
from typing import Optional

class ReportStatusAlerts(Report):

    # Prometheus API returns all alerts. We want to send only deltas in the alerts
    # report - i.e. send a *new* alert that has been fired since the last report
    # was sent, and send a “resolved” notification when an alert is removed from
    # the prometheus API.
    # To do so we keep a list of alerts (“sent_alerts”) we have already sent, and
    # use that to create a delta report in generate_alerts_report(). The alert
    # report is not sent if there are no deltas.
    # `ceph callhome reset alerts` zeros out sent_alerts list and therefore the
    # next report will contain the relevant alerts that are fetched from the
    # Prometheus API.
    sent_alerts = {}

    def __init__(self, agent) -> None:
        super().__init__(agent, 'status')

    def compile(self) -> Optional[dict]:
        report_times = ReportTimes()
        self.set_headers(report_times)
        event = EventStatusAlerts(self.agent).generate(report_times)
        # If there are no alerts to send then return and dont send the report
        if not event.has_content:
            return None

        self.add_event(event)
        return self.data
        #self.send(report)

    @staticmethod
    def resetAlerts(mock: bool = False):
        if mock:
            # If there are no relevant alerts in the cluster, an "alerts" report will not be sent.
            # "mock" is useful in this case, to allow the user to send a dummy "alerts" report to Call Home.
            mocked_alert = {'labels': {'label': 'test'}, 'activeAt': '42', 'value': '17'}
            sent_alerts = {alert_uid(mocked_alert): mocked_alert}
        else:
            sent_alerts = {}

class EventStatusAlerts(EventGeneric):
    def generate(self, report_times: ReportTimes) -> None:
        super().generate('status', 'ceph_alerts', 'Ceph cluster alerts', report_times)

        self.data["body"]["event_transaction_id"] = f"IBM_event_RedHatMarine_ceph_{self.agent.ceph_cluster_id}_{report_times.time_ms}_status_event"
        self.data["body"]["complete"] = True

        # if the status event contains alerts we add a boolean in the body to help with analytics
        self.data["body"]["alert"] =  True
        # Call Home requires the 'state' attribute in the 'body' section
        self.data["body"]["state"] = "Ok"
        content = self.gather()
        self.has_content = bool(content)
        self.set_content(content)
        return self

    def normalize_ts(self, ts: str) -> str:
        # Normalize timestamps from Prometheus to use microseconds and a
        # '+00:00' time zone instead of nanoseconds and "Z", for example:
        # 2025-05-10T08:58:16.720197621Z   (ts from Prometheus)
        # 2025-05-10T08:58:16.720197+00:00

        # Prometheus displays time in UTC, and always ends the string with 'Z'.
        # Convert "Z" to "+00:00"
        ts = ts.replace("Z", "+00:00")

        main_ts, tz = ts.split("+")
        if '.' in ts:
            date, frac = main_ts.split(".")
            frac = frac[:6]  # truncate nanoseconds → microseconds
            main_ts = f"{date}.{frac}"

        return f"{main_ts}+{tz}"

    def service_events(self, alerts: list) -> None:
        if not self.agent.enable_service_events:
            return

        # Service Events (opening a support case)
        service_events_alerts = list(filter(self.is_alert_relevant_service_events, alerts))
        if not service_events_alerts:
            self.agent.log.debug(f"No alerts for service events")
            return

        last_service_events_sent = int(self.agent.get_store('last_service_events_sent', '0'))
        now = int(time.time())
        self.agent.log.debug(f"Now = {datetime.fromtimestamp(now).strftime('%Y-%m-%d %H:%M:%S')}, \
            last_service_events_sent = {datetime.fromtimestamp(last_service_events_sent).strftime('%Y-%m-%d %H:%M:%S')}")

        if now - last_service_events_sent < self.agent.interval_service_report_seconds:  # 60 minutes by default
            self.agent.log.debug(f"Waiting to send the next service event. Now = {now}, last_service_events_sent = {last_service_events_sent}")
            return  # don't send more than one service event per hour by default

        new_events = list(filter(
            lambda e: 'activeAt' in e and datetime.fromisoformat(self.normalize_ts(e['activeAt'])).timestamp() > last_service_events_sent,
            service_events_alerts
        ))
        if not new_events:
            self.agent.log.debug(f"Found alerts for service events, but we already handled them")
            return

        self.agent.log.debug(f"Found new alerts for service events, starting workflow")
        # We set last_service_events_sent here to avoid loopers in case of
        # issues with the backend, e.g. the events are not sent successfully
        # due to a server error, thus we keep trying to send them forever.
        # Instead, we currently try to send several times, and if it fails we
        # will try to send the active alerts after
        # interval_service_report_seconds).
        self.agent.set_store('last_service_events_sent', str(now))
        WorkFlowServiceEvents(self.agent, new_events).run()

    def gather(self) -> dict:
        all_prometheus_alerts = self.get_prometheus_alerts()
        self.agent.log.debug(f"all_prometheus_alerts: {all_prometheus_alerts}")

        self.service_events(all_prometheus_alerts)

        # Sending alerts via a dedicated status report
        # Filter the alert list
        current_alerts_list = list(filter(self.is_alert_relevant_status_alerts, all_prometheus_alerts))

        current_alerts = {self.alert_uid(a):a for a in current_alerts_list}
        # Find all new alerts - alerts that are currently active but were not sent until now (not in sent_alerts)
        new_alerts = [a for uid, a in current_alerts.items() if uid not in ReportStatusAlerts.sent_alerts]
        resolved_alerts = [a for uid, a in ReportStatusAlerts.sent_alerts.items() if uid not in current_alerts]

        ReportStatusAlerts.sent_alerts = current_alerts
        if len(new_alerts) == 0 and len(resolved_alerts) == 0:
            return None  # This will prevent the report from being sent
        alerts_to_send = {'new_alerts': new_alerts, 'resolved_alerts': resolved_alerts}
        return alerts_to_send

    def alert_uid(self, alert: dict) -> str:
        """
        Retuns a unique string identifying this alert
        """
        return json.dumps(alert['labels'], sort_keys=True) + alert['activeAt'] + alert['value']

    def is_alert_relevant_status_alerts(self, alert: dict) -> bool:
        """
        Returns True if this alert should be sent, False if it should be filtered out of the report
        """
        state = alert.get('state', '')
        severity = alert.get('labels', {}).get('severity', '')

        return state == 'firing' and severity == 'critical'

    def is_alert_relevant_service_events(self, alert: dict) -> bool:
        # This list holds the names of the alerts for which we open support cases:
        alerts_for_service = [
                        'CephMonDiskspaceCritical',
                        'CephOSDFull',
                        'CephFilesystemOffline',
                        'CephFilesystemDamaged',
                        'CephPGsInactive',
                        'CephObjectMissing',
                        ]

        return alert.get('labels', {}).get('alertname', "Unknown alert") in alerts_for_service

    def get_prometheus_alerts(self):
        """
        Returns a list of all the alerts currently active in Prometheus
        """
        try:
            prometheus = Prometheus(self.agent)
            resp = prometheus.get("alerts")
            if 'data' not in resp or 'alerts' not in resp['data']:
                raise Exception(f"Prometheus returned a bad reply: {resp}")

            alerts = resp['data']['alerts']
            return alerts
        except Exception as e:
            self.agent.log.error(f"Can't fetch alerts from Prometheus: {e}")
            return [{
                    'labels': {
                        'alertname': 'callhomeErrorFetchPrometheus',
                        'severity': 'critical'
                    },
                    'annotations': {
                        'description': str(e)
                    },
                    'state': 'firing',
                    # 'activeAt' and 'value' are here for alert_uid() to work. They should be '0' so that we won't send this alert again and again
                    'activeAt': '0',
                    'value': '0'
                }]


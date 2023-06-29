
from datetime import datetime
from typing import Optional
from .exceptions import SendError
import time
import requests
import json

class ReportTimes:
    def __init__(self, now = datetime.now()):
        self.time = now.strftime("%Y-%m-%d %H:%M:%S")
        self.time_ms = int(datetime.timestamp(now) * 1000)
        self.local_time = now.strftime("%a %b %d %H:%M:%S %Z")

class Report:
    """
    Base class for all reports
    """

    def __init__(self, agent, report_type, event_classes = []):
        """
        Args:
          agent: a reference to a CallHomeAgent object, which inherits from MgrModule
        """ 
        self.agent = agent
        self.agent.log.debug(f"Instantiating {self.__class__.__name__}, report_type={report_type}")
        self.report_type = report_type
        self.event_classes = event_classes
        self.report_event_id = None
        self.events = []

    def add_event(self, event):
        self.events.append(event)
        self.data['events'].append(event.data)

    def compile(self) -> Optional[dict]:
        report_times = ReportTimes()
        self.set_headers(report_times, self.report_event_id)
        for event_class in self.event_classes:
            event = event_class(self.agent).generate(report_times)
            self.add_event(event)

        return self.data

    def run(self) -> Optional[str]:
        compiled = self.compile()
        if compiled is None:
            return None
        return self.send(compiled)

    def set_headers(self, report_times: ReportTimes, report_event_id = None) -> None:
        try:
            secrets = self.agent.get_secrets()
        except Exception as e:
            self.agent.log.error(f"Error getting encrypted identification keys for {self.report_type} report: {e}. "
                                 "Provide keys and restart IBM Ceph Call Home module")
            secrets = {'api_key': '', 'private_key': ''}

        target_space = self.agent.target_space  # One of 'prod', 'test', 'dev'

        if not report_event_id:
            report_event_id = f"IBM_chc_event_RedHatMarine_ceph_{self.agent.ceph_cluster_id}_{self.report_type}_report_{report_times.time_ms}"
        self.report_event_id = report_event_id

        self.data = {
                "agent": "RedHat_Marine_firmware_agent",
                "api_key": secrets['api_key'],
                "private_key": secrets['private_key'],
                "target_space": target_space,
                "asset": "ceph",
                "asset_id": self.agent.ceph_cluster_id,
                "asset_type": "RedHatMarine",
                "asset_vendor": "IBM",
                "asset_virtual_id": self.agent.ceph_cluster_id,
                "country_code": self.agent.customer_country_code,
                "event_id": report_event_id,
                "event_time": report_times.time,
                "event_time_ms": report_times.time_ms,
                "local_event_time": report_times.local_time,
                "software_level": {
                    "name": "ceph_software",
                    "vrmf": self.agent.version
                },
                "type": "eccnext_apisv1s",
                "version": "1.0.0.1",
                "analytics_event_source_type": "asset_event",
                "analytics_type": "ceph",
                "analytics_instance":  self.agent.ceph_cluster_id,
                "analytics_virtual_id": self.agent.ceph_cluster_id,
                "analytics_group": "Storage",
                "analytics_category": "RedHatMarine",
                "events": []
            }

        #data.update(self._header_times(report_timestamp))  # TODO: check

    def send(self, report: dict, force: bool = False) -> str:
        resp = None
        url = self.agent.target

        if self.agent.proxies:
            self.agent.log.info(f"Sending <{self.report_type}> report to <{url}> (via proxies <{self.agent.proxies}>)")
        else:
            self.agent.log.info(f"Sending <{self.report_type}> report to <{url}>")

        try:
            resp = requests.post(url=url,
                                 headers={'accept': 'application/json', 'content-type': 'application/json'},
                                 data=json.dumps(report),
                                 proxies=self.agent.proxies,
                                 timeout=60)
            try:
                self.agent.log.debug(f"Report response: {json.dumps(self.agent._filter_report(resp.json()))}")
            except:
                self.agent.log.debug(f"Report response text: {resp.text}")

            resp.raise_for_status()

            ch_response = resp.json()
            self.agent.connectivity_update(ch_response)
        except Exception as e:
            self.agent.connectivity_update_error(e)
            raise

        try:
            self.agent.health_checks.pop('CHA_ERROR_SENDING_REPORT', None)
            last_id = report.get('event_time_ms', 'Unknown')
            self.agent.log.info(f"Successfully sent <{self.report_type}> report({last_id}) to <{url}>")
            # Process unsolicited requests, i.e. requests sent to us by Call Home embedded in the HTTP response to last_contact messages.
            # In the future we may get those in the response of other message types
            self.agent.process_response(ch_response)
            return resp.text
        except Exception as e:
            explanation = resp.text if resp else ""
            raise SendError(f"Failed to send <{self.report_type}> to <{url}>: {e} {explanation}")

# Event methods
class Event:
    def __init__(self, agent):
        self.agent = agent

    def generate(self, event_type: str, component: str, report_times: ReportTimes):
        # The below line is what was the event_id in the old code (7.1).
        # There is a problem where both [event_type="status",componet="ceph_alerts"] and [event_type="status",componet="ceph_health"]
        #   can generate the same event_id if both are generated at the same millisecond. therefore we added "{component}" to the event_id
        #self.event_event_id = f"IBM_event_RedHatMarine_ceph_{self.agent.ceph_cluster_id}_{report_times.time_ms}_{event_type}_event"
        self.event_event_id = f"IBM_event_RedHatMarine_ceph_{self.agent.ceph_cluster_id}_{report_times.time_ms}_{event_type}_{component}_event"
        self.data = {
                "header": {
                    "event_id": self.event_event_id, # "IBM_event_RedHatMarine_ceph_{}_{}_{}_event".format(ceph_cluster_id, event_time_ms, event_type),
                    "event_time": report_times.time,
                    "event_time_ms": report_times.time_ms,
                    "event_type": event_type,
                    "local_event_time": report_times.local_time # TODO check if including local_event_time also works in confirm_response and log_upload/status
                    },
                "body": {
                    "component": component,
                }
        }

        if self.agent.owner_tenant_id:  # send 'tenant_id' only if the cluster opted-in to Storage Insights.
            self.data["header"]["tenant_id"] = self.agent.owner_tenant_id
        return self

    def set_content(self, content):
        # payload may or may not exist. create it if it doesn't, append to it if it does
        self.data['body'].setdefault('payload', {})['content'] = content

class EventGeneric(Event):
    def generate(self, event_type: str, component: str, description: str, report_times: ReportTimes):
        super().generate(event_type, component, report_times)
        self.data['body'].update( {
            "context": {
                "origin": 2,
                "timestamp": report_times.time_ms,
                "transid": report_times.time_ms
            },
            "description": description,
            "payload": {
                "request_time": report_times.time_ms,
                "content": {},  # will be filled later
                "ibm_customer_number": self.agent.icn,
                "customer_country_code": self.agent.customer_country_code,
                "product_id_list" : [
                    ['5900-AVA', 'D0CYVZX'],
                    ['5900-AVA', 'D0CYWZX'],
                    ['5900-AVA', 'D0CYXZX'],
                    ['5900-AVA', 'D0DKDZX'],
                    ['5900-AVA', 'E0CYUZX'],
                    ['5900-AXK', 'D0DSJZX'],
                    ['5900-AXK', 'D0DSKZX'],
                    ['5900-AXK', 'D0DSMZX'],
                    ['5900-AXK', 'D0DSLZX'],
                    ['5900-AXK', 'E0DSIZX'],
                ],
                "jti": self.agent.jwt_jti
            }
        } )
        return self


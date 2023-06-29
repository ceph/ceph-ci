import unittest
import time
import json
import os
from collections import defaultdict
from typing import Optional

from unittest.mock import MagicMock, Mock, patch

#from call_home_agent.module import Report
from call_home_agent.module import CallHomeAgent
from call_home_agent.report_last_contact import ReportLastContact, EventLastContact
from call_home_agent.report_inventory import ReportInventory, EventInventory
from call_home_agent.report_status_alerts import ReportStatusAlerts
from call_home_agent.report_status_health import ReportStatusHealth
from call_home_agent.workflow_upload_snap import WorkFlowUploadSnap
from call_home_agent.report import Report, ReportTimes
from call_home_agent.workflow_service_events import WorkFlowServiceEvents
import mgr_module
import traceback

TEST_JWT_TOKEN = r"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJ0ZXN0IiwiaWF0IjoxNjkxNzUzNDM5LCJqdGkiOiIwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAwMTIzNDU2Nzg5MCJ9.0F66k81_PmKoSd9erQoxnq73760SXs8WQTd3s8pqEFY\\"
EXPECTED_JTI = '01234567890123456789001234567890'

JWT_REG_CREDS_DICT = {"url": "test.icr.io", "username": "test_username", "password": TEST_JWT_TOKEN}
JWT_REG_CREDS =json.dumps(JWT_REG_CREDS_DICT)
PLAIN_PASSWORD_REG_CREDS_DICT = {"url": "test.icr.io", "username": "test_username", "password": "plain_password"}

class MockedMgr():
    class Log:
        def error(self, msg):
            print(msg)

        def warning(self, msg):
            print(msg)

        def info(self, msg):
            print(msg)

        def debug(self, msg):
            #print(msg)
            pass

        def exception(self, msg):
            print(msg)

    class HealthChecks:
        def pop(self, what, something):
            pass

    def __init__(self, *args, **kwargs):
        self.version = '99.9'
        self.log = self.Log()
        self.health_checks = self.HealthChecks()

    def get(self, what):
        simple_commands = ["osd_map_crush", "devices", "df", "fs_map", "mgr_map", "osd_map_tree", "osd_metadata", "osd_map", "pg_summary", "service_map"]
        if what == 'mon_map':
            return {'fsid': 'mocked_fsid'}
        elif what == 'health':
            return {'json': json.dumps({'health': 'mocked health text'})}
        elif what in simple_commands:
            return {what: f"mocked {what}"}
        else:
            raise Exception(f"Unknown get what [{what}], please mock it")

    def list_servers(self):
        return ['mock_serverA', 'mock_serverB']

    def mon_command(self, command):
        if command['prefix'] == 'status':
            if command['format'] == 'text':
                return 0, "\nceph_status:\n  cluster:\n    id:     4a7851de-8b13-11ef-85fe-525400f89c16\n    health: HEALTH_WARN\n            mon y789-node-00 is low on available space\n\n  services:\n    mon: 3 daemons, quorum y789-node-00,y789-node-02,y789-node-01 (age 11d)\n    mgr: y789-node-00.bxrhew(active, since 9d), standbys: y789-node-02.gbigcx\n    osd: 3 osds: 3 up (since 3M), 3 in (since 3M)\n\n  data:\n    pools:   1 pools, 1 pgs\n    objects: 2 objects, 449 KiB\n    usage:   328 MiB used, 15 GiB / 15 GiB avail\n    pgs:     1 active+clean", ""
            else:
                return 0, json.dumps({'health': {'status': 'mocked health status  mon_cmd'}}), ""
        elif command['prefix'] == 'versions' and command['format'] == 'json':
            return 0, json.dumps({"mon":{"ceph version 19.2.1-52.el9cp (dc69009c814f9f71fd831a91c591b9da1df69ffb) squid (stable)":3},"mgr":{"ceph version 19.2.1-52.el9cp (dc69009c814f9f71fd831a91c591b9da1df69ffb) squid (stable)":2},"osd":{"ceph version 19.2.1-52.el9cp (dc69009c814f9f71fd831a91c591b9da1df69ffb) squid (stable)":3},"overall":{"ceph version 19.2.1-52.el9cp (dc69009c814f9f71fd831a91c591b9da1df69ffb) squid (stable)":8}}), ""
        else:
            raise Exception(f"Unknown mon_command {command}")

    def remote(self, component, command, service_name=None, hostname=None, sos_params=None):
        m = MagicMock()
        m.exception_str = ''
        if command in ['list_daemons', 'get_hosts']:
            #attrs = {'hostname': 'daemon_hostname'}
            m.result = [Mock(hostname='daemon_hostname', labels=['_admin','meow'], ip='4.3.2.1', ports=[42])]
            return m
        elif command == 'sos':
            m.result = ['sosreport_case_part1 sosreport_case_part2 sosreport_case_part3']
            return m
        else:
            m.result = 'mocked hw status'
            return m

    def get_module_option(self, opt_name, default=None):
        for opt in self.MODULE_OPTIONS:
            if opt['name'] == opt_name:
                return opt['default']
        raise Exception(f"EEEEEEEEEEEEEEEEEEEEEEEEEE Can't find Option name {opt_name}")

    def get_store(self, opt_name, default=None):
        # if default is None:
        #     raise Exception(f"EEEEEEEEEEEEEEEEEEEEEEEEEE Mocked get_store requires default")
        return default

    def set_store(self, opt_name, val):
        pass

    def set_health_checks(self, val):
        pass

    def shutdown(self):
        pass

def mocked_ceph_command(self, srv_type, prefix, key=None, mgr=None, detail=None):
    if prefix == 'config-key get':
        if key == 'mgr/cephadm/registry_credentials':
            return JWT_REG_CREDS
        else:
            raise Exception(f"Unknown ceph command [{prefix}], key=[{key}], please mock it")
    elif prefix in ['status', 'health', 'osd tree', 'report', 'osd dump', 'df']:
        return f"mocked_ceph_command {prefix}"
    else:
        raise Exception(f"Unknown ceph command [{prefix}], please mock it")


original_time_time = time.time
test_object = None
debug = True
verbose = True

def mock_glob(pattern: str):
    print(f"mock_glob: globbing {pattern}")
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return [f"{current_dir}/testfile1", f"{current_dir}/testfile2", f"{current_dir}/testfile3"]

def prometheus_make_alert(name: str, activeAt: Optional[str] = None) -> dict:
    return {
            'labels': {
                'alertname': name,
                'severity': 'critical'
            },
            'annotations': {
                'description': "some alert"
            },
            'state': 'firing',
            # 'activeAt' and 'value' are here for alert_uid() to work. They should be '0' so that we won't send this alert again and again
            'activeAt': activeAt if activeAt else "2025-08-15T10:12:13.123Z",
            'value': '0'
    }

#@patch('mgr_module.MgrModule.version', '99.9')
class TestAgent(unittest.TestCase):

    ########################### Time handling ############################
    def mocked_time_time(self):
        if self.test_end and self.mocked_now > self.test_end:
            self.agent.shutdown()

        debug and print(f"#### mocked_now is {self.mocked_now}")
        return self.mocked_now

    def mocked_sleep(self, seconds):
        debug and print(f"#### mocked_sleep for {seconds} seconds")
        self.mocked_now += seconds
        #print("".join(traceback.format_stack()))

    ########################### HTTP requests ############################
    def mocked_requests_get(self, url, auth=None, data=None, headers=None, proxies=None, params=None):
        """
        Used by ReportStatusAlerts to query prometheous
        """
        m = Mock()
        if "api/v1/query" in url:
            # This is a request for Prometheous
            m.json.return_value = {'data': {'result': [{'value': "1234"}] }}
        elif "api/v1/targets" in url:
            m.json.return_value = {'data': {'activeTargets': [{'health': "up"}] }}
        else:
            m.json.return_value = {'data': {'alerts': self.prometheus_alerts }}
        return m

    def mocked_requests_post(self, url, auth=None, data=None, headers=None, proxies=None, timeout=None):
        print("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv request.post vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv")
        print(f"  URL: {url}")
        print(f"  now: {test_object.mocked_now if test_object.mocked_now is not None else 'None'}")
        event_type = None
        if data:
            try:
                data_dict = json.loads(data)
                pretty = json.dumps(data_dict, indent=4)
                try:
                    event_type = data_dict['events'][0]['header']['event_type']
                    if event_type == 'confirm_response':
                        component = 'NA'
                    else:
                        component = data_dict['events'][0]['body']['component']
                    print(f"  event_type={event_type}  component={component}")
                    self.sent_events[f"{event_type}-{component}"] += 1  # so we can assertEqual on it later
                except:
                    print('Data does not contain an event type')
                verbose and print(f"Data: {pretty}")
            except:
                print(f"Data: {data}")
        print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")

        m = Mock()
        m.raise_for_status.return_value = None
        m.text = json.dumps(self.requests_post_response)
        m.json.return_value = json.loads(m.text)
        if 'upload_tid' in url:  # ecurep
            m.json.return_value = {'id': 'upload_tid123'}
            return m
        elif 'upload_sf' in url:  # ecurep
            return m
        elif 'esupport' in url:  # call home
            if event_type == 'last_contact':
                if data_dict["events"][0]["body"]["enable_response_detail_filter"] == ["Unsolicited_Storage_Insights_RedHatMarine_ceph_Request"]:
                    if self.mock_requests_send_has_ur:
                        self.mock_requests_send_has_ur -= 1
                        print("mocked_requests_post(): Returning yes UR")
                        m.text = self.mocked_last_contact_response_yes_ur
                        if self.mock_requests_cooldown_pmr:
                            # replace the pmr so that it will be different UR for stale, but same for cooldown
                            new_pmr = self.mock_requests_cooldown_pmr.pop()
                            m.text = self.mocked_last_contact_response_yes_ur.replace('TS1234567', new_pmr)
                            print(f"####### replacing PMR to {new_pmr}")
                    else:
                        m.text = self.mocked_last_contact_response_no_ur
                    m.json.return_value = json.loads(m.text)
                else:
                    # Currently we only have two types of last_contact - one with a filter for unsolicited requests
                    # and one with a filter for a specific event_id, part of the service events workflow.

                    # The WorkFlowServiceEvents code retries querying for the service creation 10 times.
                    # we'll mock 3 replies of "not ready yet" before returning a good reply with the ticket's problem_id
                    if self.mock_service_event_no_case_opened_replies > 0:
                        # Return "ticket not opened yet" for 3 tries".
                        self.mock_service_event_no_case_opened_replies -= 1
                        m.json.return_value = {'response_state' : {'transactions': { } } }
                        m.text = json.dumps(m.json.return_value)
                    else:
                        # Return ticket opened, with the ticket ID (T1234) nested correctly.
                        event_id =  next(iter((data_dict["events"][0]["body"]["enable_response_detail_filter"])))  # Get the first key of the dict. This should be the event_id.
                        m.json.return_value = {'response_state' : {'transactions': { event_id : {'response_object': {'problem_id': 'T1234'} } } } }
                        m.text = json.dumps(m.json.return_value)

            elif event_type == 'service':
                print(f"Got service event")
                m.json.return_value = { 'response_state': 123 }
                m.text = json.dumps(m.json.return_value)
            return m
        else:
            raise Exception(f"Unknown mocked_requests_post URL [{url}], please mock it")
    ######################################################################

    def mock_mgr(self):

        CallHomeAgent.__bases__ = (MockedMgr,)
        #patch('mgr_module.MgrModule.version', '99.9').start()
        patch('call_home_agent.module.CallHomeAgent.ceph_command', mocked_ceph_command).start()
        #patch('call_home_agent.WorkFlowUploadSnap.DIAGS_FOLDER', '/tmp').start()
        patch('call_home_agent.workflow_upload_snap.DIAGS_FOLDER', '/tmp').start()
        patch('call_home_agent.module.CallHomeAgent.get_secrets',
              return_value={'api_key': 'mocked_api_key',
                            'private_key': 'mocked_private_key',
                            'ecurep_transfer_id': 'mocked_ecurep_transfer_id',
                            'ecurep_password': 'mocked_ecurep_password'}
              ).start()


        patch('requests.post', self.mocked_requests_post).start()
        patch('requests.get', self.mocked_requests_get).start()
        patch('glob.glob', mock_glob).start()
        patch('os.remove', Mock()).start()
        patch('os.path.getsize', Mock(return_value=42)).start()

    def setUp(self):
        self.mock_mgr()
        global test_object
        test_object = self

        ####### time handling #######
        #self.mocked_now = original_time_time()
        self.mocked_now = 0
        self.test_end = None

        ####### HTTP requests handling #######
        self.mocked_last_contact_response_no_ur = None
        self.mocked_last_contact_response_yes_ur = None
        self.mock_requests_send_has_ur = 0
        self.mock_requests_cooldown_pmr = []
        self.sent_events = defaultdict(int)
        self.mock_service_event_no_case_opened_replies = 3
        self.prometheus_alerts = []

        # Load the json answers that requests.post() should return
        with open(os.path.dirname(__file__) + '/response_no_pending_ur.json', 'r') as resp:
            self.mocked_last_contact_response_no_ur = resp.read()

        with open(os.path.dirname(__file__) + '/response_yes_pending_ur.json', 'r') as resp:
            self.mocked_last_contact_response_yes_ur = resp.read()

        self.requests_post_response = {'some': 'answer'}

    def test_reports(self):
        agent = CallHomeAgent()
        self.agent = agent
        ReportInventory(agent).run()
        ReportLastContact(agent).run()
        # ReportStatusAlerts: We dont fully mock the returned json to get_prometheus_alerts(), therefore
        #   it raises an exception, catches it, and generates a "Can't read from prometheus" health alert.
        ReportStatusAlerts(agent).run()
        ReportStatusHealth(agent).run()

    def test_last_contact_no_ur(self):
        agent = CallHomeAgent()
        self.agent = agent
        ReportLastContact(agent).run()
        self.assertEqual(len(agent.ur_queue), 0)
        self.assertEqual(self.sent_events['status-ceph_log_upload'], 0)
        self.assertEqual(self.sent_events['confirm_response-NA'], 0)

    def test_last_contact_yes_ur(self):
        agent = CallHomeAgent()
        self.agent = agent
        self.mock_requests_send_has_ur = True
        ReportLastContact(agent).run()
        self.assertEqual(self.sent_events['status-ceph_log_upload'], 4)
        self.assertEqual(self.sent_events['confirm_response-NA'], 1)
        self.assertEqual(len(agent.ur_queue), 0)
        self.assertEqual(len(agent.ur_stale), 1)
        self.assertEqual(len(agent.ur_cooldown), 1)

    def test_serve_and_stale(self):
        with patch('time.time', side_effect=self.mocked_time_time), patch('time.sleep', side_effect=self.mocked_sleep), patch('threading.Event.wait', side_effect=self.mocked_sleep):
            agent = CallHomeAgent()

            self.test_end = self.mocked_time_time() + 86000  # a bit less than a day
            self.mock_requests_send_has_ur = 2
            self.agent = agent
            # This test checks the UR stale mechanism by running the agent for enough time for a stale message to be old enough to be deleted
            # from the stale memory. By default, stale_timeout is 10 days but running this test, emulating 10 days takes too much time
            # so we change the stale_timeout to 1 day so it will be quicker.
            agent.stale_timeout = 86400
            agent.serve()

            self.assertEqual(len(agent.ur_queue), 0)
            self.assertEqual(len(agent.ur_stale), 1) # 1 if less than 24 hours. 0 if more
            self.assertEqual(len(agent.ur_cooldown), 0)
            self.assertEqual(self.sent_events['confirm_response-NA'], 1)

            self.mocked_sleep(1000) # now we're past 24 hours since start

            self.test_end = self.mocked_time_time() + 43200  # half a day
            self.mock_requests_send_has_ur = 2
            agent.run = True
            agent.serve()

            self.assertEqual(self.sent_events['confirm_response-NA'], 2)
            #self.assertEqual(1, 0)

    def test_serve_and_cooldown(self):
        with patch('time.time', side_effect=self.mocked_time_time), patch('time.sleep', side_effect=self.mocked_sleep), patch('threading.Event.wait', side_effect=self.mocked_sleep):
            agent = CallHomeAgent()

            self.test_end = self.mocked_time_time() + 86000  # a bit less than a day
            self.mock_requests_send_has_ur = 2
            self.mock_requests_cooldown_pmr = ['TS1234567', 'TS1234568']
            self.agent = agent
            agent.serve()

            self.assertEqual(len(agent.ur_queue), 0)
            self.assertEqual(len(agent.ur_stale), 2) # as we got 2 different PMRs, we have 2 for stale.
            self.assertEqual(len(agent.ur_cooldown), 0)
            self.assertEqual(self.sent_events['confirm_response-NA'], 2)

    def test_report_multiple_events(self):
        class ReportMultiple(Report):
            def __init__(self, agent, event_classes) -> None:
                super().__init__(agent, 'test_multiple', event_classes)

        agent = CallHomeAgent()
        self.agent = agent
        ReportMultiple(agent, [EventInventory, EventLastContact]).run()
        #self.assertEqual(1, 0)

    def test_cli_print_report_cmd(self):
        agent = CallHomeAgent()
        ret = agent.cli_show('status')
        
        print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
        print(ret.stdout)
        print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
        self.assertFalse('private_key' in ret.stdout)
        self.assertFalse('api_key' in ret.stdout)
        self.assertTrue('target_space' in ret.stdout)

    def test_cli_send_report_cmd(self):
        agent = CallHomeAgent()
        ret = agent.cli_send('status')
        
        print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
        print(ret.stdout)
        print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
        self.assertEqual(ret.stdout, 'status report sent successfully:\n{\n    "some": "answer"\n}')

    def test_cli_upload_diagnostics(self):
        agent = CallHomeAgent()
        ret = agent.cli_upload_diagnostics('ticket123', 1)
        
        print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
        print(ret.stdout)
        print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
        self.assertEqual(ret.stdout, 'Success')

    def test_cli_list_queues(self):
        agent = CallHomeAgent()
        self.mock_requests_send_has_ur = 2
        self.mock_requests_cooldown_pmr = ['TS1234567', 'TS1234568']
        ReportLastContact(agent).run()
        ReportLastContact(agent).run()
        ret = agent.cli_list_queues()
        
        print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
        print(ret.stdout)
        print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
        self.assertTrue('upload_snap-3-TS1234568-' in ret.stdout)

    def test_connectivity_status(self):
        agent = CallHomeAgent()
        status = agent.get_call_home_status()
        self.assertEqual(status['connectivity'], False)

        # Send message, expect an error because it's not returning the correct json fields
        agent.test_connectivity()
        status = agent.get_call_home_status()
        self.assertEqual(status['connectivity'], False)
        self.assertEqual(status['connectivity_error'], 'Bad response from Call Home: {\n    "some": "answer"\n}')

        self.requests_post_response = {"service": "ibm_callhome_connect", "more": "info"}
        agent.test_connectivity()
        status = agent.get_call_home_status()
        self.assertEqual(status['connectivity'], True)
        self.assertEqual(status['connectivity_error'], 'Success')

    def test_service_event(self):
        with patch('time.time', side_effect=self.mocked_time_time), patch('time.sleep', side_effect=self.mocked_sleep), patch('threading.Event.wait', side_effect=self.mocked_sleep):
            self.mock_service_event_no_case_opened_replies = 3
            agent = CallHomeAgent()
            self.agent = agent
            WorkFlowServiceEvents(agent, [prometheus_make_alert('alert_a'), prometheus_make_alert('alert_b')]).run()
            #ReportLastContact(agent).run()

            self.test_end = self.mocked_time_time() + 15 * 60
            agent.serve()

            self.assertEqual(self.sent_events['status-ceph_log_upload'], 0)
            self.assertEqual(self.sent_events['confirm_response-NA'], 1)
            self.assertEqual(len(agent.ur_queue), 0)
            self.assertEqual(len(agent.ur_stale), 0)
            self.assertEqual(len(agent.ur_cooldown), 0)

    def test_service_event_no_case_opened(self):
        with patch('time.time', side_effect=self.mocked_time_time), patch('time.sleep', side_effect=self.mocked_sleep), patch('threading.Event.wait', side_effect=self.mocked_sleep):
            self.mock_service_event_no_case_opened_replies = 60 * 60  # Greater than the number of tries that the WorkFlowServiceEvents tries.
            agent = CallHomeAgent()
            self.agent = agent
            WorkFlowServiceEvents(agent, [prometheus_make_alert('alert_a'), prometheus_make_alert('alert_b')]).run()
            #ReportLastContact(agent).run()

            self.test_end = self.mocked_time_time() + 60 * 60
            agent.serve()

            self.assertEqual(self.sent_events['status-ceph_log_upload'], 0)
            self.assertEqual(self.sent_events['confirm_response-NA'], 0)
            self.assertEqual(self.sent_events['last_contact-ceph_last_contact'], 13)  # 10 from the WorkFlowServiceEvents, 3 from generic last_contact messages
            self.assertEqual(len(agent.ur_queue), 0)
            self.assertEqual(len(agent.ur_stale), 0)
            self.assertEqual(len(agent.ur_cooldown), 0)

    def test_service_event_mixed_prom_alerts(self):
        # a mix of relevant and non relevant prometheus alerts

        with patch('time.time', side_effect=self.mocked_time_time), patch('time.sleep', side_effect=self.mocked_sleep), patch('threading.Event.wait', side_effect=self.mocked_sleep):
            self.mock_service_event_no_case_opened_replies = 3
            self.prometheus_alerts = [ prometheus_make_alert("some_alert_1"), prometheus_make_alert("CephOSDFull"),
                prometheus_make_alert("CephObjectMissing"), prometheus_make_alert("some_alert_2") ]

            # we wait 1 hour between service_events. we test it by now - last_sent. last_sent == 0, therefore
            # now must be at least 3600 for the test to work. so we changed our mocked time to >3600
            self.mocked_now = 4000

            agent = CallHomeAgent()
            self.agent = agent
            ReportStatusAlerts(agent).run()

            self.test_end = self.mocked_time_time() + 15 * 60
            #agent.serve()

            self.assertEqual(self.sent_events['status-ceph_log_upload'], 0)
            self.assertEqual(self.sent_events['confirm_response-NA'], 1)
            self.assertEqual(len(agent.ur_queue), 0)
            self.assertEqual(len(agent.ur_stale), 0)
            self.assertEqual(len(agent.ur_cooldown), 0)



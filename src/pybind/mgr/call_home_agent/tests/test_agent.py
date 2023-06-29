import unittest
import time
import json

from unittest.mock import MagicMock, Mock, patch

from call_home_agent.module import Report

TEST_JWT_TOKEN = r"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJ0ZXN0IiwiaWF0IjoxNjkxNzUzNDM5LCJqdGkiOiIwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAwMTIzNDU2Nzg5MCJ9.0F66k81_PmKoSd9erQoxnq73760SXs8WQTd3s8pqEFY\\"
EXPECTED_JTI = '01234567890123456789001234567890'

JWT_REG_CREDS_DICT = {"url": "test.icr.io", "username": "test_username", "password": TEST_JWT_TOKEN}
JWT_REG_CREDS =json.dumps(JWT_REG_CREDS_DICT)
PLAIN_PASSWORD_REG_CREDS_DICT = {"url": "test.icr.io", "username": "test_username", "password": "plain_password"}


def fake_content(mgr):
    return {'inventory': {}}

def get_test_manager():
    test_mgr = MagicMock()
    test_mgr.get = Mock(return_value={'fsid': '12345'})
    test_mgr.get_store = MagicMock(return_value=None)

    test_mgr.version = "1"
    test_mgr.target_space = "dev"
    test_mgr.valid_container_registry = r"^.+\.icr\.io"
    test_mgr.send_command = Mock(return_value=JWT_REG_CREDS)
    return test_mgr

class TestReport(unittest.TestCase):
    def setUp(self):
        """ A test report is created to be used in each test
        """
        testMgr = get_test_manager()
        self.patcher = patch('call_home_agent.dataDicts.get_settings')
        self.mock_settings = self.patcher.start()

        self.mock_settings.return_value = {'api_key': b'api_key',
                                      'private_key': b'private_key'}

        self.report = Report('inventory',
                            'ceph_inventory',
                            'Ceph cluster composition',
                            'AB54321',
                            'ibm_tenant_id',
                            fake_content,
                            "http://chesurl.com",
                            "",
                            15,
                            testMgr)

    @patch('call_home_agent.dataDicts.ceph_command')
    def test_content(self, mock_ceph_command):

        """ Verify if some strategic fields contains the right info
        """
        mock_ceph_command.return_value = JWT_REG_CREDS

        report = self.report.generate_report()

        # header fields
        self.assertEqual(report['agent'], "RedHat_Marine_firmware_agent")
        self.assertNotEqual(report['api_key'], "")
        self.assertNotEqual(report['private_key'], "")
        self.assertEqual(report['asset'], "ceph")
        self.assertEqual(report['analytics_event_source_type'], "asset_event")
        self.assertEqual(report['analytics_group'], "Storage")
        self.assertEqual(report['analytics_category'], "RedHatMarine")

        # events list
        events = report['events']
        self.assertEqual(len(events), 1)

        # event details
        event = events[0]
        self.assertTrue('header' in event.keys())
        self.assertTrue('body' in event.keys())
        self.assertEqual(event['header']['event_type'], self.report.report_type)
        self.assertEqual(event['header']['tenant_id'], 'ibm_tenant_id')
        self.assertEqual(event['body']['component'], 'ceph_inventory')

        # event payload not empty
        self.assertEqual(event['body']['payload']['content'], fake_content(MagicMock()))

    @patch('call_home_agent.dataDicts.ceph_command')
    def test_jti_from_jwt(self, mock_ceph_command):
        """ Extract jwt unique identifier from container registry
        JWT user password
        """
        mock_ceph_command.return_value = JWT_REG_CREDS
        report = self.report.generate_report()
        event = report['events'][0]
        self.assertEqual(event['body']['payload']['jti'], EXPECTED_JTI)

    @patch('call_home_agent.dataDicts.ceph_command')
    def test_jti_from_jwt_not_available(self, mock_ceph_command):
        """ Not able to extract jwt unique identifier from container registry
            JWT user password.
            Or the registry url is not the expected one
        """
        # password is not a JWT token
        testMgr = get_test_manager()
        mock_ceph_command.return_value = json.dumps(PLAIN_PASSWORD_REG_CREDS_DICT)
        report = Report('inventory',
                        'ceph_inventory',
                        'Ceph cluster composition',
                        'AB54321',
                        'ibm_tenant_id',
                        fake_content,
                        "http://chesurl.com",
                        "",
                        15,
                        testMgr)
        report_dict = report.generate_report()
        event = report_dict['events'][0]
        self.assertEqual(event['body']['payload']['jti'], "")

        # Url does not match the accepted registry url pattern
        JWT_REG_CREDS_DICT['url'] = "quay.io/user"
        mock_ceph_command.return_value = json.dumps(JWT_REG_CREDS_DICT)
        report = Report('inventory',
                        'ceph_inventory',
                        'Ceph cluster composition',
                        'AB54321',
                        'ibm_tenant_id',
                        fake_content,
                        "http://chesurl.com",
                        "",
                        15,
                        testMgr)
        report_dict = report.generate_report()
        event = report_dict['events'][0]
        self.assertEqual(event['body']['payload']['jti'], "")

    @patch('call_home_agent.dataDicts.ceph_command')
    def test_valid_registry_urls_for_jti(self, mock_ceph_command):
        testMgr = get_test_manager()
        test_credentials = JWT_REG_CREDS_DICT
        for test_url in ["cp.icr.io", "cp.icr.io/cp", "cp.stg.icr.io", "cp.stg.icr.io/cp"]:
            test_credentials["url"] = test_url
            mock_ceph_command.return_value = json.dumps(test_credentials)
            report = Report('inventory',
                            'ceph_inventory',
                            'Ceph cluster composition',
                            'AB54321',
                            'ibm_tenant_id',
                            fake_content,
                            "http://chesurl.com",
                            "",
                            15,
                            testMgr)
            report_dict = report.generate_report()
            event = report_dict['events'][0]
            self.assertEqual(event['body']['payload']['jti'], EXPECTED_JTI)

    @patch('requests.post')
    def test_send(self, mock_post):
        """ Send the report properly implies to update the last_upload attribute
        """
        t = int(self.report.last_upload)
        self.report.send()
        self.assertGreaterEqual(int(self.report.last_upload), t)

    @patch('requests.post')
    @patch('call_home_agent.dataDicts.ceph_command')
    def test_communication_error(self, mock_ceph_cmd, mock_post):
        """Any kind of error executing the "POST" will be raised
        """
        mock_ceph_cmd.return_value = {}
        mock_post.side_effect=Exception('COM Error')
        self.report.interval = 60
        self.report.last_upload = str(int(time.time()) - 90)

        with self.assertRaises(Exception) as context:
            self.report.send()
        self.assertTrue('COM Error' in str(context.exception))

    @patch('requests.post')
    def test_not_time_to_send(self, mock_post):
        """A report only can be sent when the time to send the report arrives
        """
        self.report.interval = 60
        self.report.last_upload = str(int(time.time()))
        self.report.send()
        mock_post.assert_not_called()

    @patch('requests.post')
    @patch('call_home_agent.dataDicts.ceph_command')
    def test_not_time_to_send_but_forced(self, mock_ceph_cmd, mock_post):
        """A report only can be sent when the time to send the report arrives,
           except if you force the operation
        """
        mock_ceph_cmd.return_value = {}
        self.report.interval = 60
        self.report.last_upload = str(int(time.time()))
        self.report.send(force=True)
        mock_post.assert_called()

    def tearDown(self):
        self.patcher.stop()

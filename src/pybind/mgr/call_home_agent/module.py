from .report import Report
from .report_last_contact import ReportLastContact
from .workflow_upload_snap import WorkFlowUploadSnap
from .report_ur_error import ReportURError
from .report_inventory import ReportInventory
from .report_status_alerts import ReportStatusAlerts
from .report_status_health import ReportStatusHealth
from .report_performance import ReportPerformance

from mgr_module import MgrModule, Option, HandleCommandResult, CommandResult

from typing import Optional, Tuple, Any
import datetime
import json
import os
import sys
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import base64
import re
import jwt
import traceback
import requests
import sched
import time
#from threading import Event
import threading
import importlib.util
import pathlib

from .cli import CallHomeCLICommand

class URUploadSnap:
    def __init__(self, agent, req: dict):
        self._req = req
        self.agent = agent
        self._options = self._req.get('options', {})

    def id(self) -> str:
        """
        An ID that uniquely represents this UploadSnap request.
        Currently we use this as the key for ur_stale.
        """
        return f"{self._req.get('operation', '')}-{self._options.get('level', '')}-{self._options.get('pmr', '')}-{self._options.get('si_requestid', '')}"

    def id_for_cooldown(self) -> str:
        return f"{self._req.get('operation', '')}-{self._options.get('level', '')}"

    def cooldown_timeout(self) -> int:
        """
        level 2 is SOS report which is much heavier than level 1. for SOS report, dont allow more than once every 2 hours
        for level 1, once every 5 minutes
        """
        if 'level' in self._options and int(self._options['level']) > 1:
            return self.agent.cooldown_timeout_upload_snap_2
        return self.agent.cooldown_timeout_upload_snap_1

class CallHomeAgent(MgrModule):
    """
    Provides MgrModule interface and central services for "Report" derived classes
    """
    CLICommand = CallHomeCLICommand
    # Env vars (if they exist) have preference over module options
    MODULE_OPTIONS: list[Option] = [
        Option(
            name='target',  # call home URL
            type='str',
            default = os.environ.get('CHA_TARGET', 'https://esupport.ibm.com/connect/api/v1'),
            desc='Call Home endpoint'
        ),
        Option(
            name='interval_inventory_report_seconds',
            type='int',
            min=0,
            default = int(os.environ.get('CHA_INTERVAL_INVENTORY_REPORT_SECONDS', 60 * 60 * 24)),  # one day
            desc='Time frequency for the inventory report'
        ),
        Option(
            name='interval_performance_report_seconds',
            type='int',
            min=0,
            default = int(os.environ.get('CHA_INTERVAL_PERFORMANCE_REPORT_SECONDS', 60 * 5)),  # 5 minutes
            desc='Time frequency for the performance report'
        ),
        Option(
            name='interval_status_report_seconds',
            type='int',
            min=0,
            default = int(os.environ.get('CHA_INTERVAL_STATUS_REPORT_SECONDS', 60 * 30)),  # 30 minutes
            desc='Time frequency for the status report'
        ),
        Option(
            name='interval_last_contact_report_seconds',
            type='int',
            min=0,
            default = int(os.environ.get('CHA_INTERVAL_LAST_CONTACT_REPORT_SECONDS', 60 * 30)),  # 30 minutes
            desc='Time frequency for the last contact report'
        ),
        Option(
            name='interval_alerts_report_seconds',
            type='int',
            min=0,
            default = int(os.environ.get('CHA_INTERVAL_ALERTS_REPORT_SECONDS', 60 * 5)),  # 5 minutes
            desc='Time frequency for the alerts report'
        ),
        Option(
            name='interval_service_report_seconds',
            type='int',
            min=0,
            default = int(os.environ.get('CHA_INTERVAL_SERVICE_REPORT_SECONDS', 60 * 60)),  # 60 minutes
            desc='Time frequency for checking for new alerts for service events'
        ),
        Option(
            name='customer_email',
            type='str',
            default='',
            desc='Customer contact email'
        ),
        Option(
            name='icn',
            type='str',
            default='',
            desc='IBM Customer Number'
        ),
        Option(
            name='customer_first_name',
            type='str',
            default='',
            desc='Customer first name'
        ),
        Option(
            name='customer_last_name',
            type='str',
            default='',
            desc='Customer last name'
        ),
        Option(
            name='customer_phone',
            type='str',
            default='',
            desc='Customer phone'
        ),
        Option(
            name='customer_company_name',
            type='str',
            default='',
            desc='Customer phone'
        ),
        Option(
            name='customer_address',
            type='str',
            default='',
            desc='Customer address'
        ),
        Option(
            name='customer_country_code',
            type='str',
            default='',
            desc='Customer country code'
        ),
        Option(
            name='owner_tenant_id',
            type='str',
            default="",
            desc='IBM tenant Id for IBM Storage Insigths'
        ),
        Option(
            name='owner_ibm_id',
            type='str',
            default="",
            desc='IBM w3id identifier for IBM Storage Insights'
        ),
        Option(
            name='owner_company_name',
            type='str',
            default="",
            desc='User Company name for IBM storage Insights'
        ),
        Option(
            name='owner_first_name',
            type='str',
            default="",
            desc='User first name for IBM storage Insights'
        ),
        Option(
            name='owner_last_name',
            type='str',
            default="",
            desc='User last name for IBM storage Insights'
        ),
        Option(
            name='owner_email',
            type='str',
            default="",
            desc='User email for IBM storage Insights'
        ),
        Option(
            name='proxy',
            type='str',
            default = os.environ.get('CHA_PROXY', ''),
            desc='Proxy to reach Call Home endpoint'
        ),
        Option(
            name='target_space',
            type='str',
            default = os.environ.get('CHA_TARGET_SPACE', 'prod'),  # Set to 'dev'/'test' for development/testing
            desc='Target space for reports (dev, staging or production)'
        ),
        Option(
            name='si_web_service_url',
            type='str',
            default = os.environ.get('CHA_SI_WEB_SERVICE_URL', 'https://join.insights.ibm.com/api/v1/em-integration'),
            desc='URL used to register Ceph cluster in SI (staging or production)'
        ),
        Option(
            name='valid_container_registry',
            type='str',
            default=r'^.+\.icr\.io',
            desc='Container registry pattern for urls where cephadm credentials(JWT token) are valid'
        ),
        Option(
            name='ecurep_url',
            type='str',
            default='https://www.ecurep.ibm.com',
            desc='ECuRep file exchange systems'
        ),
        Option(
            name='ecurep_userid',
            type='str',
            default="",
            desc='Userid obtained from the IBM Transfer ID service'
        ),
        Option(
            name='ecurep_password',
            type='str',
            default="",
            desc='Password obtained from the IBM Transfer ID service'
        ),
        Option(
            name='stale_timeout',
            type='int',
            default=86400 * 10,
            desc='Time interval in seconds during which requests with a repeating SI request ID will be ignored'
        ),
        Option(
            name='cooldown_timeout_upload_snap_1',
            type='int',
            default=300,
            desc='Time interval in seconds to allow a cooldown between level 1 upload snap requests'
        ),
        Option(
            name='cooldown_timeout_upload_snap_2',
            type='int',
            default=3600 * 2,
            desc='Time interval in seconds to allow a cooldown between level 2 upload snap requests'
        ),
        Option(
            name='enable_service_events',
            type='bool',
            default=False,
            desc='Enable service events'
        ),
    ]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self.reports = [
                {
                    'class': ReportInventory,
                    'name': 'inventory',
                    'interval_option_name': 'interval_inventory_report_seconds',
                },
                {
                    'class': ReportLastContact,
                    'name': 'last_contact',
                    'interval_option_name': 'interval_last_contact_report_seconds',
                },
                {
                    'class': ReportStatusAlerts,
                    'name': 'alerts',
                    'interval_option_name': 'interval_alerts_report_seconds',
                },
                {
                    'class': ReportStatusHealth,
                    'name': 'status',
                    'interval_option_name': 'interval_status_report_seconds',
                },
                {
                    'class': ReportPerformance,
                    'name': 'performance',
                    'interval_option_name': 'interval_performance_report_seconds',
                },
        ]


        self.connectivity_status = {
                'connectivity': False,  # Conenctivity status.
                'last_checked': 0,  # Unix timestamp of when the last connectivity attempt was.
                'connectivity_error': 'No connectivity attempted'  # Error is only relevant when 'connectivity'==False
        }

        # set up some members to enable the serve() method and shutdown()
        self.run = True

        # Health checks
        self.health_checks: Dict[str, Dict[str, Any]] = dict()

        # Module options
        self.refresh_options()

        # Unsolicited Request support

        # identify messages that we received in the past self.stale_timeout seconds (10 days). such messages will be ignored and removed from the queue.
        # maps unique ID to time when this entry is not relevant anymore and should be deleted
        store_ur_stale = self.get_store('ur_stale')
        if store_ur_stale is not None:
            self.ur_stale = json.loads(store_ur_stale)
            self.log.debug(f"ur_stale loaded from db after restart: {self.ur_stale}")
        else:
            self.ur_stale = {}

        # mechanism to prevent the mgr being bombarded by new requests. provides a cooldown time between processing the same type of message.
        # Requests that arrive during the time the cooldown window for that type of request is active, will wait in the queue until the cooldown
        # time is over and then will be processed.
        # key: a a string representing the operation, such as "upload_snap-2" (2 is for level 2 - include SOS report)
        # value: the time when the last message of this type was processed.
        store_ur_cooldown = self.get_store('ur_cooldown')
        if store_ur_cooldown is not None:
            self.ur_cooldown = json.loads(store_ur_cooldown)
            self.log.debug(f"ur_cooldown loaded from db after restart: {self.ur_cooldown}")
        else:
            self.ur_cooldown: dict[str, datetime] = {}

        self.ur_queue = []
        self.ceph_cluster_id = self.get('mon_map')['fsid']

        self.event = threading.Event()  # Used to wake up serve if need to refresh options or to exit the module

        # clean up 7.* and 8.0 configuration options
        if self.get_store('db_operations') is not None:
            self.log.info("Cleaning old module's db_operations")
            self.set_store('db_operations', None)

        self.scheduler = sched.scheduler(time.time, time.sleep)

    def get_jwt_jti(self) -> str:
        # Extract jti from JWT. This is another way to identify clusters in addition to the ICN.
        jwt_jti = ""
        reg_credentials_str = self.ceph_command(srv_type='mon',
                                                prefix='config-key get',
                                                key='mgr/cephadm/registry_credentials')
        if not reg_credentials_str:
            return ""

        jti_token_fail = ""
        try:
            reg_credentials = json.loads(reg_credentials_str)
            user_jwt_password = r"{}".format(reg_credentials['password'])
            registry_url = reg_credentials['url']
            if re.match(self.valid_container_registry, registry_url):
                jwt_jti = jwt.decode(user_jwt_password, options={
                                    "verify_signature": False})["jti"]
                self.log.info("JWT jti field extracted succesfully")
            else:
                jti_token_fail = f"url for registry credentials stored in <mgr/cephadm/registry_url> does not match with the expected ones <{self.valid_container_registry}>"
        except Exception as ex:
            jti_token_fail = str(ex)

        if jti_token_fail:
            self.log.warning(
                f"not able to extract <jti> from JWT token, a valid not empty jti token is required in <mgr/cephadm/registry_password> field password: {jti_token_fail}")

        return jwt_jti
        

    def refresh_options(self):
        # Note - self.get_module_option() returns the correct type, as long as a type is defined for the option
        for opt in self.MODULE_OPTIONS:
            setattr(self, opt['name'], self.get_module_option(opt['name']))
            self.log.debug(f" {opt['name']} = {getattr(self, opt['name'])}")

        self.proxies = {'http': self.proxy, 'https': self.proxy} if self.proxy else {}

        self.jwt_jti = self.get_jwt_jti()

        if self.enable_service_events:
            # validate icn and country_code
            self.validate_config()

    def ceph_command(self, srv_type: str, prefix: str, srv_spec: Optional[str] = '', inbuf: str = '', **kwargs):
        # Note: A simplified version of the function used in dashboard ceph services
        """
        :type prefix: str
        :param srv_type: mon |
        :param kwargs: will be added to argdict
        :param srv_spec: typically empty. or something like "<fs_id>:0"
        :param to_json: if true return as json format
        """
        argdict = {
            "prefix": prefix,
        }
        argdict.update({k: v for k, v in kwargs.items() if v is not None})
        result = CommandResult("")
        self.send_command(result, srv_type, srv_spec, json.dumps(argdict), "", inbuf=inbuf)
        r, outb, outs = result.wait()
        if r != 0:
            self.log.error(f"Execution of command '{prefix}' failed. (r={r}, outs=\"{outs}\", kwargs={kwargs})")
        try:
            return outb or outs
        except Exception as ex:
            self.log.error(f"Execution of command '{prefix}' failed: {ex}")
            return outb


    def validate_country_code(self, country_code: str) -> Tuple[bool, str]:
        """
        Validates a 2-letter country code using ISO 3166-1 alpha-2 codes
        """
        # Set of valid ISO 3166-1 alpha-2 country codes
        VALID_COUNTRY_CODES = {
            'AF', 'AX', 'AL', 'DZ', 'AS', 'AD', 'AO', 'AI', 'AQ', 'AG',
            'AR', 'AM', 'AW', 'AU', 'AT', 'AZ', 'BS', 'BH', 'BD', 'BB',
            'BY', 'BE', 'BZ', 'BJ', 'BM', 'BT', 'BO', 'BQ', 'BA', 'BW',
            'BV', 'BR', 'IO', 'BN', 'BG', 'BF', 'BI', 'KH', 'CM', 'CA',
            'CV', 'KY', 'CF', 'TD', 'CL', 'CN', 'CX', 'CC', 'CO', 'KM',
            'CG', 'CD', 'CK', 'CR', 'CI', 'HR', 'CU', 'CW', 'CY', 'CZ',
            'DK', 'DJ', 'DM', 'DO', 'EC', 'EG', 'SV', 'GQ', 'ER', 'EE',
            'SZ', 'ET', 'FK', 'FO', 'FJ', 'FI', 'FR', 'GF', 'PF', 'TF',
            'GA', 'GM', 'GE', 'DE', 'GH', 'GI', 'GR', 'GL', 'GD', 'GP',
            'GU', 'GT', 'GG', 'GN', 'GW', 'GY', 'HT', 'HM', 'VA', 'HN',
            'HK', 'HU', 'IS', 'IN', 'ID', 'IR', 'IQ', 'IE', 'IM', 'IL',
            'IT', 'JM', 'JP', 'JE', 'JO', 'KZ', 'KE', 'KI', 'KP', 'KR',
            'KW', 'KG', 'LA', 'LV', 'LB', 'LS', 'LR', 'LY', 'LI', 'LT',
            'LU', 'MO', 'MG', 'MW', 'MY', 'MV', 'ML', 'MT', 'MH', 'MQ',
            'MR', 'MU', 'YT', 'MX', 'FM', 'MD', 'MC', 'MN', 'ME', 'MS',
            'MA', 'MZ', 'MM', 'NA', 'NR', 'NP', 'NL', 'NC', 'NZ', 'NI',
            'NE', 'NG', 'NU', 'NF', 'MK', 'MP', 'NO', 'OM', 'PK', 'PW',
            'PS', 'PA', 'PG', 'PY', 'PE', 'PH', 'PN', 'PL', 'PT', 'PR',
            'QA', 'RE', 'RO', 'RU', 'RW', 'BL', 'SH', 'KN', 'LC', 'MF',
            'PM', 'VC', 'WS', 'SM', 'ST', 'SA', 'SN', 'RS', 'SC', 'SL',
            'SG', 'SX', 'SK', 'SI', 'SB', 'SO', 'ZA', 'GS', 'SS', 'ES',
            'LK', 'SD', 'SR', 'SJ', 'SE', 'CH', 'SY', 'TW', 'TJ', 'TZ',
            'TH', 'TL', 'TG', 'TK', 'TO', 'TT', 'TN', 'TR', 'TM', 'TC',
            'TV', 'UG', 'UA', 'AE', 'GB', 'US', 'UM', 'UY', 'UZ', 'VU',
            'VE', 'VN', 'VG', 'VI', 'WF', 'EH', 'YE', 'ZM', 'ZW'
        }

        # if the user did not populate the customer_country_code config option yet
        if country_code is None:
            return False, "Country code is not configured"

        if country_code.upper() not in VALID_COUNTRY_CODES:
            return False, f"'{country_code}' must be a valid ISO 3166-1 alpha-2 country code"

        return True, f"'{country_code}' is a valid country code"

    def validate_icn(self, icn: str) -> Tuple[bool, str]:
        # if the user did not populate the ICN config option yet
        if icn is None:
            return False, f"IBM Customer Number (ICN) is not configured"

        # ICN is exactly 7 alphanumeric characters long
        if len(icn) != 7 or not icn.isalnum():
            return False, f"Invalid ICN: '{icn}'. ICN must be exactly 7 alphanumeric characters long"

        return True, "ICN is valid"

    def validate_config(self) -> None:
        """
        Validate the mandatory fields in the config options
        """
        # ICN is a mandatory field for service events
        is_valid, msg = self.validate_icn(self.icn)
        if not is_valid:
            self.log.warning(f"CHA_INVALID_ICN: {msg}")
            self.health_checks.update({
                'CHA_INVALID_ICN': {
                    'severity': 'warning',
                    'summary': 'Call Home Agent: IBM Customer Number (ICN) is invalid. '
                               'Run "ceph callhome set icn <icn>" to set it.',
                    'detail': [msg]
                }
            })
        else:
            self.health_checks.pop('CHA_INVALID_ICN', None)

        # Country code is a mandatory field for service events
        is_valid, msg = self.validate_country_code(self.customer_country_code)
        if not is_valid:
            self.log.warning(f"CHA_INVALID_COUNTRY_CODE: {msg}")
            self.health_checks.update({
                'CHA_INVALID_COUNTRY_CODE': {
                    'severity': 'warning',
                    'summary': 'Call Home Agent: Country code is invalid. '
                               'Run "ceph callhome set country-code <country-code>" to set it.',
                    'detail': [msg]
                }
            })
        else:
            self.health_checks.pop('CHA_INVALID_COUNTRY_CODE', None)

        self.set_health_checks(self.health_checks)

    def connectivity_update(self, response: dict) -> None:
        """
        Validate that the response is from IBM call home and update the connectivity check struct
        """

        self.connectivity_status['last_checked'] = time.time()

        # When sending a message to CH, the reply contains a
        # "service"="ibm_callhome_connect". but if the message sent is of bad
        # format, CH returns an error in a different format, But this still
        # means that we connected successfully to CH.
        if (
            response.get('service', "") == 'ibm_callhome_connect'
            or response.get('body',{}).get('env' ,{}).get('namespace', '') != ''
        ):
            self.connectivity_status['connectivity'] = True
            self.connectivity_status['connectivity_error'] = "Success"
        else:
            self.connectivity_status['connectivity'] = False
            self.connectivity_status['connectivity_error'] = f"Bad response from Call Home: {json.dumps(self._filter_report(response), indent=4)}"

    def connectivity_update_error(self, message) -> None:
        self.connectivity_status['last_checked'] = time.time()
        self.connectivity_status['connectivity'] = False
        self.connectivity_status['connectivity_error'] = f"Can't connect to Call Home: {message}"

    def process_response(self, resp: dict) -> None:
        """
        Process HTTP responses we receive from call home after sending a report.
        """

        req = "unknown"  # define it to something so that if the below code throws from before the "for", for exdample from json.loads() then req will be defined
        try:
            # retrieve unsolicited requests from response
            try:
                inbound_requests = resp['response_state']['transactions']['Unsolicited_Storage_Insights_RedHatMarine_ceph_Request']['response_object']['product_request']['asset_event_detail']['body']['inbound_requests']
            except:
                # Most of the fields above do not appear at all when there is no UR in the LastContact response. This is OK and not an error, therefore we don't log this.
                return

            if not inbound_requests:
                # No UR to process
                return

            report_event_id = resp.get('transaction', {}).get('event_id', '')
            self.log.info(f"New inbound_requests = {inbound_requests} for report_event_id {report_event_id}")

            # Note: if we should just ignore stale messages then do it here. Currently we do send an error message for each stale message.
            # Add the operation to the UR queue
            for req in inbound_requests:
                # create the unique ID that identifies this message to check to compare to the stale list
                # Note: if we decide to add time_ms to stale check then it should be added here
                unique_id = URUploadSnap(self, req).id()
                if unique_id in self.ur_stale:
                    self.log.info(f"Unsolicited request {unique_id} is stale. dropping request.")
                    continue
                # protect from denial of service
                if len(self.ur_queue) > 20:
                    self.log.warning(f"Unsolicited queue too long. dropping request.")
                    continue
                self.ur_queue.append({'request': req, 'report_event_id': report_event_id})
                self.log.info(f"Queued unsolicited request for processing: {req}")
        except Exception as ex:
            self.log.error(f"process_response: error processing the following requests: {req}\nException: {ex}")

        self.ur_queue_run()  # Call immediately to deal with any UR that can be served now.

    def ur_queue_run(self) -> None:
        try:
            for ur_elem in list(self.ur_queue):  # Iterate over a copy of the list as we're deleting items from it when we execute them
                try:
                    req = ur_elem['request']
                    report_event_id = ur_elem['report_event_id']
                    req_type = req.get('operation', '')
                    if req_type == 'upload_snap':
                        ur_req = URUploadSnap(self, req)
                        # check that the request is not in the cooldown window. wait till its gone from there to continue processing the request
                        ur_cooldown_id = ur_req.id_for_cooldown()
                        if ur_cooldown_id in self.ur_cooldown:
                            continue
                        WorkFlowUploadSnap(self, req, ur_req.id(), report_event_id).run()
                        self.ur_queue.remove(ur_elem)
                    else:
                        self.log.warning(f"Unknown unsolicited request of type '{req_type}'. Deleting it from queue")
                        ReportURError(self, report_event_id).run()  # May not have "operation" nor "options" in "req"
                        self.ur_queue.remove(ur_elem)
                        continue

                    # the cooldown timeout depend on the message type, so we need to add it here.
                    # The stale timeout is constant, so we'll check it when cleaning the list, therefore we'll be able to change it in runtime
                    self.ur_cooldown[ur_cooldown_id] = int(time.time()) + ur_req.cooldown_timeout()
                    self.ur_stale[ur_req.id()] = int(time.time())
                except Exception as e:
                    self.log.error(f"Error processing ur_queue: {e}\n{traceback.format_exc()}")
                    raise

            now = time.time()
            # Clean cooldown list
            for k, v in list(self.ur_cooldown.items()):
                if v < now:
                    del self.ur_cooldown[k]

            # Clean stale lists
            for k, v in list(self.ur_stale.items()):
                if v + self.stale_timeout < now:
                    del self.ur_stale[k]
        finally:
            self.set_store('ur_stale', json.dumps(self.ur_stale))
            self.set_store('ur_cooldown', json.dumps(self.ur_cooldown))

    def get_ecurep_user_pass(self) -> Tuple[str, str]:
        if self.ecurep_userid and self.ecurep_password:
            return self.ecurep_userid, self.ecurep_password
        else:
            try:
                id_data = self.get_secrets()
                # bail out early when the keys are missing
                return id_data['ecurep_transfer_id'], id_data['ecurep_password']
            except Exception as e:
                self.log.error(f"Error loading ECuRep keys: {e}")
                raise

    def get_secrets(self) -> dict:
        decryption_key = b'yDVH70MMpzBnu5Y1dKfJrw=='
        decyption_nonce = b'1K6HRTiLD80laBi6'
        if 'UNITTEST' in os.environ:
            return {'api_key': 'test_api_key', 'private_key': 'test_private_key'}

        try:
            encrypted_keys = self._load_encrypted_keys()
            aes_key = base64.b64decode(decryption_key)
            nonce = base64.b64decode(decyption_nonce)
            aesgcm = AESGCM(aes_key)
            clear_keys = aesgcm.decrypt(nonce, encrypted_keys, b'')
            keys = json.loads(clear_keys)
            return keys
        except Exception as e:
            raise Exception(f"Error getting encrypted settings: {e}")

    def _load_encrypted_keys(self) -> bytes:
        call_home_keys = '/usr/share/ceph/mgr/call_home_agent/ceph_call_home'  # default location of the key file
        key_file = os.environ.get('CALLHOMEKEYSFILE', call_home_keys)
        if not os.path.isfile(key_file):
            raise Exception(f"Can't find key file {key_file}")

        with open(key_file, 'rb') as f:
            return f.read()

    # Scheduling API: functions called by Report class to schedule the next run of the report.
    #    We can implement the underlying scheduling engine using coroutimes, threads or event loop.

    def run_scheduled_ur_queue_run(self) -> None:
        try:
            self.ur_queue_run();
            self.health_checks.pop('CHA_ERROR_SERVING_UR', None)
            self.scheduler.enter(30, 1, self.run_scheduled_ur_queue_run)
        except Exception as ex:
            send_error = str(ex)
            self.log.error(f"Error running uncolicited request handler: {ex}\n{traceback.format_exc()}")
            self.health_checks.update({
                'CHA_ERROR_SERVING_UR': {
                    'severity': 'error',
                    'summary': f"IBM Ceph Call Home Agent manager module: Error running uncolicited request handler",
                    'detail': [send_error]
                }
            })

        self.set_health_checks(self.health_checks)

    def run_scheduled_report(self, report_class, interval_option_name, last_upload_option_name) -> None:
        """
        Called from the scheduler to run a report
        report_class: One of the report or workflow classes - not a report object
        """
        # Save to store the time in which we last tried to send this report. even if we fail to send.
        # this will help not send in a loop if the sending itself crashes the manager.
        self.set_store(last_upload_option_name, str(int(time.time())))  # argument 2 must be str or None

        try:
            report_class(self).run()
        except Exception as ex:
            send_error = str(ex)
            self.log.error(f"Error running report/workflow {report_class.__name__}: {ex}\n{traceback.format_exc()}")
            self.health_checks.update({
                'CHA_ERROR_SENDING_REPORT': {
                    'severity': 'error',
                    'summary': f"IBM Ceph Call Home Agent manager module: error sending <{report_class.__name__}> report to endpoint {self.target}",
                    'detail': [send_error]
                }
            })

        self.set_health_checks(self.health_checks)
        wait_time = getattr(self, interval_option_name)
        self.scheduler.enter(wait_time, 1, self.run_scheduled_report, argument=(report_class, interval_option_name, last_upload_option_name))

    def schedule_tasks(self) -> None:
        for report in self.reports:
            # interval==0 means it's disabled
            interval = getattr(self, report['interval_option_name'])
            if interval == 0:
                continue

            # Get the last time it ran
            last_upload_option_name = f"report_{report['class'].__name__}_last_upload"
            last_upload = int(self.get_store(last_upload_option_name, 0))

            now = int(time.time())

            # We want to immediately send all reports the first time that Ceph runs (and therfore there is no report_*_last_upload in the DB).
            # this is for the inventory report to be sent immediately (and not after 24h)
            if last_upload == 0:
                next_send = now
            else:
                next_send = max(last_upload + interval, now)

            self.scheduler.enter(next_send - now, 1, self.run_scheduled_report, argument=(report['class'], report['interval_option_name'], last_upload_option_name))

        # Schedule the Uncolicited Request handler
        self.scheduler.enter(30, 1, self.run_scheduled_ur_queue_run)

    def config_notify(self) -> None:
        """
        This only affects changes in ceph config options.
        To change configuration using env. vars a restart of the module
        will be needed or the change in one config option will refresh
        configuration coming from env vars
        """
        self.refresh_options()
        # Reset the scheduler - effectively emptying it
        self.scheduler = sched.scheduler(time.time, time.sleep)
        self.schedule_tasks()
        self.event.set()

    def serve(self):
        self.log.info('Starting IBM Ceph Call Home Agent')
        self.schedule_tasks()
        while self.run:
            # Passing False causes the scheduler.run() to return the time until the next event. therefore we're not blocked in
            # the scheduler, but we block ourselves using self.event.sleep() which can be interrupted by self.event.set()
            # which we use in shutdown() and refresh_options()
            next_event_seconds = self.scheduler.run(False)
            self.event.wait(next_event_seconds)
            self.event.clear()

        self.log.info('Call home agent finished')

    def shutdown(self) -> None:
        self.log.info('Stopping IBM call home module')
        self.run = False
        self.event.set()
        return super().shutdown()

    @CallHomeCLICommand.Read('callhome stop')
    def cli_stop(self) -> Tuple[int, str, str]:
        self.shutdown()
        return HandleCommandResult(stdout=f'Remember to disable the call home module')

    @CallHomeCLICommand.Read('callhome reset alerts')
    def cli_reset_alerts(self, mock: Optional[bool] = False) -> Tuple[int, str, str]:
        """
        Resets the local list of alerts that were sent to Call Home to allow
        for existing alerts to be resent.

        :param mock: generates a dummy alert
            If there are no relevant alerts in the cluster, an "alerts" report will not be sent.
            "--mock" is useful in this case, to allow the user to send a dummy "alerts" report to Call Home.
        """
        ReportStatusAlerts.resetAlerts(mock)
        return HandleCommandResult(stdout=f"Sent alerts list has been reset. Next alerts report will send all current alerts.")

    def _filter_report(self, report_dict: dict, fields_to_remove = ['api_key', 'private_key']) -> dict:
        for field in fields_to_remove:
            report_dict.pop(field, None)

        report_dict.get('transaction', {}).pop('api_key', None)
        return report_dict

    def _find_report_by_name(self, name: str) -> dict:
        found_reports = list(filter(lambda r: r['name'] == name, self.reports))
        if not found_reports:
            return None
        return found_reports[0]

    @CallHomeCLICommand.Read('callhome show')
    def cli_show(self, report_type: str) -> Tuple[int, str, str]:
        """
            Prints the report requested.
            Available reports: inventory, status, last_contact, alerts, performance
            Example:
                ceph callhome show inventory
        """
        report = self._find_report_by_name(report_type)
        if report is None:
            return HandleCommandResult(stderr=f"Unknown report type {report_type}.")

        if report_type == 'alerts':
            # The "alerts" report only sends alerts that are not in 'sent_alerts', and then updates 'sent_alerts'
            # with the alerts sent. For 'callhome show' not to affect the regular workflow, we need to restore
            # 'sent_alerts' to what it was before 'callhome show' generated the alerts report.
            tmp_sent_alerts = ReportStatusAlerts.sent_alerts

        report_dict = report['class'](self).compile()
        if report_dict is None:
            return HandleCommandResult(stdout=f"Report {report_type} is empty. Nothing to send.")

        filtered_report = self._filter_report(report_dict)

        if report_type == 'alerts':
            ReportStatusAlerts.sent_alerts = tmp_sent_alerts

        return HandleCommandResult(stdout=f"{json.dumps(filtered_report, indent=4)}")

    def test_connectivity(self) -> Tuple[int, str, str]:
        return self.cli_send("status")

    def get_call_home_status(self) -> dict[any, any]:
        return self.connectivity_status

    @CallHomeCLICommand.Read('callhome connectivity status')
    def cli_connectivity_status(self) -> Tuple[int, str, str]:
        return HandleCommandResult(stdout=json.dumps(self.connectivity_status, indent=4))

    @CallHomeCLICommand.Read('callhome send')
    def cli_send(self, report_type: str) -> Tuple[int, str, str]:
        """
            Command for sending the report requested.
            Available reports: inventory, status, last_contact, alerts, performance
            Example:
                ceph callhome send inventory
        """
        report = self._find_report_by_name(report_type)
        if report is None:
            return HandleCommandResult(stderr=f"Unknown report type {report_type}.", retval=-1)

        try:
            resp = report['class'](self).run()
        except Exception as ex:
            return HandleCommandResult(stderr=str(ex), retval=-1)

        if resp == None:
            return HandleCommandResult(stdout=f'{report_type} report: Nothing to send\n')

        try:
            resp = json.dumps(self._filter_report(json.loads(resp)), indent=4)
        except:
            pass
        return HandleCommandResult(stdout=f'{report_type} report sent successfully:\n{resp}')

    @CallHomeCLICommand.Read('callhome list-tenants')
    def cli_list_tenants(self, owner_ibm_id: str, owner_company_name: str,
                       owner_first_name: str, owner_last_name: str,
                       owner_email: str) -> Tuple[int, str, str]:
        """
        Retrieves the list of tenant ids linked with an specific IBM id owner
        """
        mon_map = self.get('mon_map')
        mon_ips = ','.join([mon['addr'] for mon in mon_map['mons']])
        owner_data = {'owner-ibm-id': owner_ibm_id,
                'company-name': owner_company_name,
                'owner-first-name': owner_first_name,
                'owner-last-name': owner_last_name,
                'owner-email': owner_email,
                'check-only': True,
                'device-serial': mon_map['fsid'],
                'device-IP': mon_ips
                }

        resp = None
        try:
            resp = requests.post(url=self.si_web_service_url,
                                headers={'accept': 'application/json',
                                        'content-type': 'application/json',
                                        'IBM-SRM-SenderApp': 'CEPH-EM',
                                        'IBM-SRM-Request': 'SI-SignUp-Check'},
                                data=json.dumps(owner_data),
                                proxies=self.proxy,
                                timeout=30)

            resp.raise_for_status()
        except Exception as ex:
            explanation = resp.text if resp else str(ex)
            self.log.error(f"Failed to list tenants: {explanation}")
            return HandleCommandResult(stderr=f"Failed to list tenants: {explanation}")
        else:
            return HandleCommandResult(stdout=f'{json.dumps(resp.json(), indent=4)}')

    @CallHomeCLICommand.Write('callhome set tenant')
    def cli_set_tenant(self, owner_tenant_id: str, owner_ibm_id: str,
                      owner_company_name: str, owner_first_name: str,
                      owner_last_name: str, owner_email: str) -> Tuple[int, str, str]:
        """
        Set the IBM tenant id included in reports sent to IBM Storage Insights
        """
        try:
            self.set_module_option('owner_tenant_id', owner_tenant_id)
            self.set_module_option('owner_ibm_id', owner_ibm_id)
            self.set_module_option('owner_company_name', owner_company_name)
            self.set_module_option('owner_first_name', owner_first_name)
            self.set_module_option('owner_last_name', owner_last_name)
            self.set_module_option('owner_email', owner_email)
        except Exception as ex:
            return HandleCommandResult(stderr=str(ex))
        else:
            return HandleCommandResult(stdout=f'IBM tenant id set to {owner_tenant_id}')
        finally:
            self.refresh_options()  # This will always run, no matter what.

    @CallHomeCLICommand.Read('callhome get user info')
    def cli_get_user_info(self) -> Tuple[int, str, str]:
        """
        Show the information about the customer used to identify the customer
        in IBM call home and IBM storage insights systems
        """
        return HandleCommandResult(stdout=json.dumps(
            {'IBM_call_home': {
                    'icn': self.icn,
                    'customer_first_name': self.customer_first_name,
                    'customer_last_name': self.customer_last_name,
                    'customer_phone': self.customer_phone,
                    'customer_address': self.customer_address,
                    'customer_email': self.customer_email,
                    'customer_company_name': self.customer_company_name,
                    'customer_country_code': self.customer_country_code
                },
             'IBM_storage_insights': {
                    'owner_ibm_id': self.owner_ibm_id,
                    'owner_company_name': self.owner_company_name,
                    'owner_first_name': self.owner_first_name,
                    'owner_last_name': self.owner_last_name,
                    'owner_email': self.owner_email,
                    'owner_tenant_id': self.owner_tenant_id
                },
            }))

    @CallHomeCLICommand.Read('callhome upload diagnostics')
    def cli_upload_diagnostics(self, support_ticket: str, level: int) -> Tuple[int, str, str]:
        """
        Upload Ceph cluster diagnostics to Ecurep for a specific customer support ticket.
        """
        # The upload happends immediately without the constraints of self.ur_stale or self.ur_cooldown
        # and does not populate those after sending. No need to clear any queue for this command to
        # be executed
        try:
            request = {
                    'options': {
                        'pmr': support_ticket,
                        'level': level
                        # No need to simulate si_requestid
                        }
                    }
            WorkFlowUploadSnap(self, request, 'cli_upload_diagnostics', None).run()
        except Exception as ex:
            return HandleCommandResult(stderr=f"Error sending diagnostics: {ex}")
        else:
            return HandleCommandResult(stdout='Success')

    @CallHomeCLICommand.Read('callhome list queues')
    def cli_list_queues(self) -> Tuple[int, str, str]:
        """
        Show the state of the unsolicited requests queues
        """
        ret = {'ur_queue': self.ur_queue, 'ur_stale': self.ur_stale, 'ur_cooldown': self.ur_cooldown}
        return HandleCommandResult(stdout=json.dumps(ret, indent=4))

    @CallHomeCLICommand.Write('callhome clear queues')
    def cli_clear_queues(self) -> Tuple[int, str, str]:
        """
        Clear the unsolicited requests queues
        """

        self.ur_queue = []
        self.ur_stale = {}
        self.ur_cooldown = {}
        self.set_store('ur_stale', json.dumps(self.ur_stale))
        self.set_store('ur_cooldown', json.dumps(self.ur_cooldown))

        return HandleCommandResult(stdout="Success")

    @CallHomeCLICommand.Write('callhome set country-code')
    def cli_set_country_code(self, country_code: str) -> Tuple[int, str, str]:
        """
        Set a 2 letter country code
        """
        is_valid, msg = self.validate_country_code(country_code)

        if not is_valid:
            self.log.error(f"Invalid country code: {msg}")
            return HandleCommandResult(retval=1, stderr=f'Invalid country code: {msg}')

        try:
            self.set_module_option('customer_country_code', country_code.upper())
        except Exception as e:
            return HandleCommandResult(retval=1, stderr=str(e))
        else:
            return HandleCommandResult()
        finally:
            self.refresh_options()  # This will always run, no matter what.

    @CallHomeCLICommand.Write('callhome set icn')
    def cli_set_icn(self, icn: str) -> Tuple[int, str, str]:
        """
        Set an IBM customer number (7-character alphanumeric string)
        """
        is_valid, msg = self.validate_icn(icn)

        if not is_valid:
            self.log.warning(msg)
            return HandleCommandResult(retval=1, stderr=msg)

        try:
            self.set_module_option('icn', icn)
        except Exception as e:
            return HandleCommandResult(retval=1, stderr=str(e))
        else:
            return HandleCommandResult()
        finally:
            self.refresh_options()  # This will always run, no matter what.


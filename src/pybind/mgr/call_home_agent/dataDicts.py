from datetime import datetime
from typing import Any, Optional
import json
import os
import jwt
import re
import time
from mgr_module import CommandResult

from .config import get_settings

# Constants for operations types:
UPLOAD_SNAP = 'upload_snap'
UPLOAD_FILE = 'upload_file'
DISABLE_SI_MESSAGES = 'disable_si_messages'
CONFIRM_RESPONSE = 'confirm_response'
NOT_SUPPORTED = 'unknown_operation'

# Constants for operation status
OPERATION_STATUS_NEW = 'READY'
OPERATION_STATUS_IN_PROGRESS = 'IN_PROGRESS'
OPERATION_STATUS_COMPLETE = 'COMPLETE'
OPERATION_STATUS_ERROR = 'ERROR'
OPERATION_STATUS_REQUEST_REJECTED = 'REQUEST_REJECTED'

#Constants for operations status delivery
ST_NOT_SENT = 0
ST_SENT = 1

def confirm_response_event(ceph_cluster_id: str, report_timestamp: float,
                           tenant_id: str) -> dict:
    """
    Return a confirm response event
    """
    event_time = datetime.fromtimestamp(report_timestamp).strftime("%Y-%m-%d %H:%M:%S")
    event_time_ms = int(report_timestamp * 1000)

    return {
        "header": {
                "event_type": "confirm_response",
                "event_id": f"IBM_event_RedHatMarine_ceph_{ceph_cluster_id}_{event_time_ms}_confirm_response_event",
                "event_time": f"{event_time}",
                "event_time_ms": event_time_ms,
                "tenant_id": tenant_id,
        },
        "body": {
                "event_transaction_id": "Unsolicited_Storage_Insights_RedHatMarine_ceph_Request",
                "event_type": "last_contact",
                "component": "ceph_operations"
        }
    }

def upload_snap_operation_event(ceph_cluster_id: str, report_timestamp: float,
                                tenant_id: str, operation: dict) -> dict:
    """
    Return an event based in the operation passed as parameter
    """
    event_time = datetime.fromtimestamp(report_timestamp).strftime("%Y-%m-%d %H:%M:%S")
    event_time_ms = int(report_timestamp * 1000)

    return {
            "header": {
                "event_type": "status",
                "event_id": f"IBM_event_RedHatMarine_ceph_{ceph_cluster_id}_{event_time_ms}_upload_snap_status__event",
                "event_time": f"{event_time}",
                "event_time_ms": event_time_ms,
                "tenant_id": tenant_id,
            },
            "body": {
                "event_transaction_id": "Unsolicited_Storage_Insights_RedHatMarine_ceph_Request",
                "product":  "Red Hat Ceph",
                "component": "ceph_log_upload",
                "description":  operation['description'],
                "state" : f"{operation['status']} ({operation['progress']}%)",
                "complete" : (operation['status'] == OPERATION_STATUS_COMPLETE),
                "payload": {
                    "action": "Unsolicited_Storage_Insights_RedHatMarine_ceph_Request",
                    "description": operation['description'],
                    "state" : operation['status'],
                    "progress": operation['progress'],
                    "complete" : (operation['status'] == OPERATION_STATUS_COMPLETE),
                    "si_requestid": operation['si_requestid'],
                }
            }
        }

class ReportHeader:
    def collect(report_type: str, ceph_cluster_id: str, ceph_version: str,
                report_timestamp: float, mgr_module: Any, target_space: str = 'prod',
                operation_event_id: str = '') -> dict:
        try:
            id_data = get_settings()
        except Exception as e:
            mgr_module.log.error(f"Error getting encrypted identification keys for {report_type} report: {e}. "
                                 "Provide keys and restart IBM Ceph Call Home module")
            id_data = {'api_key': '', 'private_key': ''}

        report_time = datetime.fromtimestamp(report_timestamp).strftime("%Y-%m-%d %H:%M:%S")
        report_time_ms = int(report_timestamp * 1000)
        local_report_time = datetime.fromtimestamp(report_timestamp).strftime("%a %b %d %H:%M:%S %Z")

        if not operation_event_id:
            event_id = "IBM_chc_event_RedHatMarine_ceph_{}_{}_report_{}".format(ceph_cluster_id, report_type, report_time_ms)
        else:
            event_id = operation_event_id

        return {
                "agent": "RedHat_Marine_firmware_agent",
                "api_key": "{}".format(id_data['api_key']),
                "private_key": "{}".format(id_data['private_key']),
                "target_space": "{}".format(target_space),
                "asset": "ceph",
                "asset_id": "{}".format(ceph_cluster_id),
                "asset_type": "RedHatMarine",
                "asset_vendor": "IBM",
                "asset_virtual_id": "{}".format(ceph_cluster_id),
                "country_code": "",
                "event_id": event_id,
                "event_time": "{}".format(report_time),
                "event_time_ms": report_time_ms,
                "local_event_time": "{}".format(local_report_time),
                "software_level": {
                    "name": "ceph_software",
                    "vrmf": "{}".format(ceph_version)
                },
                "type": "eccnext_apisv1s",
                "version": "1.0.0.1",
                "analytics_event_source_type": "asset_event",
                "analytics_type": "ceph",
                "analytics_instance":  "{}".format(ceph_cluster_id),
                "analytics_virtual_id": "{}".format(ceph_cluster_id),
                "analytics_group": "Storage",
                "analytics_category": "RedHatMarine",
                "events": []
            }

class ReportEvent():
    def collect(event_type: str, component: str, report_timestamp: float, ceph_cluster_id: str,
                icn: str, tenant_id: str, description: str, content: dict,
                mgr_module: Any, operation_key: str = "") -> dict:

        # OPERATION STATUS Reports:
        # ----------------------------------------------------------------------
        event_data = {}
        if operation_key != "":
            try:
                operation = content["type"]
                if operation == UPLOAD_SNAP:
                    event_data = upload_snap_operation_event(ceph_cluster_id,
                                                             report_timestamp,
                                                             tenant_id,
                                                             content)
                elif operation == CONFIRM_RESPONSE:
                    event_data = confirm_response_event(ceph_cluster_id,
                                                        report_timestamp,
                                                        tenant_id)
            except Exception:
                mgr_module.log.error(f'not able to obtain event data: {ex}')
            return event_data

        # event time data
        event_time = datetime.fromtimestamp(report_timestamp).strftime("%Y-%m-%d %H:%M:%S")
        event_time_ms = int(report_timestamp * 1000)
        local_event_time = datetime.fromtimestamp(report_timestamp).strftime("%a %b %d %H:%M:%S %Z")

        # INVENTORY; CLUSTER STATUS; LAST_CONTACT reports
        # ----------------------------------------------------------------------
        # Extract jti from JWT. This is another way to identify clusters in addition to the ICN.
        jwt_jti = ""
        reg_credentials_str = ceph_command(mgr=mgr_module, srv_type='mon',
                                           prefix='config-key get',
                                           key='mgr/cephadm/registry_credentials')
        if reg_credentials_str:
            jti_token_fail = ""
            try:
                reg_credentials = json.loads(reg_credentials_str)
                user_jwt_password = r"{}".format(reg_credentials['password'])
                registry_url = reg_credentials['url']
                if re.match(mgr_module.valid_container_registry, registry_url):
                    jwt_jti = jwt.decode(user_jwt_password, options={
                                        "verify_signature": False})["jti"]
                    mgr_module.log.info("JWT jti field extracted succesfully")
                else:
                    jti_token_fail = f"url for registry credentials stored in <mgr/cephadm/registry_url> does not match with the expected ones <{mgr_module.valid_container_registry}>"
            except Exception as ex:
                jti_token_fail = str(ex)

            if jti_token_fail:
                mgr_module.log.warning(
                    f"not able to extract <jti> from JWT token, a valid not empty jti token is required in <mgr/cephadm/registry_password> field password: {jti_token_fail}")

        event_data = {
                "header": {
                    "event_id": "IBM_event_RedHatMarine_ceph_{}_{}_{}_event".format(ceph_cluster_id, event_time_ms, event_type),
                    "event_time": "{}".format(event_time),
                    "event_time_ms": event_time_ms,
                    "event_type": "{}".format(event_type),
                    "local_event_time": "{}".format(local_event_time)
                },
                "body": {
                    "component": component,
                    "context": {
                        "origin": 2,
                        "timestamp": event_time_ms,
                        "transid": event_time_ms
                    },
                    "description": "".format(description),
                    "payload": {
                        "request_time": event_time_ms,
                        "content": content,
                        "ibm_customer_number": icn,
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
                        "jti": jwt_jti
                    }
                }
            }

        if event_type == 'inventory':
            if tenant_id:
                event_data["header"]["tenant_id"] = "{}".format(tenant_id)

        if event_type == 'status':
            event_data["body"]["event_transaction_id"] = "IBM_event_RedHatMarine_ceph_{}_{}_{}_event".format(ceph_cluster_id, event_time_ms, event_type)

            if component != 'ceph_alerts':
                event_data["body"]["state"] =  "{}".format(content['status']['health']['status'])
            else:
                # if the status event contains alerts we add a boolean in the body to help with analytics
                event_data["body"]["alert"] =  True
                # Call Home requires the 'state' attribute in the 'body' section
                event_data["body"]["state"] = "Ok"

            event_data["body"]["complete"] = True

            if tenant_id:
                event_data["header"]["tenant_id"] = "{}".format(tenant_id)

        if event_type == 'last_contact':
            # Additional fields to enable response with commands
            event_data["body"]["context"]["messagetype"] = 1
            event_data["body"]["enable_response_detail"] = True
            event_data["body"]["enable_response_detail_filter"] = ["Unsolicited_Storage_Insights_RedHatMarine_ceph_Request"]

        return event_data

def ceph_command(mgr: Any, srv_type, prefix, srv_spec='', inbuf='', **kwargs):
    # type: (Any, str, str, Optional[str], str, Any) -> Any
    #
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
    mgr.send_command(result, srv_type, srv_spec, json.dumps(argdict), "", inbuf=inbuf)
    r, outb, outs = result.wait()
    if r != 0:
        mgr.log.error(f"Execution of command '{prefix}' failed. (r={r}, outs=\"{outs}\", kwargs={kwargs})")
    try:
        return outb or outs
    except Exception as ex:
        mgr.log.error(f"Execution of command '{prefix}' failed: {ex}")
        return outb

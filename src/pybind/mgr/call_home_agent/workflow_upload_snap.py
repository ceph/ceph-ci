from .report import Report, ReportTimes, Event
from .report_ur_error import ReportURError
from .exceptions import *
import time
import urllib.parse
from typing import Tuple, Optional
import glob
import os
import traceback
import re
import requests

# Constants for operation status
OPERATION_STATUS_NEW = 'READY'
OPERATION_STATUS_IN_PROGRESS = 'IN_PROGRESS'
OPERATION_STATUS_COMPLETE = 'COMPLETE'
OPERATION_STATUS_ERROR = 'ERROR'
OPERATION_STATUS_REQUEST_REJECTED = 'REQUEST_REJECTED'

# Constant for store default ceph logs folder
# Diagnostic files are collected in this folder
DIAGS_FOLDER = '/var/log/ceph'

class WorkFlowUploadSnap:
    def __init__(self, agent, req, req_id, report_event_id, send_status_log_upload = True):
        """
            send_status_log_upload: Should the workflow send percent complete reports to SI. Set to False in service_events.
        """
        self.agent = agent
        self.req = req
        self.req_id = req_id  # unique ID for this request
        self.pmr = self.req.get('options', {}).get('pmr', None)
        self.report_event_id = report_event_id
        self._event_id_counter = 0
        self.send_status_log_upload = send_status_log_upload
        self.si_requestid = self.req.get('options', {}).get('si_requestid', '')

    def next_event_id(self):
        # self.report_event_id may be None if the report was triggered by CLI cli_upload_diagnostics
        ret = self.report_event_id if self.report_event_id else "cli" + f"-{self._event_id_counter}"
        self._event_id_counter += 1
        return ret

    def run(self):
        self.agent.log.info(f"WorkFlowUploadSnap <{self.req_id}> : Processing new request {self.req}")
        if not self.pmr:
            self.agent.log.warning(f"WorkFlowUploadSnap <{self.req_id}> : Error - No PMR in request.")
            ReportURError(self.agent, self.next_event_id()).run()
            return

        try:
            commands_file = self.collect_diagnostic_commands()
            sos_files_pattern = ""
            snap_level = int(self.req.get('options', {}).get('level', 1))
            if snap_level > 1:
                sos_files_pattern = self.collect_sos_report()
            # Send commands file:
            self.upload_file(commands_file, percent_complete = 1 if sos_files_pattern else 100)

            # Send sos file splitted when we have files
            if sos_files_pattern:
                sos_file_name = f'{sos_files_pattern[:-2]}.xz'
                self.upload_file(sos_file_name, sos_files_pattern)

            self.agent.log.info(f"WorkFlowUploadSnap <{self.req_id}> :  Completed operation")
        except Exception as ex:
            self.agent.log.error(f'Operations ({self.req_id}): Error processing operation {self.req}. Exception={ex} trace={traceback.format_exc()}')
            if self.send_status_log_upload:
                ReportStatusLogUpload(self.agent, self.next_event_id(), self.si_requestid, 0, f"ERROR: {ex}", OPERATION_STATUS_ERROR).run()

        # if it was ok or not, we always report the state
        ReportConfirmResponse(self.agent, self.next_event_id()).run()
        self.agent.log.info(f"WorkFlowUploadSnap <{self.req_id}> : Finished processing {self.req}")

    def collect_diagnostic_commands(self) -> str:
        """
        Collect information from the cluster

            ceph status
            ceph health detail
            ceph osd tree
            ceph report
            ceph osd dump
            ceph df

        """
        output = ""
        output += "\nceph status\n" + self.agent.ceph_command(srv_type='mon', prefix='status')
        output += "\nceph health detail\n" + self.agent.ceph_command(srv_type='mon', prefix='health', detail='detail')
        output += "\nceph osd tree\n" + self.agent.ceph_command(srv_type='mon', prefix='osd tree')
        output += "\nceph report\n" + self.agent.ceph_command(srv_type='mon', prefix='report')
        output += "\nceph osd dump\n" + self.agent.ceph_command(srv_type='mon', prefix='osd dump')
        output += "\nceph df detail\n" + self.agent.ceph_command(srv_type='mon', prefix='df', detail='detail')

        self.agent.log.info(f"WorkFlowUploadSnap <{self.req_id}> : diagnostics commands collected")

        try:
            cmds_file_prefix = 'ceph_commands_case'
            # Remove previous commands files
            for file in glob.glob(f'{DIAGS_FOLDER}/{cmds_file_prefix}*'):
                os.remove(file)
            timestamp_sos_file = int(time.time() * 1000)
            file_name = f'{cmds_file_prefix}_{self.pmr}_{timestamp_sos_file}.txt'
            with open(f'{DIAGS_FOLDER}/{file_name}', 'w') as commands_file:
                commands_file.write(output)
            self.agent.log.info(f"WorkFlowUploadSnap <{self.req_id}> : diagnostics commands stored in file {file_name}")
            return file_name
        except Exception as ex:
            raise Exception(f"WorkFlowUploadSnap <{self.req_id}> : Error trying to save the commands file for diagnostics: {ex} trace={traceback.format_exc()}")

        return ""

    def collect_sos_report(self) -> str:
        """
        SOS report gathered from a Ceph Monitor node
        Best node to execute the sos command is
        1. Monitor + admin node + active mgr
        2. Monitor + admin node
        3. monitor
        """

        # Remove previous sos report files:
        for file in glob.glob(f'{DIAGS_FOLDER}/sosreport_case_*'):
            os.remove(file)

        # Get the best monitor node to execute the sos report
        best_mon, active_mgr = self.get_best_collect_node()
        mgr_target = ""
        if best_mon != active_mgr and active_mgr:
            mgr_target = f"--mgr-target {active_mgr}"
        self.agent.log.info(f"WorkFlowUploadSnap <{self.req_id}> : selected host for sos command is {best_mon}, active manager is {active_mgr}")

        # Execute the sos report command
        sos_cmd_execution = self.agent.remote('cephadm', 'sos',
                                          hostname = best_mon,
                                          sos_params = f'{mgr_target} report --batch --quiet --case-id {self.pmr}')
        self.agent.log.info(f"WorkFlowUploadSnap <{self.req_id}> : sos command executed succesfully: {sos_cmd_execution.result}")
        if sos_cmd_execution.exception_str:
            raise Exception(f"Error trying to get the sos report files for diagnostics(error_code): {sos_cmd_execution.exception_str}")

        # output is like:
        # ['New sos report files can be found in /var/log/ceph/<fsid>/sosreport_case_124_1706548742636_*']
        pattern = r'sosreport_case_\S+'
        matches = re.findall(pattern, sos_cmd_execution.result[0])
        if matches:
            self.agent.log.info(f"WorkFlowUploadSnap <{self.req_id}> : sos command files pattern is: {matches[0]}")
            result = matches[0]
        else:
            self.agent.log.info(f"WorkFlowUploadSnap <{self.req_id}> : sos report files pattern not found in: {sos_cmd_execution.result}")
            result = ""

        # If there is any issue executing the command, the output will be like:
        # ['Issue executing <['sos', 'report', '--batch', '--quiet', '--case-id', 'TS015034298', '-p', 'container']>: 0:[plugin:ceph_mon] Failed to find ceph version, command collection will be limited
        #
        # New sos report files can be found in /var/log/ceph/<fsid>/sosreport_case_TS015034298_1709809018376_*']
        # in this case, we leave a warning in the log about the issue
        pattern = r'^Issue executing.*'
        matches = re.findall(pattern, sos_cmd_execution.result[0])
        if matches:
            self.agent.log.info(f"WorkFlowUploadSnap <{self.req_id}> : review sos command execution in {best_mon}: {matches[0]}")

        return result

    def get_best_collect_node(self) -> Tuple[str, str]:
        """
        Select the best monitor node where to run a sos report command
        retuns the best monitor node and the active manager
        """
        nodes = {}
        active_manager = ""
        best_monitor = ""

        # We add all the monitors
        monitors = self.agent.remote('cephadm', 'list_daemons', service_name='mon')
        if monitors.exception_str:
            raise Exception(monitors.exception_str)

        for daemon in monitors.result:
            nodes[daemon.hostname] = 1

        # lets add one point to a monitor if it is a cephadm admin node
        cluster_nodes = self.agent.remote('cephadm', 'get_hosts')
        if cluster_nodes.exception_str:
            raise Exception(cluster_nodes.exception_str)

        for host in cluster_nodes.result:
            if '_admin' in host.labels:
                try:
                    nodes[host.hostname] += 1
                    break
                except KeyError:
                    pass

        # get the active mgr.
        managers = self.agent.remote('cephadm', 'list_daemons', service_name='mgr')
        if managers.exception_str:
            raise Exception(monitors.exception_str)

        for daemon in managers.result:
            if daemon.is_active:
                active_manager = daemon.hostname
                try:
                    nodes[daemon.hostname] += 1
                except KeyError:
                    pass

        # get the winner monitor
        best_monitor = max(nodes, key=nodes.get)

        return best_monitor, active_manager

    def upload_file(self, file_name: str, chunk_pattern: str = '', percent_complete: int = 100) -> None:
        """
        Upload a file to ecurep.
        chunk_pattern: If provided, the file is divided in chunks
        percent_complete: will send `percent_complete` percent in the ReportStatusLogUpload message.
            If level == 2, we send 1% after the diagnostics file and before the sos report files,
            if level == 1, we send 100% after the diagnostics file, as there are no more files to send
        """

        # We first consider the module options to allow for flexible
        # workarounds should we need them, otherwise we load the default keys
        auth = self.agent.get_ecurep_user_pass()
        if self.agent.owner_company_name:
            owner = self.agent.owner_company_name
        else:
            owner = "MyCompanyUploadClient"

        resp = None
        file_path = 'None'

        # Get the unique Upload ID for the file
        try:
            #None 1. Obtain the file id to upload the file
            ecurep_file_id_url = f'{self.agent.ecurep_url}/app/upload_tid?name={urllib.parse.quote(file_name)}&client={urllib.parse.quote(owner)}'
            self.agent.log.info(f"WorkFlowUploadSnap <{self.req_id}> : getting unique upload id from <{ecurep_file_id_url}>")
            resp = requests.post(url=ecurep_file_id_url, auth=auth, timeout=30)
            resp.raise_for_status()
            file_id_for_upload = resp.json()['id']  # throw on purpose if there is no file_id_for_upload
            self.agent.log.info(f"WorkFlowUploadSnap <{self.req_id}> : unique id for upload is <{file_id_for_upload}>")
        except Exception as ex:
            explanation = resp.text if resp else ""
            raise SendError(f'WorkFlowUploadSnap <{self.req_id}> : Failed to send <{file_name}> to <{ecurep_file_id_url}>: {ex}: {explanation} trace={traceback.format_exc()}')

        try:
            # 2. Upload the file
            ecurep_file_upload_url = f'{self.agent.ecurep_url}/app/upload_sf/files/{file_id_for_upload}?case_id={urllib.parse.quote(self.pmr)}&client={urllib.parse.quote(owner)}'
            file_size = 0
            if chunk_pattern:
                files_to_upload = (glob.glob(f'{DIAGS_FOLDER}/{chunk_pattern}'))
                for part in files_to_upload:
                    file_size += os.path.getsize(part)
            else:
                files_to_upload = [f'{DIAGS_FOLDER}/{file_name}']
                file_size = os.path.getsize(f'{DIAGS_FOLDER}/{file_name}')

            start_byte = 0
            part_sent = 0
            self.agent.log.info(f"WorkFlowUploadSnap <{self.req_id}> : uploading file {file_name} to <{ecurep_file_upload_url}>")
            for file_path in sorted(files_to_upload):
                chunk_size = os.path.getsize(file_path)
                with open(file_path, 'rb') as file:
                    if chunk_pattern:
                        self.agent.log.info(f"WorkFlowUploadSnap <{self.req_id}> : uploading part {file_path} to <{ecurep_file_upload_url}>")
                    resp = requests.post(url = ecurep_file_upload_url,
                                        data = file.read(),
                                        headers = {'Content-Type': 'application/octet-stream',
                                                   'X-File-Name': file_name,
                                                   'X-File-Size': f'{file_size}',
                                                   'Content-Range': f'bytes {start_byte}-{chunk_size + start_byte}/{file_size}'
                                        },
                    )
                    self.agent.log.info(f'WorkFlowUploadSnap <{self.req_id}> : uploaded {file_name} -> bytes {start_byte}-{chunk_size + start_byte}/{file_size}')
                    resp.raise_for_status()
                start_byte += chunk_size
                part_sent += 1
                if self.send_status_log_upload:
                    if chunk_pattern:
                        percent_progress = int(part_sent/len(files_to_upload) * 100)
                        status = OPERATION_STATUS_COMPLETE if percent_progress == 100 else OPERATION_STATUS_IN_PROGRESS
                        ReportStatusLogUpload(self.agent, self.next_event_id(), self.si_requestid, percent_progress, f"file <{file_name}> is being sent", status).run()
                    else:
                        status = OPERATION_STATUS_COMPLETE if percent_complete == 100 else OPERATION_STATUS_IN_PROGRESS
                        ReportStatusLogUpload(self.agent, self.next_event_id(), self.si_requestid, percent_complete, status, status).run()
        except Exception as ex:
            explanation = resp.text if resp else ""
            raise SendError(f'WorkFlowUploadSnap <{self.req_id}> : Failed to send <{file_path}> to <{ecurep_file_upload_url}>: {ex}: {explanation} trace={traceback.format_exc()}')

class ReportStatusLogUpload(Report):
    def __init__(self, agent, report_event_id, si_requestid, percent_progress: int, description: str, status: str):
        super().__init__(agent, 'upload_snap_progress')
        self.percent_progress = percent_progress
        self.description = description
        self.status = status
        self.report_event_id = report_event_id  # We use the same report envalope event_id that we received in the upload_snap UR
        self.si_requestid = si_requestid

    def compile(self) -> Optional[dict]:
        # We override run because this event gets a non standard generate arguments
        report_times = ReportTimes()
        self.set_headers(report_times, self.report_event_id)
        event = EventStatusLogUpload(self.agent).generate(report_times, self.si_requestid, self.percent_progress, self.description, self.status)
        self.add_event(event)
        return self.data

class EventStatusLogUpload(Event):
    def gather(self) -> dict:
        return {}

    def generate(self, report_times: ReportTimes, si_requestid: str, percent_progress: int, description: str, status: str) -> None:
        super().generate('status', 'ceph_log_upload', report_times)

        complete = percent_progress == 100
        self.data["body"] = {
            "component": "ceph_log_upload",
            "event_transaction_id": "Unsolicited_Storage_Insights_RedHatMarine_ceph_Request",
            "product":  "Red Hat Ceph",
            "description": description,
            "state" : f"{status} ({percent_progress}%)",
            "complete" : complete,
            "payload": {
                "action": "Unsolicited_Storage_Insights_RedHatMarine_ceph_Request",
                "description": description,
                "state" : status,
                "progress": percent_progress,
                "complete" : complete,
                "si_requestid": si_requestid,
            }
        }
        return self

class ReportConfirmResponse(Report):
    def __init__(self, agent, report_event_id) -> None:
        super().__init__(agent, 'confirm_response', [EventConfirmResponse])
        self.report_event_id = report_event_id  # We use the same report envalope event_id that we received in the upload_snap UR

class EventConfirmResponse(Event):
    def gather(self) -> dict:
        return {}

    def generate(self, report_times: ReportTimes):
        super().generate('confirm_response', 'ceph_operations', report_times)

        self.data["body"] = {
                "event_transaction_id": "Unsolicited_Storage_Insights_RedHatMarine_ceph_Request",
                "event_type": "last_contact",
        }
        return self

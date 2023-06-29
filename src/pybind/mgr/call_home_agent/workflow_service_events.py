from .report import Report, ReportTimes, Event
from .report_ur_error import ReportURError
from .report_service import ReportService
from .report_last_contact_service_query import ReportLastContactServiceQuery
from .workflow_upload_snap import WorkFlowUploadSnap
from .exceptions import *
import time
import urllib.parse
from typing import Tuple, Optional
import glob
import os
import traceback
import re
import requests
import json

class WorkFlowServiceEvents:
    def __init__(self, agent, alerts: list):
        self.agent = agent
        self.alerts = alerts
        self.service_query_num_tries = 0

    def run(self) -> None:
        self.agent.log.info(f"WorkFlowServiceEvents: Processing new request")
        try:
            report_service = ReportService(self.agent, self.alerts)
            response_text = report_service.run()
            response = json.loads(response_text)

            # Validate the response to the ReportService request
            if 'response_state' not in response:
                self.agent.log.error(f'WorkFlowServiceEvents: Bad reply to ReportService(): {response_text}')
                return

            self.report_service_event_id = report_service.events[0].event_event_id

            # Ticket will take time to be created. Poll for the creation completion.
            self.schedule_service_query()

        except Exception as ex:
            self.agent.log.error(f'Error in creating service event. Exception={ex} trace={traceback.format_exc()}')

    def schedule_service_query(self) -> None:
        if self.service_query_num_tries >= 10:
            self.agent.log.warning(f"WorkFlowServiceEvents: Did not receive a service_query reply in 10 tries.")
            return
        self.service_query_num_tries += 1
        self.agent.scheduler.enter(30, 1, self.run_scheduled_service_query)  # we might get the timeout from a conf option in a later version

    def run_scheduled_service_query(self) -> None:
        response_text = ReportLastContactServiceQuery(self.agent, self.report_service_event_id).run()
        try:
            response = json.loads(response_text)
            transactions = response['response_state']['transactions']  # Will raise if we got a bad reply
            if not transactions or self.report_service_event_id not in transactions:  # No reply yet
                self.schedule_service_query()
                return
            else:
                problem_id = transactions[self.report_service_event_id]['response_object']['problem_id']
        except Exception as ex:
            self.agent.log.error(f'WorkFlowServiceEvents: Error querying for service creation. Exception={ex} response={response_text}')
            return

        try:
            request = {
                    'options': {
                        'pmr': problem_id,
                        'level': 2  # Includes an SOS report
                        # No need to simulate si_requestid
                        }
                    }
            self.agent.log.debug(f"Starting WorkFlowUploadSnap for problem_id: {problem_id}")
            WorkFlowUploadSnap(self.agent, request, 'service_request', None, False).run()
        except Exception as ex:
            return HandleCommandResult(stderr=f"Error sending service request diagnostics: {ex}")


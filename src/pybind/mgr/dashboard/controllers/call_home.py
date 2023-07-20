# -*- coding: utf-8 -*-

import json

from typing import Tuple
from . import APIDoc, APIRouter, RESTController, Endpoint
from .. import mgr
from ..exceptions import DashboardException

@APIRouter('/call_home')
@APIDoc("Call Home Management API", "CallHome")
class CallHome(RESTController):

    def list(self, ibm_id: str, company_name: str,
                       first_name: str, last_name: str,
                       email: str) -> Tuple[int, str, str]:
        try:
            error_code, out, err = mgr.remote('call_home_agent', 'list_tenants', ibm_id, company_name,
                                              first_name, last_name, email)
            if error_code != 0:
                raise DashboardException(f'Listing tenants error: {err}')
        except ImportError as ie:
            raise DashboardException(f'Listing teanants error: {ie}')
        except RuntimeError as ie:
            raise DashboardException(f'Listing tenants error: {ie}')
        
        return json.loads(out)

    def set(self, tenant_id: str, ibm_id: str, company_name: str,
                first_name: str, last_name: str, email: str) -> Tuple[int, str, str]:
        try:
            error_code, _, err = mgr.remote('call_home_agent', 'set_tenant_id', tenant_id, ibm_id,
                                                company_name, first_name, last_name, email)
            if error_code != 0:
                raise DashboardException(f'Error setting tenant id: {err}')
        except ImportError as ie:
            raise DashboardException(f'Error setting tenant id: {ie}')
        except RuntimeError as ie:
            raise DashboardException(f'Error setting tenant id: {ie}')
    
    @Endpoint('GET')
    def download(self, report_type: str):
        try:
            error_code, out, err = mgr.remote('call_home_agent', 'print_report_cmd', report_type)
            if error_code != 0:
                raise DashboardException(f'Error downloading report: {err}')
        except RuntimeError as e:
            raise DashboardException(e, component='call_home')
        
        return json.loads(out)
        
    @Endpoint('GET')
    def info(self):
        try:
            error_code, out, err = mgr.remote('call_home_agent', 'customer')
            if error_code != 0:
                raise DashboardException(f'Error getting customer info: {err}')
        except RuntimeError as e:
            raise DashboardException(e, component='call_home')
        
        return json.loads(out)

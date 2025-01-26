# -*- coding: utf-8 -*-
import errno
import json
from typing import Any, Dict, Optional, Callable

import yaml
from mgr_module import CLICheckNonemptyFileInput, CLICommand, CLIReadCommand, \
    CLIWriteCommand, HandleCommandResult, HandlerFuncType

from ..exceptions import DashboardException
from ..rest_client import RequestException
from .nvmeof_conf import ManagedByOrchestratorException, \
    NvmeofGatewayAlreadyExists, NvmeofGatewaysConfig


@CLIReadCommand('dashboard nvmeof-gateway-list')
def list_nvmeof_gateways(_):
    '''
    List NVMe-oF gateways
    '''
    return 0, json.dumps(NvmeofGatewaysConfig.get_gateways_config()), ''


@CLIWriteCommand('dashboard nvmeof-gateway-add')
@CLICheckNonemptyFileInput(desc='NVMe-oF gateway configuration')
def add_nvmeof_gateway(_, inbuf, name: str, group: str, daemon_name: str):
    '''
    Add NVMe-oF gateway configuration. Gateway URL read from -i <file>
    '''
    service_url = inbuf
    try:
        NvmeofGatewaysConfig.add_gateway(name, service_url, group, daemon_name)
        return 0, 'Success', ''
    except NvmeofGatewayAlreadyExists as ex:
        return -errno.EEXIST, '', str(ex)
    except ManagedByOrchestratorException as ex:
        return -errno.EINVAL, '', str(ex)
    except RequestException as ex:
        return -errno.EINVAL, '', str(ex)


@CLIWriteCommand('dashboard nvmeof-gateway-rm')
def remove_nvmeof_gateway(_, name: str, daemon_name: str = ''):
    '''
    Remove NVMe-oF gateway configuration
    '''
    try:
        NvmeofGatewaysConfig.remove_gateway(name, daemon_name)
        return 0, 'Success', ''
    except ManagedByOrchestratorException as ex:
        return -errno.EINVAL, '', str(ex)


class OutputModifiers:
    @staticmethod
    def subsystem_add(output: Optional[Dict[str, Any]], success: bool, cmd_dict: Dict[str, Any], e: Exception = None):
        if success:
            return {"success": f"Added subsystem: {cmd_dict['nqn']}"}
        else: 
            return {"failure": f"Failed to add subsystem: {cmd_dict['nqn']}. Error: {str(e)}"}

    @staticmethod
    def subsystem_del(output: Optional[Dict[str, Any]], success: bool, cmd_dict: Dict[str, Any], e: Exception = None):
        if success:
            return {"success": f"Deleted subsystem: {cmd_dict['nqn']}"}
        else: 
            return {"failure": f"Failed to delete subsystem: {cmd_dict['nqn']}. Error: {str(e)}"}
        #"Deleting subsystem {args.subsystem}: Successful"
        #"Failure deleting subsystem {args.subsystem}:\n{ex}"

    @staticmethod
    def listener_add(output: Optional[Dict[str, Any]], success: bool, cmd_dict: Dict[str, Any], e: Exception = None):
        if success:
            return {"success": f"Added {cmd_dict['nqn']} listener at {cmd_dict['traddr']}:{cmd_dict['trsvcid']}"}
        else: 
            return {"failure": f"Failed to add {cmd_dict['nqn']} listener at {cmd_dict['traddr']}:{cmd_dict['trsvcid']}. Error: {str(e)}"}
        #"Adding {args.subsystem} listener at {traddr}:{args.trsvcid}: Successful"
        #"Failure adding {args.subsystem} listener at {traddr}:{args.trsvcid}:\n{ex}

    @staticmethod
    def listener_del(output: Optional[Dict[str, Any]], success: bool, cmd_dict: Dict[str, Any], e: Exception = None):
        if success:
            return {"success": f"Deleted {cmd_dict['nqn']} listener at {cmd_dict['traddr']}:{cmd_dict['trsvcid']}"}
        else: 
            return {"failure": f"Failed to delete {cmd_dict['nqn']} listener at {cmd_dict['traddr']}:{cmd_dict['trsvcid']}. Error: {str(e)}"}
        #"Failure deleting listener {traddr}:{args.trsvcid} from {args.subsystem}:\n{ex}"
        #"Deleting listener {traddr}:{args.trsvcid} from {args.subsystem} {host_msg}: Successful"

    @staticmethod
    def namespace_add(output: Optional[Dict[str, Any]], success: bool, cmd_dict: Dict[str, Any], e: Exception = None):
        if success:
            return {"success": f"Added namespace {output['nsid'] if output else ''} to subsystem: {cmd_dict['nqn']}"}
        else: 
            return {"failure": f"Failed to add namespace {output['nsid'] if output else ''} to subsystem: {cmd_dict['nqn']}"}
        #"Adding namespace {ret.nsid} to {args.subsystem}: Successful"
        #"Failure adding namespace {nsid_msg}to {args.subsystem}"

    @staticmethod
    def namespace_del(output: Optional[Dict[str, Any]], success: bool, cmd_dict: Dict[str, Any], e: Exception = None):
        if success:
            return {"success": f"Deleted namespace {output['nsid'] if output else ''} to subsystem: {cmd_dict['nqn']}"}
        else: 
            return {"failure": f"Failed to del namespace {output['nsid'] if output else ''} to subsystem: {cmd_dict['nqn']}"}
        #"Deleting namespace {args.nsid} from {args.subsystem}: Successful"
        #Failure deleting namespace:\n{ex}"

    @staticmethod
    def host_add(output: Optional[Dict[str, Any]], success: bool, cmd_dict: Dict[str, Any], e: Exception = None):
        if success:
            if cmd_dict['host'] == '*':
                return {"success": f"Allowed open host access to subsystem: {cmd_dict['nqn']}"}
            else:
                return {"success": f"Added host {cmd_dict['host']} subsystem: {cmd_dict['nqn']}"}
        else: 
            if cmd_dict['host'] == '*':
                return {"failure": f"Failed to open host access to subsystem: {cmd_dict['nqn']}"}    
            else: 
                return {"failure": f"Failed to add host {cmd_dict['host']} subsystem: {cmd_dict['nqn']}"}    
        """
        if one_host_nqn == "*":
                    errmsg = f"Failure allowing open host access to {args.subsystem}"
                else:
                    errmsg = f"Failure adding host {one_host_nqn} to {args.subsystem}"
        
        if one_host_nqn == "*":
                        out_func(f"Allowing open host access to {args.subsystem}: Successful")
                    else:
                        out_func(f"Adding host {one_host_nqn} to {args.subsystem}: Successful")
        """

    @staticmethod
    def host_del(output: Optional[Dict[str, Any]], success: bool, cmd_dict: Dict[str, Any], e: Exception = None):
        if success:
            if cmd_dict['host'] == '*':
                return {"success": f"Disabled host access to subsystem: {cmd_dict['nqn']}"}
            else:
                return {"success": f"Removed host {cmd_dict['host']} access to subsystem: {cmd_dict['nqn']}"}
        else: 
            if cmd_dict['host'] == '*':
                return {"failure": f"Failed to disable host access to subsystem: {cmd_dict['nqn']}"}    
            else: 
                return {"failure": f"Failed to remove host {cmd_dict['host']} access to subsystem: {cmd_dict['nqn']}"}    
        """
        if one_host_nqn == "*":
                    errmsg = f"Failure disabling open host access to {args.subsystem}"
                else:
                    errmsg = f"Failure removing host {one_host_nqn} access to {args.subsystem}"
                    
        if one_host_nqn == "*":
                        out_func(f"Disabling open host access to {args.subsystem}: Successful")
                    else:
                        out_func(f"Removing host {one_host_nqn} access from "
                                 f"{args.subsystem}: Successful")
        """


class NvmeofCLICommand(CLICommand):
    def __init__(self, prefix, perm = 'rw', poll = False, out_modifier: Callable[[Optional[Dict[str, Any]], bool, Dict[str, Any], Exception], Dict[str, Any]] = None):
        self.out_modifier = out_modifier
        super().__init__(prefix, perm, poll)
    
    def __call__(self, func) -> HandlerFuncType:  # type: ignore
        # pylint: disable=useless-super-delegation
        """
        This method is being overriden solely to be able to disable the linters checks for typing.
        The NvmeofCLICommand decorator assumes a different type returned from the
        function it wraps compared to CLICmmand, breaking a Liskov substitution principal,
        hence triggering linters alerts.
        """
        return super().__call__(func)

    def call(self,
             mgr: Any,
             cmd_dict: Dict[str, Any],
             inbuf: Optional[str] = None) -> HandleCommandResult:
        try:
            ret = super().call(mgr, cmd_dict, inbuf)
            if self.out_modifier:
                ret = self.out_modifier(ret, True, cmd_dict)
            out_format = cmd_dict.get('format')
            if out_format == 'json' or not out_format:
                if ret is None:
                    out = ''
                else:
                    out = json.dumps(ret)
            elif out_format == 'yaml':
                if ret is None:
                    out = ''
                else:
                    out = yaml.dump(ret)
            else:
                return HandleCommandResult(-errno.EINVAL, '',
                                           f"format '{out_format}' is not implemented")
            return HandleCommandResult(0, out, '')
        except DashboardException as e:
            if self.out_modifier:
                self.out_modifier(None, False, cmd_dict, e)
            return HandleCommandResult(-errno.EINVAL, '', str(e))

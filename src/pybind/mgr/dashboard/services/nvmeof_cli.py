# -*- coding: utf-8 -*-
import errno
import json
from abc import ABC, abstractmethod
from typing import Annotated, Any, Dict, List, NamedTuple, Optional, Type, \
    Union, get_args, get_origin, get_type_hints

import yaml
from mgr_module import CLICheckNonemptyFileInput, CLICommand, CLIReadCommand, \
    CLIWriteCommand, HandleCommandResult, HandlerFuncType
from prettytable import PrettyTable

from ..exceptions import DashboardException
from ..model.nvmeof import CliFlags, CliHeader
from ..rest_client import RequestException
from .nvmeof_conf import ManagedByOrchestratorException, \
    NvmeofGatewayAlreadyExists, NvmeofGatewaysConfig


from .nvmeof_conf import NvmeofGatewaysConfig

@CLIReadCommand('dashboard tomer')
def tomer_debug(_): 
    service_name, gateway_addr = NvmeofGatewaysConfig.get_service_info()
    root_ca_cert = str(NvmeofGatewaysConfig.get_root_ca_cert(service_name))
    client_key = None
    client_cert = None
    exc1 = None
    trace1 = None
    exc2 = None
    trace2 = None
    exc3 = None
    trace3 = None
    response = None
    if root_ca_cert:
        client_key = NvmeofGatewaysConfig.get_client_key(service_name)
        client_cert = NvmeofGatewaysConfig.get_client_cert(service_name)
        server_cert = NvmeofGatewaysConfig.get_server_cert(service_name)
    gw_config = NvmeofGatewaysConfig.get_gateways_config()
    
    try:
        from ..services.nvmeof_client import NVMeoFClient, convert_to_model, \
            empty_response, handle_nvmeof_error, pick
        from google.protobuf.json_format import MessageToDict
        client = NVMeoFClient()
        
        gw_info = client.stub.get_gateway_info(
                NVMeoFClient.pb2.get_gateway_info_req()
            )
        response = NVMeoFClient.pb2.gw_version(status=gw_info.status,
                                            error_message=gw_info.error_message,
                                            version=gw_info.version)
        response = MessageToDict(response, including_default_value_fields=True,
                                         preserving_proto_field_name=True)
    except Exception as e:
        import traceback
        trace1 = traceback.format_exc()
        exc1 = str(e)
        
    try:
        import grpc
        print('Securely connecting to: %s', client.gateway_addr)
        credentials = grpc.ssl_channel_credentials(
            root_certificates=root_ca_cert,
            private_key=client_key,
            certificate_chain=client_cert,
        )
        channel = grpc.secure_channel(self.gateway_addr, credentials)
        print('success!!!')
    except Exception as e:
        trace2 = traceback.format_exc()
        exc2 = str(e)
        print('exception!\n' + str(e) + '\n' + traceback.format_exc()+ "\n")
        
    try:
        import grpc
        print('Securely connecting to: %s', client.gateway_addr)
        credentials = grpc.ssl_channel_credentials(
            root_certificates=server_cert,
            private_key=client_key,
            certificate_chain=client_cert,
        )
        channel = grpc.secure_channel(self.gateway_addr, credentials)
        print('success 2!!!')
    except Exception as e:
        trace3 = traceback.format_exc()
        exc3 = str(e)
        print('exception2!\n' + str(e) + '\n' + traceback.format_exc()+ "\n")
    resp = {"root_ca_cert": root_ca_cert,
            "client_key": str(client_key),
            "client_cert": str(client_cert),
            "server_cert": str(server_cert),
            "gw_config": gw_config,
            "gw_addr": gateway_addr,
            "service_name": service_name,
            "exc1": exc1,
            "trace1": trace1,
            "exc2": exc2,
            "trace2": trace2,
            "exc3": exc3,
            "trace3": trace3,
            "response": response,
            "client_gw_addr": client.gateway_addr}
    return 0, json.dumps(resp), ''


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


def convert_from_bytes(num_in_bytes):
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    size = float(num_in_bytes)
    unit_index = 0

    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1

    # Round to no decimal if it's an integer, otherwise show 1 decimal place
    if size.is_integer():
        size_str = f"{int(size)}"
    else:
        size_str = f"{size:.1f}"

    return f"{size_str}{units[unit_index]}"


class OutputFormatter(ABC):
    @abstractmethod
    def format_output(self, data, model):
        """Format the given data for output."""
        raise NotImplementedError()


class AnnotatedDataTextOutputFormatter(OutputFormatter):
    def _snake_case_to_title(self, s):
        return s.replace('_', ' ').title()

    def _create_table(self, field_names):
        table = PrettyTable(border=True)
        titles = [self._snake_case_to_title(field) for field in field_names]
        table.field_names = titles
        table.align = 'l'
        table.padding_width = 0
        return table

    def _get_text_output(self, data):
        if isinstance(data, list):
            return self._get_list_text_output(data)
        return self._get_object_text_output(data)

    def _get_list_text_output(self, data):
        columns = list(dict.fromkeys([key for obj in data for key in obj.keys()]))
        table = self._create_table(columns)
        for d in data:
            row = []
            for col in columns:
                row.append(str(d.get(col)))
            table.add_row(row)
        return table.get_string()

    def _get_object_text_output(self, data):
        columns = [k for k in data.keys() if k not in ["status", "error_message"]]
        table = self._create_table(columns)
        row = []
        for col in columns:
            row.append(str(data.get(col)))
        table.add_row(row)
        return table.get_string()

    def _is_list_of_complex_type(self, value):
        if not isinstance(value, list):
            return False

        if not value:
            return None

        primitives = (int, float, str, bool, bytes)

        return not isinstance(value[0], primitives)

    def _select_list_field(self, data: Dict) -> Optional[str]:
        for key, value in data.items():
            if self._is_list_of_complex_type(value):
                return key
        return None

    def is_namedtuple_type(self, obj):
        return isinstance(obj, type) and issubclass(obj, tuple) and hasattr(obj, '_fields')

    # pylint: disable=too-many-branches
    def process_dict(self, input_dict: dict,
                     nt_class: Type[NamedTuple],
                     is_top_level: bool) -> Union[Dict, str, List]:
        result: Dict = {}
        if not input_dict:
            return result
        hints = get_type_hints(nt_class, include_extras=True)

        for field, type_hint in hints.items():
            if field not in input_dict:
                continue

            value = input_dict[field]
            origin = get_origin(type_hint)

            actual_type = type_hint
            annotations = []
            output_name = field
            skip = False

            if origin is Annotated:
                actual_type, *annotations = get_args(type_hint)
                for annotation in annotations:
                    if annotation == CliFlags.DROP:
                        skip = True
                        break
                    if isinstance(annotation, CliHeader):
                        output_name = annotation.label
                    elif is_top_level and annotation == CliFlags.EXCLUSIVE_LIST:
                        assert get_origin(actual_type) == list
                        assert len(get_args(actual_type)) == 1
                        return [self.process_dict(item, get_args(actual_type)[0],
                                                  False) for item in value]
                    elif is_top_level and annotation == CliFlags.EXCLUSIVE_RESULT:
                        return f"Failure: {input_dict.get('error_message')}" if bool(
                            input_dict[field]) else "Success"
                    elif annotation == CliFlags.SIZE:
                        value = convert_from_bytes(int(input_dict[field]))

            if skip:
                continue

            # If it's a nested namedtuple and value is a dict, recurse
            if self.is_namedtuple_type(actual_type) and isinstance(value, dict):
                result[output_name] = self.process_dict(value, actual_type, False)
            else:
                result[output_name] = value

        return result

    def _convert_to_text_output(self, data, model):
        data = self.process_dict(data, model, True)
        if isinstance(data, str):
            return data
        return self._get_text_output(data)

    def format_output(self, data, model):
        return self._convert_to_text_output(data, model)


class NvmeofCLICommand(CLICommand):
    def __init__(self, prefix, model: Type[NamedTuple], perm='rw', poll=False):
        super().__init__(prefix, perm, poll)
        self._output_formatter = AnnotatedDataTextOutputFormatter()
        self._model = model

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
            out_format = cmd_dict.get('format')
            if ret is None:
                out = ''
            if out_format == 'plain' or not out_format:
                out = self._output_formatter.format_output(ret, self._model)
            elif out_format == 'json':
                out = json.dumps(ret)
            elif out_format == 'yaml':
                out = yaml.dump(ret)
            else:
                return HandleCommandResult(-errno.EINVAL, '',
                                           f"format '{out_format}' is not implemented")
            return HandleCommandResult(0, out, '')
        except DashboardException as e:
            return HandleCommandResult(-errno.EINVAL, '', str(e))

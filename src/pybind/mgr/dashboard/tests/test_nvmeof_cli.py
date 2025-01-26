import errno
from unittest.mock import MagicMock

import pytest
from mgr_module import CLICommand, HandleCommandResult

from ..services.nvmeof_cli import NvmeofCLICommand


@pytest.fixture(scope="class", name="sample_command")
def fixture_sample_command():
    test_cmd = "test command"

    @NvmeofCLICommand(test_cmd)
    def func(_): # noqa # pylint: disable=unused-variable
        return {'a': '1', 'b': 2}
    yield test_cmd
    del NvmeofCLICommand.COMMANDS[test_cmd]
    assert test_cmd not in NvmeofCLICommand.COMMANDS
    
    
@pytest.fixture(scope='class', name='sample_modified_output')
def fixture_sample_modified_output():
    return 'modified_output'


def fixture_sample_output_modifier_func(sample_modified_output):
    func = MagicMock(return_value=sample_modified_output)
    return func


@pytest.fixture(scope="class", name="sample_command_with_out_mod")
def fixture_sample_command_with_out_mod(sample_modified_output):    
    test_cmd = "test command"
    
    def out_mod(a,b,c):
        return sample_modified_output
    @NvmeofCLICommand(test_cmd, out_mod)
    def func(_): # noqa # pylint: disable=unused-variable
        return {'a': '1', 'b': 2}
    yield test_cmd
    del NvmeofCLICommand.COMMANDS[test_cmd]
    assert test_cmd not in NvmeofCLICommand.COMMANDS
    

@pytest.fixture(scope="class", name='base_call_result_mock')
def fixture_base_call_result_mock(monkeypatch):
    return  {'a': 'b'}
    
@pytest.fixture(scope="class", name='base_call_mock')
def fixture_base_call_mock(monkeypatch, base_call_result_mock):
    super_mock = MagicMock()
    super_mock.return_value = base_call_result_mock
    monkeypatch.setattr(CLICommand, 'call', super_mock)
    return super_mock


@pytest.fixture(name='base_call_return_none_mock')
def fixture_base_call_return_none_mock(monkeypatch):
    mock_result = None
    super_mock = MagicMock()
    super_mock.return_value = mock_result
    monkeypatch.setattr(CLICommand, 'call', super_mock)
    return super_mock


class TestNvmeofCLICommand:
    def test_command_being_added(self, sample_command):
        assert sample_command in NvmeofCLICommand.COMMANDS
        assert isinstance(NvmeofCLICommand.COMMANDS[sample_command], NvmeofCLICommand)

    def test_command_return_cmd_result_default_format(self, base_call_mock, sample_command):
        result = NvmeofCLICommand.COMMANDS[sample_command].call(MagicMock(), {})
        assert isinstance(result, HandleCommandResult)
        assert result.retval == 0
        assert result.stdout == '{"a": "b"}'
        assert result.stderr == ''
        base_call_mock.assert_called_once()

    def test_command_return_cmd_result_json_format(self, base_call_mock, sample_command):
        result = NvmeofCLICommand.COMMANDS[sample_command].call(MagicMock(), {'format': 'json'})
        assert isinstance(result, HandleCommandResult)
        assert result.retval == 0
        assert result.stdout == '{"a": "b"}'
        assert result.stderr == ''
        base_call_mock.assert_called_once()

    def test_command_return_cmd_result_yaml_format(self, base_call_mock, sample_command):
        result = NvmeofCLICommand.COMMANDS[sample_command].call(MagicMock(), {'format': 'yaml'})
        assert isinstance(result, HandleCommandResult)
        assert result.retval == 0
        assert result.stdout == 'a: b\n'
        assert result.stderr == ''
        base_call_mock.assert_called_once()

    def test_command_return_cmd_result_invalid_format(self, base_call_mock, sample_command):
        mock_result = {'a': 'b'}
        super_mock = MagicMock()
        super_mock.call.return_value = mock_result

        result = NvmeofCLICommand.COMMANDS[sample_command].call(MagicMock(), {'format': 'invalid'})
        assert isinstance(result, HandleCommandResult)
        assert result.retval == -errno.EINVAL
        assert result.stdout == ''
        assert result.stderr
        base_call_mock.assert_called_once()

    def test_command_return_empty_cmd_result(self, base_call_return_none_mock, sample_command):
        result = NvmeofCLICommand.COMMANDS[sample_command].call(MagicMock(), {})
        assert isinstance(result, HandleCommandResult)
        assert result.retval == 0
        assert result.stdout == ''
        assert result.stderr == ''
        base_call_return_none_mock.assert_called_once()
        
    def test_command_out_modifier_called(self, sample_command_with_out_mod, fixture_sample_output_modifier_func, sample_modified_output, base_call_result_mock):
        cmd_dict_mock = MagicMock()
        result = NvmeofCLICommand.COMMANDS[sample_command_with_out_mod].call(MagicMock(), cmd_dict_mock)
        assert isinstance(result, HandleCommandResult)
        assert result.retval == 0
        assert result.stdout == sample_modified_output
        assert result.stderr == ''
        fixture_sample_output_modifier_func.assert_called_once_with(base_call_result_mock, True, cmd_dict_mock)

    def test_command_out_modifier_called_on_error():
        pass
import json
from textwrap import dedent
from unittest import mock

import pytest

from tests.fixtures import with_cephadm_ctx, cephadm_fs, import_cephadm

_cephadm = import_cephadm()

class TestCallHomeIntegration:

    @mock.patch('cephadm.logger')
    def test_verify_call_home_settings(self, logger, cephadm_fs):
        cmd = [
            'bootstrap',
            '--mon-ip', '192.168.1.1'
        ]

        # test with directly set settings
        with with_cephadm_ctx(cmd) as ctx:
            # no enable flags, verify should be no-op
            _cephadm.verify_call_home_settings(ctx)

            # enable but no settings, should fail
            ctx.enable_ibm_call_home = True
            with pytest.raises(_cephadm.Error):
                _cephadm.verify_call_home_settings(ctx)

            # enable one of the settings, should still fail
            ctx.call_home_icn = 'ibm-customer-number'
            with pytest.raises(_cephadm.Error):
                _cephadm.verify_call_home_settings(ctx)

            # enable some, but not all required settings, should still fail
            ctx.ceph_call_home_contact_email = 'email'
            ctx.ceph_call_home_contact_phone = 'phone'
            with pytest.raises(_cephadm.Error):
                _cephadm.verify_call_home_settings(ctx)

            # enable remaining settings, should pass
            ctx.ceph_call_home_contact_first_name = 'fname'
            ctx.ceph_call_home_contact_last_name = 'lname'
            ctx.ceph_call_home_country_code = 'US'
            _cephadm.verify_call_home_settings(ctx)

        # test with settings through config file
        with with_cephadm_ctx(cmd) as ctx:
            # no enable flags, verify should be no-op
            _cephadm.verify_call_home_settings(ctx)

            # enable but no settings, should fail
            ctx.enable_ibm_call_home = True
            with pytest.raises(_cephadm.Error):
                _cephadm.verify_call_home_settings(ctx)

            # enable one of the settings, should still fail
            call_home_config = {'icn': 'ibm-customer-number'}
            cephadm_fs.create_file('call-home-config.json', contents=json.dumps(call_home_config))
            ctx.call_home_config = 'call-home-config.json'
            with pytest.raises(_cephadm.Error):
                _cephadm.verify_call_home_settings(ctx)

            # enable some, but not all required settings, should still fail
            call_home_config['email'] =  'email'
            call_home_config['phone'] =  'phone'
            cephadm_fs.remove('call-home-config.json')
            cephadm_fs.create_file('call-home-config.json', contents=json.dumps(call_home_config))
            with pytest.raises(_cephadm.Error):
                _cephadm.verify_call_home_settings(ctx)

            # enable remaining settings, should pass
            call_home_config['first_name'] =  'fname'
            call_home_config['last_name'] =  'lname'
            call_home_config['country_code'] =  'US'
            cephadm_fs.remove('call-home-config.json')
            cephadm_fs.create_file('call-home-config.json', contents=json.dumps(call_home_config))
            _cephadm.verify_call_home_settings(ctx)

    @mock.patch('cephadm.logger')
    def test_verify_storage_insights_settings(self, logger, cephadm_fs):
        cmd = [
            'bootstrap',
            '--mon-ip', '192.168.1.1'
        ]

        # test with directly set settings
        with with_cephadm_ctx(cmd) as ctx:
            # no enable flags, verify should be no-op
            _cephadm.verify_call_home_settings(ctx)

            # enable but no settings, should fail
            ctx.enable_storage_insights = True
            with pytest.raises(_cephadm.Error):
                _cephadm.verify_call_home_settings(ctx)

            # and tenant id, should now pass
            ctx.storage_insights_tenant_id = 'tenant_id'
            _cephadm.verify_call_home_settings(ctx)

        # test with settings through config file
        with with_cephadm_ctx(cmd) as ctx:
            # no enable flags, verify should be no-op
            _cephadm.verify_call_home_settings(ctx)

            # enable but no settings, should fail
            ctx.enable_storage_insights = True
            with pytest.raises(_cephadm.Error):
                _cephadm.verify_call_home_settings(ctx)

            # add tenant id, should now pass
            storage_insights_config = {'tenant_id': 'tenant-id'}
            cephadm_fs.create_file('storage-insights.json', contents=json.dumps(storage_insights_config))
            ctx.storage_insights_config = 'storage-insights.json'
            _cephadm.verify_call_home_settings(ctx)

from cephadmlib.daemons.smb import _truncated_vhostname, _MAX_HOSTNAME_LEN


class TestSafeVhostname:
    def test_short_name_unchanged(self):
        assert _truncated_vhostname('smb1-short-host') == 'smb1-short-host'

    def test_empty_string_unchanged(self):
        assert _truncated_vhostname('') == ''

    def test_one_over_limit_is_shortened(self):
        raw = (
            'smb1-example-host-with-a-very-long-descriptive-hostname'
            '-number-01'
        )
        assert len(raw) == 65
        fixed = _truncated_vhostname(raw)
        assert len(fixed) == _MAX_HOSTNAME_LEN
        assert fixed.startswith(raw[:55])

    def test_similar_long_names_do_not_collide(self):
        a = 'smb1-' + 'z' * 100 + 'AAAA'
        b = 'smb1-' + 'z' * 100 + 'BBBB'
        assert _truncated_vhostname(a) != _truncated_vhostname(b)

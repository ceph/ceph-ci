from collections import OrderedDict
import errno
import re
from typing import Optional, List, Any, Dict, Union


def normalize_hostname(hostname: str) -> str:
    """Normalize hostname to lowercase for case-insensitive matching."""
    return hostname.lower()


def assert_valid_host(name: str) -> None:
    p = re.compile('^[a-zA-Z0-9-]+$')
    if len(name) > 250:
        raise AssertionError(f'{name}: name is too long (max 250 chars)') from None
    for part in name.split('.'):
        if len(part) == 0:
            raise AssertionError('.-delimited name component must not be empty '
                                 f'but got {name}') from None
        if len(part) > 63:
            raise AssertionError('.-delimited name component must not be more '
                                 f'than 63 chars but got {name}') from None
        if not p.match(part):
            raise AssertionError('name component must include only a-z, 0-9, '
                                 f'and - but got {name}') from None


def assert_valid_oob(oob: Dict[str, str]) -> None:
    fields = ['username', 'password']
    try:
        for field in fields:
            assert field in oob.keys()
    except AssertionError as e:
        raise SpecValidationError(str(e))


class SpecValidationError(Exception):
    """
    Defining an exception here is a bit problematic, cause you cannot properly catch it,
    if it was raised in a different mgr module.
    """
    def __init__(self,
                 msg: str,
                 errno: int = -errno.EINVAL):
        super(SpecValidationError, self).__init__(msg)
        self.errno = errno


class HostSpec(object):
    """
    Information about hosts. Like e.g. ``kubectl get nodes``
    """
    def __init__(self,
                 hostname: str,
                 addr: Optional[str] = None,
                 labels: Optional[List[str]] = None,
                 status: Optional[str] = None,
                 location: Optional[Dict[str, str]] = None,
                 topological_labels: Optional[Union[Dict[str, str], str, List[str]]] = None,
                 oob: Optional[Dict[str, str]] = None,
                 ):
        self.service_type = 'host'

        #: the bare hostname on the host. Not the FQDN.
        self.hostname = normalize_hostname(hostname)

        #: DNS name or IP address to reach it
        self.addr = addr or normalize_hostname(hostname)

        #: label(s), if any
        self.labels = labels or []

        #: human readable status
        self.status = status or ''

        self.location = location

        self.topological_labels = self.parse_topological_labels(topological_labels)

        #: oob details, if provided
        self.oob = oob

    def validate(self) -> None:
        assert_valid_host(self.hostname)
        if self.oob:
            assert_valid_oob(self.oob)

    def to_json(self) -> Dict[str, Any]:
        r: Dict[str, Any] = {
            'hostname': self.hostname,
            'addr': self.addr,
            'labels': list(OrderedDict.fromkeys((self.labels))),
            'status': self.status,
        }
        if self.location:
            r['location'] = self.location
        if self.topological_labels:
            r['topological_labels'] = self.topological_labels
        if self.oob:
            r['oob'] = self.oob
        return r

    @classmethod
    def from_json(cls, host_spec: dict) -> 'HostSpec':
        host_spec = cls.normalize_json(host_spec)
        _cls = cls(
            host_spec['hostname'],
            host_spec['addr'] if 'addr' in host_spec else None,
            list(OrderedDict.fromkeys(
                host_spec['labels'])) if 'labels' in host_spec else None,
            host_spec['status'] if 'status' in host_spec else None,
            host_spec.get('location'),
            host_spec.get('topological_labels') if 'topological_labels' in host_spec else None,
            host_spec['oob'] if 'oob' in host_spec else None,
        )
        return _cls

    @staticmethod
    def normalize_json(host_spec: dict) -> dict:
        if 'hostname' in host_spec:
            host_spec['hostname'] = normalize_hostname(host_spec['hostname'])
        labels = host_spec.get('labels')
        if labels is not None:
            if isinstance(labels, str):
                host_spec['labels'] = [labels]
            elif (
                    not isinstance(labels, list)
                    or any(not isinstance(v, str) for v in labels)
            ):
                raise SpecValidationError(
                    f'Labels ({labels}) must be a string or list of strings'
                )

        loc = host_spec.get('location')
        if loc is not None:
            if (
                    not isinstance(loc, dict)
                    or any(not isinstance(k, str) for k in loc.keys())
                    or any(not isinstance(v, str) for v in loc.values())
            ):
                raise SpecValidationError(
                    f'Location ({loc}) must be a dictionary of strings to strings'
                )

        tlabels = host_spec.get('topological_labels')
        if tlabels is not None:
            if (
                    not isinstance(tlabels, dict)
                    or any(not isinstance(k, str) for k in tlabels.keys())
                    or any(not isinstance(v, str) for v in tlabels.values())
            ):
                raise SpecValidationError(
                    f'Topological labels ({tlabels}) must be a dictionary of strings to strings'
                )

        return host_spec

    def __repr__(self) -> str:
        args = [self.hostname]  # type: List[Any]
        if self.addr is not None:
            args.append(self.addr)
        if self.labels:
            args.append(self.labels)
        if self.status:
            args.append(self.status)
        if self.location:
            args.append(self.location)
        if self.topological_labels:
            args.append(self.topological_labels)

        return "HostSpec({})".format(', '.join(map(repr, args)))

    def matches_topological_labels(
        self,
        topological_labels: Union[str, List[str], Dict[str, str]]
    ) -> bool:
        # checks if provided topological labels are either a perfect
        # match or subset of the topological labels of this HostSpec object
        # with the exception of when the provided topological labels are empty
        # which returns false even though empty could be considered a subset
        # of the HostSpec object's topological labels
        tlabels = self.parse_topological_labels(topological_labels)
        if not self.topological_labels or not tlabels:
            return False
        for tlabel_key, tlabel_value in tlabels.items():
            if tlabel_key not in self.topological_labels:
                return False
            if self.topological_labels[tlabel_key] != tlabel_value:
                return False
        return True

    @staticmethod
    def parse_topological_labels(
        topological_labels: Optional[Union[str, List[str], Dict[str, str]]]
    ) -> Optional[Dict[str, str]]:
        # Get case where we got no labels out of the way
        if not topological_labels:
            return None

        tlabels: Dict[str, str] = {}
        tlabels_list: List[str] = []

        # Combine str and List[str] case. After this block
        # either topological_labels was a Dict, or
        # tlabels_list will be populated
        if isinstance(topological_labels, str):
            for tlabel in topological_labels.split(','):
                tlabels_list.append(tlabel)
        elif isinstance(topological_labels, List):
            for tlabel in topological_labels:
                if not isinstance(tlabel, str):
                    raise SpecValidationError(
                        f'Got non-string topological label {tlabel}'
                    )
                if ',' in tlabel:
                    tlabels_list.extend(tlabel.split(','))
                else:
                    tlabels_list.append(tlabel)

        # Now that we've combined str and List[str] case into just
        # List[str] (marked by tlabels_list being populated), if we have
        # that case, convert into a Dict[str, str]
        if tlabels_list:
            for tlabel in tlabels_list:
                if tlabel.find('=') == -1 or len(tlabel.split('=')) != 2:
                    raise SpecValidationError(
                        f'Got topological label "{tlabel}" not containing a single "=". Format '
                        'should be "key1=val1,key2=val2..."'
                    )
                tlabel_key, tlabel_val = tlabel.split('=')
                if tlabel_key in tlabels:
                    raise SpecValidationError(
                        f'Invalid topological labels. Found duplicate key {tlabel_key}')
                tlabels[tlabel_key] = tlabel_val

        # At this point if we got a str or List[str], tlabels is
        # already populated. If we got None, we returned immediately.
        # So If tlabels isn't populated, topological_labels
        # should be a Dict[str, str] itself
        if not tlabels:
            if not isinstance(topological_labels, dict):
                raise SpecValidationError(
                    f'Got topological labels {topological_labels} of unexpected '
                    f'type {type(topological_labels)}'
                )
            for tlabel_key, tlabel_value in topological_labels.items():
                if not isinstance(tlabel_key, str):
                    raise SpecValidationError(
                        f'Got topological label key {tlabel_key} of unexpected '
                        f'type {type(tlabel_key)}'
                    )
                if not isinstance(tlabel_value, str):
                    raise SpecValidationError(
                        f'Got topological label value {tlabel_value} of unexpected '
                        f'type {type(tlabel_value)}'
                    )
            tlabels = topological_labels
        return tlabels

    def __str__(self) -> str:
        if self.hostname != self.addr:
            return f'{self.hostname} ({self.addr})'
        return self.hostname

    def __eq__(self, other: Any) -> bool:
        # Let's omit `status` for the moment, as it is still the very same host.
        if not isinstance(other, HostSpec):
            return NotImplemented
        return self.hostname == other.hostname and \
            self.addr == other.addr and \
            sorted(self.labels) == sorted(other.labels) and \
            self.location == other.location and \
            self.topological_labels == other.topological_labels

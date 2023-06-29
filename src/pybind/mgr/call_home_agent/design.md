
# Abbriviations / Glossery

- **UR** : Unsolicited Request. A request message that we receive from "IBM Call
  Home". Currently we receive those in the HTTP reply for the "Last Contact"
report.
- **agent** : Instance of the CallHomeAgent class, which is the main class of the
  module. Usually it gets passed to constructors and functions as the first
argument so that they can use the CallHomeAgent to get configuration, etc. Note
that CallHomeAgent inherits from MgrModule, therefore all of its API is
available through the "agent".

---

# Classes

The main classes are those inheriting from or implementing the interface of:

- **WorkFlow** (interface)
- **Report** (base class)
- **Event** (base class)

WorkFlow implements the logic of _what_ to do, while Report and Event are only tools / macros that help generate the report JSON format.
A new report object, and the Event object (or objects) in it are created every time that a reports needs to be sent, and destroyed after the report is sent.
They do not live past a single report.

# Helper classes and interfaces:

- **reportTimes** - Holds the time of the report, and provides an API to get time, time_ms and local_time fields needed in the report.
- **URInfo** (interface) : Provides information about a UR, such as its ID for stale, ID for cooldown, timeout for cooldown, etc.

Currently, the current implementation does not require sending more than one
event in a report, and each event+report are sent in one, and only one,
workflow. So even thought the design supports having any number of events in a
report, and a report to be sent from any workflow, for readability sake, the
derived events are declared in the same file as in which the report class that uses them is declared.
E.g., `class EventStatusHealth` and `class ReportStatusHealth` are both declared in `ReportStatusHealth.py`.

---

## Workflow

An interface (not a base class).
Workflow classes implement a "workflow" that requires sending more than one Report.
Currently the only one is `WorkFlowUploadSnap`, which implements the following:

- WorkFlowUploadSnap:
  - collects diagnostics commands
  - collects SOS report if needed
  - uploads those reports to ECuRep while sending *ReportStatusLogUpload* with the progress
  - sends *ReportConfirmResponse* to mark that we processed this UR

### Interface

- `__init__(self, agent, req, req_id)`
- run()

## Report

Report implements the envalope of a message sent to IBM Call Home. Most of the reports are similar, but there are small changes
between them, therefore a specific report, such as ReportLastContact, inherits from Report and expands it with specific changes.

### Report base class

You can override any of these methods in derived classes to change the behavior

#### `__init__(self, agent, report_type, event_classes = [])`
Initialize the Report object

- `report_type`: string
- `event_classes`: list of `Event*` classes which should be included in this report. E.g. [EventStatusHealth, EventLastContact]

#### `compile(self)`
Creates the report, instantiate the Event classes given at `__init__` and calls their `generate()` to create the events and add them to the report
compile can return None if there is nothing to send.

This method is overriden in `ReportStatusAlerts` in order to return None if
there were no changes in alerts and therefore no need to send the report.
`run()`, which calls `compile()` will check the return for None and won't call
`send()` if there is None to send.

#### `run(self)`
Calls `self.compile()`, and if there is any data returned then calls `self.send()` to actually send the report.

### `send(self, report: dict, force: bool = False)`
Sends the report

## Event

Event implements the specific event that is sent in a report. The Event base class fills the boilerplate of the event such as the time fields.
Specific events inherit from Event and implement the difference.

#### `__init__`
Each derived Event can implements its own signature.

#### `gather(self)`
Most classes that derive from `Event` will implement `gather(self)` which collects and returns a dictionary of the payload data that needs to be sent in this event. (I.e. without the Event headers)
Afterwhich the derived class with override `generate` to push this data into the Event payload. See `EventInventory.generate` for example.

### Event base class

#### `__init__(self, agent)`
Initialize the Event object

#### `generate(self, event_type: str, component: str, report_times: ReportTimes)`
creates the event dictionary (headers etc).
usually is overriden in derived classes. The override method must call `super.gather` in its first line.

#### `set_content(self, content)`
Set the `body.payload.content` to `content`.
Usually called by an overridden gather method.

#### `data`
Member variable. Contains the populated event dictionary.

### EventGeneric base class
Inherits from `Event` and is used as a base class for `ReportStatusHealth`, `ReportInventory`, `EventLastContact` and `EventStatusAlerts`

#### `generate(self, event_type: str, component: str, description: str, report_times: ReportTimes)`
overrides Event.generate and adds information that is common to all the reports that are listed above.


## URInfo interface
Helper interface to provide information about a UR, such as its ID for stale, ID for cooldown, timeout for cooldown, etc.
usually short lived. i.e:
```python
if URUploadSnap(req).id_for_cooldown() in self.ur_cooldowns:
    continue  # don't process until the cooldown is over
```

Each UR type must implement the following:

- `__init__(self, req: dict)`: Intialize the URInfo. `req` is the UR `request` received.
- `id(self)`: Returns a unique ID representing this UR for `stale` handling - i.e. uniquely identifying this UR.
- `id_for_cooldown(self)`: Returns an ID representing this TYPE of UR for
  "cooldown" purposes, and not the specific instance thereof.  i.e. all
  UploadSnap URs of the same level should receive the same ID: `upload_snap-1`
  for upload snap level 1 (1 == no SOS report)
- `cooldown_timeout(self)`: Return the time in seconds that this type of UR should have. Example:
  - for "upload snap" level 2 the cooldown timeout is 2 hours because it's a heavy operation on the cluster,
  - for "upload snap" level 1 the cooldown timeout is 5 minutes because its a light operation

Currently there is only one implementation of this interface - URUploadSnap()


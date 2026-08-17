# dtcurrie:walkie:radio

A member's radio: this machine's microphone and speaker joined to a channel.

It runs two pumps in opposite directions — the microphone to the hub's uplink,
and the hub's downlink to the speaker — and it can change channel at runtime
without a config edit, which is the point of the model.

## Configuration

```json
{
  "name": "radio",
  "api": "rdk:component:generic",
  "model": "dtcurrie:walkie:radio",
  "attributes": {
    "source": "mic",
    "sink": "speaker",
    "uplink": "hub-uplink",
    "downlink": "hub-downlink",
    "member": "alpha",
    "channel": "ops",
    "gate_mode": "manual",
    "sample_rate": 16000,
    "num_channels": 1
  }
}
```

| Attribute | Type | Default | Description |
|---|---|---|---|
| `source` | string | required | The local `audio_in` to listen to |
| `sink` | string | required | The local `audio_out` to play through |
| `uplink` | string | required | The hub's uplink. Normally remote, so use the prefixed short name |
| `downlink` | string | required | The hub's downlink, likewise |
| `channel` | string | required | The channel to join at startup |
| `member` | string | the component's name | Who this radio is on the network. Must be unique |
| `gate_mode` | string | `manual` | `manual` (push-to-talk), `vox` or `open` |
| `start_talking` | bool | `false` | Open the gate as soon as the radio is built |
| `vox_threshold_dbfs` | float | `-40` | The level above which vox opens the gate |
| `vox_hangover_ms` | int | `800` | How long vox holds the gate open after the level drops |
| `sample_rate` | int | unset | The format this machine's microphone produces |
| `num_channels` | int | unset | See above |
| `max_queued_chunks` | int | `6` | How much audio may wait for a busy endpoint |
| `reconnect_ms` | int | `1000` | Delay between attempts to reopen a stream |

### Notes on the attributes

**All four endpoints are optional dependencies.** Two of them normally live on
the hub, and a required dependency on a remote resource blocks construction
entirely whenever that part is unreachable. As optional ones, the radio is
always built and can tell you which side is missing — which is exactly the
question you have when the hub is asleep.

**`member` must be unique across the network.** It decides who holds a channel
and it is what keeps you from hearing your own voice. Two radios sharing a name
share a floor identity *and* mutually suppress each other's audio: they can talk
over each other and neither hears the other. The bus warns when it sees it.

**`sample_rate`/`num_channels` must match the channel** if that channel declares
a format. Nothing resamples anywhere, so the hub refuses a mismatched
transmission rather than sending garbled audio to every listener.

**The listening side is always ungated.** Gating exists to stop a microphone
feeding a speaker it can hear; there is no microphone on that side, so refusing
to play what the channel sent would just be deafness.

## DoCommand

Keys are applied in a fixed order within one call: channel, then gate mode, then
talk. So `{"channel": "ops", "talk": true}` keys up on the new channel, never
the old one.

| Command | Effect |
|---|---|
| `{"channel": "logistics"}` | Retune both pumps. Refused if the hub does not carry it |
| `{"talk": true}` / `{"talk": false}` | Open or close the manual gate |
| `{"talk": true, "seconds": 5}` | Open the gate and close it again after 5 seconds |
| `{"gate_mode": "vox"}` | Change the gating strategy |
| `{"stats": true}` | Everything below |

A retune is checked against the hub's channel list *before* either pump is
touched, so a typo fails on the command rather than becoming silent deafness. If
the hub cannot be reached the retune is allowed — the radio is already reporting
that it is disconnected, and refusing would make a disconnected radio impossible
to pre-tune.

## Reading the counters

| Field | Meaning |
|---|---|
| `member`, `channel` | Who this radio is, and what it is tuned to |
| `ready`, `can_talk`, `can_listen` | Whether each half is wired up |
| `source_available`, `sink_available`, `uplink_available`, `downlink_available` | Which endpoints resolved |
| `gate_open`, `gate_mode`, `talking` | The talking gate |
| `busy_rejections` | Chunks not sent because somebody else held the channel |
| `retunes` | How many times this radio has changed channel |
| `tx_chunks_in` | Chunks read from the microphone. Climbs whether or not the gate is open |
| `tx_chunks_out`, `tx_bytes_out` | What actually reached the hub |
| `tx_peak_dbfs` | The microphone's level. `-120` is true digital silence |
| `tx_silent_seconds` | How long the microphone has been silent |
| `tx_format_mismatch` | Chunks that could not be carried at all |
| `tx_format_unexpected` | Chunks whose format differed from the configured one. Still forwarded |
| `rx_chunks_in` | Chunks received from the channel |
| `rx_chunks_dropped` | Chunks discarded because the speaker could not keep up |
| `hub_heartbeat_age_ms` | How long since the hub's last empty chunk |
| `tx_last_error`, `rx_last_error` | The most recent failure on each side |

When an endpoint is missing its counters are **omitted, not zeroed**. A zero
would read as "nothing has happened"; the truth is "this half does not exist
yet".

`busy_rejections` is its own counter and is never derived from the chunk
counters. The audio_out client sends its stream header without waiting for an
acknowledgement, so a refused transmission always gets one chunk out of the door
before the refusal comes back — `tx_chunks_out` counts it.

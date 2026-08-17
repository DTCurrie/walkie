# dtcurrie:walkie:bus

The switchboard. It declares which channels a hub carries and routes audio
between the members tuned to each one.

A bus has no audio API of its own — the `uplink` and `downlink` components are
the audio-facing side. All three must be configured on the same machine and in
the same module, because they share routing state in process.

## Configuration

```json
{
  "name": "bus",
  "api": "rdk:component:generic",
  "model": "dtcurrie:walkie:bus",
  "attributes": {
    "channels": [
      { "name": "ops" },
      { "name": "logistics", "sample_rate": 16000, "num_channels": 1 }
    ],
    "sample_rate": 16000,
    "num_channels": 1
  }
}
```

| Attribute | Type | Default | Description |
|---|---|---|---|
| `channels` | list | required | The channels this hub carries. A member may only tune to one named here |
| `channels[].name` | string | required | The channel name members use |
| `channels[].sample_rate` | int | unset | When set with `num_channels`, enforced: a talker in another format is refused |
| `channels[].num_channels` | int | unset | See above |
| `sample_rate` | int | `16000` | The format for channels that declare none |
| `num_channels` | int | `1` | See above |
| `max_queued_chunks` | int | `8` | How much audio may wait for one listener before the oldest is discarded |
| `floor_hangover_ms` | int | `800` | How long a talker who stops keeps the right to carry on |
| `floor_idle_ms` | int | `2000` | How long a transmission may send nothing before the hub takes its channel back |
| `keepalive_ms` | int | `10000` | How often an idle listener gets an empty chunk. `-1` disables it |

### On the defaults

**`sample_rate` defaults to 16000, not 48000.** The hub copies every talker to
every listener, so a ten-member channel at 48kHz mono is nearly 7 Mbit/s leaving
one machine. 16kHz is a third of that and is ample for speech.

**`max_queued_chunks` is small on purpose.** There are five bounded buffers
between a microphone and a far speaker, and the RDK owns two of them at a fixed
depth of 8. A generous value here buys nothing but latency.

**`floor_hangover_ms` cannot be set below 400.** A talking pump tears its stream
down after 400ms of quiet, so a shorter hangover would expire before the previous
transmission had finished closing and let a bystander take the channel
mid-sentence. The config is rejected rather than silently clamped.

**`floor_idle_ms` is what recovers a channel from a member that lost power.** The
viam rpc server sets no keepalive parameters, so a dead member leaves its stream
open at the hub until the OS TCP stack gives up — minutes to hours. Nothing else
in the stack detects this.

**`keepalive_ms` sends a chunk carrying no audio.** That is invisible to every
counter a listening radio keeps, so it cannot be confused with real audio, but it
lets a member tell a quiet channel from a hub that has stopped answering.

## DoCommand

### `{"stats": true}`

Returns `channels_detail`, one entry per channel:

| Field | Meaning |
|---|---|
| `name` | The channel |
| `format` | What it carries, e.g. `16000Hz/1ch` |
| `listeners` | How many members are subscribed |
| `members` | Their names |
| `holder` | Who is talking right now, or `""` |
| `transmissions` | How many transmissions the channel has carried |
| `busy_rejections` | How many talkers were turned away |
| `revocations` | How many transmissions were cut off — by the watchdog, a shutdown, or `clear_floor` |
| `chunks_sent` | Chunks delivered to listeners |
| `chunks_dropped` | Chunks discarded because a listener could not keep up |
| `keepalives` | Empty chunks sent to idle listeners |

### `{"channels": true}`

Returns `channels`, the declared names. This is what a radio asks before
retuning, so a bad channel is refused with an actionable error instead of
becoming silent deafness.

### `{"clear_floor": "<channel>"}`

Frees a channel by hand and reports `was_holding`. The talker's transmission is
aborted. An escape hatch — the watchdog handles the case this exists for.

## Notes

- Reconfiguring a bus drops every listener and talker on the network for about a
  second while it rebuilds. It is self-healing; `retunes` and the radios'
  reconnect counters make the blip visible rather than mysterious.
- Member identity is self-asserted and unauthenticated. Two radios with the same
  `member` share a floor and mute each other; the bus warns when it sees it.

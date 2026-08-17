# walkie

A channel-based audio network for Viam machines. Any number of machines join
named channels; a member's microphone reaches every speaker tuned to the same
channel, and a member can change channel at runtime without a config edit.

It is the many-to-many companion to
[`dtcurrie:talkback`](https://github.com/DTCurrie/viam-talkback), which links
exactly two machines. If you want one microphone to reach one speaker, use
talkback. If you want a radio net, use this.

## What this fills in

The Viam platform gives you `audio_in` and `audio_out`, and nothing that joins
them. It also gives you no fan-out of any kind: `gostream` is video only, the
WebRTC stream service carries no audio, and the one 1-to-N primitive that does
exist — `camera/rtppassthrough` — is RTP-typed and bound to the camera API.

So this module supplies two things. A **pump**, which runs the
`GetAudio` → `PlayStream` loop the platform does not, and a **bus**, which routes
one talker to many listeners with a walkie-talkie's floor discipline.

Everything else is ordinary Viam: one gRPC remote per member, no new transport,
no discovery protocol, no ports to open beyond the one viam-server already uses.

## Topology

One machine is the **hub**. Every other machine adds a single remote pointing at
it, so the config cost is O(N), not the O(N²) a full mesh would need.

```
  MEMBER alpha                    HUB                        MEMBER bravo
  ┌──────────────┐         ┌──────────────────┐         ┌──────────────┐
  │ mic          │──TX────▶│ uplink audio_out │         │          mic │
  │ speaker  ◀───┼──RX─────│ downlink audio_in│────────▶│ speaker      │
  │ radio        │         │ bus  ch: ops     │         │ radio        │
  │  channel:ops │         │      ch: logi    │         │  channel:ops │
  └──────────────┘         └──────────────────┘         └──────────────┘
       1 remote            floor control + fan-out           1 remote
```

The hub needs exactly **two** audio endpoints, not two per channel. Both Viam
audio APIs carry an `extra` map that is delivered per caller — `GetAudio` gets
`req.Extra` once per RPC, `PlayStream` gets it in the stream header — so a
member says which channel it wants on every call. That is also what makes
retuning a property of the member rather than of the hub's config.

## The models

| Model | API | Where | Role |
|---|---|---|---|
| `dtcurrie:walkie:bus` | `generic` | hub | Declares the channels; owns the routing, the floor, and the watchdog |
| `dtcurrie:walkie:uplink` | `audio_out` | hub | Members talk into it |
| `dtcurrie:walkie:downlink` | `audio_in` | hub | Members listen to it |
| `dtcurrie:walkie:radio` | `generic` | member | A microphone and speaker joined to a channel |
| `dtcurrie:walkie:switch` | `switch` | member | Drives a radio: push-to-talk, or a channel dial |

Per-model attribute tables and DoCommand references are in
[dtcurrie_walkie_bus.md](dtcurrie_walkie_bus.md),
[dtcurrie_walkie_uplink.md](dtcurrie_walkie_uplink.md),
[dtcurrie_walkie_downlink.md](dtcurrie_walkie_downlink.md),
[dtcurrie_walkie_radio.md](dtcurrie_walkie_radio.md) and
[dtcurrie_walkie_switch.md](dtcurrie_walkie_switch.md).

## Setting one up

### 1. The hub

The hub needs no audio hardware at all — it is a switchboard, not a speaker. See
[etc/hub.json](etc/hub.json).

```json
{ "name": "bus", "api": "rdk:component:generic", "model": "dtcurrie:walkie:bus",
  "attributes": { "channels": [{ "name": "ops" }, { "name": "logistics" }],
                  "sample_rate": 16000, "num_channels": 1 } },
{ "name": "uplink",   "api": "rdk:component:audio_out", "model": "dtcurrie:walkie:uplink",
  "attributes": { "bus": "bus" } },
{ "name": "downlink", "api": "rdk:component:audio_in",  "model": "dtcurrie:walkie:downlink",
  "attributes": { "bus": "bus" } }
```

All three must be on the same machine and in the same module. They share routing
state in process, which is what makes the fan-out possible; a bus on another
part would arrive as an RPC client with no routing state at all. The endpoints
check this at construction and say so.

### 2. Each member

Add one remote pointing at the hub, **with a prefix**, then a radio. See
[etc/member.json](etc/member.json).

```json
"remotes": [{ "name": "hub", "address": "...", "prefix": "hub-", ... }],
"components": [
  { "name": "radio", "api": "rdk:component:generic", "model": "dtcurrie:walkie:radio",
    "attributes": { "source": "mic", "sink": "speaker",
                    "uplink": "hub-uplink", "downlink": "hub-downlink",
                    "member": "alpha", "channel": "ops",
                    "sample_rate": 16000, "num_channels": 1 } },
  { "name": "ptt", "api": "rdk:component:switch", "model": "dtcurrie:walkie:switch",
    "attributes": { "radio": "radio", "mode": "ptt" } },
  { "name": "channel", "api": "rdk:component:switch", "model": "dtcurrie:walkie:switch",
    "attributes": { "radio": "radio", "mode": "channel",
                    "positions": ["ops", "logistics"] } }
]
```

Give every member a distinct `member` name. It decides who holds a channel, and
it is what keeps a member from hearing their own voice — two radios sharing a
name will share a floor and mute each other. The bus warns loudly if it sees it,
but it cannot prevent it.

### 3. Talk

Set the `ptt` switch to position 1, or `DoCommand {"talk": true}` on the radio.
Set the `channel` switch to pick a channel, or `DoCommand {"channel": "logistics"}`.

## The remote naming rule

Name remote resources by **prefix + short name** — `hub-uplink`, never
`hub:uplink`.

`resource.Dependencies.Lookup` skips its forgiving short-name scan the moment a
name carries a remote segment, so `hub:uplink` resolves to nothing and fails
with a bare "dependency not found". A fully-qualified
`rdk:component:audio_out/hub-uplink` is no better: it parses, but the naive
split on `:` yields `Remote="rdk:component"` and it never resolves either. The
prefixed short name is not merely preferred, it is the only form that works, and
`Validate` rejects the others with the fix in the message.

The app's add-remote flow has no prefix field, so this is one you hand-edit in
the JSON config.

## Talking, and taking turns

Only one member may talk on a channel at a time. **First talker wins**: a second
member is refused outright with "channel busy", and their audio is dropped
rather than mixed in.

That is a real walkie-talkie's behaviour, and it also sidesteps a pile of
machinery — mixing would need every talker on a channel to agree on a format,
plus a jitter buffer to align them.

Three details worth knowing:

- **A rejection arrives about one chunk late.** The audio_out client sends its
  stream header without waiting for an acknowledgement, so a refused talker gets
  one chunk out of the door before the refusal comes back. Budget ~40–80ms, and
  read `busy_rejections` rather than inferring anything from the chunk counters.
- **A talker who stops keeps the channel briefly.** `floor_hangover_ms` (800ms
  by default) reserves it for them, because a talking pump tears its stream down
  after 400ms of quiet and a breath mid-sentence should not hand the channel
  away.
- **A member who vanishes loses it after `floor_idle_ms`.** This one is not
  optional: the viam rpc server sets no keepalive parameters, so a member that
  loses power leaves its stream open at the hub for minutes. Without the
  watchdog, its channel would be held for exactly that long.

## Gating

| `gate_mode` | Behaviour | When |
|---|---|---|
| `manual` (default) | Transmits only while the gate is held open | Push-to-talk. The safe default: nothing is transmitted until somebody asks |
| `vox` | Opens on sound above `vox_threshold_dbfs`, closes after the hangover | Hands-free, at the cost of clipping quiet onsets and of holding the channel from anyone else whenever the room is noisy |
| `open` | Transmits everything | Only with headphones, or where no speaker on the channel can be heard by this microphone |

A radio's **listening** side is always open. Gating exists to stop a microphone
feeding a speaker it can hear, and there is no microphone on that side.

## Formats and bandwidth

Nothing here resamples. A chunk carries its talker's true format, and a
listener's speaker is told what it really is.

The default is **16kHz mono**, not 48kHz, because the hub copies every talker to
every listener: ten members on a 48kHz mono channel is nearly 7 Mbit/s leaving
one machine, where 16kHz is a third of that and ample for speech. Declaring
`sample_rate`/`num_channels` on a channel makes it enforced — a talker in
another format is refused at the hub, which turns "garbled audio at every
listener" into one clear error at the one machine that caused it.

## Diagnostics

`cmd/cli` builds a standalone harness that speaks the same APIs the module does.
It is not shipped in the module tarball.

```
go run ./cmd/cli resources --addr localhost:8080
go run ./cmd/cli listen  --addr localhost:8080 --name downlink --channel ops --member probe
go run ./cmd/cli talk    --addr localhost:8080 --name uplink --channel ops --member probe --seconds 3
go run ./cmd/cli roster  --addr localhost:8080 --name bus
go run ./cmd/cli channels --addr localhost:8080 --name uplink
go run ./cmd/cli tune    --addr localhost:8080 --name radio --channel logistics
go run ./cmd/cli ptt     --addr localhost:8080 --name radio --on
go run ./cmd/cli stats   --addr localhost:8080 --name radio
```

Every subcommand takes `--api-key`/`--api-key-id` for cloud machines, or reads
`$VIAM_API_KEY`/`$VIAM_API_KEY_ID`. Without them the connection is insecure,
which is what a local `viam-server -config ...` on the LAN expects.

`listen` is the one to reach for first. It joins a channel and prints a live
peak meter, and it counts the hub's heartbeats separately from real audio — so
it distinguishes the three states that otherwise look identical from a silent
speaker:

- nothing at all, not even a heartbeat → the hub is not reachable
- heartbeats but no audio → the hub is healthy and nobody is talking
- audio pinned at −120 dBFS → something is transmitting digital silence, which
  usually means a microphone that the OS is refusing to hand over

## Reading the counters

`DoCommand {"stats": true}` on a radio, or `roster` on the bus.

| Symptom | Look at | Likely cause |
|---|---|---|
| Nobody hears you | `busy_rejections` climbing | Somebody else holds the channel |
| Nobody hears you | `tx_chunks_in` at 0 | The microphone is not producing anything |
| Nobody hears you | `tx_peak_dbfs` at −120 with `tx_chunks_in` climbing | The microphone is producing digital silence — on macOS, usually a TCC denial |
| You hear nothing | `hub_heartbeat_age_ms` climbing without bound | The hub has stopped answering |
| You hear nothing | `rx_chunks_in` at 0, heartbeats fine | Right hub, quiet channel — or you are tuned to a different one |
| Choppy audio | `rx_chunks_dropped` climbing | The speaker cannot keep up, or the network is bursting |
| Nothing works, no errors | `channel` | Check what the radio is actually tuned to |

## Platform notes

- **macOS** gates microphone access through TCC, and the prompt is attributed to
  the *responsible process* — the terminal or launch agent that started
  viam-server, not the module. A denied microphone produces a perfectly
  well-formed stream of zero samples with no error anywhere, which is why the
  silence meter exists.
- **Linux** needs the user running viam-server to be in the `audio` group, and
  PulseAudio is per-user, so a system service and a login session do not see the
  same devices.
- The hub needs no audio hardware and no `system-audio` module at all.

## Development

```
make build         # build bin/walkie
make test          # go test ./... -race
make lint          # gofmt -s, go vet, golangci-lint
make system-audio  # fetch viam:system-audio into ~/.viam/local-modules
make dev-config    # materialise etc/dev-single-machine.local.json with real paths
make module.tar.gz # the packaged module
```

`make dev-config` stands the whole thing up on one machine — a hub, a member on
the real microphone and speaker, and a second member on the RDK's fake audio
devices — which is enough to watch audio cross a channel before wiring up a
second machine.

```
viam-server -config etc/dev-single-machine.local.json -debug
```

## Tested against

| | |
|---|---|
| `go.viam.com/rdk` | v1.2.0 |
| `viam:system-audio` | 0.0.7-rc3 minimum on any machine with a speaker |
| Go | 1.25.10 |

`system-audio` only grew `PlayStream` in 0.0.7-rc3; 0.0.6 and earlier serve only
`Play`, which cannot carry a live stream. The registry's default pin is not
always the newest version, so set it explicitly on member machines. Its own
README stale-names its models `viam:audio:*`, but the published package
registers `viam:system-audio:*`.

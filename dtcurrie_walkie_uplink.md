# dtcurrie:walkie:uplink

The hub endpoint members talk into. It implements `rdk:component:audio_out`, so
a member's radio simply plays into it and the hub does the rest: takes the
channel's floor, then fans every chunk out to the channel's listeners.

Must be on the same machine and in the same module as its bus.

## Configuration

```json
{
  "name": "uplink",
  "api": "rdk:component:audio_out",
  "model": "dtcurrie:walkie:uplink",
  "attributes": { "bus": "bus" }
}
```

| Attribute | Type | Default | Description |
|---|---|---|---|
| `bus` | string | required | The `dtcurrie:walkie:bus` this endpoint feeds |
| `channel` | string | unset | A default channel for callers that do not name one in `extra` |
| `member` | string | unset | A default member, likewise |

`channel` and `member` exist so an endpoint can be dedicated to one member on
one channel and driven by something that cannot set `extra` — a plain talkback
link, say. A radio always sets both, so most configs need neither.

## How a member names its channel

Both are read from the `extra` map on `PlayStream`, which the audio_out API
carries in the stream header:

```json
{ "channel": "ops", "member": "alpha" }
```

`member` cannot be defaulted to a blank: self-echo suppression matches on that
name, so an anonymous talker could not be kept out of their own speaker.

## Behaviour

**`PlayStream`** takes the floor and carries the transmission. It returns:

| Condition | gRPC code |
|---|---|
| Drained to close | success |
| Another member holds the channel | `FailedPrecondition`, message contains `walkie: channel busy` |
| The floor was taken back mid-transmission | `Aborted` |
| The channel is not declared | `NotFound` |
| No channel/member, or no format in the stream header | `InvalidArgument` |
| The channel enforces a format and this is not it | `InvalidArgument` |
| The bus is closed (mid-rebuild) | `Unavailable` |

`FailedPrecondition` for busy is deliberate. `Unavailable` is what gRPC
synthesizes for transport failure, so using it would make "somebody else is
talking" indistinguishable from "the hub is down" — and telling those apart is
the whole reason a radio counts busy rejections.

**`Play` is refused** with `Unimplemented`. It takes a complete buffer, which is
the record-then-send model this module exists to avoid, and there would be no
transmission for a channel floor to mean anything about.

**`Properties`** reports pcm16 and the format of the endpoint's default channel,
or the bus default. With several channels at several formats there is no single
truthful answer; per-channel enforcement happens when a transmission opens.

## DoCommand

### `{"channels": true}`

Returns `channels`, the names the bus declares. A radio calls this before
retuning so an unknown channel fails on the command that caused it.

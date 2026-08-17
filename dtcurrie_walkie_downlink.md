# dtcurrie:walkie:downlink

The hub endpoint members listen to. It implements `rdk:component:audio_in`, so a
member's radio simply captures from it, and receives whatever is being said on
the channel it asked for.

Must be on the same machine and in the same module as its bus.

## Configuration

```json
{
  "name": "downlink",
  "api": "rdk:component:audio_in",
  "model": "dtcurrie:walkie:downlink",
  "attributes": { "bus": "bus" }
}
```

| Attribute | Type | Default | Description |
|---|---|---|---|
| `bus` | string | required | The `dtcurrie:walkie:bus` this endpoint reads from |
| `channel` | string | unset | A default channel for callers that do not name one in `extra` |
| `member` | string | unset | A default member, likewise |

## How a member names its channel

Read from the `extra` map on `GetAudio`, which the audio_in API delivers once
per RPC:

```json
{ "channel": "ops", "member": "alpha" }
```

`member` is what keeps a talker from hearing their own voice: a listener whose
member name matches the current talker's is skipped. It is required for that
reason.

## Behaviour

**`GetAudio`** subscribes the caller to their channel and streams it.

- **The first chunk always arrives immediately, and carries no audio.** The RDK's
  audio_in client blocks on one receive before `GetAudio` returns, so on a
  channel where nobody happens to be talking a listener would otherwise sit
  inside `GetAudio` reporting itself disconnected — indistinguishable from a
  dead hub.
- **Empty chunks continue on a cadence** (`keepalive_ms` on the bus). A listening
  radio drops them before any counter sees them, so they cannot be mistaken for
  audio, but their age is reported as `hub_heartbeat_age_ms`.
- **`durationSeconds` is honoured**, which the RDK data collector needs in order
  to finalise a WAV file. Zero means "until the caller goes away", which is what
  a radio wants.
- **`previousTimestampNs` is ignored.** It asks a source to resume where a
  previous stream left off; there is no backlog on a live channel, only whatever
  is being said now.
- **A codec other than `pcm16` is refused** rather than quietly served as pcm16.
  Nothing here transcodes.

| Condition | gRPC code |
|---|---|
| The channel is not declared | `NotFound` |
| No channel or member could be determined | `InvalidArgument` |
| The bus is closed (mid-rebuild) | `Unavailable` |

A listener's stream ends cleanly when the bus shuts down, so members see an
orderly end of stream and reconnect rather than logging an error.

## DoCommand

### `{"channels": true}`

Returns `channels`, as the uplink does, so a radio can ask whichever endpoint it
can reach.

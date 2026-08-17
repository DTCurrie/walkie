# dtcurrie:walkie:switch

A switch that drives a `dtcurrie:walkie:radio`. It has two modes: a talk button,
or a channel dial.

Both put a radio's controls on the machine page as a real control rather than a
DoCommand somebody has to remember, and a member usually has one of each.

## Configuration

Push-to-talk:

```json
{
  "name": "ptt",
  "api": "rdk:component:switch",
  "model": "dtcurrie:walkie:switch",
  "attributes": { "radio": "radio", "mode": "ptt", "positions": ["idle", "talk"] }
}
```

A channel dial:

```json
{
  "name": "channel",
  "api": "rdk:component:switch",
  "model": "dtcurrie:walkie:switch",
  "attributes": {
    "radio": "radio",
    "mode": "channel",
    "positions": ["ops", "logistics"]
  }
}
```

| Attribute | Type | Default | Description |
|---|---|---|---|
| `radio` | string | required | The radio this switch drives |
| `mode` | string | `ptt` | `ptt` or `channel` |
| `positions` | list | `["idle", "talk"]` in ptt mode | What each position selects, in order |

The radio is a **required** dependency, unlike the radio's own endpoints: a
switch with nothing to drive has no reason to exist, and the two always live on
the same machine.

### Positions

In **ptt** mode they are gate states — `idle`, `talk`, `vox` — and default to a
plain two-position toggle. Add `vox` for a three-position switch.

In **channel** mode they are channel names and are **required**, because the
module cannot know which of the hub's channels this machine should be offered.
They become the switch's labels, so the machine page shows the channel names.

## Behaviour

**`SetPosition`** sends the radio the command for that position. Selecting
`talk` sets both `talk` and `gate_mode` in one call, so arriving from `vox`
closes the vox gate as well as opening the manual one.

**`GetPosition`** reads the radio's actual state rather than reporting back
whatever it last set. A switch therefore tells the truth when something else
moved the radio underneath it — another switch, a DoCommand, or the vox gate
itself.

It falls back to the last position it set in two cases, both logged at debug:

- the radio cannot be reached, which would otherwise fail a read the app makes
  constantly
- the radio is in a state this switch has no position for, such as `vox` on a
  two-position toggle or a channel this dial does not offer. Reporting position
  zero would be a worse lie

**`DoCommand`** passes straight through to the radio, so a switch is a complete
control surface on its own — you can key up, retune and read stats without also
having to reach the radio component.

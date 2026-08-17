// Command walkie-cli is a diagnostic harness for bringing up a walkie network,
// speaking the same APIs the module does. Run with -h for subcommands; see the
// README's Diagnostics section.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"go.viam.com/rdk/components/audioin"
	"go.viam.com/rdk/components/audioout"
	"go.viam.com/rdk/components/generic"
	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/robot/client"
	rutils "go.viam.com/rdk/utils"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/DTCurrie/viam-comms/audio/pcm"
	"walkie/internal/bus"
)

func main() {
	if err := realMain(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func realMain() error {
	if len(os.Args) < 2 {
		usage()
		return errors.New("a subcommand is required")
	}

	args := os.Args[2:]
	switch os.Args[1] {
	case "resources":
		return cmdResources(args)
	case "channels":
		return cmdChannels(args)
	case "roster":
		return cmdRoster(args)
	case "listen":
		return cmdListen(args)
	case "talk":
		return cmdTalk(args)
	case "tune":
		return cmdTune(args)
	case "ptt":
		return cmdPTT(args)
	case "stats":
		return cmdStats(args)
	case "-h", "--help", "help":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("unknown subcommand %q", os.Args[1])
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `walkie-cli - diagnostics for a Viam walkie channel network

  resources  list the resources a machine exposes
  channels   ask a hub endpoint which channels it carries
  roster     dump a bus: who is listening, who is talking, what got dropped
  listen     subscribe to a channel and print a live peak meter
  talk       transmit a test tone onto a channel
  tune       change a radio's channel at runtime
  ptt        open or close a radio's push-to-talk gate
  stats      dump a radio's counters

  walkie-cli resources --addr <host:port>
  walkie-cli channels  --addr <host:port> --name hub-uplink
  walkie-cli roster    --addr <host:port> --name bus
  walkie-cli listen    --addr <host:port> --name downlink --channel ops --member probe
  walkie-cli talk      --addr <host:port> --name uplink --channel ops --member probe --seconds 3
  walkie-cli tune      --addr <host:port> --name radio --channel logistics
  walkie-cli ptt       --addr <host:port> --name radio --on
  walkie-cli stats     --addr <host:port> --name radio

listen is the one to reach for first: it joins a channel and prints a live peak
meter, telling "nobody is talking" from "the hub is unreachable" from "audio is
arriving but it is digital silence" without touching a speaker.

Every subcommand takes --api-key/--api-key-id for cloud machines. Without them
the connection is insecure, which is what a local viam-server on the LAN wants.

Run a subcommand with -h for its flags.
`)
}

// conn holds the flags every subcommand needs to reach a machine.
type conn struct {
	addr     string
	apiKey   string
	apiKeyID string
}

// bind deliberately does NOT default these from the environment:
// flag.PrintDefaults would print the key verbatim in -h output, which is what
// gets pasted into bug reports. dial reads the environment.
func (c *conn) bind(fs *flag.FlagSet) {
	fs.StringVar(&c.addr, "addr", "localhost:8080", "machine address, host:port or a .viam.cloud address")
	fs.StringVar(&c.apiKey, "api-key", "", "API key payload for cloud machines; defaults to $VIAM_API_KEY")
	fs.StringVar(&c.apiKeyID, "api-key-id", "", "API key id for cloud machines; defaults to $VIAM_API_KEY_ID")
}

func (c *conn) dial(ctx context.Context, logger logging.Logger) (*client.RobotClient, error) {
	if c.apiKey == "" {
		c.apiKey = os.Getenv("VIAM_API_KEY")
	}
	if c.apiKeyID == "" {
		c.apiKeyID = os.Getenv("VIAM_API_KEY_ID")
	}

	var opts []client.DialOption
	if c.apiKey != "" && c.apiKeyID != "" {
		opts = append(opts, client.WithEntityCredentials(c.apiKeyID, client.Credentials{
			Type:    client.CredentialsTypeAPIKey,
			Payload: c.apiKey,
		}))
	} else {
		// A local viam-server started from a config file with no auth block.
		opts = append(opts, client.WithInsecure())
	}
	machine, err := client.New(ctx, c.addr, logger, client.WithDialOptions(opts...))
	if err != nil {
		return nil, fmt.Errorf("could not connect to %s: %w", c.addr, err)
	}
	return machine, nil
}

// signalContext cancels on the first Ctrl-C so streams unwind cleanly.
func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

func newLogger() logging.Logger {
	logger := logging.NewLogger("walkie-cli")
	logger.SetLevel(logging.WARN)
	return logger
}

// identityFlags are the channel and member every hub call needs.
type identityFlags struct {
	channel string
	member  string
}

func (i *identityFlags) bind(fs *flag.FlagSet, defaultMember string) {
	fs.StringVar(&i.channel, "channel", "", "channel to join (required)")
	fs.StringVar(&i.member, "member", defaultMember,
		"who to identify as; must not clash with a real radio, or you will mute each other")
}

func (i *identityFlags) extra() (map[string]interface{}, error) {
	if i.channel == "" {
		return nil, errors.New("--channel is required")
	}
	return map[string]interface{}{"channel": i.channel, "member": i.member}, nil
}

func cmdResources(args []string) error {
	fs := flag.NewFlagSet("resources", flag.ExitOnError)
	var c conn
	c.bind(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := signalContext()
	defer cancel()

	machine, err := c.dial(ctx, newLogger())
	if err != nil {
		return err
	}
	defer func() { _ = machine.Close(ctx) }()

	names := machine.ResourceNames()
	if len(names) == 0 {
		fmt.Println("(no resources)")
		return nil
	}
	sorted := make([]string, 0, len(names))
	for _, n := range names {
		sorted = append(sorted, n.String())
	}
	sort.Strings(sorted)
	for _, n := range sorted {
		fmt.Println(n)
	}
	return nil
}

func cmdChannels(args []string) error {
	fs := flag.NewFlagSet("channels", flag.ExitOnError)
	var c conn
	c.bind(fs)
	name := fs.String("name", "uplink", "a walkie uplink, downlink or bus resource name")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := signalContext()
	defer cancel()

	machine, err := c.dial(ctx, newLogger())
	if err != nil {
		return err
	}
	defer func() { _ = machine.Close(ctx) }()

	// Try the audio endpoints first, then fall back to the bus itself, so the
	// same command works whichever resource the operator happens to know.
	resp, err := doCommandOnAny(ctx, machine, *name, map[string]interface{}{"channels": true})
	if err != nil {
		return err
	}
	list, ok := resp["channels"].([]interface{})
	if !ok {
		return fmt.Errorf("%q did not answer with a channel list; is it a walkie resource?", *name)
	}
	if len(list) == 0 {
		fmt.Println("(no channels)")
		return nil
	}
	for _, ch := range list {
		fmt.Println(ch)
	}
	return nil
}

// doCommandOnAny sends a command to a resource without needing to be told which
// API it is, which matters because a hub has one of each.
func doCommandOnAny(ctx context.Context, machine *client.RobotClient, name string,
	cmd map[string]interface{},
) (map[string]interface{}, error) {
	if res, err := audioout.FromProvider(machine, name); err == nil {
		if resp, err := res.DoCommand(ctx, cmd); err == nil {
			return resp, nil
		}
	}
	if res, err := audioin.FromProvider(machine, name); err == nil {
		if resp, err := res.DoCommand(ctx, cmd); err == nil {
			return resp, nil
		}
	}
	res, err := generic.FromProvider(machine, name)
	if err != nil {
		return nil, fmt.Errorf("could not find a resource called %q: %w", name, err)
	}
	return res.DoCommand(ctx, cmd)
}

func cmdRoster(args []string) error {
	fs := flag.NewFlagSet("roster", flag.ExitOnError)
	var c conn
	c.bind(fs)
	name := fs.String("name", "bus", "walkie bus resource name")
	asJSON := fs.Bool("json", false, "print the raw response")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := signalContext()
	defer cancel()

	machine, err := c.dial(ctx, newLogger())
	if err != nil {
		return err
	}
	defer func() { _ = machine.Close(ctx) }()

	res, err := generic.FromProvider(machine, *name)
	if err != nil {
		return err
	}
	resp, err := res.DoCommand(ctx, map[string]interface{}{"stats": true})
	if err != nil {
		return err
	}
	if *asJSON {
		return printJSON(resp)
	}

	detail, ok := resp["channels_detail"].([]interface{})
	if !ok {
		return printJSON(resp)
	}
	for _, entry := range detail {
		ch, ok := entry.(map[string]interface{})
		if !ok {
			continue
		}
		holder, _ := ch["holder"].(string)
		if holder == "" {
			holder = "-"
		}
		fmt.Printf("%-16s %-12s listeners=%-3v holder=%-10s tx=%-5v busy=%-4v dropped=%v\n",
			ch["name"], ch["format"], ch["listeners"], holder,
			ch["transmissions"], ch["busy_rejections"], ch["chunks_dropped"])
		if members, ok := ch["members"].([]interface{}); ok && len(members) > 0 {
			names := make([]string, 0, len(members))
			for _, m := range members {
				names = append(names, fmt.Sprint(m))
			}
			fmt.Printf("%18s%s\n", "", strings.Join(names, ", "))
		}
	}
	return nil
}

// cmdListen is the primary bring-up instrument.
func cmdListen(args []string) error {
	fs := flag.NewFlagSet("listen", flag.ExitOnError)
	var (
		c  conn
		id identityFlags
	)
	c.bind(fs)
	id.bind(fs, "walkie-cli-listen")
	name := fs.String("name", "downlink", "walkie downlink resource name")
	seconds := fs.Int("seconds", 0, "stop after this many seconds; 0 listens until Ctrl-C")
	if err := fs.Parse(args); err != nil {
		return err
	}
	extra, err := id.extra()
	if err != nil {
		return err
	}

	ctx, cancel := signalContext()
	defer cancel()
	if *seconds > 0 {
		var stop context.CancelFunc
		ctx, stop = context.WithTimeout(ctx, time.Duration(*seconds)*time.Second)
		defer stop()
	}

	machine, err := c.dial(ctx, newLogger())
	if err != nil {
		return err
	}
	defer func() { _ = machine.Close(ctx) }()

	down, err := audioin.FromProvider(machine, *name)
	if err != nil {
		return err
	}

	chunks, err := down.GetAudio(ctx, rutils.CodecPCM16, 0, 0, extra)
	if err != nil {
		return fmt.Errorf("could not join channel %q: %w", id.channel, err)
	}

	fmt.Printf("listening on %q as %q; Ctrl-C to stop\n", id.channel, id.member)

	var (
		audible    int
		heartbeats int
		lastPrint  time.Time
	)
	for chunk := range chunks {
		if chunk == nil {
			continue
		}
		// A chunk with no audio is the hub's heartbeat. Counting it separately
		// is the whole point: it tells a quiet channel from a dead hub.
		if len(chunk.AudioData) == 0 {
			heartbeats++
			if heartbeats == 1 {
				fmt.Println("hub reachable (heartbeat received), channel quiet")
			}
			continue
		}

		audible++
		peak := pcm.PeakDBFS(chunk.AudioData)
		if time.Since(lastPrint) > 100*time.Millisecond {
			lastPrint = time.Now()
			format := "?"
			if chunk.AudioInfo != nil {
				format = fmt.Sprintf("%dHz/%dch",
					chunk.AudioInfo.SampleRateHz, chunk.AudioInfo.NumChannels)
			}
			fmt.Printf("\r%s %7.1f dBFS  %s  chunks=%d  ", meter(peak), peak, format, audible)
		}
	}
	fmt.Println()

	switch {
	case audible == 0 && heartbeats == 0:
		return errors.New("nothing arrived at all, not even a heartbeat: the hub is not reachable, " +
			"or this is not a walkie downlink")
	case audible == 0:
		fmt.Printf("no audio, but %d heartbeats: the hub is healthy and nobody is talking on %q\n",
			heartbeats, id.channel)
	default:
		fmt.Printf("heard %d chunks of audio on %q\n", audible, id.channel)
	}
	return nil
}

// cmdTalk transmits a test tone, so a channel can be exercised without a real
// microphone anywhere near it.
func cmdTalk(args []string) error {
	fs := flag.NewFlagSet("talk", flag.ExitOnError)
	var (
		c  conn
		id identityFlags
	)
	c.bind(fs)
	id.bind(fs, "walkie-cli-talk")
	name := fs.String("name", "uplink", "walkie uplink resource name")
	seconds := fs.Int("seconds", 3, "how long to transmit")
	rate := fs.Int("rate", 16000, "sample rate; must match what the channel carries")
	channels := fs.Int("channels", 1, "channel count")
	silent := fs.Bool("silent", false, "send digital silence instead of a tone")
	if err := fs.Parse(args); err != nil {
		return err
	}
	extra, err := id.extra()
	if err != nil {
		return err
	}

	format := pcm.Format{SampleRateHz: *rate, NumChannels: *channels}
	if err := format.Valid(); err != nil {
		return err
	}

	ctx, cancel := signalContext()
	defer cancel()

	machine, err := c.dial(ctx, newLogger())
	if err != nil {
		return err
	}
	defer func() { _ = machine.Close(ctx) }()

	up, err := audioout.FromProvider(machine, *name)
	if err != nil {
		return err
	}

	// 20ms chunks, which is roughly what a real microphone module produces.
	const chunkMs = 20
	frames := format.SampleRateHz * chunkMs / 1000
	total := (*seconds * 1000) / chunkMs

	chunks := make(chan []byte)
	done := make(chan error, 1)
	go func() {
		done <- up.PlayStream(ctx, format.AudioInfo(rutils.CodecPCM16), chunks, extra)
	}()

	fmt.Printf("transmitting %ds on %q as %q\n", *seconds, id.channel, id.member)

	ticker := time.NewTicker(chunkMs * time.Millisecond)
	defer ticker.Stop()

	var phase float64
	sent := 0
	for sent < total {
		select {
		case <-ctx.Done():
			close(chunks)
			<-done
			return ctx.Err()
		case err := <-done:
			// The hub refused us, and nothing reads the chunks channel after
			// PlayStream returns, so this arm is what stops the send below
			// blocking forever.
			return describeTalkError(err, id.channel)
		case <-ticker.C:
		}

		data := make([]byte, frames*format.NumChannels*2)
		if !*silent {
			phase = fillTone(data, format, phase)
		}
		select {
		case chunks <- data:
			sent++
		case err := <-done:
			return describeTalkError(err, id.channel)
		}
	}

	close(chunks)
	if err := <-done; err != nil {
		return describeTalkError(err, id.channel)
	}
	fmt.Printf("sent %d chunks\n", sent)
	return nil
}

// describeTalkError turns the hub's refusal into something actionable.
func describeTalkError(err error, channel string) error {
	switch {
	case err == nil:
		return nil
	case bus.IsBusy(err):
		return fmt.Errorf("channel %q is busy: somebody else holds it. "+
			"First talker wins, so wait for them to finish", channel)
	case status.Code(err) == codes.NotFound:
		return fmt.Errorf("the hub does not carry a channel called %q; run "+
			"`walkie-cli channels` to see what it does carry", channel)
	case status.Code(err) == codes.InvalidArgument:
		return fmt.Errorf("the hub rejected this transmission: %w "+
			"(try --rate and --channels to match the channel)", err)
	default:
		return err
	}
}

// fillTone writes a 440Hz sine at a comfortable level, returning the phase to
// carry into the next chunk so consecutive chunks do not click.
func fillTone(data []byte, f pcm.Format, phase float64) float64 {
	const (
		freq      = 440.0
		amplitude = 8000.0
	)
	step := 2 * math.Pi * freq / float64(f.SampleRateHz)
	for i := 0; i+2*f.NumChannels <= len(data); i += 2 * f.NumChannels {
		v := int16(amplitude * math.Sin(phase))
		for ch := range f.NumChannels {
			data[i+2*ch] = byte(v)
			data[i+2*ch+1] = byte(v >> 8)
		}
		phase += step
	}
	return math.Mod(phase, 2*math.Pi)
}

func cmdTune(args []string) error {
	fs := flag.NewFlagSet("tune", flag.ExitOnError)
	var c conn
	c.bind(fs)
	name := fs.String("name", "radio", "walkie radio resource name")
	channel := fs.String("channel", "", "channel to tune to (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *channel == "" {
		return errors.New("--channel is required")
	}
	return radioCommand(*name, &c, map[string]interface{}{"channel": *channel})
}

func cmdPTT(args []string) error {
	fs := flag.NewFlagSet("ptt", flag.ExitOnError)
	var c conn
	c.bind(fs)
	name := fs.String("name", "radio", "walkie radio resource name")
	on := fs.Bool("on", false, "open the gate")
	off := fs.Bool("off", false, "close the gate")
	seconds := fs.Float64("seconds", 0, "close the gate again after this long")
	mode := fs.String("mode", "", `set the gate mode: "manual", "vox" or "open"`)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *on && *off {
		return errors.New("pass one of --on or --off, not both")
	}

	cmd := map[string]interface{}{}
	if *mode != "" {
		cmd["gate_mode"] = *mode
	}
	if *on || *off {
		cmd["talk"] = *on
		if *on && *seconds > 0 {
			cmd["seconds"] = *seconds
		}
	}
	if len(cmd) == 0 {
		cmd["stats"] = true
	}
	return radioCommand(*name, &c, cmd)
}

func cmdStats(args []string) error {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	var c conn
	c.bind(fs)
	name := fs.String("name", "radio", "walkie radio resource name")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return radioCommand(*name, &c, map[string]interface{}{"stats": true})
}

func radioCommand(name string, c *conn, cmd map[string]interface{}) error {
	ctx, cancel := signalContext()
	defer cancel()

	machine, err := c.dial(ctx, newLogger())
	if err != nil {
		return err
	}
	defer func() { _ = machine.Close(ctx) }()

	res, err := generic.FromProvider(machine, name)
	if err != nil {
		return err
	}
	resp, err := res.DoCommand(ctx, cmd)
	if err != nil {
		return err
	}
	return printJSON(resp)
}

func printJSON(v interface{}) error {
	out, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}

// meter renders a peak level as a coarse bar, so a glance tells you whether
// what is arriving is speech or digital silence.
func meter(dbfs float64) string {
	const width = 30
	// Map -60..0 dBFS onto the bar.
	frac := (dbfs + 60) / 60
	if frac < 0 {
		frac = 0
	}
	if frac > 1 {
		frac = 1
	}
	n := int(frac * width)
	return "[" + strings.Repeat("#", n) + strings.Repeat(".", width-n) + "]"
}

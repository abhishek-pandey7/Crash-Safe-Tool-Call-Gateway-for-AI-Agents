// Command gatewayctl inspects and settles operations in the gateway's store.
//
//	gatewayctl list    [-state unknown] [-tool name] [-limit 50]
//	gatewayctl show    <operation-key>
//	gatewayctl resolve <operation-key> -outcome completed|not_executed -evidence "what you checked" [-by name]
//	gatewayctl sweep
//	gatewayctl purge   -retention 720h
//
// It connects with -database or GATEWAY_DATABASE_URL.
//
// resolve is for outcomes the gateway cannot know: a person checks the
// external system and records what they found. It only applies to
// operations that are unknown, or whose owner vanished mid-dispatch, and it
// refuses to run without evidence. Retries of a completed operation then
// receive a result saying an operator confirmed it; retries of a
// not_executed one run the call.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/operator"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/store"
)

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

const usage = `usage:
  gatewayctl list    [-state unknown] [-tool name] [-limit 50]
  gatewayctl show    <operation-key>
  gatewayctl resolve <operation-key> -outcome completed|not_executed -evidence "..." [-by name]
  gatewayctl sweep
  gatewayctl purge   -retention 720h
global flag: -database URL (default $GATEWAY_DATABASE_URL)
`

// run executes one command and returns the process exit code.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	global := flag.NewFlagSet("gatewayctl", flag.ContinueOnError)
	global.SetOutput(stderr)
	dbURL := global.String("database", os.Getenv("GATEWAY_DATABASE_URL"), "Postgres URL")
	global.Usage = func() { fmt.Fprint(stderr, usage) }
	if err := global.Parse(args); err != nil {
		return 2
	}
	rest := global.Args()
	if len(rest) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	if *dbURL == "" {
		fmt.Fprintln(stderr, "gatewayctl: no database; set -database or GATEWAY_DATABASE_URL")
		return 2
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	st, err := store.Open(ctx, *dbURL)
	if err != nil {
		fmt.Fprintln(stderr, "gatewayctl:", err)
		return 1
	}
	defer st.Close()

	cmd, cmdArgs := rest[0], rest[1:]
	switch cmd {
	case "list":
		err = list(ctx, st, cmdArgs, stdout, stderr)
	case "show":
		err = show(ctx, st, cmdArgs, stdout)
	case "resolve":
		err = resolve(ctx, st, cmdArgs, stdout, stderr)
	case "sweep":
		var released, unknown int64
		released, unknown, err = st.Sweep(ctx, whoami())
		if err == nil {
			fmt.Fprintf(stdout, "released %d abandoned claims, marked %d abandoned dispatches unknown\n", released, unknown)
		}
	case "purge":
		err = purge(ctx, st, cmdArgs, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "gatewayctl: unknown command %q\n%s", cmd, usage)
		return 2
	}
	if errors.Is(err, errUsage) {
		return 2
	}
	if err != nil {
		fmt.Fprintln(stderr, "gatewayctl:", err)
		return 1
	}
	return 0
}

var errUsage = errors.New("usage")

// whoami names whoever is running the command, for the audit trail.
func whoami() string {
	for _, v := range []string{"USER", "USERNAME"} {
		if u := os.Getenv(v); u != "" {
			return "operator:" + u
		}
	}
	return "operator"
}

func fmtTime(t *time.Time) string {
	if t == nil {
		return "-"
	}
	return t.UTC().Format(time.RFC3339)
}

func list(ctx context.Context, st *store.Store, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	fs.SetOutput(stderr)
	states := fs.String("state", "", "comma-separated states, e.g. unknown,dispatching")
	tool := fs.String("tool", "", "only this tool")
	limit := fs.Int("limit", 50, "maximum rows")
	if err := fs.Parse(args); err != nil {
		return errUsage
	}
	f := store.ListFilter{Tool: *tool, Limit: *limit}
	for s := range strings.SplitSeq(*states, ",") {
		if s = strings.TrimSpace(s); s != "" {
			f.States = append(f.States, store.State(s))
		}
	}
	ops, err := st.List(ctx, f)
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "KEY\tTOOL\tSTATE\tPRINCIPAL\tATTEMPTS\tDISPATCHED")
	for _, op := range ops {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\t%s\n", op.Key, op.Tool, op.State, op.Principal, op.Attempts, fmtTime(op.DispatchedAt))
	}
	return w.Flush()
}

func show(ctx context.Context, st *store.Store, args []string, stdout io.Writer) error {
	if len(args) != 1 {
		return fmt.Errorf("show takes one operation key")
	}
	op, err := st.Get(ctx, args[0])
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	for _, row := range [][2]string{
		{"key", op.Key}, {"tool", op.Tool}, {"policy", op.Policy}, {"state", string(op.State)},
		{"principal", op.Principal}, {"upstream", op.Upstream}, {"key source", op.Source},
		{"owner", op.Owner}, {"fence", fmt.Sprint(op.Fence)}, {"attempts", fmt.Sprint(op.Attempts)},
		{"created", fmtTime(&op.CreatedAt)}, {"dispatched", fmtTime(op.DispatchedAt)},
		{"finished", fmtTime(op.FinishedAt)}, {"result", resultSummary(op)},
	} {
		fmt.Fprintf(w, "%s:\t%s\n", row[0], row[1])
	}
	if err := w.Flush(); err != nil {
		return err
	}
	events, err := st.Events(ctx, op.Key)
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, "\nevents:")
	w = tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	for _, e := range events {
		from := string(e.From)
		if from == "" {
			from = "-"
		}
		fmt.Fprintf(w, "  %s\t%s -> %s\tfence %d\t%s\t%s\n", e.At.UTC().Format(time.RFC3339Nano), from, e.To, e.Fence, e.Instance, e.Detail)
	}
	return w.Flush()
}

func resultSummary(op *store.Operation) string {
	switch {
	case op.Result != nil:
		return fmt.Sprintf("%s, %d bytes stored", op.ResultKind, len(op.Result))
	case op.ResultPurged:
		return "removed after the retention period"
	case len(op.ResultHash) > 0:
		return "hash only"
	default:
		return "none"
	}
}

func resolve(ctx context.Context, st *store.Store, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		fmt.Fprint(stderr, usage)
		return errUsage
	}
	key := args[0]
	fs := flag.NewFlagSet("resolve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	outcome := fs.String("outcome", "", "completed or not_executed")
	evidence := fs.String("evidence", "", "what you checked and found (required)")
	by := fs.String("by", whoami(), "who is resolving")
	if err := fs.Parse(args[1:]); err != nil {
		return errUsage
	}
	from, err := operator.Resolve(ctx, st, key, store.State(*outcome), *evidence, *by)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "%s: %s -> %s\n", key, from, *outcome)
	return nil
}

func purge(ctx context.Context, st *store.Store, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("purge", flag.ContinueOnError)
	fs.SetOutput(stderr)
	retention := fs.Duration("retention", 0, "remove result bodies older than this")
	if err := fs.Parse(args); err != nil {
		return errUsage
	}
	if *retention <= 0 {
		return fmt.Errorf("purge needs a positive -retention")
	}
	n, err := st.PurgeResults(ctx, *retention)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "removed %d result bodies; their keys are kept\n", n)
	return nil
}

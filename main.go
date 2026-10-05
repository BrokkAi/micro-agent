// Command micro-agent is a minimal Agent Client Protocol coding agent backed
// by OpenRouter. Run it with stdio connected to an ACP client.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/BrokkAi/micro-agent/internal/agent"
	"github.com/BrokkAi/micro-agent/internal/config"
)

var version = "dev"

func main() {
	configPath := flag.String("config", "", "path to config.json (default: user config dir, or $MICRO_AGENT_CONFIG)")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: micro-agent [-config path]          serve ACP over stdio\n       micro-agent [-config path] login    store an OpenRouter API key\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	if *showVersion {
		fmt.Println(version)
		return
	}
	if err := run(*configPath, flag.Args()); err != nil {
		fmt.Fprintln(os.Stderr, "micro-agent:", err)
		os.Exit(1)
	}
}

func run(configPath string, args []string) error {
	if configPath == "" {
		var err error
		if configPath, err = config.DefaultPath(); err != nil {
			return err
		}
	}
	store, err := config.Load(configPath)
	if err != nil {
		return err
	}
	switch {
	case len(args) == 0:
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
		defer stop()
		return agent.Serve(ctx, agent.New(store, version), os.Stdin, os.Stdout)
	case args[0] == "login":
		return login(store)
	}
	flag.Usage()
	return fmt.Errorf("unknown command %q", args[0])
}

func login(store *config.Store) error {
	fmt.Print("OpenRouter API key (https://openrouter.ai/keys): ")
	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	key := strings.TrimSpace(line)
	if key == "" {
		if err != nil {
			return err
		}
		return fmt.Errorf("no key entered")
	}
	if err := store.SetAPIKey(key); err != nil {
		return err
	}
	fmt.Println("Saved to", store.Path())
	return nil
}

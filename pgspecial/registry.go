package pgspecial

import (
	"context"
	"strings"
)

type SpecialHandler func(ctx context.Context, db DB, args string, verbose bool) (*Result, error)

var command_map = map[string]SpecialCommand{}

func RegisterCommand(cmdRegistry SpecialCommandRegistry) {

	normalize := func(s string) string {
		if cmdRegistry.CaseSensitive {
			return s
		}
		return strings.ToLower(s)
	}

	cmd := SpecialCommand{
		Cmd:           cmdRegistry.Cmd,
		Description:   cmdRegistry.Description,
		Syntax:        cmdRegistry.Syntax,
		CaseSensitive: cmdRegistry.CaseSensitive,
		Handler:       cmdRegistry.Handler,
	}

	command_map[normalize(cmdRegistry.Cmd)] = cmd

	for _, alias := range cmdRegistry.Alias {
		command_map[normalize(alias)] = cmd
	}

}

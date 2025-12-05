package pgspecial

import "context"


type SpecialHandler func(ctx context.Context, db DB, args string) (*Result, error)



var registry = map[string]SpecialCommand{}

func Register(cmd SpecialCommand) {
	registry[cmd.Name] = cmd
}

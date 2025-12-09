// this package contains special command types
package pgxspecial

// SpecialCommand represents a parsed and executable special command.
//
// It contains the normalized command name, descriptive metadata, and the handler
// function invoked during execution. SpecialCommand values are stored internally
type SpecialCommand struct {
	Cmd           string
	Syntax        string
	Description   string
	Handler       SpecialHandler
	CaseSensitive bool
}

// SpecialCommandRegistry describes a special command registration.
//
// It defines the command name, optional aliases, documentation metadata, and
// execution handler used when registering commands via RegisterCommand.
type SpecialCommandRegistry struct {
	Cmd          string
	Alias         []string
	Syntax        string
	Description   string
	Handler       SpecialHandler
	CaseSensitive bool
}

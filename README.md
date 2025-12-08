# pgxspecial

`pgxspecial` is a Go library that provides an API to execute PostgreSQL meta-commands (a.k.a. “special” or “backslash” commands), modeled on the behavior of tools like `psql` and inspired by the Python library [pgspecial](https://github.com/dbcli/pgspecial).

## Features

- Execute `psql`-style backslash commands directly from Go code  
- Get structured metadata about databases: tables, types, functions, schemas, roles — not just raw SQL results  
- Works with `pgx/v5` and `pgxpool` (or any adapter implementing the included DB interface)  
- Detailed introspection: types, indexes, tablespaces, privileges, and more  

## Installation

```bash
go get github.com/balaji01-4d/pgxspecial
````

## Basic Usage (Go API)

```go
import (
    "context"
    "fmt"
    "log"

    "github.com/balaji01-4d/pgxspecial"
    "github.com/jackc/pgx/v5/pgxpool"
)

func main() {
    ctx := context.Background()
    pool, err := pgxpool.New(ctx, "postgres://user:password@localhost:5432/database?sslmode=disable")
    if err != nil {
        log.Fatalf("Unable to connect: %v\n", err)
    }
    defer pool.Close()

    // Example: list all databases
    res, isSpecial, err := pgxspecial.ExecuteSpecialCommand(ctx, pool, "\\l")
    if err != nil {
        log.Fatalf("Special command error: %v\n", err)
    }
    if isSpecial {
        fmt.Println("Databases:")
        for _, row := range res.Rows {
            fmt.Println(row)
        }
    }
}
```

## Supported Commands

| Command           | Description                                    |
| ----------------- | ---------------------------------------------- |
| `\l`              | List all databases                             |
| `\d`		        | Describe table, view, sequence or index        |
| `\dT`             | List all data types                            |
| `\ddp`            | List default privileges                        |
| `\dD`             | List all domains                               |
| `\dE`             | List all foreign tables                        |
| `\df`             | List all functions                             |
| `\dp`             | List table / view / sequence access privileges |
| `\du`             | List all roles                                 |
| `\dn`             | List all schemas                               |
| `\db`             | List all tablespaces                           |
| `\sf`             | Show a function’s definition                   |
| `\dx` | List installed extensions                      |

> **Note:** Some commands, such as `\d` & `\dx`, are still in development and may not work yet.

## Example: Describe a Table

```go
res, isSpecial, err := pgxspecial.ExecuteSpecialCommand(ctx, pool, "\\d public.my_table")
if err != nil {
    panic(err)
}
if isSpecial {
    fmt.Println("Columns:")
    for _, col := range res.Rows {
        fmt.Printf("- %v\n", col)
    }
}
```

This returns structured metadata including column names, types, constraints, indexes, triggers, etc.

## Contributing

Contributions are welcome!
Feel free to open issues or submit pull requests for bug fixes, new commands, improved tests, or documentation enhancements.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

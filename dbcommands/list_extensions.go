package dbcommands

import (
	"context"
	"strconv"
	"strings"

	"github.com/balaji01-4d/pgxspecial"
	"github.com/balaji01-4d/pgxspecial/database"
	"github.com/jackc/pgx/v5"
)

func init() {
	pgxspecial.RegisterCommand(pgxspecial.SpecialCommandRegistry{
		Cmd:           "\\dx",
		Description:   "List extensions.",
		Syntax:        "\\dx[+] [pattern]",
		Handler:       ListExtensions,
		CaseSensitive: true,
	})
}

// verbose is ignored for now
func ListExtensions(ctx context.Context, db database.Queryer, pattern string, verbose bool) (pgx.Rows, error) {
	var sb strings.Builder
	args := []any{}
	argIndex := 1

	sb.WriteString(`
	 SELECT e.extname AS name,
             e.extversion AS version,
             n.nspname AS schema,
             c.description AS description
      FROM pg_catalog.pg_extension e
           LEFT JOIN pg_catalog.pg_namespace n
             ON n.oid = e.extnamespace
           LEFT JOIN pg_catalog.pg_description c
             ON c.objoid = e.oid
                AND c.classoid = 'pg_catalog.pg_extension'::pg_catalog.regclass
	`)

	if pattern != "" {
		_, tablePattern := sqlNamePattern(pattern)
		sb.WriteString(" WHERE e.extname ~ $" + strconv.Itoa(argIndex) + " ")
		args = append(args, tablePattern)
	}

	sb.WriteString(" ORDER BY 1, 2;")
	rows, err := db.Query(ctx, sb.String(), args...)
	return rows, err
}

// it is not used currently but may be useful in future implementations
func findExtension(ctx context.Context, db database.Queryer, extName string) (pgx.Rows, error) {
	var sb strings.Builder

	sb.WriteString(`
			SELECT e.extname, e.oid
            FROM pg_catalog.pg_extension e
	`)

	if extName != "" {
		sb.WriteString(" WHERE e.extname = $1 ")
	}

	sb.WriteString(" ORDER BY 1, 2;")

	rows, err := db.Query(ctx, sb.String(), extName)
	return rows, err
}

// it is not used currently but may be useful in future implementations
func describeExtension(ctx context.Context, db database.Queryer, oid uint32) (pgx.Rows, error) {
	var sb strings.Builder

	sb.WriteString(`
	SELECT  pg_catalog.pg_describe_object(classid, objid, 0)
                    AS object_description
            FROM    pg_catalog.pg_depend
            WHERE   refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
                    AND refobjid = $1
                    AND deptype = 'e'
            ORDER BY 1;
	`)

	rows, err := db.Query(ctx, sb.String(), oid)
	return rows, err
}

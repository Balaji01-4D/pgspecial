package pgspecial

import (
	"context"
	"strconv"
	"strings"
)

func sqlNamePattern(pattern string) (schema, table string) {
	inQuotes := false
	var buf strings.Builder
	var schemaBuf *string

	for i := 0; i < len(pattern); i++ {
		c := pattern[i]

		switch {
		case c == '"':
			if inQuotes && i+1 < len(pattern) && pattern[i+1] == '"' {
				buf.WriteByte('"')
				i++
			} else {
				inQuotes = !inQuotes
			}

		case !inQuotes && c >= 'A' && c <= 'Z':
			buf.WriteByte(byte(c + 32))

		case !inQuotes && c == '*':
			buf.WriteString(".*")

		case !inQuotes && c == '?':
			buf.WriteByte('.')

		case !inQuotes && c == '.':
			s := buf.String()
			schemaBuf = &s
			buf.Reset()

		default:
			if c == '$' || (inQuotes && strings.ContainsRune("|*+?()[]{}.^\\", rune(c))) {
				buf.WriteByte('\\')
			}
			buf.WriteByte(c)
		}
	}

	if buf.Len() > 0 {
		table = "^(" + buf.String() + ")$"
	}
	if schemaBuf != nil {
		schema = "^(" + *schemaBuf + ")$"
	}

	return schema, table
}

func ListDatabases(ctx context.Context, db DB, pattern string, verbose bool) (*Result, error) {
	var sb strings.Builder
	args := []any{}
	argIndex := 1

	sb.WriteString(
		`SELECT d.datname as name,
        pg_catalog.pg_get_userbyid(d.datdba) as owner,
        pg_catalog.pg_encoding_to_char(d.encoding) as encoding,
        d.datcollate as collate,
        d.datctype as ctype,
        pg_catalog.array_to_string(d.datacl, E'\n') AS access_privileges
		`)

	if verbose {
		sb.WriteString(
			`, 
			CASE WHEN pg_catalog.has_database_privilege(d.datname, 'CONNECT')
				THEN pg_catalog.pg_size_pretty(pg_catalog.pg_database_size(d.datname))
				ELSE 'No Access'
            END as size,
            t.spcname as "Tablespace",
            pg_catalog.shobj_description(d.oid, 'pg_database') as description
	`)
	}

	sb.WriteString(`
	FROM pg_catalog.pg_database d
	`)

	if verbose {
		sb.WriteString(`JOIN pg_catalog.pg_tablespace t on d.dattablespace = t.oid`)
	}

	if pattern != "" {
		_, tablePattern := sqlNamePattern(pattern)

		if tablePattern != "" {
			sb.WriteString("\nWHERE d.datname ~ $" + strconv.Itoa(argIndex) + " ")
			args = append(args, tablePattern)
		}
	}

	sb.WriteString("\nORDER BY 1;")
	rows, err := db.Query(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}
	
	res := &Result{
		Title:   "DATABASES",
		Rows:    rows,
		Columns: rows.FieldDescriptions(),
		Status:  "OKAY",
	}

	return res, nil
}

func ListSchemas(ctx context.Context, db DB, pattern string, verbose bool) (*Result, error) {
	var sb strings.Builder
	args := []any{}
	argIndex := 1

	sb.WriteString(`
	SELECT n.nspname AS name, pg_catalog.pg_get_userbyid(n.nspowner) AS owner
	`)

	if verbose {
		sb.WriteString(`
		, pg_catalog.array_to_string(n.nspacl, E'\\n') AS access_privileges, pg_catalog.obj_description(n.oid, 'pg_namespace') AS description
		`)
	}
	sb.WriteString(`FROM pg_catalog.pg_namespace n WHERE n.nspname`)

	if pattern != "" {
		_, tablePattern := sqlNamePattern(pattern)

		if tablePattern != "" {
			sb.WriteString("~ $" + strconv.Itoa(argIndex) + " ")
			args = append(args, tablePattern)
		}
	} else {
		sb.WriteString(`
		!~ '^pg_' AND n.nspname <> 'information_schema'\n
		`)
	}

	sb.WriteString("ORDER BY 1")
	rows, err := db.Query(ctx, sb.String(), args...)

	if err != nil {
		return nil, err
	}

	res := &Result{
		Title:   "Schema",
		Rows:    rows,
		Columns: rows.FieldDescriptions(),
		Status:  "OKAY",
	}
	return res, nil
}

func ListPrivileges(ctx context.Context, db DB, pattern string, verbose bool) (*Result, error) {
	var sb strings.Builder
	args := []any{}
	argIndex := 1

	sb.WriteString(`
	        SELECT n.nspname as schema,
          c.relname as name,
          CASE c.relkind WHEN 'r' THEN 'table'
                         WHEN 'v' THEN 'view'
                         WHEN 'm' THEN 'materialized view'
                         WHEN 'S' THEN 'sequence'
                         WHEN 'f' THEN 'foreign table'
                         WHEN 'p' THEN 'partitioned table' END as type,
          pg_catalog.array_to_string(c.relacl, E'\n') AS access_privileges,

          pg_catalog.array_to_string(ARRAY(
            SELECT attname || E':\n  ' || pg_catalog.array_to_string(attacl, E'\n  ')
            FROM pg_catalog.pg_attribute a
            WHERE attrelid = c.oid AND NOT attisdropped AND attacl IS NOT NULL
          ), E'\n') AS column_privileges,
          pg_catalog.array_to_string(ARRAY(
            SELECT polname
            || CASE WHEN NOT polpermissive THEN
               E' (RESTRICTIVE)'
               ELSE '' END
            || CASE WHEN polcmd != '*' THEN
                   E' (' || polcmd::pg_catalog.text || E'):'
               ELSE E':'
               END
            || CASE WHEN polqual IS NOT NULL THEN
                   E'\n  (u): ' || pg_catalog.pg_get_expr(polqual, polrelid)
               ELSE E''
               END
            || CASE WHEN polwithcheck IS NOT NULL THEN
                   E'\n  (c): ' || pg_catalog.pg_get_expr(polwithcheck, polrelid)
               ELSE E''
               END    || CASE WHEN polroles <> '{0}' THEN
                   E'\n  to: ' || pg_catalog.array_to_string(
                       ARRAY(
                           SELECT rolname
                           FROM pg_catalog.pg_roles
                           WHERE oid = ANY (polroles)
                           ORDER BY 1
                       ), E', ')
               ELSE E''
               END
            FROM pg_catalog.pg_policy pol
            WHERE polrelid = c.oid), E'\n')
            AS policies
        FROM pg_catalog.pg_class c
             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		  WHERE c.relkind IN ('r','v','m','S','f','p')
	`)

	if pattern != "" {
		schema, table := sqlNamePattern(pattern)
		if table != "" {
			sb.WriteString(" AND c.relname OPERATOR(pg_catalog.~) $" + strconv.Itoa(argIndex) + " COLLATE pg_catalog.default ")
			args = append(args, table)
			argIndex++
		}
		if schema != "" {
			sb.WriteString(" AND n.nspname OPERATOR(pg_catalog.~) $" + strconv.Itoa(argIndex) + "COLLATE pg_catalog.default ")
			args = append(args, schema)
		}
	} else {
		sb.WriteString(" AND pg_catalog.pg_table_is_visible(c.oid) ")
	}

	sb.WriteString("  AND n.nspname !~ '^pg_'")
	sb.WriteString(" ORDER BY 1, 2")
	println("final sql\n", sb.String(), args)
	rows, err := db.Query(ctx, sb.String(), args...)

	if err != nil {
		return nil, err
	}

	res := &Result{
		Title:   "Privileges",
		Rows:    rows,
		Columns: rows.FieldDescriptions(),
		Status:  "OKAY",
	}
	return res, nil
}

func ListRoles(ctx context.Context, db DB, pattern string, verbose bool) (*Result, error) {
	var sb strings.Builder
	args := []any{}
	argIndex := 1

	sb.WriteString(`
	   SELECT r.rolname,
                r.rolsuper,
                r.rolinherit,
                r.rolcreaterole,
                r.rolcreatedb,
                r.rolcanlogin,
                r.rolconnlimit,
                r.rolvaliduntil,
                ARRAY(SELECT b.rolname FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid) WHERE m.member = r.oid) as memberof,
	`)

	if verbose {
		sb.WriteString("pg_catalog.shobj_description(r.oid, 'pg_authid') AS description, ")
	} 
	sb.WriteString(`
	 	r.rolreplication
			FROM pg_catalog.pg_roles r
	`)

	if pattern != "" {
		_, tablePattern := sqlNamePattern(pattern)
		if tablePattern != "" {
			sb.WriteString(" WHERE r.rolname ~ $" + strconv.Itoa(argIndex) + " ")
			args = append(args, tablePattern)
		}
	}
	
	sb.WriteString(" ORDER BY 1;")
	rows, err := db.Query(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}

	res := &Result{
		Title:   "ROLES",
		Rows:    rows,
		Columns: rows.FieldDescriptions(),
		Status:  "OKAY",
	}
	
	return res, nil
}

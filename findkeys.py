import graphviz
import psycopg2
import psycopg2.errors
import random
import sys
import uuid


colors = ['aquamarine', 'bisque', 'black', 'blue', 'brown', 'cadetblue', 'chartreuse', 'coral', 'cornflowerblue',
          'cyan', 'darkgreen', 'darkgrey', 'deepskyblue', 'firebrick', 'gold', 'goldenrod', 'green', 'greenyellow',
          'hotpink', 'indigo', 'magenta', 'navyblue', 'olive', 'orange', 'orangered', 'orchid', 'purple', 'red',
          'royalblue', 'salmon', 'sandybrown', 'seagreen', 'sienna', 'skyblue', 'tan', 'turquoise', 'violet']


def _print(message):
    print('findkeys: ' + message, file=sys.stderr)


def _autocommit(conn, enable):
    conn.rollback()
    conn.set_session(autocommit=enable)


def _is_key(conn, table, column):
    cur = conn.cursor()
    try:
        try:
            cur.execute('SELECT 1 FROM ' + table[0] + '.' + table[1] + ' GROUP BY "' + column +
                        '" HAVING count(*) > 1 LIMIT 1')
            row = cur.fetchone()
            if row is None:
                return True
            return False
        except psycopg2.Error:
            return False
    finally:
        cur.close()


def _get_column_list(conn, table):
    tmp_columns = []
    cur = conn.cursor()
    try:
        cur.execute('SELECT column_name FROM information_schema.columns WHERE table_schema=\'' + table[
            0] + '\' AND table_name=\'' + table[1] + '\'')
        while True:
            row = cur.fetchone()
            if row is None:
                break
            c = row[0]
            if c.startswith('__'):
                continue
            if c == 'jsonb' or c == 'creation_date' or c == 'created_by':
                continue
            tmp_columns.append(c)
    finally:
        cur.close()
    columns = []
    for c in tmp_columns:
        k = _is_key(conn, table, c)
        columns.append((c, k))
    return columns


def _get_table_list(conn):
    tmp_tables = []
    cur = conn.cursor()
    try:
        cur.execute('SELECT schemaname, tablename FROM metadb.track ORDER BY schemaname, tablename')
        while True:
            row = cur.fetchone()
            if row is None:
                break
            #########################################################################
            # TODO - temporary workaround since transformed tables are not yet marked
            if '__t' in row[1]:
                continue
            #########################################################################
            if row[1].startswith('rmb_'):
                continue
            tmp_tables.append((row[0], row[1]))
    finally:
        cur.close()
    tables = []
    for t in tmp_tables:
        columns = _get_column_list(conn, t)
        tables.append((t[0], t[1], columns))
    return tables


def _is_uuid(s):
    if s is None:
        return False
    try:
        _ = uuid.UUID(s)
    except ValueError:
        return False
    return True


def _get_sample_data(conn, table, column):
    cur = conn.cursor()
    try:
        cur.execute('SELECT "' + column + '" FROM ' + table[0] + '.' + table[1] + ' LIMIT 1')
        row = cur.fetchone()
        if row is None:
            return None
        return row[0]
    finally:
        cur.close()


def _is_table_empty(conn, table):
    cur = conn.cursor()
    try:
        cur.execute('SELECT 1 FROM ' + table[0] + '.' + table[1] + ' LIMIT 1')
        row = cur.fetchone()
        if row is None:
            return True
        return False
    finally:
        cur.close()


def _is_foreign_key(conn, table2, column2, table1, column1):
    if _is_table_empty(conn, table2):
        return False
    if _is_table_empty(conn, table1):
        return False
    if not column1[1]:
        return False
    cur = conn.cursor()
    try:
        try:
            cur.execute('SELECT 1 FROM ' + table2[0] + '.' + table2[1] + ' r2 LEFT JOIN ' +
                        table1[0] + '.' + table1[1] + ' r1 ON r2.' + column2[0] + '=r1.' + column1[0] +
                        ' WHERE r2.' + column2[0] + ' IS NOT NULL AND r1.' + column1[0] + ' IS NULL LIMIT 1')
            row = cur.fetchone()
            if row is None:
                return True
            return False
        except psycopg2.Error:
            return False
    finally:
        cur.close()


# Search for foreign keys in table
def _search_table_foreign_keys(conn, tables, table, refs):
    columns = table[2]
    for c in columns:
        sample = _get_sample_data(conn, table, c[0])
        if not _is_uuid(str(sample)):
            continue
        for t1 in tables:
            columns1 = t1[2]
            for c1 in columns1:
                t = table[0] + '.' + table[1]
                ref = (t, c[0], t1[0] + '.' + t1[1], c1[0])
                if ref[0] == ref[2]:
                    continue
                if _is_foreign_key(conn, table, c, t1, c1):
                    refs.append(ref)


def make_graph(tables, refs):
    dot = graphviz.Digraph(
        graph_attr={'pad': '0.5', 'nodesep': '0.5', 'ranksep': '2', 'rankdir': 'LR', 'ordering': 'out'},
        node_attr={'shape': 'plain', 'fontname': 'Monospace'}
    )
    for t in tables:
        label = ['<<table border="0" cellborder="1" cellpadding="4" cellspacing="0">',
                 '<tr><td><b> ' + t[0] + '.' + t[1] + ' </b></td></tr>']
        for c in t[2]:
            label.append('<tr><td port="' + c[0] + '" align="left"> ' + c[0] + ' </td></tr>')
        label.append('</table>>')
        dot.node(t[0] + '.' + t[1], label=''.join(label))
    for r in refs:
        dot.edge(r[0] + ':' + r[1], r[2] + ':' + r[3], style='bold', color=colors[random.randint(0, len(colors)-1)])
    return dot


def extract(dsn):
    conn = psycopg2.connect(dsn)
    _print('collecting metadata from all tables')
    try:
        _autocommit(conn, True)
        tables = _get_table_list(conn)
        refs = []
        for t in tables:
            _print('reading table: ' + t[0] + '.' + t[1])
            _search_table_foreign_keys(conn, tables, t, refs)
    finally:
        conn.close()
    dot = make_graph(tables, refs)
    _print('rendering')
    dot.render('output', cleanup=True, view=False)
    _print('output written to file: output.pdf')

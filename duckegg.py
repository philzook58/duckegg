import cProfile
from typing import Any, List
import duckdb
from dataclasses import dataclass
from collections import defaultdict


@dataclass(frozen=True)
class Var:
    name: str


de__counter = 0


def FreshVar():
    global de__counter
    de__counter += 1
    return Var(f"duckegg_x{de__counter}")


def Vars(xs):
    return [Var(x) for x in xs.split()]


@dataclass
class Atom:
    name: str
    args: List[Any]

    def __repr__(self):
        args = ",".join(map(repr, self.args))
        return f"{self.name}({args})"


@dataclass
class Term:
    name: str
    args: List[Any]

    def flatten(self):
        clauses = []
        newargs = []
        for arg in self.args:
            if isinstance(arg, Term):
                v, c = arg.flatten()
                clauses += c
                newargs.append(v)
            else:
                newargs.append(arg)
        res = FreshVar()
        newargs.append(res)
        rel = Atom(self.name, newargs)
        clauses.append(rel)
        return res, clauses

    def __repr__(self):
        args = ",".join(map(repr, self.args))
        return f"{self.name}({args})"


class Clause():
    def __init__(self, head, body):
        self.head = head
        self.body = body

    def expand_functions(self):
        body = []
        # flatten parts of the body
        for rel in self.body:
            newargs = []
            for arg in rel.args:
                if isinstance(arg, Term):
                    v, rels = arg.flatten()
                    body += rels
                    newargs.append(v)
                else:
                    newargs.append(arg)
            body.append(Atom(rel.name, newargs))
        newargs = []
        newhead = []
        # flatten head, but generate accumulative clause per
        for arg in self.head.args:
            if isinstance(arg, Term):
                v, rels = arg.flatten()
                newhead += rels
                newargs.append(v)
            else:
                newargs.append(arg)
        newhead.append(Atom(self.head.name, newargs))
        clauses = []
        while len(newhead) > 0:
            # This is cheeky. We don't need the full flattened head prefix, only one branch
            h = newhead.pop()
            clauses.append(Clause(h, body + newhead))
        return reversed(clauses)  # Clause(newhead, newrels)

    def normalize(self):
        return self.expand_functions()

    def compile(self):
        global de__counter
        assert isinstance(self.head, Atom)
        # map from variables to columns where they appear
        # We use WHERE clauses and let SQL do the heavy lifting
        varmap = defaultdict(list)
        queries = []
        for rel in self.body:
            # Every relation in the body creates a new FROM term bounded to
            # a freshname because we may have multiple instances of the same relation
            # The arguments are processed to fill a varmap, which becomes the WHERE clause
            name = rel.name
            args = rel.args
            freshname = name + str(de__counter)
            de__counter += 1
            queries += [f"{name} AS {freshname}"]
            for n, arg in enumerate(args):
                varmap[arg] += [f"{freshname}.x{n}"]

        if len(queries) > 0:
            query = " FROM " + ", ".join(queries) + " "
        else:  # Facts have no query body
            # Really weird hack. Rethink this one.
            query = "FROM VALUES (42) AS dummy"
        # Building the WHERE clause
        # Sharing argument variables becomes an equality constraint
        # Literals arguments become a literal constraint
        conditions = []
        for v, argset in varmap.items():
            for arg in argset:
                if type(v) == int:  # a literal constraint
                    conditions.append(f"{arg} = {v}")
                elif isinstance(v, Var):  # a variable constraint
                    if argset[0] != arg:
                        conditions.append(f"{argset[0]} = {arg}")
                else:
                    print(v, argset)
                    assert False

        # Unbound vatiables in head need a fresh identifier.
        # We use duckdb sequence feature for this
        nextval = "nextval('counter')"
        """
        def conv_headarg(arg):
            if isinstance(arg, int):
                return str(arg)
            elif arg in varmap:
                return varmap[arg][0]
            else:
                return nextval
        headargs = [conv_headarg(arg) for arg in self.head.args]
        selects = ", ".join(headargs)
        insert = f"INSERT INTO {self.head.name} SELECT DISTINCT {selects} "
        """

        # There are always wheres here?
        # unique_wheres = " AND ".join(
        #    [f"x{n} = {v}" for n, v in enumerate(headargs) if v != nextval])  # The != nextval does the skolem check
        def conv_headarg(arg):
            if isinstance(arg, int):
                return str(arg)
            elif arg in varmap:
                return varmap[arg][0]
            else:
                return nextval
        unique_wheres = " AND ".join(
            [f"x{n} = {conv_headarg(v)}" for n, v in enumerate(self.head.args) if v in varmap or isinstance(v, int)])  # The != nextval does the skolem check
        where_clause = f"WHERE {unique_wheres}" if len(
            unique_wheres) > 0 else ""
        unique = f"NOT EXISTS(SELECT * FROM {self.head.name} {where_clause})"
        conditions.append(unique)
        # return insert + query + " WHERE " + " AND ".join(conditions)
        # These are the variables needed from the query to fill out the head.
        headargs = ", ".join([f"{varmap[arg][0]} AS {arg.name}" for arg in self.head.args if isinstance(
            arg, Var) and arg in varmap])
        if headargs == "":
            headargs = "*"
        subquery = f"SELECT DISTINCT {headargs} " + query + \
            " WHERE " + " AND ".join(conditions)

        def conv_headarg(arg):
            if isinstance(arg, int):
                return str(arg)
            elif isinstance(arg, Var):
                if arg in varmap:
                    return arg.name
                else:
                    return nextval
            else:
                assert False
        selects = ", ".join([conv_headarg(arg) for arg in self.head.args])
        insert = f"INSERT INTO {self.head.name} SELECT {selects} FROM ({subquery})"
        return insert

    def __repr__(self):
        return f"{repr(self.head)} :- {repr(self.body)}"


class Solver():
    def __init__(self, debug=False):
        self.con = duckdb.connect(database=':memory:')
        self.con.execute("CREATE SEQUENCE counter START 1;")
        self.con.execute(
            "CREATE TABLE duckegg_root(i integer primary key, j integer NOT NULL);")
        self.con.execute(
            "CREATE TABLE duckegg_edge(i integer NOT NULL, j integer NOT NULL);")
        self.funcs = set()
        self.rules = []
        self.debug = False

    def Relation(self, name, arity):
        args = ",".join([f"x{n} INTEGER NOT NULL" for n in range(arity)])
        self.execute(f"CREATE TABLE  IF NOT EXISTS {name}({args})")
        return lambda *args: Atom(name, args)

    def Function(self, name, arity):
        args = ",".join([f"x{n} INTEGER NOT NULL" for n in range(arity+1)])
        self.execute(f"CREATE TABLE IF NOT EXISTS {name}({args})")
        self.execute(f"CREATE TABLE IF NOT EXISTS duckegg_temp_{name}({args})")
        self.funcs.add((name, arity))
        return lambda *args: Term(name, args)

    def normalize_root(self):
        self.execute("""
            WITH RECURSIVE
            path(i,j) AS (
                select * from duckegg_edge
                union
                SELECT r1.i, r2.j FROM duckegg_edge AS r1, path as r2 where r1.j = r2.i
            )
            INSERT INTO duckegg_root
                select i, min(j) from path
                group by i

            """)
        # duckegg_edge is now free
        self.execute("DELETE FROM duckegg_edge")

    # add(x,x2,x3) :- add(x1,x2,x3), root(x1,x).
    def canonize_tables(self):
        for name, arity in self.funcs:
            for n in range(arity + 1):
                # find all updated entries and insert into temp table
                args = [f"x{i}" for i in range(arity+1)]
                args[n] = "duckegg_root.j"
                args = ", ".join(args)
                self.execute(f"""
                INSERT INTO duckegg_temp_{name}
                SELECT DISTINCT {args}
                FROM plus, duckegg_root
                WHERE x{n} = duckegg_root.i""")

                # clean out stale entries from plus
                self.execute(f"""
                DELETE FROM {name}
                USING duckegg_root
                WHERE x{n} = duckegg_root.i
                """)

                # remove things in temp from plus
                conds = [
                    f"{name}.x{i} = duckegg_temp_{name}.x{i}" for i in range(arity+1)]
                where = " AND ".join(conds)
                self.execute(f"""
                   DELETE FROM {name}
                   USING duckegg_temp_{name}
                   WHERE {where}
                 """)

                self.execute(f"""
                   INSERT INTO {name}
                   SELECT * FROM duckegg_temp_{name}""")
                self.execute(f"DELETE FROM duckegg_temp_{name}")

        # we have used up all the info from duckegg_root now and may safely forget it.
        self.execute("DELETE FROM duckegg_root")

    def execute(self, query):
        if self.debug:
            print(query)
        self.con.execute(query)

    def congruence(self):
        for name, arity in self.funcs:
            wheres = " AND ".join([f"f1.x{n} = f2.x{n}" for n in range(arity)])
            res = arity
            self.execute(f"""
            INSERT INTO duckegg_edge
            SELECT DISTINCT f2.x{res}, min(f1.x{res})
            FROM {name} as f1, {name} as f2
            WHERE {wheres} {"AND" if arity > 0 else ""} f1.x{res} < f2.x{res}
            GROUP BY f2.x{res}
            """)
            # bug. consider n = 0

    def rebuild(self):
        for i in range(1):
            self.congruence()
            # if duckegg_edge empty: break
            self.normalize_root()
            self.canonize_tables()

    def add(self, rule):
        if isinstance(rule, Clause):
            for rule in rule.normalize():
                self.rules.append(rule)
        elif isinstance(rule, Atom):  # add facts
            for rule in Clause(rule, []).normalize():
                self.execute(rule.compile())
        elif isinstance(rule, list):
            for rule in rule:
                self.add(rule)
        else:
            assert False

    def rule(self, head, body):
        self.add(Clause(head, body))

    def query(self, name):
        self.con.execute(f"SELECT * FROM {name}")
        return self.con.fetchall()

    def enode_count(self):
        count = 0
        for name, _ in self.funcs:
            self.execute(f"SELECT COUNT(*) FROM {name}")
            size = self.con.fetchone()[0]
            count += size
        return count

    def solve(self, n=10):
        stmts = [rule.compile() for rule in self.rules]
        for iter in range(n):
            print(f"Iter {iter}, {self.enode_count()} ENodes")
            for stmt in stmts:
                self.execute(stmt)
                self.rebuild()


if __name__ == "__main__":
    x, y, z, w = Vars("x y z w")
    s = Solver()
    plus = s.Relation("plus", 3)
    plusf = s.Function("plus", 2)

    s.add(Clause(plus(x, y, z), [plus(y, x, z)]))

    N = 11
    for k in range(1, N):
        s.add(plus(-2*k, -2*k-1, -2*k-2))
    s.add(Clause(plus(plusf(x, y), z, w), [plus(x, plusf(y, z), w)]))
    s.add(Clause(plus(x, plusf(y, z), w), [plus(plusf(x, y), z, w)]))
    cProfile.run('s.solve(n=5)')
    print(len(s.query("plus")))
    size = len(s.query("plus"))
    s.con.execute(
        "select sum(col0) from (select count(*) - 1 as col0 from plus group by x0, x1, x2 having count(*) > 1)")
    dups = s.con.fetchone()[0]
    if dups == None:
        dups = 0
    print(
        f"plus table size: {size}, duplicates: {dups}, Expected size: {3**N - 2**(N+1) + 1}")

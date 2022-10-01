

import cProfile
from functools import reduce
from typing import Any
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
    return Var(f"x{de__counter}")


def Vars(xs):
    return [Var(x) for x in xs.split()]


@dataclass
class Atom:
    name: str
    args: list[Any]

    def all_funcs(self):
        funcs = set()
        for arg in self.args:
            if isinstance(arg, Term):
                funcs.update(arg.all_funcs())
        return funcs


def Relation(name):
    return lambda *args: Atom(name, args)


def create(sym):
    name, arity = sym
    args = ",".join([f"x{n} INTEGER NOT NULL" for n in range(arity)])
    unique_args = ",".join([f"x{n}" for n in range(arity)])
    return f"CREATE TABLE {name}({args})"
    # , CONSTRAINT UC{name} UNIQUE ({unique_args}))"


class Term():
    def __init__(self, name, *args):
        self.name = name
        self.args = args

    def all_funcs(self):
        acc = set()
        acc.add((self.name, len(self.args)))
        for arg in self.args:
            if isinstance(arg, Term):
                acc.update(arg.all_funcs())
        return acc

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
        rel = Relation(self.name)(*newargs)
        clauses.append(rel)
        return res, clauses

    def __repr__(self):
        args = ",".join(map(repr, self.args))
        return f"{self.name}({args})"


def Function(name):
    return lambda *args: Term(name, *args)


# Rule?


class Clause():
    def __init__(self, head, body):
        self.head = head
        self.body = body

    def all_syms(self):
        syms = set([(self.head.name, len(self.head.args))])
        for rel in self.body:
            syms.add((rel.name, len(rel.args)))
        return syms

    def all_funcs(self):
        funcs = set()
        funcs.update(self.head.all_funcs())
        for rel in self.body:
            funcs.update(rel.all_funcs())
        return funcs

    # def __and__(self, rhs):
    #    return Clause(self.head + rhs.head, self.body + rhs.body)

    # def __le__(self, rhs):
    #    return Clause(self.head, self.body + [rhs.head])
    def expand_head(self):
        if isinstance(self.head, list):
            return [Clause(rel, self.body) for rel in self.head]
        else:
            return [self]

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
        return self.expand_functions()  # .expand_head()

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


"""
con = duckdb.connect(database=':memory:')
for sym in c.all_syms():
    con.execute(create(sym))
con.execute(c.compile())
"""
# c2 = Clause(edge(1, 2), [])
# print(c2.compile())


class Solver():
    def __init__(self):
        self.con = duckdb.connect(database=':memory:')
        self.con.execute("CREATE SEQUENCE counter START 1;")
        self.con.execute(
            "CREATE TABLE duckegg_root(i integer primary key, j integer NOT NULL);")
        # self.con.execute(
        #    "CREATE TABLE duckegg_root(i integer NOT NULL, j integer NOT NULL);")
        self.con.execute(
            "CREATE TABLE duckegg_edge(i integer NOT NULL, j integer NOT NULL);")
        self.funcs = set()
        self.rules = []
        self.debug = False
        #self.con.execute("PRAGMA enable_profiling")

    # honestly, do either of these make sense?
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
        self.execute("select count(*) from duckegg_root")
        print(f"duckegg_root size:{self.con.fetchone()}")
        self.execute("select count(*) from duckegg_edge")
        print(f"duckegg_edge size:{self.con.fetchone()}")
        self.execute("select count(*) from plus")
        print(f"plus size:{self.con.fetchone()}")
        s.con.execute(
            "select sum(col0) from (select count(*) as col0 from plus group by x0, x1, x2 having count(*) > 1)")
        print(f"dups {s.con.fetchone()}")
        # duckegg_edge is now free
        self.execute("DELETE FROM duckegg_edge")

    def normroot2(self):
        self.execute("""
        INSERT INTO duckegg_root
        SELECT distinct i, j from duckegg_edge
        --group by i
        """)
        self.execute("DELETE FROM duckegg_edge")
    # add(x,y,z) :- add(x1,x2,x3), root(x1,x), root(x2,y), root(x3,z).

    def canonize_tables(self):
        for name, arity in self.funcs:
            for n in range(arity + 1):
                # We need to delete rows that canonize to duplicates
                # Because of unique constraint on table.
                wheres = " AND ".join(
                    [f"x{i} = good.x{i}" for i in range(arity+1) if i != n])
                self.execute(f"""
                DELETE FROM {name}
                USING duckegg_root
                WHERE x{n} = duckegg_root.i
                 AND EXISTS (
                    SELECT *
                    FROM {name} AS good
                    WHERE
                    x{n} = duckegg_root.j
                    {"AND" if wheres != "" else ""}
                    {wheres}

                 )
                """)

                # sets = ",".join([f"x{i} = root{i}.j" for i in range(arity+1)])
                # froms = ",".join(
                #    [f"duckegg_root as root{i}" for i in range(arity+1)])
                # wheres = " AND ".join(
                #    [f"root{i}.i = x{i}" for i in range(arity+1)])
                self.execute(f"""
                UPDATE {name}
                SET x{n} = duckegg_root.j
                FROM duckegg_root
                WHERE x{n} = duckegg_root.i
                """)
        # we have used up all the info from duckegg_root now and may safely forget it.
        self.con.execute("DELETE FROM duckegg_root")
    # edge() :-

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
            SELECT DISTINCT f2.x{res}, f1.x{res}
            FROM {name} as f1, {name} as f2
            WHERE {wheres} {"AND" if arity > 0 else ""} f1.x{res} < f2.x{res}
            """)
            # bug. consider n = 0

    def rebuild(self):
        for i in range(2):
            self.congruence()
            # if duckegg_edge empty: break
            self.normalize_root()
            # self.normroot2()
            self.canonize_tables()

    def add(self, rule):
        if isinstance(rule, Clause):
            self.funcs.update(rule.all_funcs())
            print(rule.all_funcs())
            for rule in rule.normalize():
                self.rules.append(rule)
        elif isinstance(rule, Atom):  # add facts
            self.funcs.update(rule.all_funcs())
            self.rules.append(Clause(rule, []))
        elif isinstance(rule, list):
            for rule in rule:
                self.add(rule)
        else:
            assert False

        # con.execute(f"CREATE TABLE {}")
    def query(self, name):
        self.con.execute(f"SELECT * from {name}")
        return self.con.fetchall()

    def solve(self, n=10):
        rules = [normrule for rule in self.rules for normrule in rule.normalize()]
        # print(rules)
        syms = {sym for rule in rules for sym in rule.all_syms()}
        for sym in syms:
            self.execute(create(sym))
        stmts = []
        for rule in rules:
            stmts.append(rule.compile())
        # print(stmts)
        for iter in range(n):
            for stmt in stmts:
                self.execute(stmt)
            self.rebuild()


x, y, z = Vars("x y z")
"""
path = Relation("path")
edge = Relation("edge")
c = Clause(path(x, z), [edge(x, y), path(y, z)])
# c = path("x", "z") <= edge("x", "y") & path("y", "z")
print(c.compile())

s = Solver()
s.add(c)
# s.add(c2)
s.add(Clause(path(x, y), [edge(x, y)]))
s.add(edge(2, 3))
s.add(edge(1, 2))
s.solve()
s.query("path")
s.con.execute("SELECT * from path")
print(s.con.fetchall())

s = Solver()
s.add(edge(x, y))
s.solve()
print(s.query("edge"))

s = Solver()
s.funcs.add(("plus", 2))
plus = Relation("plus")
s.add(plus(1, 2, 3))
s.add(plus(1, 2, 4))
s.add(plus(1, 2, 5))
s.add(plus(3, 4, 6))
s.add(plus(5, 5, 7))
s.solve()
s.rebuild()
print(s.query("plus"))
print(s.query("duckegg_edge"))
print(s.query("duckegg_root"))


plus = Function("plus")
t = plus(plus(1, 2), 3)
print(t.flatten())

even = Relation("even")
print(Clause(even(plus(x, y)), [even(plus(y, x))]).normalize())
zero = Function("zero")
succ = Function("succ")
nat = Relation("nat")

s = Solver()
s.add(Clause(nat(x), [nat(succ(x))]))
s.add(nat(succ(succ(zero()))))
s.solve()
print(s.query("nat"))
print(s.query("succ"))
print(s.query("zero"))
print(s.funcs)

"""

plus = Relation("plus")
plusf = Function("plus")
s = Solver()
s.funcs.add(("plus", 2))
s.add(Clause(plus(x, y, z), [plus(y, x, z)]))
s.add(plus(-1, -2, -3))
s.add(plus(-3, -4, -5))
s.add(plus(-5, -6, -7))
s.add(plus(-7, -8, -9))
s.add(plus(-9, -10, -11))
s.add(plus(-11, -12, -13))
# s.add(plus(1, reduce(plusf, [FreshVar for x in range(3)]), 3))
w = Var("w")
s.add(Clause(plus(plusf(x, y), z, w), [plus(x, plusf(y, z), w)]))
s.add(Clause(plus(x, plusf(y, z), w), [plus(plusf(x, y), z, w)]))
cProfile.run('s.solve(n=6)')
print(len(s.query("plus")))
size = len(s.query("plus"))
s.con.execute(
    "select sum(col0) from (select count(*) - 1 as col0 from plus group by x0, x1, x2 having count(*) > 1)")
dups = s.con.fetchone()[0]
print(size, dups, size - dups, 3**7 - 2**8 + 1)

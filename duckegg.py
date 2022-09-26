

from typing import Any
import duckdb
import uuid
from dataclasses import dataclass
from collections import defaultdict


@dataclass(frozen=True)
class Var:
    name: str


def Vars(xs):
    return [Var(x) for x in xs.split()]


@dataclass
class Atom:
    name: str
    args: list[Any]


def Relation(name):
    return lambda *args: Atom(name, args)


def create(sym):
    name, arity = sym
    args = ",".join([f"x{n} INTEGER NOT NULL" for n in range(arity)])
    unique_args = ",".join([f"x{n}" for n in range(arity)])
    return f"CREATE TABLE {name}({args}, CONSTRAINT UC{name} UNIQUE ({unique_args}))"


class FunctionInstance():
    def __init__(self, name, *args):
        self.name = name
        self.args = args

    def flatten(self):
        clause = []
        newargs = []
        for arg in self.args:
            if isinstance(arg, FunctionInstance):
                v, c = arg.flatten()
                clauses += c
                newargs.append(v)
            else:
                newargs.append(arg)
        res = f"x{uuid.uuid1()}"
        newargs.append(res)
        rel = Relation(self.name)(*newargs)
        clauses.append(rel)
        return res, clauses


class Function():
    def __init__(self, name):
        self.name = name

    def __call__(self, *args):
        return FunctionInstance(self.name, *args)

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
        newrels = []
        for rel in self.body:
            newargs = []
            for arg in rel.args:
                if isinstance(arg, FunctionInstance):
                    v, rels = arg.flatten()
                    newrels += rels
                    newargs.append(v)
                else:
                    newargs.append(arg)
            newrels.append(Atom(rel.name, newargs))
        return Clause(self.head, newrels)

    def normalize(self):
        return self.expand_functions().expand_head()

    def compile(self):
        assert isinstance(self.head, Atom)
        # map from variables to columns where they appear
        # We use WHERE clauses and let SQL do the heavy lifting
        varmap = defaultdict(list)
        queries = []
        for rel in self.body:
            name = rel.name
            args = rel.args
            freshname = name + str(uuid.uuid4())[:6]
            # Hmm. cols is unecessary
            # cols = ",".join([f"x{n}" for n in range(len(args))])
            queries += [f"{name} AS {freshname}"]  # ({cols})
            for n, arg in enumerate(args):
                varmap[arg] += [f"{freshname}.x{n}"]

        if len(queries) > 0:
            query = " FROM " + ", ".join(queries) + " "
        else:  # Facts have no query body
            query = ""
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

        nextval = "nextval('counter')"

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

    # There are always wheres here?
        unique_wheres = " AND ".join(
            [f"x{n} = {v}" for n, v in enumerate(headargs) if v != nextval])  # The != nextval does the skolem check
        where_clause = f"WHERE {unique_wheres}" if len(
            unique_wheres) > 0 else ""
        unique = f"NOT EXISTS(SELECT * FROM {self.head.name} {where_clause})"
        conditions.append(unique)
        return insert + query + " WHERE " + " AND ".join(conditions)


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
        self.funcs = set()
        self.rules = []

    def add(self, rule):
        if isinstance(rule, Clause):
            for rule in rule.normalize():
                self.rules.append(rule)
        elif isinstance(rule, Atom):  # add facts
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
        syms = {sym for rule in self.rules for sym in rule.all_syms()}
        for sym in syms:
            self.con.execute(create(sym))
        stmts = []
        for rule in self.rules:
            stmts.append(rule.compile())
        print(stmts)
        for iter in range(n):
            for stmt in stmts:
                self.con.execute(stmt)


x, y, z = Vars("x y z")
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

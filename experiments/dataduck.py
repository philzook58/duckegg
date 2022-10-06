from pprint import pprint
import time
import cProfile
from typing import Any, List
import duckdb
from dataclasses import dataclass
from collections import defaultdict
from copy import copy


@dataclass(frozen=True)
class Var:
    name: str


fresh_counter = 0


def FreshVar():
    global fresh_counter
    fresh_counter += 1
    return Var(f"duckegg_x{fresh_counter}")


def Vars(xs):
    return [Var(x) for x in xs.split()]


@dataclass
class Atom:
    name: str
    args: List[Any]

    def __repr__(self):
        args = ",".join(map(repr, self.args))
        return f"{self.name}({args})"


# https://duckdb.org/docs/sql/data_types/overview
INTEGER = "INTEGER"
STRING = "STRING"
DOUBLE = "DOUBLE"


def delta(name):
    return f"dataduck_delta_{name}"


def new(name):
    return f"dataduck_new_{name}"


class Solver():
    def __init__(self, debug=False):
        self.con = duckdb.connect(database=':memory:')
        self.rules = []
        self.rels = {}
        self.debug = debug
        self.stats = defaultdict(int)

    def execute(self, stmt):
        if self.debug:
            # print(stmt)
            start_time = time.time()
        self.con.execute(stmt)
        if self.debug:
            end_time = time.time()
            self.stats[stmt] += end_time - start_time

    def Relation(self, name, *types):
        if name not in self.rels:
            self.rels[name] = types
            args = ", ".join(
                [f"x{n} {typ} NOT NULL" for n, typ in enumerate(types)])
            self.execute(f"CREATE TABLE {name}({args})")
            self.execute(f"CREATE TABLE {new(name)}({args})")
            self.execute(f"CREATE TABLE {delta(name)}({args})")
            args = ", ".join(
                [f"x{n}" for n, _typ in enumerate(types)])
            # self.execute(
            #    f"CREATE UNIQUE INDEX dataduck_index_{name} ON {name} ({args});")
        else:
            assert self.rels[name] == types
        return lambda *args: Atom(name, args)

    def add_rule(self, head, body):
        assert len(body) > 0
        self.rules.append((head, body))

    def add_fact(self, fact: Atom):
        args = ", ".join(map(str, fact.args))
        self.execute(f"INSERT INTO {delta(fact.name)} VALUES ({args})")
        self.execute(f"INSERT INTO {fact.name} VALUES ({args})")  # bug

    def compile(self, head, body):
        assert isinstance(head, Atom)
        # map from variables to columns where they appear
        # We use WHERE clauses and let SQL do the heavy lifting
        counter = 0

        def fresh(name):
            nonlocal counter
            counter += 1
            return f"dataduck_{name}{counter}"
        varmap = defaultdict(list)
        froms = []
        for rel in body:
            # Every relation in the body creates a new FROM term bounded to
            # a freshname because we may have multiple instances of the same relation
            # The arguments are processed to fill a varmap, which becomes the WHERE clause
            name = rel.name
            args = rel.args
            freshname = fresh(name)
            froms.append(f"{name} AS {freshname}")
            for n, arg in enumerate(args):
                varmap[arg] += [f"{freshname}.x{n}"]

        wheres = []
        for v, argset in varmap.items():
            for arg in argset:
                if isinstance(v, int):  # a literal constraint
                    wheres.append(f"{arg} = {v}")
                elif isinstance(v, Var):  # a variable constraint
                    if argset[0] != arg:
                        wheres.append(f"{argset[0]} = {arg}")
                else:
                    print(v, argset)
                    assert False
        if len(wheres) > 0:
            wheres = " WHERE " + " AND ".join(wheres)
        else:
            wheres = ""
        # Semi-naive bodies

        def conv_headarg(arg):
            if isinstance(arg, int):
                return str(arg)
            elif arg in varmap:
                return varmap[arg][0]
            else:
                print("Invalid head arg", arg)
                assert False
        selects = ", ".join([conv_headarg(arg) for arg in head.args])
        stmts = []
        for n in range(len(froms)):
            froms1 = copy(froms)
            froms1[n] = delta(froms1[n])  # cheating a little here
            froms1 = ", ".join(froms1)
            stmts.append(
                f"INSERT INTO {new(head.name)} SELECT DISTINCT {selects} FROM {froms1}{wheres} ")
        return stmts

    def run(self):
        stmts = []
        for head, body in self.rules:
            stmts += self.compile(head, body)
        iter = 0
        while True:
            #iter += 1
            # print(iter)
            for stmt in stmts:
                self.execute(stmt)
            num_new = 0
            for name, types in self.rels.items():
                self.execute(f"DELETE FROM {delta(name)}")
                #self.execute(f"DROP TABLE {delta(name)}")
                # self.execute(
                #    f"CREATE TABLE {delta(name)}(x0 INTEGER NOT NULL, x1 INTEGER NOT NULL)")
                wheres = " AND ".join(
                    [f"{new(name)}.x{n} = {name}.x{n}" for n in range(len(types))])
                #self.execute("PRAGMA enable_profiling;")
                #self.execute("PRAGMA force_index_join;")
                self.execute(
                    f"INSERT INTO {delta(name)} SELECT DISTINCT * FROM {new(name)} WHERE NOT EXISTS (SELECT * FROM {name} WHERE {wheres})")
                #self.execute("PRAGMA disable_force_index_join;")
                # self.execute(
                #    f"INSERT INTO {delta(name)} SELECT DISTINCT * FROM {new(name)}")
                #self.execute("PRAGMA disable_profiling;")
                # wheres = ", ".join(
                #    [f"{new(name)}.x{n}" for n in range(len(types))])
                # self.execute(
                #    f"INSERT INTO {delta(name)} SELECT DISTINCT * FROM {new(name)} WHERE * NOT IN (SELECT * FROM {name})")
                self.execute(
                    f"INSERT INTO {name} SELECT * FROM {delta(name)}")
                self.execute(f"SELECT COUNT(*) FROM {new(name)}")
                n = self.con.fetchone()[0]
                #print(new(name), n)
                self.execute(f"DELETE FROM {new(name)}")

                self.execute(f"SELECT COUNT(*) FROM {delta(name)}")
                n = self.con.fetchone()[0]
                #print(delta(name), n)
                num_new += n
            if num_new == 0:
                break


s = Solver(debug=True)
#s.con.execute("SET preserve_insertion_order TO False;")
#s.con.execute("SET checkpoint_threshold TO '10GB';")
#s.con.execute("SET wal_autocheckpoint TO '10GB';")
edge = s.Relation("edge", INTEGER, INTEGER)
path = s.Relation("path", INTEGER, INTEGER)
N = 300
for i in range(N):
    s.add_fact(edge(i, i+1))
# s.add_fact(edge(2, 3))
x, y, z = Vars("x y z")
s.add_rule(path(x, y), [edge(x, y)])
s.add_rule(path(x, z), [edge(x, y), path(y, z)])
cProfile.run('s.run()', sort='cumtime')
# s.run()

pprint(sorted(s.stats.items(), key=lambda s: s[1], reverse=True)[:10])

print(s.con.table("path").df())
#s.con.execute("SELECT * FROM duckdb_settings();")
# pprint(s.con.fetchall())

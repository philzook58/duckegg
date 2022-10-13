
from pprint import pprint
import subprocess
import sqlite3
import tempfile


class SouffleSolver():
    def __init__(self, db="souffle.sqlite", execname="souffle", compiled=False):
        self.execname = execname
        self.options = {"compiled": compiled}
        self.rules = []
        self.rels = []
        self.db = db
        self.con = sqlite3.connect(database=db)

    def Relation(self, name, *types):
        self.rels.append((name, types))

        def res(*args):
            args = ", ".join(map(str, args))
            return f"{name}({args})"
        return res

    def add_rule(self, head, body):
        self.rules.append((head, body))

    def add_fact(self, fact):
        self.add_rule(fact, [])

    def compile(self, head, body):
        if len(body) == 0:
            return f"{head}."
        else:
            body = ", ".join(body)
            return f"{head} :- {body}."

    def run(self):
        stmts = []
        # .type json = {} |
        for name, types in self.rels:
            args = ", ".join([f"x{n} : {typ}" for n, typ in enumerate(types)])
            stmts.append(f".decl {name}({args})")
            stmts.append(f".output {name}(IO=sqlite, filename=\"{self.db}\")")
        for head, body in self.rules:
            stmts.append(self.compile(head, body))
        pprint(stmts)
        with tempfile.NamedTemporaryFile(suffix=".dl") as fp:
            fp.writelines([stmt.encode() + b"\n" for stmt in stmts])
            fp.flush()
            res = subprocess.run([self.execname, fp.name], capture_output=True)
            print(res.stdout.decode())
            print(res.stderr.decode())


def Vars(vs):
    return vs.split()


s = SouffleSolver()
edge = s.Relation("edge", "number", "number")
path = s.Relation("path", "number", "number")
s.add_fact(edge(1, 2))
s.add_fact(edge(2, 3))
x, y, z = Vars("x y z")
print(x, y, z)
s.add_rule(path(x, y), [edge(x, y)])
s.add_rule(path(x, z), [edge(x, y), path(y, z)])
print(s.rules)
s.run()

res = s.con.execute("SELECT * FROM path")
print(res.fetchall())

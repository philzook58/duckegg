
# What about making the syntax closer in alignment to SQL.
class Solver():


s = Solver()
s.add("INSERT INTO path SELECT edge.i, path.j FROM edge, path")


# My current representation of relations as (name, args) is probably pretty bad
class RelationInstance():

    # Relations probably should be typed.
    # Schema?


class Relation():
    def __init__(self, name, *args):
        self.name = name
        self.args = args

    def __call__(self, *args):
        return Relation(self.name, *args)


What is a pattern anyway? Is it an atom?


"""

"""


class Atom():
    def __init__(self, schema, *args):
        self.schema = schema
        self.args = args

    def query_sql(self, queries, varmap):
        froms += f"FROM {self.schema.name} AS "
        for n in self.args:
            varmap +=


class Relation():
    def __init__(self, name):
        self.name = name

    def __call__(self, *args):
        return Atom(self, *args)


class And():
    def __init__(self, *formulas):
        self.formulas = formulas

    def __and__(self, rhs):
        return And(self.formulas + rhs.formulas)

    def __le__(self, rhs):
        return Clause(self.formulas, rhs.formulas)


"""
Hmm.

ADTs are hard to compile. The nesting is a pain.

f(x) :- x.
becomes
f(x, nextval('counter'))

It's very similar to a function call.
res = f(x) :- 
becomes 
(p :- q /\ y) :- x)

f(x,y) :- g(g(x))

add(x,y,z) :- z = (select z from add where x = x, y = y) OR nextval

Eq(a,b)

Seperate issues of nextval vs nesting.

What if we allowed NULL in dependent positions

Save intermediate points in relations.
Make a machine.

makefresh and just allow later fixup.

Exists
ExistsUnique
Not
Impl

match x:
    case Exists
    case Not
    

Ok, the convention of unbound variables getting a freshval in head is pretty good.
It also is pretty easy to skolem check since I'm doing that not exists clause anyway

Fork.
An Egg 
vs datalog - seminaive


Hmm. Getting this to emit a souffle backend might not be so hard either.

It would be simpler to make root not have i as a primary key constraint.

A stronger analysis probably could be good to know what a rule actually depends on.


Perhaps I need select distinct to happen before calling nextval


Call rebuild a lot.
The recursive path computation isn't worth it? Weird.
Do at least some of the aggregate during the congruence pass

Deleting is really expensive apparently
It might be nice to do congruence right in the rule.


    def normroot2(self):
        self.execute("""
        INSERT INTO duckegg_root
        SELECT distinct i, j from duckegg_edge
        --group by i
        """)
        self.execute("DELETE FROM duckegg_edge")


        self.execute("select count(*) from duckegg_root")
        print(f"duckegg_root size:{self.con.fetchone()}")
        self.execute("select count(*) from duckegg_edge")
        print(f"duckegg_edge size:{self.con.fetchone()}")
        self.execute("select count(*) from plus")
        print(f"plus size:{self.con.fetchone()}")
        s.con.execute(
            "select sum(col0) from (select count(*) as col0 from plus group by x0, x1, x2 having count(*) > 1)")
        print(f"dups {s.con.fetchone()}")



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

                            # Clean up duplicates
            cols = ", ".join(f"x{n}" for n in range(arity+1))
            self.execute(f"""
                DELETE FROM {name}
                WHERE rowid NOT IN
                (
                    SELECT MIN(rowid) AS MaxRecordID
                    FROM plus
                    GROUP BY {cols}
                );
            """)

            syms = {sym for rule in rules for sym in rule.all_syms()}

        def create(sym):
            name, arity = sym
            args = ",".join([f"x{n} INTEGER NOT NULL" for n in range(arity)])
            unique_args = ",".join([f"x{n}" for n in range(arity)])
            self.execute(f"CREATE TABLE  IF NOT EXISTS {name}({args})")
        for sym in syms:
            create(sym)
        for func, arity in self.funcs:
            create((f"duckegg_temp_{func}", arity+1))

                            # Does this make any sense at all?
                # We need to delete rows that canonize to duplicates
                # Because of unique constraint on table.
                # wheres = " AND ".join(
                #    [f"x{i} = good.x{i}" for i in range(arity+1) if i != n])
                # self.execute(f"""
                # DELETE FROM {name}
                # USING duckegg_root
                # WHERE x{n} = duckegg_root.i
                # AND EXISTS (
                #    SELECT *
                #    FROM {name} AS good
                #    WHERE
                #    x{n} = duckegg_root.j
                #    {"AND" if wheres != "" else ""}
                #    {wheres}
                # )
                # """)


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

            def all_funcs(self):
        acc = set()
        acc.add((self.name, len(self.args)))
        for arg in self.args:
            if isinstance(arg, Term):
                acc.update(arg.all_funcs())
        return acc

            def all_funcs(self):
        funcs = set()
        for arg in self.args:
            if isinstance(arg, Term):
                funcs.update(arg.all_funcs())
        return funcs

            def expand_head(self):
        if isinstance(self.head, list):
            return [Clause(rel, self.body) for rel in self.head]
        else:
            return [self]

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
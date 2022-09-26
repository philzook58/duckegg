
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


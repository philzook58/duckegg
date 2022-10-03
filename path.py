from duckegg import *
x, y, z = Vars("x y z")
s = Solver()
path = s.Relation("path", 2)
edge = s.Relation("edge", 2)

s.add(Clause(path(x, y), [edge(x, y)]))
s.add(Clause(path(x, z), [edge(x, y), path(y, z)]))

s.add(edge(2, 3))
s.add(edge(1, 2))
s.solve()
print(s.query("path"))

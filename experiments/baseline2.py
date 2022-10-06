from itertools import groupby


def strata1(edge):
    # path(x,y) :- edge(x,y)
    edge_y = {y: list(g) for (y, g) in groupby(edge, key=lambda t: t[1])}
    path = {(x, y) for (x, y) in edge}
    deltapath = path
    while True:
        newpath = set()
        # path(x,y) :- edge(x,y), path(y,z).
        newpath.update({(x, z)
                       for (y, z) in deltapath for (x, _) in edge_y.get(y, [])})
        # if we have not discovered any new tuples return
        if newpath.issubset(path):
            return path
        else:
            # merge new tuples into path for next iteration
            deltapath = newpath.difference(path)
            path.update(newpath)


edge = {(i, i+1) for i in range(1000)}
#edge.add((999, 0))
path = strata1(edge)
print(len(path))

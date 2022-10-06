db = set()
N = 1000
for i in range(N):
    for j in range(i, N):
        db.add((i, j))

print(len(db))

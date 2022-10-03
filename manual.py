import cProfile
import duckdb
import timeit

con = duckdb.connect(database=':memory:')
con.execute("CREATE SEQUENCE counter START 1;")
con.execute(
    "CREATE TABLE duckegg_root(i BIGINT primary key, j BIGINT NOT NULL);")
con.execute("CREATE TABLE duckegg_edge(i BIGINT NOT NULL, j BIGINT NOT NULL);")
con.execute(
    "CREATE TABLE plus(x0 BIGINT NOT NULL, x1 BIGINT NOT NULL, x2 BIGINT NOT NULL);")
con.execute(
    "CREATE TABLE temp_plus(x0 BIGINT NOT NULL, x1 BIGINT NOT NULL, x2 BIGINT NOT NULL);")
# , CONSTRAINT UCplus UNIQUE (x0,x1,x2));")


def getsize():
    con.execute(
        "select count(*) from plus")
    size = con.fetchone()[0]
    return size


def initplus(x, y, z):
    con.execute(f"INSERT INTO plus VALUES ({x},{y},{z})")


def delete_copies():
    pass
    # delete copies used to delete duplicated things from plus.
    # This shouldn't be necessary anymore
    # It was very slow.

    # con.execute("""
    # DELETE FROM plus as plus1
    # using plus as plus2
    # WHERE plus1.x0 = plus2.x0 AND plus1.x1 = plus2.x1 AND plus1.x2 = plus2.x2 AND plus1.rowid < plus2.rowid
    # """)
    # con.execute("""
    # WHERE rowid NOT IN
    # (
    #   SELECT MAX(rowid) AS MaxRecordID
    #   FROM plus
    #   GROUP BY x0, x1, x2
    # );
    # """)
    # con.execute("""
    # DELETE FROM plus
    # WHERE rowid IN
    # (
    #   SELECT MAX(rowid) AS MaxRecordID
    #   FROM plus
    #   GROUP BY x0, x1, x2
    #   HAVING COUNT(*) > 1
    # );
    # """)
    for n in range(3):
        con.execute(f"""
        DELETE FROM plus
        USING duckegg_root
        WHERE duckegg_root.j = x{n} AND
        plus.rowid NOT IN
        (
        SELECT MAX(plus.rowid) AS MaxRecordID
        FROM plus
        WHERE duckegg_root.j = x{n}
        GROUP BY x0, x1, x2

        );
        """)


def canon():
    for n in range(3):
        othern = [0, 1, 2]
        othern.remove(n)
        # delete from plus anything where the normalization is already in there.

        def d4():
            con.execute(f"""
            DELETE FROM plus USING duckegg_root WHERE x{n} = duckegg_root.i
            AND EXISTS(SELECT * FROM plus AS good WHERE x{n}=duckegg_root.j
                AND x{othern[0]}=good.x{othern[0]} AND x{othern[1]}=good.x{othern[1]}
            );""")
        # d4()
        # con.execute(f"""
        # DELETE FROM plus using duckegg_root, plus WHERE duckegg_root
        # """)
        # I could select distinct in temp table
        # Then delete everything stale
        # Then add in new good stuff.
        # con.execute(f"""
        # UPDATE plus
        # SET x{n} = duckegg_root.j
        # FROM duckegg_root
        # WHERE x{n} = duckegg_root.i""")
        args = ["x0", "x1", "x2"]
        args[n] = "duckegg_root.j"
        args = ", ".join(args)
        # selct_args = ["x0", "x1", "x2"]
        # args[n] = "duckegg_root.j"
        # args = ", ".join(args)

        # The temp table seemed the best way to
        # get a select distinct happening
        # also get rid of duplicates
        # Attemping to filter out of temp_plus those keys for which

        def d1():
            # con.execute(f"""
            # WITH temp(x0,x1,x2) AS (SELECT DISTINCT {args}
            # FROM plus, duckegg_root
            # WHERE x{n} = duckegg_root.i)
            # INSERT INTO temp_plus
            # SELECT * FROM temp WHERE
            # NOT EXISTS (SELECT * FROM plus as plus2
            # WHERE plus2.x0 = temp.x0 AND plus2.x1 = temp.x1 AND plus2.x2 = temp.x2)""")
            con.execute(f"""
            INSERT INTO temp_plus
            SELECT DISTINCT {args}
            FROM plus, duckegg_root
            WHERE x{n} = duckegg_root.i""")
        d1()

        def d2():
            con.execute(f"""
            DELETE FROM plus
            USING duckegg_root
            WHERE x{n}=duckegg_root.i
            """)
        # con.execute("PRAGMA enable_profiling;")
        d2()
        # con.execute("PRAGMA disable_profiling;")

        def d3():
            con.execute(f"""
         DELETE FROM plus
         USING temp_plus
         WHERE plus.x0=temp_plus.x0 AND plus.x1=temp_plus.x1 AND plus.x2=temp_plus.x2
         """)

        d3()

        def d5():
            con.execute(f"""
            INSERT INTO plus
            SELECT * FROM temp_plus""")
        d5()
        # WHERE NOT EXISTS (select * from plus as plus2 where
        #    plus2.x0 = temp_plus.x0 AND plus2.x1 = temp_plus.x1 AND plus2.x2 = temp_plus.x2
        # )

        def d6():
            con.execute("DELETE FROM temp_plus")
        d6()
        # idea: we can filter on where duplcates might occur using duckegg_root
        # segfaults
        # con.execute(f"""
        # DELETE FROM plus
        # USING duckegg_root
        # WHERE duckegg_root.j = x{n} AND
        # plus.rowid NOT IN (
        #    SELECT MAX(rowid) AS MaxRecordID
        #    FROM plus
        #    WHERE x{n} = duckegg_root.j
        #    GROUP BY x0, x1, x2
        # );
        # """)
    # con.execute("PRAGMA enable_profiling;")
    # con.execute(f"""
    #    UPDATE plus
    #    SET x0 = duckegg_root.j
    #    FROM duckegg_root
    #    WHERE x0 = duckegg_root.i;
    #            UPDATE plus
    #    SET x1 = duckegg_root.j
    #    FROM duckegg_root
    #    WHERE x1 = duckegg_root.i;
    #            UPDATE plus
    #    SET x2 = duckegg_root.j
    #    FROM duckegg_root
    #    WHERE x2 = duckegg_root.i;
    #    """)
    # con.execute("PRAGMA disable_profiling;")
    # delete_copies()

    def d7():
        con.execute("DELETE FROM duckegg_root")
    d7()

    # con.execute("PRAGMA enable_profiling;")

    # con.execute("PRAGMA disable_profiling;")


def congruence():
    # Huh. Aggregating at this step is worth it
    con.execute("""INSERT INTO duckegg_edge
            SELECT DISTINCT f2.x2, min(f1.x2)
            FROM plus as f1, plus as f2
            WHERE f1.x0 = f2.x0 AND f1.x1 = f2.x1 AND f1.x2 < f2.x2
            GROUP BY f2.x2""")


def root():
    # not much is happening in here, so no matter what you do it's fine.
    con.execute("""
     WITH RECURSIVE
            path(i, j) AS (
                select * from duckegg_edge
                union
                SELECT r1.i, r2.j FROM duckegg_edge AS r1, path as r2 where r1.j = r2.i
            )
            INSERT INTO duckegg_root
                select i, min(j) from path
                group by i""")
    # con.execute("""
    # WITH
    #        path(i, j) AS(
    #            select * from duckegg_edge
    #            union
    #            SELECT r1.i, r2.j FROM duckegg_edge AS r1, duckegg_edge as r2 where r1.j=r2.i
    #        )
    #        INSERT INTO duckegg_root
    #            select i, min(j) from path
    #            group by i""")
    # con.execute("""
    #        INSERT INTO duckegg_root
    #            select i, min(j) from duckegg_edge
    #         group by i""")
    con.execute("DELETE FROM duckegg_edge")


def rebuild():
    for i in range(1):
        print("before rebuild", getsize())
        congruence()
        root()
        canon()
        print("after rebuild", getsize())


def search():
    con.execute("""
    -- commutativity
    INSERT INTO plus SELECT x, y, z FROM
    (SELECT DISTINCT plus0.x1 AS x, plus0.x0 AS y, plus0.x2 AS z
     FROM plus AS plus0  WHERE NOT EXISTS(SELECT * FROM plus
     WHERE x0 = plus0.x1 AND x1 = plus0.x0 AND x2 = plus0.x2));
     """)
    rebuild()
    print("assoc left")
    con.execute("""
    -- assoc left 1
    INSERT INTO plus SELECT x, y, nextval('counter') FROM
    (SELECT DISTINCT plus6.x0 AS x, plus5.x0 AS y
    FROM plus AS plus5, plus AS plus6
    WHERE plus5.x2 = plus6.x1 AND
    NOT EXISTS(SELECT * FROM plus WHERE x0 = plus6.x0 AND x1 = plus5.x0));""")
    rebuild()
    con.execute("""
    -- assoc left 2
    INSERT INTO plus SELECT x2, z, w FROM
    (SELECT DISTINCT plus9.x2 AS x2, plus7.x1 AS z, plus8.x2 AS w
    FROM plus AS plus7, plus AS plus8, plus AS plus9
    WHERE plus7.x0 = plus9.x1 AND plus7.x2 = plus8.x1 AND plus8.x0 = plus9.x0 AND
    NOT EXISTS(SELECT * FROM plus WHERE x0 = plus9.x2 AND x1 = plus7.x1 AND x2 = plus8.x2));
    """)
    # Idea: put anything that causes a congruence directly into duckegg_edge
    # con.execute("""
    # -- assoc left 2
    # INSERT INTO duckegg_edge SELECT b, min(a) FROM
    # (SELECT DISTINCT plusy.x2 as a, plus8.x2 as b
    # FROM plus AS plus7, plus AS plus8, plus AS plus9, plus as plusy
    # WHERE plus7.x0 = plus9.x1 AND plus7.x2 = plus8.x1 AND plus8.x0 = plus9.x0 AND
    # plusy.x0 = plus9.x2 AND plusy.x1 = plus7.x1 AND plusy.x2 < plus8.x2)
    # GROUP BY b;
    # """)
    rebuild()
    print("assoc right")
    con.execute("""
    -- assoc right 1
    INSERT INTO plus SELECT y, z, nextval('counter') FROM
    (SELECT DISTINCT plus10.x1 AS y, plus11.x1 AS z
    FROM plus AS plus10, plus AS plus11
    WHERE plus10.x2=plus11.x0 AND
    NOT EXISTS(SELECT * FROM plus WHERE x0=plus10.x1 AND x1=plus11.x1));
    """)
    rebuild()
    con.execute("""
    -- assoc right 2
    INSERT INTO plus SELECT x, x4, w FROM
    (SELECT DISTINCT plus12.x0 AS x, plus14.x2 AS x4, plus13.x2 AS w
    FROM plus AS plus12, plus AS plus13, plus AS plus14
    WHERE plus12.x1=plus14.x0 AND plus12.x2=plus13.x0 AND plus13.x1=plus14.x1 AND
    NOT EXISTS(SELECT * FROM plus WHERE x0=plus12.x0 AND x1=plus14.x2 AND x2 = plus13.x2));
    """)
    rebuild()


# con.execute("PRAGMA profiling_output='prof.json';")
N = 10
for k in range(1, N):
    initplus(-2*k, -2*k-1, -2*k-2)
#    initplus(-3, -4, -5)
#    initplus(-5, -6, -7)
#    initplus(-7, -8, -9)
# initplus(-9, -10, -11)
#    initplus(-11, -12, -13)


def run():
    for i in range(5):
        search()
        con.execute(
            "select count(*) from plus")
        size = con.fetchone()[0]
        print(size)
        """
        for i in range(2):
            print("rebuild")
            rebuild()
            con.execute(
                "select count(*) from plus")
            size = con.fetchone()[0]
            print(size)
        """


cProfile.run('run()', sort="cumtime")
con.execute(
    "select count(*) from plus")
size = con.fetchone()[0]
con.execute(
    "select sum(col0) from (select count(*) - 1 as col0 from plus group by x0, x1, x2 having count(*) > 1)")
dups = con.fetchone()[0]
if dups == None:
    dups = 0
print(size, dups, size - dups, 3**N - 2**(N+1) + 1)

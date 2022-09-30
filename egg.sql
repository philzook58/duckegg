-- run as `duckdb < egg.sql`
-- SELECT * from (values (1));


--WITH RECURSIVE
--edge(i,j) AS (VALUES (1,1), (2,1), (3,2), (4,3)),
--root(i,j) AS (
--    SELECT foo.i, min(foo.j) 
 --   FROM (select * from edge 
--         UNION  
--         (SELECT r1.i, r2.j FROM edge AS r1, edge as r2 where r1.j = r2.i) -- (edge as r1 INNER JOIN edge as r2 ON r1.j = r2.i)
--      ) AS foo(i,j) -- (SELECT i, k FROM root AS (i,j), root as (j1,k) where j = j1)
--    GROUP BY foo.i
--  )
--  SELECT * from root;


CREATE TABLE edge(i integer, j integer);
CREATE TABLE root(i integer primary key, j integer NOT NULL);

INSERT INTO edge VALUES (2,1), (3,3);
INSERT INTO root VALUES (1,1), (2,2), (3,3);


INSERT INTO edge SELECT * FROM root as r where not exists (select * from edge where edge.i = r.i and edge.j = r.j);

WITH RECURSIVE
-- This union is not supported in recusrive cte. odd
--edge(i,j) AS (select * from dedge union all select * from root),

path(i,j) AS (
    select * from edge
    union
    SELECT r1.i, r2.j FROM edge AS r1, path as r2 where r1.j = r2.i
),
goodroot(i,j) AS (
    select i, min(j) from path 
    group by i
)
UPDATE root
SET j = goodroot.j
FROM goodroot
WHERE root.i = goodroot.i
;

SELECT * from root;

/*
The convention of leaving out (i,i) seem smart.
Also after canonizing, we can destroy the union find. That also seems smart.


goodroot(i,j) AS (
    select i, least(i, min(j)) from path 
    group by i
)




WITH RECURSIVE
edge(i,j) AS (VALUES (1,1), (2,1), (3,2), (4,3)),
root(i,j) AS (
    select * from edge
    UNION
    SELECT r1.i, r2.j FROM edge AS r1, root as r2 where r1.j = r2.i
)
-- select * from root;
select i, min(j) from root 
group by i;
*/

.quit
--    (SELECT r1.i, r2.j FROM edge AS r1, edge as r2 where r1.j = r2.i)
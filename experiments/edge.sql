
CREATE TABLE edge(i INTEGER NOT NULL, j INTEGER NOT NULL); -- , PRIMARY KEY (i,j)) -- WITHOUT ROWID;
CREATE TABLE path(i INTEGER NOT NULL, j INTEGER NOT NULL); --, PRIMARY KEY (i,j)); 

WITH RECURSIVE
    temp(i,j) AS 
    (SELECT 0,1
    UNION
    SELECT j, j+1 FROM temp WHERE temp.j < 1000)
INSERT INTO edge SELECT * FROM temp;

SELECT COUNT(*) FROM edge;

WITH RECURSIVE
    temp(i,j) AS 
    (SELECT * FROM edge
    UNION
    SELECT edge.i, temp.j FROM edge, temp WHERE edge.j = temp.i)
INSERT INTO path SELECT * FROM temp;

SELECT COUNT(*) FROM path;


 

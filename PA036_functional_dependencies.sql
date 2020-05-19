-- FUNCTIONAL DEPENDENCIES PA036 Database System Project

-- shows basic statistics of table
SELECT * FROM pg_stats WHERE tablename = 'postal_codes';

-- EXPERIMENT 1
-- using various SELECTS on table of postal codes. using statistics doesn't help at all

-- select query without statistics
DROP STATISTICS IF EXISTS stats_postal_codes;
EXPLAIN ANALYZE SELECT * FROM postal_codes WHERE cast(zip_code AS text) LIKE '9%' AND state = 'Alaska';
-- Seq Scan on postal_codes  (cost=0.00..1076.70 rows=2 width=23) (actual time=0.013..3.222 rows=371 loops=1)
EXPLAIN ANALYZE SELECT * FROM postal_codes WHERE zip_code < 15000 AND state = 'Massachusetts';
-- Seq Scan on postal_codes  (cost=0.00..876.02 rows=91 width=23) (actual time=2.304..3.862 rows=1217 loops=1)

-- creates statistics about dependencies and displays the result
CREATE STATISTICS stats_postal_codes (dependencies) ON zip_code, state FROM postal_codes;
ANALYZE postal_codes;
SELECT stxname, stxkeys, stxdependencies FROM pg_statistic_ext WHERE stxname = 'stats_postal_codes';
-- there is clear dependency of zip_code on state {"1 => 3": 1.000000}

-- select query with statistics
EXPLAIN ANALYZE SELECT * FROM postal_codes WHERE cast(zip_code AS text) LIKE '9%' AND state = 'Alaska';
-- Seq Scan on postal_codes  (cost=0.00..1076.70 rows=2 width=23) (actual time=0.009..3.101 rows=371 loops=1)
EXPLAIN ANALYZE SELECT * FROM postal_codes WHERE zip_code < 15000 AND state = 'Massachusetts';
-- Seq Scan on postal_codes  (cost=0.00..876.02 rows=89 width=23) (actual time=1.903..2.928 rows=1217 loops=1)
-- unfortunately no difference when using or not using statistics

DROP STATISTICS IF EXISTS stats_postal_codes;
DROP TABLE IF EXISTS postal_codes;


-- EXPERIMENT 2
-- table with randomly generated integers
-- using simple SELECT where using statistics improves row estimate
DROP STATISTICS IF EXISTS stats_random_table;
DROP TABLE IF EXISTS random_table;
CREATE TABLE random_table (column1 int, column2 int);
INSERT INTO random_table SELECT i/10000, i/100000 FROM generate_series (1,10000000) i;
-- using SELECT on one column with estimate of row is reasonably good
EXPLAIN ANALYZE SELECT * FROM random_table where column1 = 1;
-- using SELECT on two functionally dependent columns without statistics results in bad row estimate
EXPLAIN ANALYZE SELECT * FROM random_table where column1 = 1 and column2 = 0;
-- bad row estimate: Gather  (cost=1000.00..107762.20 rows=139 width=8) (actual time=2.051..263.571 rows=10000 loops=1)
-- it is because column1 and column2 are correlated

-- when using statistics situation is much better
CREATE STATISTICS stats_random_table (dependencies) on column1, column2 from random_table;
ANALYZE random_table;
SELECT stxname, stxkeys, stxdependencies FROM pg_statistic_ext WHERE stxname = 'stats_random_table';
-- column1 and column2 are correlated: {"1 => 2": 1.000000}
EXPLAIN ANALYZE SELECT * FROM random_table where column1 = 1 and column2 = 0;
-- good row estimate: Gather  (cost=1000.00..108702.36 rows=9545 width=8) (actual time=0.976..244.213 rows=10000 loops=1)
-- estimate is almost same as actual number of rows


-- EXPERIMENT 3
-- table with randomly generated integers
-- simple SELECT with '<' comparisons, statistics helps but estimate is still bit off
DROP STATISTICS IF EXISTS stats_random_table;
DROP TABLE IF EXISTS random_table;
CREATE TABLE random_table (column1 int,column2 int);
INSERT INTO random_table SELECT i/10000, i/100000 FROM generate_series (1,10000000) i;
EXPLAIN ANALYZE SELECT * FROM random_table where column1 < 1 and column2 < 10;
-- bad row estimate: Seq Scan on random_table  (cost=0.00..194248.72 rows=1111116 width=8) (actual time=0.032..1079.888 rows=9999 loops=1)
-- estimate is 111x bigger than actual result

-- let's create statistics and try again
CREATE STATISTICS stats_random_table (dependencies) on column1, column2 from random_table;
ANALYZE random_table;
EXPLAIN ANALYZE SELECT * FROM random_table where column1 < 1 and column2 < 10;
-- better row estimate: Gather  (cost=1000.00..107883.60 rows=1353 width=8) (actual time=0.667..330.558 rows=9999 loops=1)
-- this time it's better but estimate is 7x smaller than actual result


-- EXPERIMENT 4
-- table with randomly generated integers
-- test DELETE
DROP STATISTICS IF EXISTS stats_random_table;
DROP TABLE IF EXISTS random_table;
CREATE TABLE random_table (column1 int, column2 int);
INSERT INTO random_table SELECT i/10000, i/100000 FROM generate_series (1,10000000) i;
-- DELETE query without using statistics
EXPLAIN ANALYZE DELETE FROM random_table WHERE column1 < 1 and column2 < 10;
-- Delete on random_table  (cost=0.00..194248.72 rows=1111116 width=6) (actual time=946.899..946.899 rows=0 loops=1)

-- Let's create table again so we can perform DELETE again on fresh data
DROP TABLE IF EXISTS random_table;
CREATE TABLE random_table (column1 int, column2 int);
INSERT INTO random_table SELECT i/10000, i/100000 FROM generate_series (1,10000000) i;
CREATE STATISTICS stats_random_table (dependencies) on column1, column2 from random_table;
ANALYZE random_table;
-- DELETE when statistics are available
EXPLAIN ANALYZE DELETE FROM random_table WHERE column1 < 1 and column2 < 10;
-- Delete on random_table  (cost=0.00..194247.65 rows=1366 width=6) (actual time=765.328..765.328 rows=0 loops=1)
-- estimate as well as actual time of query execution improved when statistics were available


-- EXPERIMENT 5
-- table with randomly generated integers
-- test UPDATE
DROP STATISTICS IF EXISTS stats_random_table;
DROP TABLE IF EXISTS random_table;
CREATE TABLE random_table (column1 int, column2 int);
INSERT INTO random_table SELECT i/10000, i/100000 FROM generate_series (1,10000000) i;
EXPLAIN ANALYZE UPDATE random_table SET column2 = column1 * 10 WHERE column1 = 1;
-- Update on random_table  (cost=0.00..169373.60 rows=50000 width=14) (actual time=1151.536..1151.536 rows=0 loops=1)

DROP TABLE IF EXISTS random_table;
CREATE TABLE random_table (column1 int, column2 int);
INSERT INTO random_table SELECT i/10000, i/100000 FROM generate_series (1,10000000) i;
EXPLAIN ANALYZE UPDATE random_table SET column2 = column1 * 10 WHERE column2 < column1;
-- Update on random_table  (cost=0.00..177581.03 rows=3333326 width=14) (actual time=40853.613..40853.613 rows=0 loops=1)


-- let's try again with statistics
DROP STATISTICS IF EXISTS stats_random_table;
DROP TABLE IF EXISTS random_table;
CREATE TABLE random_table (column1 int, column2 int);
INSERT INTO random_table SELECT i/10000, i/100000 FROM generate_series (1,10000000) i;
CREATE STATISTICS stats_random_table (dependencies) on column1, column2 from random_table;
ANALYZE random_table;
EXPLAIN ANALYZE UPDATE random_table SET column2 = column1 * 10 WHERE column1 = 1;
-- Update on random_table  (cost=0.00..169272.51 rows=9562 width=14) (actual time=882.346..882.346 rows=0 loops=1)
-- There slight improvement but could be also random fluctuations

DROP STATISTICS IF EXISTS stats_random_table;
DROP TABLE IF EXISTS random_table;
CREATE TABLE random_table (column1 int, column2 int);
INSERT INTO random_table SELECT i/10000, i/100000 FROM generate_series (1,10000000) i;
CREATE STATISTICS stats_random_table (dependencies) on column1, column2 from random_table;
ANALYZE random_table;
EXPLAIN ANALYZE UPDATE random_table SET column2 = column1 * 10 WHERE column2 < column1;
-- Update on random_table  (cost=0.00..177581.03 rows=3333326 width=14) (actual time=40978.632..40978.632 rows=0 loops=1)
-- There is no improvement at all


-- EXPERIMENT 6
-- table with randomly generated integers
-- test INSERT
DROP STATISTICS IF EXISTS stats_random_table;
DROP TABLE IF EXISTS random_table;
CREATE TABLE random_table (column1 int, column2 int);
INSERT INTO random_table SELECT i/10000, i/100000 FROM generate_series (1,5000000) i;

-- row estimate will be bad we know that from previous experiment, but let's check regardless
EXPLAIN ANALYZE SELECT * FROM random_table where column1 = 1 and column2 = 0;
-- Gather  (cost=1000.00..54386.65 rows=125 width=8) (actual time=1.268..160.188 rows=10000 loops=1)

-- creating statistics will improve estimate we know that too
CREATE STATISTICS stats_random_table (dependencies) on column1, column2 from random_table;
ANALYZE random_table;
EXPLAIN ANALYZE SELECT * FROM random_table where column1 = 1 and column2 = 0;
-- Gather  (cost=1000.00..55365.70 rows=9917 width=8) (actual time=1.723..126.418 rows=10000 loops=1)

-- but let's what happens when we add more rows into table which have same functional dependency as data already in table
INSERT INTO random_table SELECT i/10000, i/100000 FROM generate_series (5000000,10000000) i;
EXPLAIN ANALYZE SELECT * FROM random_table where column1 = 1 and column2 = 0;
-- Gather  (cost=1000.00..109731.30 rows=19833 width=8) (actual time=0.927..283.684 rows=10000 loops=1)
-- Estimate is this time twice as big as the real number of rows

-- so we have to update statistics
ANALYZE random_table;
EXPLAIN ANALYZE SELECT * FROM random_table where column1 = 1 and column2 = 0;
-- Gather  (cost=1000.00..108703.77 rows=9559 width=8) (actual time=0.937..252.038 rows=10000 loops=1)
-- Now the estimate is good once again, planner knows that new rows were added but it doesn't know that added data have same format as data already in table


-- EXPERIMENT 7
-- two tables with randomly generated integers
-- test NATURAL JOIN
DROP TABLE IF EXISTS random_table1;
DROP TABLE IF EXISTS random_table2;
DROP STATISTICS IF EXISTS random_table1;
DROP STATISTICS IF EXISTS random_table2;
CREATE TABLE random_table1 (column1 int, column2 int);
CREATE TABLE random_table2 (column2 int, column3 int);
INSERT INTO random_table1 SELECT i/100, i/1000 FROM generate_series (1,50000) i;
INSERT INTO random_table2 SELECT i/1000, i*10 FROM generate_series (1,50000) i;

-- without statistics
EXPLAIN ANALYZE SELECT * FROM random_table1 NATURAL JOIN random_table2;
-- Merge Join  (cost=9281.59..198324.67 rows=12586148 width=12) (actual time=22.548..7436.662 rows=49998002 loops=1), Execution time: 8772.687 ms
-- Without statistics performing NATURAL JOIN on two random table takes substantial amount of time

-- When using statistic situation improves
CREATE STATISTICS stats_random_table1 (dependencies) on column1, column2 from random_table1;
CREATE STATISTICS stats_random_table2 (dependencies) on column2, column3 from random_table2;
ANALYZE random_table1;
ANALYZE random_table2;
EXPLAIN ANALYZE SELECT * FROM random_table1 NATURAL JOIN random_table2;
-- Hash Join  (cost=1347.00..567356.36 rows=49997486 width=12) (actual time=10.308..4869.999 rows=49998002 loops=1), Execution time: 6206.766 ms
-- With statistics it is better, planner also used Hash Join Instead of Merge Join


-- EXPERIMENT 8
-- two tables with randomly generated integers
-- test RIGHT JOIN
DROP TABLE IF EXISTS random_table1;
DROP TABLE IF EXISTS random_table2;
DROP STATISTICS IF EXISTS random_table1;
DROP STATISTICS IF EXISTS random_table2;
CREATE TABLE random_table1 (column1 int, column2 int);
CREATE TABLE random_table2 (column2 int, column3 int);
INSERT INTO random_table1 SELECT i/100, i/1000 FROM generate_series (1,50000) i;
-- table1 contains values in column2 that are different from values in random_table2 column2
INSERT INTO random_table2 SELECT i/1000, i*10 FROM generate_series (1,50000) i;
INSERT INTO random_table2 SELECT i/10000, i*10 FROM generate_series (1,50000) i;

-- without statistics
EXPLAIN ANALYZE SELECT * FROM random_table1 RIGHT JOIN random_table2 ON random_table1.column2 = random_table2.column2;
-- Merge Right Join  (cost=14400.45..391385.32 rows=25115601 width=16) (actual time=55.713..20203.801 rows=99988003 loops=1), Execution time: 22880.855 ms

-- with statistics
CREATE STATISTICS stats_random_table1 (dependencies) on column1, column2 from random_table1;
CREATE STATISTICS stats_random_table2 (dependencies) on column2, column3 from random_table2;
ANALYZE random_table1;
ANALYZE random_table2;
EXPLAIN ANALYZE SELECT * FROM random_table1 RIGHT JOIN random_table2 ON random_table1.column2 = random_table2.column2;
-- Hash Left Join  (cost=1347.00..1135620.43 rows=99883043 width=16) (actual time=8.682..8944.983 rows=99988003 loops=1), Execution time: 11591.906 ms
-- With statistics execution time is much better, planner also decides to use Hash Left Join instead of Merge Right Join


-- EXPERIMENT 9
-- two tables with randomly generated integers
-- test LEFT JOIN
DROP TABLE IF EXISTS random_table1;
DROP TABLE IF EXISTS random_table2;
DROP STATISTICS IF EXISTS random_table1;
DROP STATISTICS IF EXISTS random_table2;
CREATE TABLE random_table1 (column1 int, column2 int);
CREATE TABLE random_table2 (column2 int, column3 int);
INSERT INTO random_table1 SELECT i/100, i/1000 FROM generate_series (1,50000) i;
-- table1 contains values in column2 that are different from values in random_table2 column2
INSERT INTO random_table2 SELECT i/1000, i*10 FROM generate_series (1,50000) i;
INSERT INTO random_table2 SELECT i/10000, i*10 FROM generate_series (1,50000) i;

-- without statistics
EXPLAIN ANALYZE SELECT * FROM random_table1 LEFT JOIN random_table2 ON random_table1.column2 = random_table2.column2;
-- Merge Left Join  (cost=14400.45..391385.32 rows=25115601 width=16) (actual time=54.612..20430.815 rows=99988003 loops=1), Execution time: 23141.798 ms

-- with statistics
CREATE STATISTICS stats_random_table1 (dependencies) on column1, column2 from random_table1;
CREATE STATISTICS stats_random_table2 (dependencies) on column2, column3 from random_table2;
ANALYZE random_table1;
ANALYZE random_table2;
EXPLAIN ANALYZE SELECT * FROM random_table1 LEFT JOIN random_table2 ON random_table1.column2 = random_table2.column2;
-- Hash Right Join  (cost=1347.00..1140912.42 rows=100749742 width=16) (actual time=9.070..8947.548 rows=99988003 loops=1), Execution time: 11594.609 ms
-- Execution time is again better and planner switches to opposite JOIN as in Experiment 7


-- EXPERIMENT 10
-- two tables with randomly generated integers
-- test RIGHT JOIN with WHERE clause
DROP TABLE IF EXISTS random_table1;
DROP TABLE IF EXISTS random_table2;
DROP STATISTICS IF EXISTS random_table1;
DROP STATISTICS IF EXISTS random_table2;
CREATE TABLE random_table1 (column1 int, column2 int);
CREATE TABLE random_table2 (column2 int, column3 int);
INSERT INTO random_table1 SELECT i/100, i/1000 FROM generate_series (1,50000) i;
-- table1 contains values in column2 that are different from values in random_table2 column2
INSERT INTO random_table2 SELECT i/1000, i*10 FROM generate_series (1,50000) i;
INSERT INTO random_table2 SELECT i/10000, i*10 FROM generate_series (1,50000) i;

-- without statistics
EXPLAIN ANALYZE SELECT * FROM random_table1 RIGHT JOIN random_table2 ON random_table1.column2 = random_table2.column2 WHERE column1 > 10;
-- Merge Join  (cost=11781.96..137443.58 rows=8371867 width=16) (actual time=62.119..18445.890 rows=87901001 loops=1), Execution time: 20864.135 ms
EXPLAIN ANALYZE SELECT * FROM random_table1 RIGHT JOIN random_table2 ON random_table1.column2 = random_table2.column2 WHERE column1 = 1 and random_table1.column2 = 0;
-- Nested Loop  (cost=0.00..2943.18 rows=22254 width=16) (actual time=0.021..159.560 rows=1099800 loops=1), Execution time: 191.533 ms

-- with statistics
CREATE STATISTICS stats_random_table1 (dependencies) on column1, column2 from random_table1;
CREATE STATISTICS stats_random_table2 (dependencies) on column2, column3 from random_table2;
ANALYZE random_table1;
ANALYZE random_table2;

EXPLAIN ANALYZE SELECT * FROM random_table1 RIGHT JOIN random_table2 ON random_table1.column2 = random_table2.column2 WHERE column1 > 10;
-- Hash Join  (cost=1458.25..1118608.73 rows=98533248 width=16) (actual time=10.519..7881.624 rows=87901001 loops=1), Execution time: 10242.430 ms
-- Similar to Experiment 7, with statistics better execution time and Hash Join is used instead of Merge Join
EXPLAIN ANALYZE SELECT * FROM random_table1 RIGHT JOIN random_table2 ON random_table1.column2 = random_table2.column2 WHERE column1 = 1 and random_table1.column2 = 0;
-- Nested Loop  (cost=0.00..16840.25 rows=1134000 width=16) (actual time=0.032..164.679 rows=1099800 loops=1), Execution time: 197.551 ms
-- improved row estimate

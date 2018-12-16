CREATE TABLE intvals(val int, color text);

-- Test empty table
SELECT median(val) FROM intvals;

-- Integers with odd number of values
INSERT INTO intvals VALUES
       (1, 'a'),
       (2, 'c'),
       (9, 'b'),
       (7, 'c'),
       (2, 'd'),
       (-3, 'd'),
       (2, 'e');

SELECT * FROM intvals ORDER BY val;
SELECT median(val) FROM intvals;

-- Integers with NULLs and even number of values
INSERT INTO intvals VALUES
       (99, 'a'),
       (NULL, 'a'),
       (NULL, 'e'),
       (NULL, 'b'),
       (7, 'c'),
       (0, 'd');

SELECT * FROM intvals ORDER BY val;
SELECT median(val) FROM intvals;

-- Text values
CREATE TABLE textvals(val text, color int);

INSERT INTO textvals VALUES
       ('erik', 1),
       ('mat', 3),
       ('rob', 8),
       ('david', 9),
       ('lee', 2);

SELECT * FROM textvals ORDER BY val;
SELECT median(val) FROM textvals;

-- Text values even
INSERT INTO textvals VALUES
       ('lea', 2);
SELECT median(val) FROM textvals;

-- Test large table with timestamps
CREATE TABLE timestampvals (val timestamptz);

INSERT INTO timestampvals(val)
SELECT TIMESTAMP 'epoch' + (i * INTERVAL '1 second')
FROM generate_series(0, 100000) as T(i);

SELECT median(val) FROM timestampvals;

-- Test timestamptzs even
INSERT INTO timestampvals VALUES
       ('1970-01-02 03:46:41');

SELECT median(val) FROM timestampvals;

-- Test timestamps even
CREATE TABLE timestampvals_gmt (val timestamp);

INSERT INTO timestampvals_gmt(val)
SELECT TIMESTAMP 'epoch' + (i * INTERVAL '1 second')
FROM generate_series(0, 3) as T(i);

SELECT median(val) FROM timestampvals_gmt;

-- Test decimals even
CREATE TABLE decimalvals (val decimal);
INSERT INTO decimalvals VALUES
       (1.125),
       (3.4);

SELECT median(val) FROM decimalvals;

-- Test floats even
CREATE TABLE floatvals (val float);
INSERT INTO floatvals VALUES
       (1.125),
       (3.4);

SELECT median(val) FROM floatvals;

-- Test cash even
CREATE TABLE moneyvals (val money);
INSERT INTO moneyvals VALUES
       ('12.34'),
       ('101.34');

SELECT median(val) FROM moneyvals;

-- Test intervals even
CREATE TABLE intervalvals (val interval);
INSERT INTO intervalvals VALUES
       ('1 day'),
       ('2 days');

SELECT median(val) FROM intervalvals;

-- Test ints group by
SELECT color, median(val) FROM intvals GROUP BY color;

-- Test multiple usage
SELECT median(val), median(color) FROM intvals;

-- Test parallel usage on a single field
SELECT median(val), median(val) FROM intvals;

-- Test window context
SELECT median(x) OVER (ORDER BY n ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM (VALUES (1, 1.0::float8), (2, 2.0::float8), (3, 3.0::float8)) AS v (n,x);

-- Test window context with OVER (PARTITION BY) statement
SELECT color, median(val) OVER (PARTITION BY color) from intvals;

-- Test moving-aggregate mode
SELECT array_agg(val) OVER (ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) FROM generate_series(0, 10) as val;
SELECT median(val) OVER (ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) FROM generate_series(0, 10) as val;

CREATE TABLE golden (i INTEGER, b BIGINT, s VARCHAR, d DECIMAL(10,2), dt DATE, f DOUBLE, ok BOOLEAN, l INTEGER[], st STRUCT(a INTEGER, b VARCHAR));
INSERT INTO golden VALUES (1, 10000000000, 'one', 1.25, DATE '2024-01-02', 0.5, true, [1, 2], {'a': 1, 'b': 'x'}), (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL), (3, -3, 'three', -3.50, DATE '1999-12-31', -2.25, false, [], {'a': NULL, 'b': 'z'});
CREATE TABLE golden_big AS SELECT n FROM range(3000) t(n);

CREATE TABLE IF NOT EXISTS {{ params.table }} (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    duration INTERVAL,
    explicit BOOLEAN,
    release DATE,
    popularity INT,
    type VARCHAR(10),
    age INT
);

TRUNCATE TABLE {{ params.table }} RESTART IDENTITY;
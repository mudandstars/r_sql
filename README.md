# Custom SQL Interpreter
### Reconstructing (something like) postgreSQL as a learning project in Rust


### What r_sql can do

1. Parse simple queries (SELECT, INSERT INTO, CREATE TABLE)
2. Handle simple reads/writes to binary format:

### What r_sql cannot (yet) do

1. More complex queries, including subqueries, SORT BY, ORDER BY, LIMIT, functions like COUNT() and the like
2. Indices
    1. on each primary key
    2. available to add them
3. Basic Constraints, such as NOT NULL, DEFAULT \_, UNIQUE, ..

# Custom SQL Interpreter & Engine

### Reconstructing a relational database as a learning project in Rust

### Check it out

1. Clone repository and build binary using `cargo build --release`
2. Use in one of two ways

    a. Using cli arg:
    `pathto/binary "SQL query"`

    b. Using the interactive shell. Start it by executing the binary without cli args. Quit it by submitting the character 'q'.

### What r_sql can do

1. Parse simple queries (SELECT (WHERE, AND), INSERT INTO, CREATE TABLE, CREATE INDEX)
2. Handle simple reads/writes
3. Use indices on columns for heavily improved read performance


### What r_sql cannot (yet) do

1. More complex queries, including subqueries, SORT BY, ORDER BY, LIMIT, functions like COUNT() and the like
2. Basic Constraints, such as NOT NULL, DEFAULT \_, UNIQUE, ..

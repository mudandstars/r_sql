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
3. Use indices on columns for improved read performance


### What r_sql cannot (yet) do

1. More complex queries, including subqueries, SORT BY, ORDER BY, LIMIT, functions like COUNT() and the like
2. Constraints, such as NOT NULL, DEFAULT \_, UNIQUE, ..


### TODOS
-   indices: allow a leaf to point to multiple data pages
-   basic constraints, such as NOT NULL, DEFAULT, UNIQUE
-   implement WAL
-   implement more of the common SQL syntax, such as DROP and UPDATE and DELETE

### Limitations

I consciously chose to not implement logic for the following

1. I don't handle the case that a single record exceeds the size of a datapage
2. I don't implement DB metadata like users or privileges

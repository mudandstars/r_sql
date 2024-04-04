allow insertion using the meta-data constraints
    meta-data doesnt load properly, thats why insert doesnt work
    check all open todos

-   implement io engine
-   indices
    -   B-Tree
    -   each leaf node points to a data page (fixed size 16kB used by InnoDB by mySQL)
    -   separate files for indices
-   basic constraints
-   implement DROP and DELETE

-   limitations:
    -   I don't handle the case that a single record exceeds the size of a datapage
    -   I don't implement DB metadata, such as users or privileges

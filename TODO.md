-   before insertion, check value satisfies column (type) constraints
-   use data pages to handle record saving and retrieving
-   enable select to correctly use the selected columns only

-   in progress: implement io engine
-   indices
    -   B-Tree
    -   each leaf node points to a data page (fixed size 16kB used by InnoDB by mySQL)
    -   separate files for indices
-   basic constraints
-   implement DROP and DELETE
-   implement WAL

-   limitations:
    -   I don't handle the case that a single record exceeds the size of a datapage
    -   I don't implement DB metadata, such as users or privileges

-   indices
    - NOW:
        - use index file on select
-   basic constraints
-   implement DROP and UPDATE and DELETE and WHERE and AND
-   implement WAL

-   limitations:
    -   I don't handle the case that a single record exceeds the size of a datapage
    -   I don't implement DB metadata like users or privileges

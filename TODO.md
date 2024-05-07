-   indices
    - allow a leaf to point to multiple data pages
-   basic constraints
-   implement DROP and UPDATE and DELETE
-   implement WAL

-   limitations:
    -   I don't handle the case that a single record exceeds the size of a datapage
    -   I don't implement DB metadata like users or privileges

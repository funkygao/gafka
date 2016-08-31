# InnoDB transaction

### Redo/Undo log

- redo log

  ib_logfile0 ib_logfile1

- undo log

  ibdata1

### Table

Each table has 2 hidden columns:
- #txn 
- rollback pointer 

Copying a version of a row from the table into the undo log and the linked list of row versions are called MVCC.


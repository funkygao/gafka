# golang tips

### string and []byte conversion optimization

- golang avoids extra allocations when []byte keys are used to lookup entries in map[string] collections: m[string(key)]
- golang avoids extra allocations in `for range` clauses where strings are converted to []byte
  - for i,v := range []byte(str) {...}

### recover

Calling recover() will do the trick only when it is called directly in your deferred function.

### range

The data values generated in the "range" clause are copies of the actual collection elements.

They are not references to the original items. 
This means that updating the values will not change the original data. 
It also means that taking the address of the values will not give you pointers to the original data.

### defer

Arguments for a deferred function call are evaluated when the defer statement is evaluated (not when the function is actually executing).

### Read and Write Operation Reordering


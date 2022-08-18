What is it?
	A LSM-tree based kv store.
	Use leveled compaction.
	No concurrency control support now.
	A toy project to get myself familiar with rust.

How to develop it?
	Design first.
	Start simple.
	Test Driven.

What is the user interface?
	Cli.
		qikv put *key* *value*
		qikv scan *key1* [key2]
		qikv rm *key*

What is the library api?
	KvStore corresponding to an isolated store with configs like path, compaction size.
		Insert(key, value) -> Option<OldValue> 
		Delete(key) -> Option<OldValue> 
		Scan(key1, key2) -> Option<Vec<Value>>

Where to start test?
	Surely not the highest interface.
	Memtable.
	Crash recovery from log.
	Write to SStable.
	Compaction.

When does compaction happen?
	When the size of a level reaches certain N.

What is the difference between level 0 and above?
	Newly flushed memtable belongs to level 0.
	SStables in level 0 may have overlapping key ranges which doesn't exist in higher level.

How does compaction work?
	When the level is 0, pick a sstable and find other sstables overlapping with it, then find all overlapping sstables in the next level, start compaction, add the result sstable to the next level and remove old ones.
	When the level is >= 1, pick a sstable and find all overlapping sstables in the next level, same.

When to flush memtable?
	When the log is larger than certain size.
	Or?
	There is so many strategies and parameters.

What do I need to write to log?
	Everything needed to recover the memtable from crash.
		Insertion.
		Deletion.

What is the data flow?
	Cli -> Log -> Memtable -> SStable.

What is the Memtable api?
	Insert
	Delete
	Scan
	Flush2Disk
	RecoverFromLog


What kind of data structure should be used for Memtable?
	Self-balanced trees like AVL tree and red-black tree.
	RocksDB recommends skiplist which supports concurrent write.
		Why does LevelDB's skiplist have only write-write conflict?
			Analyse RW steps.
				Finished. Parial write doesn't affect normal read although order cannot be relied on.
				A new node is visible after there is at least one forward pointer pointing to it.
					Finished. Parial write doesn't affect read.
					A new node becomes visible immediately after the first forward pointer points to it.
		Why does LevelDB and RocksDB both require no item deletion?
			Delete() conflicts with Get()?
		Do we support duplicate key?
			Yes, pairs with duplicate key are structured in a row.
				No way. Then we cannot update kv pairs and place tombstone.
			Value as a list.
				Need an additional length attribute for every pair.
			Add additional info on key to make it unique. Can be implemented at upper layer as key+uid.
			We choose the last one.
		How to represent Key/Value type?
			Raw bytes. Use Vec<u8> or String.
			Use Option for Value and None means Tombstone. Use alias.
			Tombstone only exists in memtable and log.
		How to mark deletion properly?
			LevelDB encodes it in the key.

Why do we need to store integers in big endianness?
	For lexical sorting. (Make bytes sorting consistent with numberical sorting)
	More info here. https://cornerwings.github.io/2019/10/lexical-sorting/
			

Why do we ever need atomic write of a scalar if cpu operations are atomic anyway?
	To sync cpu caches with main memory.
	Specificaly, atomic.Store flushes value to main memory(all the way down cpu caches).
	And atomic.Load loads value from main memory(discards old cpu caches on this value).
	You will need atomic operations when multiple cpus share one value on memory which is mostly the case.
	
What does log do?
	Used for crash recovery.
	Only record writes.
	Only depends on kvstore path.
	How to encode record?

Why do we need batch write?
	Combine multiple io operations into a single one.

How to set up a rust project with test?
	Unit tests and integration test.

There maybe multiple log files at the same time due to fast writes and slow flushing of memtables.
So is flushing of memtables async to read/write of db?
	To keep it simple, we just use a single log file and check its size in memtable(sync way).

How does db know whether to recover from log?
	Simple.
	If there exist log files, then a crash happened.
	When db shuts down gracefully, we flush memtable to SStable and clean log files.


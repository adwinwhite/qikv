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
				No, it won't work.
				If get() returns None, that's Tombstone or nonexistence?
			Tombstone only exists in memtable and log.
				No, it's not. 
				Tombstone can affect pairs in older SStables.
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

What are properties of SStables?
	Each key is unique in one file.
	Pairs are sorted by key.

What is the format of sstable files?
	Oh, levelDB's table format is truly sophisticated.
	I'll just use a much simpler one.
	Since main purpose of SStable is to speed up query access, the only additional data we store is sparse index.
		[ Record * N ]
		[ Index * M ]
		[ Size of index ]

What does in-memory sstable do?
	Get()
	::Compaction(SStables)

What happens when we point access a pair?
	Test bloom filter.
	Search memtables.
	Search SStables from level 0 to higher.
		Lookup manifest to find out which SStables are in serving state.
		Check their key ranges from lower level to higher.
		Scan SStables.
	It should be a consistent view.

How to scan over memtable, SStables?
	Use iterator like LevelDB.
		Get all iterator.
		Generate the smallest one each time like in compaction.
	And remember that sstables in the same level are not overlapping except for level 0.

What's the format of manifest?

What's the detailed process of compaction?
	Flush memtable to level 0 SStables.
	When there are four level 0 SStables, compact all of them to produce level 1 SStables.(Use a new file every 2MB)
	Continuing to flush memtable to level 0 SStables.
	When there are four level 0 SStables, compact all of them and overlapping level 1 SStables.
	If combined size of level L(>=1) SStables > 10^L MB, choose a SStables (rotate the chosed range over key space) and its overlapping SStables in level L+1 to compact.

How to compact async if it's flushing memtable triggering all this?
	When it's time to flush a memtable, just add it to a to-be-compacted list. The compaction thread will process it.
	New another memtable to store in-coming data.


How sparse the SStable index should be?
	Every 16 pair. Maybe.

If we use bincode to serialize memtable to disk, we cannot get the sparse index since offsets are unknown.
Try partitioning memtable?


sstables is like log. So I should use reader/writer pattern?
OO or functional?

How to organize modules?
	Manifest depends on SStables or vice versa.

I don't understand lifetimes in Rust at all!
	I got it.
	Lifetime is a period in that owned value doesn't change its memory location(aka not moved or dropped).
	<'a> means there must exist some lifetime "a"
	In a function, input references annotated with "a" must be all valid in "a", and output references annotated with "a" are only valid in "a".
	For struct, it's the same. Just think in terms of its constructor and the constructed instance as the output reference.

Should I seperate sparse index from actual data?

I should make a more general combined iterator that can combine iterators from memtable and so on. 
	Use trait object.

Todo!
	Improve Iterator so that it can return references or what.
		Use custom encoding.
			Type mismatch.
	Or easy construction.
		Create an owned type to hold sstables.
			Then where to place it?
		Let's creata a sst manager to own all active sstables.
			And a sst cheap handle?
	Lazy load of a level to use only one iterator.
	Iterator over whole db.
	Well consider lazy load later:
		SSTable lazy load.
		Lazy get().
		Lazy compaction? (Seems not necessary)



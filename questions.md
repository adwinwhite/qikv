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
	In a function, all input references annotated with "a" must be valid in "a", and output references annotated with "a" are only valid in "a".
	For struct, it's the same. Just think in terms of its constructor and the constructed instance as the output reference.

Should I seperate sparse index from actual data?

I should make a more general combined iterator that can combine iterators from memtable and so on. 
	Use trait object.

What about error handling? Fail to load SST file?
	Panic bravely? 
	Only panic in main()?

Why doesn't level 1 sst file exist?
	Check whether it's created first.
	Read the wrong path.

How to recover from crash? Log replay + snapshot.
	memtable from log.
	manifest from another log.
	bloomfilter?

How to record operations?
	Use newtype pattern to warp old structs?
		requires new function name.
		need to disable unrecorded function version.

How to avoid half snapshot of manifest?
	Only switch after the new snapshot is completely created.

How to ensure atomicity of recording and operation?

How to initialize from normal exit?
	new empty memtable.
	manfest from directories/files.

How to implement crash recovery for manifest?
	Manifest only changes in two operations: flush level 0 and compact.
	We need to make sure they are atomic so that whenever we look into manifest, it's a valid view.

How does ssts changes in manifest?
	Only two ways: flush level 0 and compact.
	Let's make them atomic.

How does other fields change in manifest?
	NewId in flushing level 0 and compaction.
	NextCompact only in compaction.

How to make sst_id update atomic in compaction?
	Do not change in-memory manifest.
	Add changes to manifest to a commit batch while doing compaction.
	After compaction completes, append the batch to log in a single O_DIRECT | O_SYNC write.(This is the mark of completion. If crash happends before, the changes just don't exist. Otherwise, they come to effect.)
	Apply changes to in-memory structure.
	Clean up obsolete files.

	Do the same for flushing level 0 SST.
	So I don't need to wrap around operations of manifest. Just log them.

How can get return value if not making changes to manifest?
	Multiple version?
		That's too strong.
		We are single threaded.
	Clone?
		Too much cost.
	Assign action an index monotonically?
	Seperate get and set operations.
		And view/change a local state(clone parial manifest).
			new_id => just a u64.
			compact_key => only use once in one compaction.

Can I combine new_id log and latest_id?
	No, use local state.

When to snapshot manifest?


How to test manifest recovery?
	1. test fresh start.
	2. test normal exit.
	3. test crash recovery.
		How to make it crash?
			Spawn a subprocess and kill it later.
		How to check its consistency?
			It can only be one of two valid states.
			And they have equivalent data.
				For flush_level0, recover memtable first.

What if bloomfilter grows too large?

How to check memtable's size without counting every insertion?

How to purge useless Tombstones?
	Check level in compaction.

Why use lazy loading of SST?
	In large levels, there might be many overlappings.

How to simplify sstable.rs?
	SSTGroup is too general.
	Only need to consider two kinds of compaction: compact level 0 and above.
		Both can use a combined iterator of level L and L + 1. 
			Level 0: a combined iterator of level 0 sst and a level iterator of level 1.
			Level >= 1: a sst iterator of level L and a level iterator of level L + 1.
		Level iterator can be lazy since they are not overlapped.

How to sort sst easily and lazily?
	Compaction often requires sorted sst.
	And lazy loading requires sorting before loading.
	Sorting sst above level 0 requires only key ranges.

Why test_check_sst_size() fails sometimes?
	Failure reasons: 
		Bincode failed to decode SST file - {Unexpected End, Invalid Integer Type}. (sstable.rs:146)
			Whether are they all in level 0?
				Yes, and they are all the first load { level = 0, id = 0 }.
		Panicked at "attempt to substract with overflow". (sstable.rs:143)
			Write twice to sst_id { level = 0, id = 0 }.
			And only synced data.
			So the reader may get wrong file metadata like length and parsed invalid footer.
	Abnormal Observation:
		Flush memtable twice consecutively to { level = 0, id = 0 }.
			Use id = 0 twice.
			Bad initialization in manifest.next_sst_id().
	Add a test for this. (Done)

Is there a generic way of recovering a state machine?
	Like a proc macro.
	So that I don't have to implement the same idea twice for manifest and memtable.

What if data is too large for log write to be atomic?
	Use a scalar to represent the latest successfully write index.
	Use a scalar to represent the latest successfully write offset.
	Use a commit action to represent the batch has been successfully written.
		But there maybe half-written batch which will cause decode error.
			And we can't make the naive assumption that all decode error is due to half-written batch.
			Or we can.

How to implement crash recovery?
	Manifest recovery.
	Memtable recovery.
	Do I need to recover bloomfilter too?
		Easy if it's gracefully closed.

How to write crash recovery test?
	Client-Server model.
	Check whether client sees consistent data.

Why does recovered memtable appear totally different?
	But occasionally it is consistent.
	Forgot to join thread so data writing may not be completed.

How to iterate pairs in the whole store?
	Combined iterator from { memtable, level0 ssts, level iterators }.
	How to implement level iterator in a lazy way?
		Self referential.
		Change borrowed and borrowing reference at the same time.


Todo!
	Iterate the whole kvstore.
	Crash recovery.
		Manifest recovery.
		Memtable recovery.
	Purge useless Tombstones.
	Avoid copy in iterator.
	Simplify sstable.rs.
	Test manifest recovery in store.rs.
	Support concurrency.
	Support transaction.
	Check levelDB and RocksDB's in-memory SSTable.
	Improve Iterator so that it can return references or what.
		Use custom encoding.
			Type mismatch.
				Which with which?
		Zero-copy serde/deserde.
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



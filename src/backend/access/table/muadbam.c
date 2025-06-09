/*-------------------------------------------------------------------------
 *
 * muadbam.c
 *	  MuaDB table access method code
 *
 * MuaDB is a key-value based table access method for PostgreSQL that 
 * demonstrates how to implement a custom storage engine using the 
 * Table Access Method (TAM) interface.
 *
 * Portions Copyright (c) 2025, MuaDB Team
 *
 * IDENTIFICATION
 *	  src/backend/access/table/muadbam.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/tableam.h"
#include "access/relscan.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/rel.h"

/* Function prototype for handler */
Datum muadb_tableam_handler(PG_FUNCTION_ARGS);

/* ----------------------------------------------------------------
 * MuaDB specific structures and declarations
 * ---------------------------------------------------------------- */

/*
 * MuaDB scan descriptor - extends the base TableScanDesc
 */
typedef struct MuadbScanDescData
{
	TableScanDescData rs_base;	/* Base scan descriptor */
	
	/* MuaDB specific scan state */
	BlockNumber		muadb_current_block;
	OffsetNumber	muadb_current_offset;
	bool			muadb_scan_finished;
} MuadbScanDescData;

typedef struct MuadbScanDescData *MuadbScanDesc;

/* ----------------------------------------------------------------
 * MuaDB slot functions
 * ---------------------------------------------------------------- */

static const TupleTableSlotOps *
muadbam_slot_callbacks(Relation relation)
{
	elog(LOG, "MuaDB: Getting slot callbacks for relation %s", 
		 RelationGetRelationName(relation));
	
	/*
	 * For now, we'll use the same slot callbacks as heap.
	 * In a real implementation, you might want custom slots.
	 */
	return &TTSOpsBufferHeapTuple;
}

/* ----------------------------------------------------------------
 * MuaDB scan functions  
 * ---------------------------------------------------------------- */

static TableScanDesc
muadbam_scan_begin(Relation relation, Snapshot snapshot,
				   int nkeys, ScanKey key,
				   ParallelTableScanDesc pscan,
				   uint32 flags)
{
	MuadbScanDesc scan;
	
	elog(LOG, "MuaDB: Beginning scan of relation %s (OID: %u)", 
		 RelationGetRelationName(relation), RelationGetRelid(relation));

	scan = (MuadbScanDesc) palloc0(sizeof(MuadbScanDescData));
	
	/* Initialize base scan descriptor */
	scan->rs_base.rs_rd = relation;
	scan->rs_base.rs_snapshot = snapshot;
	scan->rs_base.rs_nkeys = nkeys;
	scan->rs_base.rs_key = key;
	scan->rs_base.rs_flags = flags;
	
	/* Initialize MuaDB specific fields */
	scan->muadb_current_block = 0;
	scan->muadb_current_offset = FirstOffsetNumber;
	scan->muadb_scan_finished = false;

	return (TableScanDesc) scan;
}

static void
muadbam_scan_end(TableScanDesc scan)
{
	MuadbScanDesc muadb_scan = (MuadbScanDesc) scan;
	
	elog(LOG, "MuaDB: Ending scan of relation %s", 
		 RelationGetRelationName(scan->rs_rd));
	
	pfree(muadb_scan);
}

static void
muadbam_scan_rescan(TableScanDesc scan, ScanKey key, bool set_params,
					bool allow_strat, bool allow_sync, bool allow_pagemode)
{
	MuadbScanDesc muadb_scan = (MuadbScanDesc) scan;
	
	elog(LOG, "MuaDB: Rescanning relation %s", 
		 RelationGetRelationName(scan->rs_rd));
	
	/* Reset scan state */
	muadb_scan->muadb_current_block = 0;
	muadb_scan->muadb_current_offset = FirstOffsetNumber;
	muadb_scan->muadb_scan_finished = false;
	
	/* Update scan keys if provided */
	if (key != NULL)
		scan->rs_key = key;
}

static bool
muadbam_scan_getnextslot(TableScanDesc scan, ScanDirection direction, TupleTableSlot *slot)
{
	MuadbScanDesc muadb_scan = (MuadbScanDesc) scan;
	
	elog(LOG, "MuaDB: Getting next slot from relation %s", 
		 RelationGetRelationName(scan->rs_rd));
	
	/* For now, just return false (no tuples found) */
	/* In a real implementation, this would iterate through MuaDB storage */
	ExecClearTuple(slot);
	muadb_scan->muadb_scan_finished = true;
	
	return false;
}

/* ----------------------------------------------------------------
 * MuaDB tuple manipulation functions
 * ---------------------------------------------------------------- */

static void
muadbam_tuple_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
					 int options, BulkInsertState bistate)
{
	bool shouldFree = true;
	HeapTuple tuple;
	
	elog(LOG, "MuaDB: Inserting tuple into relation %s (OID: %u) - "
		 "cid=%u, options=0x%x",
		 RelationGetRelationName(relation), RelationGetRelid(relation),
		 cid, options);

	/*
	 * For now, we'll delegate to heap for basic functionality.
	 * In a real implementation, this would:
	 * 1. Serialize the tuple to a key-value format
	 * 2. Store it in MuaDB storage 
	 * 3. Set the slot's TID to indicate where it was stored
	 */
	
	tuple = ExecFetchSlotHeapTuple(slot, true, &shouldFree);
	
	/* Update the tuple with table oid */
	slot->tts_tableOid = RelationGetRelid(relation);
	tuple->t_tableOid = slot->tts_tableOid;

	/* For now, use heap_insert but with MuaDB logging */
	heap_insert(relation, tuple, cid, options, bistate);
	ItemPointerCopy(&tuple->t_self, &slot->tts_tid);
	
	elog(LOG, "MuaDB: Successfully inserted tuple at TID (%u,%u)",
		 ItemPointerGetBlockNumber(&slot->tts_tid),
		 ItemPointerGetOffsetNumber(&slot->tts_tid));

	if (shouldFree)
		pfree(tuple);
}

static void
muadbam_tuple_insert_speculative(Relation relation, TupleTableSlot *slot,
								  CommandId cid, int options,
								  BulkInsertState bistate, uint32 specToken)
{
	bool shouldFree = true;
	HeapTuple tuple;
	
	elog(LOG, "MuaDB: Speculative insert into relation %s (token: %u)", 
		 RelationGetRelationName(relation), specToken);
	
	/* Delegate to regular heap for now */
	tuple = ExecFetchSlotHeapTuple(slot, true, &shouldFree);

	slot->tts_tableOid = RelationGetRelid(relation);
	tuple->t_tableOid = slot->tts_tableOid;

	HeapTupleHeaderSetSpeculativeToken(tuple->t_data, specToken);
	options |= HEAP_INSERT_SPECULATIVE;

	heap_insert(relation, tuple, cid, options, bistate);
	ItemPointerCopy(&tuple->t_self, &slot->tts_tid);

	if (shouldFree)
		pfree(tuple);
}

static void
muadbam_tuple_complete_speculative(Relation relation, TupleTableSlot *slot,
									uint32 specToken, bool succeeded)
{
	elog(LOG, "MuaDB: Completing speculative insert (token: %u, succeeded: %s)", 
		 specToken, succeeded ? "true" : "false");
	
	/* Delegate to heap for now */
	if (succeeded)
		heap_finish_speculative(relation, &slot->tts_tid);
	else
		heap_abort_speculative(relation, &slot->tts_tid);
}

static TM_Result
muadbam_tuple_delete(Relation relation, ItemPointer tid, CommandId cid,
					 Snapshot snapshot, Snapshot crosscheck, bool wait,
					 TM_FailureData *tmfd, bool changingPart)
{
	elog(LOG, "MuaDB: Deleting tuple from relation %s at TID (%u,%u)", 
		 RelationGetRelationName(relation),
		 ItemPointerGetBlockNumber(tid),
		 ItemPointerGetOffsetNumber(tid));
	
	/* Delegate to heap for now */
	return heap_delete(relation, tid, cid, crosscheck, wait, tmfd, changingPart);
}

static TM_Result
muadbam_tuple_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
					 CommandId cid, Snapshot snapshot, Snapshot crosscheck,
					 bool wait, TM_FailureData *tmfd,
					 LockTupleMode *lockmode, TU_UpdateIndexes *update_indexes)
{
	bool shouldFree = true;
	HeapTuple tuple;
	TM_Result result;
	
	elog(LOG, "MuaDB: Updating tuple in relation %s at TID (%u,%u)", 
		 RelationGetRelationName(relation),
		 ItemPointerGetBlockNumber(otid),
		 ItemPointerGetOffsetNumber(otid));
	
	/* Delegate to heap for now */
	tuple = ExecFetchSlotHeapTuple(slot, true, &shouldFree);

	slot->tts_tableOid = RelationGetRelid(relation);
	tuple->t_tableOid = slot->tts_tableOid;

	result = heap_update(relation, otid, tuple, cid, crosscheck, wait,
						 tmfd, lockmode, update_indexes);
	ItemPointerCopy(&tuple->t_self, &slot->tts_tid);

	if (shouldFree)
		pfree(tuple);

	return result;
}

static TM_Result
muadbam_tuple_lock(Relation relation, ItemPointer tid, Snapshot snapshot,
				   TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
				   LockWaitPolicy wait_policy, uint8 flags,
				   TM_FailureData *tmfd)
{
	BufferHeapTupleTableSlot *bslot;
	TM_Result result;
	Buffer buffer;
	HeapTuple tuple;
	bool follow_updates;
	
	elog(LOG, "MuaDB: Locking tuple in relation %s at TID (%u,%u)", 
		 RelationGetRelationName(relation),
		 ItemPointerGetBlockNumber(tid),
		 ItemPointerGetOffsetNumber(tid));
	
	/* For now, delegate to heap - this is complex to implement from scratch */
	bslot = (BufferHeapTupleTableSlot *) slot;
	tuple = &bslot->base.tupdata;

	follow_updates = (flags & TUPLE_LOCK_FLAG_LOCK_UPDATE_IN_PROGRESS) != 0;
	tmfd->traversed = false;

	Assert(TTS_IS_BUFFERTUPLE(slot));

	tuple->t_self = *tid;
	result = heap_lock_tuple(relation, tuple, cid, mode, wait_policy,
							 follow_updates, &buffer, tmfd);

	if (result == TM_Ok)
	{
		ExecStoreBufferHeapTuple(tuple, slot, buffer);
	}
	else
	{
		ExecClearTuple(slot);
		ReleaseBuffer(buffer);
	}

	return result;
}

/* ----------------------------------------------------------------
 * Minimal implementations for required callbacks
 * ---------------------------------------------------------------- */

static void 
muadbam_multi_insert(Relation rel, TupleTableSlot **slots, int nslots,
					 CommandId cid, int options, BulkInsertState bistate)
{
	int i;
	
	elog(LOG, "MuaDB: Multi-inserting %d tuples into relation %s", 
		 nslots, RelationGetRelationName(rel));
	
	/* For now, just call single insert multiple times */
	for (i = 0; i < nslots; i++)
	{
		muadbam_tuple_insert(rel, slots[i], cid, options, bistate);
	}
}

static size_t 
muadbam_parallelscan_estimate(Relation rel)
{
	/* Delegate to heap's parallel scan estimate */
	return table_block_parallelscan_estimate(rel);
}

static Size 
muadbam_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
	/* Delegate to heap's parallel scan initialization */
	return table_block_parallelscan_initialize(rel, pscan);
}

static void 
muadbam_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
	/* Delegate to heap's parallel scan reinitialize */
	table_block_parallelscan_reinitialize(rel, pscan);
}

static IndexFetchTableData *
muadbam_index_fetch_begin(Relation rel)
{
	const TableAmRoutine *heapam;
	
	elog(LOG, "MuaDB: Beginning index fetch for relation %s", RelationGetRelationName(rel));
	
	/* Get heap table access method and delegate */
	heapam = GetHeapamTableAmRoutine();
	return heapam->index_fetch_begin(rel);
}

static void 
muadbam_index_fetch_reset(IndexFetchTableData *scan)
{
	const TableAmRoutine *heapam;
	
	/* Get heap table access method and delegate */
	heapam = GetHeapamTableAmRoutine();
	heapam->index_fetch_reset(scan);
}

static void 
muadbam_index_fetch_end(IndexFetchTableData *scan)
{
	const TableAmRoutine *heapam;
	
	elog(LOG, "MuaDB: Ending index fetch");
	
	/* Get heap table access method and delegate */
	heapam = GetHeapamTableAmRoutine();
	heapam->index_fetch_end(scan);
}

static bool 
muadbam_index_fetch_tuple(struct IndexFetchTableData *scan, ItemPointer tid,
						   Snapshot snapshot, TupleTableSlot *slot, bool *call_again, bool *all_dead)
{
	const TableAmRoutine *heapam;
	
	elog(LOG, "MuaDB: Fetching tuple via index at TID (%u,%u)",
		 ItemPointerGetBlockNumber(tid), ItemPointerGetOffsetNumber(tid));
	
	/* Get heap table access method and delegate */
	heapam = GetHeapamTableAmRoutine();
	return heapam->index_fetch_tuple(scan, tid, snapshot, slot, call_again, all_dead);
}

static bool 
muadbam_fetch_row_version(Relation relation, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot)
{
	const TableAmRoutine *heapam;
	
	elog(LOG, "MuaDB: Fetching row version from relation %s at TID (%u,%u)",
		 RelationGetRelationName(relation),
		 ItemPointerGetBlockNumber(tid), ItemPointerGetOffsetNumber(tid));
	
	/* Get heap table access method and delegate */
	heapam = GetHeapamTableAmRoutine();
	return heapam->tuple_fetch_row_version(relation, tid, snapshot, slot);
}

static void 
muadbam_get_latest_tid(TableScanDesc scan, ItemPointer tid)
{
	elog(LOG, "MuaDB: Getting latest TID for relation %s", RelationGetRelationName(scan->rs_rd));
	
	/* Delegate to heap */
	heap_get_latest_tid(scan, tid);
}

static bool 
muadbam_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	const TableAmRoutine *heapam;
	
	/* Get heap table access method and delegate */
	heapam = GetHeapamTableAmRoutine();
	return heapam->tuple_tid_valid(scan, tid);
}

static bool 
muadbam_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot, Snapshot snapshot)
{
	const TableAmRoutine *heapam;
	
	/* Get heap table access method and delegate */
	heapam = GetHeapamTableAmRoutine();
	return heapam->tuple_satisfies_snapshot(rel, slot, snapshot);
}

static TransactionId 
muadbam_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{
	elog(LOG, "MuaDB: Index delete tuples for relation %s", RelationGetRelationName(rel));
	
	/* Delegate to heap */
	return heap_index_delete_tuples(rel, delstate);
}

static void 
muadbam_relation_set_new_filelocator(Relation rel, const RelFileLocator *newrlocator,
									  char persistence, TransactionId *freezeXid, MultiXactId *minmulti)
{
	const TableAmRoutine *heapam;
	
	elog(LOG, "MuaDB: Setting new filelocator for relation %s", RelationGetRelationName(rel));
	
	/* Get heap table access method and delegate */
	heapam = GetHeapamTableAmRoutine();
	heapam->relation_set_new_filelocator(rel, newrlocator, persistence, freezeXid, minmulti);
}

static void 
muadbam_relation_nontransactional_truncate(Relation rel)
{
	const TableAmRoutine *heapam;
	
	elog(LOG, "MuaDB: Non-transactional truncate of relation %s", RelationGetRelationName(rel));
	
	/* Get heap table access method and delegate */
	heapam = GetHeapamTableAmRoutine();
	heapam->relation_nontransactional_truncate(rel);
}

static void 
muadbam_relation_copy_data(Relation rel, const RelFileLocator *newrlocator)
{
	const TableAmRoutine *heapam;
	
	elog(LOG, "MuaDB: Copying data for relation %s", RelationGetRelationName(rel));
	
	/* Get heap table access method and delegate */
	heapam = GetHeapamTableAmRoutine();
	heapam->relation_copy_data(rel, newrlocator);
}

static void 
muadbam_relation_copy_for_cluster(Relation OldHeap, Relation NewHeap, Relation OldIndex,
								   bool use_sort, TransactionId OldestXmin, TransactionId *xid_cutoff,
								   MultiXactId *multi_cutoff, double *num_tuples,
								   double *tups_vacuumed, double *tups_recently_dead)
{
	const TableAmRoutine *heapam;
	
	elog(LOG, "MuaDB: Copying for cluster from %s to %s", 
		 RelationGetRelationName(OldHeap), RelationGetRelationName(NewHeap));
	
	/* Get heap table access method and delegate */
	heapam = GetHeapamTableAmRoutine();
	heapam->relation_copy_for_cluster(OldHeap, NewHeap, OldIndex, use_sort, OldestXmin,
									  xid_cutoff, multi_cutoff, num_tuples, tups_vacuumed, tups_recently_dead);
}

static void 
muadbam_relation_vacuum(Relation rel, struct VacuumParams *params, BufferAccessStrategy bstrategy)
{
	elog(LOG, "MuaDB: Vacuuming relation %s", RelationGetRelationName(rel));
	
	/* Delegate to heap */
	heap_vacuum_rel(rel, params, bstrategy);
}

static bool 
muadbam_scan_analyze_next_block(TableScanDesc scan, ReadStream *stream)
{
	const TableAmRoutine *heapam;
	
	elog(LOG, "MuaDB: Analyze next block for relation %s", RelationGetRelationName(scan->rs_rd));
	
	/* Get heap table access method and delegate */
	heapam = GetHeapamTableAmRoutine();
	return heapam->scan_analyze_next_block(scan, stream);
}

static bool 
muadbam_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
								double *liverows, double *deadrows, TupleTableSlot *slot)
{
	const TableAmRoutine *heapam;
	
	/* Get heap table access method and delegate */
	heapam = GetHeapamTableAmRoutine();
	return heapam->scan_analyze_next_tuple(scan, OldestXmin, liverows, deadrows, slot);
}

static double 
muadbam_index_build_range_scan(Relation heapRelation, Relation indexRelation, IndexInfo *indexInfo,
							   bool allow_sync, bool anyvisible, bool progress, BlockNumber start_blockno,
							   BlockNumber numblocks, IndexBuildCallback callback, void *callback_state,
							   TableScanDesc scan)
{
	const TableAmRoutine *heapam;
	
	elog(LOG, "MuaDB: Index build range scan for relation %s", RelationGetRelationName(heapRelation));
	
	/* Get heap table access method and delegate */
	heapam = GetHeapamTableAmRoutine();
	return heapam->index_build_range_scan(heapRelation, indexRelation, indexInfo, allow_sync, anyvisible,
										  progress, start_blockno, numblocks, callback, callback_state, scan);
}

static void 
muadbam_index_validate_scan(Relation heapRelation, Relation indexRelation, IndexInfo *indexInfo,
							Snapshot snapshot, struct ValidateIndexState *state)
{
	const TableAmRoutine *heapam;
	
	elog(LOG, "MuaDB: Index validate scan for relation %s", RelationGetRelationName(heapRelation));
	
	/* Get heap table access method and delegate */
	heapam = GetHeapamTableAmRoutine();
	heapam->index_validate_scan(heapRelation, indexRelation, indexInfo, snapshot, state);
}

static uint64 
muadbam_relation_size(Relation rel, ForkNumber forkNumber)
{
	elog(LOG, "MuaDB: Getting relation size for %s", RelationGetRelationName(rel));
	
	/* Delegate to heap */
	return table_block_relation_size(rel, forkNumber);
}

static bool 
muadbam_relation_needs_toast_table(Relation rel)
{
	elog(LOG, "MuaDB: Checking if relation %s needs TOAST table", RelationGetRelationName(rel));
	
	/* We disabled TOAST, so always return false */
	return false;
}

static Oid 
muadbam_relation_toast_am(Relation rel)
{
	elog(LOG, "MuaDB: Getting TOAST AM for relation %s", RelationGetRelationName(rel));
	
	/* Since we don't use TOAST, return InvalidOid */
	return InvalidOid;
}

static void 
muadbam_relation_fetch_toast_slice(Relation toastrel, Oid valueid, int32 attrsize,
									int32 sliceoffset, int32 slicelength, struct varlena *result)
{
	elog(ERROR, "MuaDB: TOAST not supported - should not be called");
}

static void 
muadbam_estimate_rel_size(Relation rel, int32 *attr_widths, BlockNumber *pages,
						   double *tuples, double *allvisfrac)
{
	const TableAmRoutine *heapam;
	
	elog(LOG, "MuaDB: Estimating relation size for %s", RelationGetRelationName(rel));
	
	/* Get heap table access method and delegate */
	heapam = GetHeapamTableAmRoutine();
	heapam->relation_estimate_size(rel, attr_widths, pages, tuples, allvisfrac);
}

static bool 
muadbam_scan_bitmap_next_block(TableScanDesc scan, struct TBMIterateResult *tbmres)
{
	const TableAmRoutine *heapam;
	
	elog(LOG, "MuaDB: Bitmap scan next block for relation %s", RelationGetRelationName(scan->rs_rd));
	
	/* Get heap table access method and delegate */
	heapam = GetHeapamTableAmRoutine();
	return heapam->scan_bitmap_next_block(scan, tbmres);
}

static bool 
muadbam_scan_bitmap_next_tuple(TableScanDesc scan, struct TBMIterateResult *tbmres, TupleTableSlot *slot)
{
	const TableAmRoutine *heapam;
	
	/* Get heap table access method and delegate */
	heapam = GetHeapamTableAmRoutine();
	return heapam->scan_bitmap_next_tuple(scan, tbmres, slot);
}

static bool 
muadbam_scan_sample_next_block(TableScanDesc scan, struct SampleScanState *scanstate)
{
	const TableAmRoutine *heapam;
	
	elog(LOG, "MuaDB: Sample scan next block for relation %s", RelationGetRelationName(scan->rs_rd));
	
	/* Get heap table access method and delegate */
	heapam = GetHeapamTableAmRoutine();
	return heapam->scan_sample_next_block(scan, scanstate);
}

static bool 
muadbam_scan_sample_next_tuple(TableScanDesc scan, struct SampleScanState *scanstate, TupleTableSlot *slot)
{
	const TableAmRoutine *heapam;
	
	/* Get heap table access method and delegate */
	heapam = GetHeapamTableAmRoutine();
	return heapam->scan_sample_next_tuple(scan, scanstate, slot);
}

/* ----------------------------------------------------------------
 * MuaDB Table Access Method definition
 * ---------------------------------------------------------------- */

static const TableAmRoutine muadbam_methods = {
	.type = T_TableAmRoutine,

	.slot_callbacks = muadbam_slot_callbacks,

	.scan_begin = muadbam_scan_begin,
	.scan_end = muadbam_scan_end,
	.scan_rescan = muadbam_scan_rescan,
	.scan_getnextslot = muadbam_scan_getnextslot,

	.scan_set_tidrange = NULL,  /* Not implemented yet */
	.scan_getnextslot_tidrange = NULL,  /* Not implemented yet */

	.parallelscan_estimate = muadbam_parallelscan_estimate,
	.parallelscan_initialize = muadbam_parallelscan_initialize,
	.parallelscan_reinitialize = muadbam_parallelscan_reinitialize,

	.index_fetch_begin = muadbam_index_fetch_begin,
	.index_fetch_reset = muadbam_index_fetch_reset,
	.index_fetch_end = muadbam_index_fetch_end,
	.index_fetch_tuple = muadbam_index_fetch_tuple,

	.tuple_insert = muadbam_tuple_insert,
	.tuple_insert_speculative = muadbam_tuple_insert_speculative,
	.tuple_complete_speculative = muadbam_tuple_complete_speculative,
	.multi_insert = muadbam_multi_insert,
	.tuple_delete = muadbam_tuple_delete,
	.tuple_update = muadbam_tuple_update,
	.tuple_lock = muadbam_tuple_lock,

	.tuple_fetch_row_version = muadbam_fetch_row_version,
	.tuple_get_latest_tid = muadbam_get_latest_tid,
	.tuple_tid_valid = muadbam_tuple_tid_valid,
	.tuple_satisfies_snapshot = muadbam_tuple_satisfies_snapshot,
	.index_delete_tuples = muadbam_index_delete_tuples,

	.relation_set_new_filelocator = muadbam_relation_set_new_filelocator,
	.relation_nontransactional_truncate = muadbam_relation_nontransactional_truncate,
	.relation_copy_data = muadbam_relation_copy_data,
	.relation_copy_for_cluster = muadbam_relation_copy_for_cluster,
	.relation_vacuum = muadbam_relation_vacuum,
	.scan_analyze_next_block = muadbam_scan_analyze_next_block,
	.scan_analyze_next_tuple = muadbam_scan_analyze_next_tuple,
	.index_build_range_scan = muadbam_index_build_range_scan,
	.index_validate_scan = muadbam_index_validate_scan,

	.relation_size = muadbam_relation_size,
	.relation_needs_toast_table = muadbam_relation_needs_toast_table,
	.relation_toast_am = muadbam_relation_toast_am,
	.relation_fetch_toast_slice = muadbam_relation_fetch_toast_slice,

	.relation_estimate_size = muadbam_estimate_rel_size,

	.scan_bitmap_next_block = muadbam_scan_bitmap_next_block,
	.scan_bitmap_next_tuple = muadbam_scan_bitmap_next_tuple,
	.scan_sample_next_block = muadbam_scan_sample_next_block,
	.scan_sample_next_tuple = muadbam_scan_sample_next_tuple
};

/* ----------------------------------------------------------------
 * MuaDB handler function
 * ---------------------------------------------------------------- */

Datum
muadb_tableam_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&muadbam_methods);
} 
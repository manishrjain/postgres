#include "postgres.h"
#include "muadb_storage.h"
#include "utils/elog.h"
#include "storage/itemptr.h"

/*
 * MuaDB storage integration functions
 */

void
muadb_store_tuple(Relation relation, HeapTuple tup)
{
	elog(LOG, "MuaDB: Storing tuple for relation %s (OID: %u) - "
		 "tuple_len=%u, self=(%u,%u), tableOid=%u, data_ptr=%p",
		 RelationGetRelationName(relation), RelationGetRelid(relation),
		 tup->t_len,
		 ItemPointerGetBlockNumber(&tup->t_self),
		 ItemPointerGetOffsetNumber(&tup->t_self),
		 tup->t_tableOid,
		 tup->t_data);
}

void
muadb_remove_tuple(Relation relation, ItemPointer tid)
{
	elog(LOG, "MuaDB: Removing tuple for relation %s (OID: %u) - "
		 "tid=(%u,%u)",
		 RelationGetRelationName(relation), RelationGetRelid(relation),
		 ItemPointerGetBlockNumber(tid),
		 ItemPointerGetOffsetNumber(tid));
}

void
muadb_get_tuple(Relation relation, ItemPointer tid, HeapTuple tuple)
{
	elog(LOG, "MuaDB: Getting tuple for relation %s (OID: %u) - "
		 "tid=(%u,%u)",
		 RelationGetRelationName(relation), RelationGetRelid(relation),
		 ItemPointerGetBlockNumber(tid),
		 ItemPointerGetOffsetNumber(tid));
}
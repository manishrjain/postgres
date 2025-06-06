#include "postgres.h"
#include "muadb_storage.h"
#include "utils/elog.h"

/*
 * MuaDB storage integration functions
 */

void
muadb_store_tuple(Relation relation, HeapTuple tup)
{
	elog(LOG, "MuaDB: Storing tuple for relation %s (OID: %u)",
		 RelationGetRelationName(relation), RelationGetRelid(relation));
}

void
muadb_remove_tuple(Relation relation, ItemPointer tid)
{
	elog(LOG, "MuaDB: Removing tuple for relation %s (OID: %u)",
		 RelationGetRelationName(relation), RelationGetRelid(relation));
}

void
muadb_get_tuple(Relation relation, ItemPointer tid, HeapTuple tuple)
{
	elog(LOG, "MuaDB: Getting tuple for relation %s (OID: %u)",
		 RelationGetRelationName(relation), RelationGetRelid(relation));
} 
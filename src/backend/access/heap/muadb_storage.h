#ifndef MUADB_STORAGE_H
#define MUADB_STORAGE_H

#include "postgres.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "storage/buf.h"
#include "utils/rel.h"

/* MuaDB storage functions */
extern void muadb_store_tuple(Relation relation, HeapTuple tup);
extern void muadb_remove_tuple(Relation relation, ItemPointer tid);
extern void muadb_get_tuple(Relation relation, ItemPointer tid, HeapTuple tuple);

#endif /* MUADB_STORAGE_H */ 
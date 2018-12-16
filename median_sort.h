#include <lib/ilist.h>
#include <lib/pairingheap.h>
#include <catalog/pg_type.h>
#include <utils/builtins.h>

typedef struct MedianSort
{
	Oid			datum_type;
	MemoryContext mem_context;
	pairingheap min_heap;
	pairingheap max_heap;
	uint64		max_heap_size;
	uint64		min_heap_size;
	dlist_head	items;
}			MedianSort;

typedef struct Item
{
	Datum		datum;
	pairingheap_node heap_node;
	pairingheap *own_heap;
	dlist_node	list_node;
} Item;

extern MedianSort * median_sort_init(Oid datum_type, Oid sort_collation, MemoryContext mem_context);
extern Item *median_sort_add_datum(MedianSort * median_sort, Datum datum);
extern void median_sort_remove_datum(MedianSort * median_sort, Datum datum);
extern Datum median_sort_median(MedianSort * median_sort, bool *isnull);
extern Datum get_mean_of_two(Oid arg_type, Datum val1, Datum val2, bool *isnull);
extern void median_sort_rebalance(MedianSort * median_sort);

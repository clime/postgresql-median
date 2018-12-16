#include <lib/ilist.h>
#include <lib/pairingheap.h>
#include <catalog/pg_type.h>
#include <utils/builtins.h>

/*
 * 1. Take two Heap(1 Min Heap and 1 Max Heap)
 *    Max Heap will contain first half number of elements
 *    Min Heap will contain second half number of elements
 * 
 * 2. Compare new number from stream with top of Max Heap, 
 *    if it is smaller or equal add that number in max heap. 
 *    Otherwise add number in Min Heap.
 * 
 * 3. if min Heap has more elements than Max Heap 
 *    then remove top element of Min Heap and add in Max Heap.
 *    if max Heap has more than one element than in Min Heap 
 *    then remove top element of Max Heap and add in Min Heap.
 * 
 * 4. If Both heaps has equal number of elements then
 *    median will be half of sum of max element from Max Heap and min element from Min Heap.
 *    Otherwise median will be max element from the first partition.
 */

typedef struct MedianSort
{
    Oid           datum_type;
    MemoryContext mem_context;
    pairingheap   min_heap;
    pairingheap   max_heap;
    uint64        max_heap_size;
    uint64        min_heap_size;
    dlist_head    items;
    FunctionCallInfo fcinfo;
} MedianSort;

typedef struct Item
{
    Datum datum;
    pairingheap_node heap_node;
    dlist_node       list_node;
    FunctionCallInfo fcinfo;
    Oid           datum_type;
} Item;

extern MedianSort *median_sort_init(FunctionCallInfo fcinfo, Oid datum_type, Oid sort_collation, MemoryContext mem_context);
extern Item *median_sort_add_datum(MedianSort* median_sort, Datum datum);
extern void median_sort_remove_datum(MedianSort* median_sort, Datum datum);
extern Datum median_sort_median(MedianSort* median_sort, bool *isnull);

Datum get_mean_of_two(Oid arg_type, Datum val1, Datum val2, bool *isnull);
void median_sort_rebalance(MedianSort *median_sort);

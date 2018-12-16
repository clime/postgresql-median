#include <postgres.h>
#include <assert.h>
#include <lib/ilist.h>
#include <lib/pairingheap.h>
#include <utils/sortsupport.h>
#include <parser/parse_oper.h>
#include <catalog/pg_type.h>
#include <utils/builtins.h>
#include <utils/numeric.h>
#include <utils/timestamp.h>
#include <utils/cash.h>

#include "median_sort.h"

#define INT_MEAN(i1, i2) (i1/2 + i2/2 + (i1%2 + i2%2)/2)
#define FLOAT_MEAN(f1, f2) (f1/2 + f2/2)

/*
 * Agorithm used to determine the median
 * -------------------------------------
 *
 * 1) Take two Heap(1 Min Heap and 1 Max Heap)
 *    Max Heap will contain first half number of elements
 *    Min Heap will contain second half number of elements
 *
 * 2) Compare new number from stream with top of Max Heap,
 *    if it is smaller or equal add that number in max heap.
 *    Otherwise add number in Min Heap.
 *
 * 3) if min Heap has more elements than Max Heap
 *    then remove top element of Min Heap and add in Max Heap.
 *    if max Heap has more than one element than in Min Heap
 *    then remove top element of Max Heap and add in Min Heap.
 *
 * 4) If Both heaps has equal number of elements then
 *    median will be half of sum of max element from Max Heap and min element from Min Heap.
 *    Otherwise median will be max element from the first partition.
 */

static int heap_pairingheap_comparator(const pairingheap_node *a, const pairingheap_node *b, void *arg);

MedianSort *
median_sort_init(Oid datum_type, Oid sort_collation, MemoryContext mem_context)
{
    MedianSort *median_sort;
	Oid         lt_op, gt_op;
	bool		is_hashable;
	MemoryContext prev_context;
    SortSupport min_heap_ssup, max_heap_ssup;

	prev_context = MemoryContextSwitchTo(mem_context);

	get_sort_group_operators(datum_type, true, false, false, &lt_op, NULL, &gt_op, &is_hashable);

	max_heap_ssup = (SortSupport) palloc0(sizeof(SortSupportData));
    max_heap_ssup->ssup_cxt = CurrentMemoryContext;
    max_heap_ssup->ssup_collation = sort_collation;
    max_heap_ssup->ssup_nulls_first = false;
    max_heap_ssup->abbreviate = false;
	PrepareSortSupportFromOrderingOp(gt_op, max_heap_ssup);

	min_heap_ssup = (SortSupport) palloc0(sizeof(SortSupportData));
    min_heap_ssup->ssup_cxt = CurrentMemoryContext;
    min_heap_ssup->ssup_collation = sort_collation;
    min_heap_ssup->ssup_nulls_first = false;
    min_heap_ssup->abbreviate = false;
	PrepareSortSupportFromOrderingOp(lt_op, min_heap_ssup);

    median_sort = (MedianSort *) palloc(sizeof(MedianSort));
    median_sort->datum_type = datum_type;

    dlist_init(&median_sort->items);

	median_sort->max_heap.ph_compare = heap_pairingheap_comparator;
    median_sort->max_heap.ph_arg = max_heap_ssup;
    median_sort->max_heap.ph_root = NULL;

	median_sort->min_heap.ph_compare = heap_pairingheap_comparator;
    median_sort->min_heap.ph_arg = min_heap_ssup;
    median_sort->min_heap.ph_root = NULL;

    median_sort->max_heap_size = 0;
    median_sort->min_heap_size = 0;

    median_sort->mem_context = mem_context;

	MemoryContextSwitchTo(prev_context);

    return median_sort;
}

void
median_sort_remove_datum(MedianSort *median_sort, Datum datum)
{
}

Item *
median_sort_add_datum(MedianSort *median_sort, Datum datum)
{
    Item *item, *max_heap_first_item;
	MemoryContext prev_context;

	prev_context = MemoryContextSwitchTo(median_sort->mem_context);

    item = (Item *) palloc(sizeof(Item));
    item->datum = datum;

    if (median_sort->max_heap_size != 0)
    {
        int cmp_result;
        
        max_heap_first_item = pairingheap_container(Item, heap_node, pairingheap_first(&median_sort->max_heap));
	    cmp_result = ApplySortComparator(datum, false, max_heap_first_item->datum, false, median_sort->max_heap.ph_arg);

        if (cmp_result <= 0)
        {
            pairingheap_add(&median_sort->max_heap, &item->heap_node);
            median_sort->max_heap_size++;
        }
        else
        {
            pairingheap_add(&median_sort->min_heap, &item->heap_node);
            median_sort->min_heap_size++;
        }
    }
    else
    {
        pairingheap_add(&median_sort->max_heap, &item->heap_node);
        median_sort->max_heap_size++;
    }

    median_sort_rebalance(median_sort);

    dlist_push_head(&median_sort->items, &item->list_node);

	MemoryContextSwitchTo(prev_context);

    return item;
}

Datum
median_sort_median(MedianSort *median_sort, bool *is_null)
{
    Item *item1, *item2;

    assert(median_sort->max_heap_size >= median_sort->min_heap_size);

    if (median_sort->max_heap_size > median_sort->min_heap_size)
    {
        item1 = pairingheap_container(Item, heap_node, pairingheap_first(&median_sort->max_heap));
        *is_null = false;
        return item1->datum;
    }

    if (median_sort->max_heap_size > 0 && median_sort->min_heap_size > 0)
    {
        item1 = pairingheap_container(Item, heap_node, pairingheap_first(&median_sort->max_heap));
        item2 = pairingheap_container(Item, heap_node, pairingheap_first(&median_sort->min_heap));
    }
    else
    {
        *is_null = true;
        return (Datum) 0;
    }

    return get_mean_of_two(median_sort->datum_type, item1->datum, item2->datum, is_null);
}

void
median_sort_rebalance(MedianSort *median_sort)
{
    pairingheap_node *node;

    while (median_sort->min_heap_size > median_sort->max_heap_size)
    {
        node = pairingheap_remove_first(&median_sort->min_heap);
        median_sort->min_heap_size--;

        pairingheap_add(&median_sort->max_heap, node);
        median_sort->max_heap_size++;
    }

    while (median_sort->max_heap_size > median_sort->min_heap_size + 1)
    {
        node = pairingheap_remove_first(&median_sort->max_heap);
        median_sort->max_heap_size--;

        pairingheap_add(&median_sort->min_heap, node);
        median_sort->min_heap_size++;
    }

}

int
heap_pairingheap_comparator (
        const pairingheap_node *a,
        const pairingheap_node *b,
        void *arg)
{
	SortSupport ssup = (SortSupport) arg;
    Item *item1, *item2;

    item1 = pairingheap_container(Item, heap_node, (pairingheap_node*) a);
    item2 = pairingheap_container(Item, heap_node, (pairingheap_node*) b);

	return ApplySortComparator(item1->datum, false, item2->datum, false, ssup);
}

Datum
get_mean_of_two(Oid arg_type, Datum val1, Datum val2, bool *is_null)
{
    *is_null = false;

	switch (arg_type)
	{
		case INT2OID:
			{
				int16		i1,
							i2,
							iretval;

				i1 = DatumGetInt16(val1);
				i2 = DatumGetInt16(val2);
				iretval = INT_MEAN(i1, i2);
				PG_RETURN_INT16(iretval);
			}
		case INT4OID:
			{
				int32		i1,
							i2,
							iretval;

				i1 = DatumGetInt32(val1);
				i2 = DatumGetInt32(val2);
				iretval = INT_MEAN(i1, i2);
				PG_RETURN_INT32(iretval);
			}
		case INT8OID:
			{
				int64		i1,
							i2,
							iretval;

				i1 = DatumGetInt64(val1);
				i2 = DatumGetInt64(val2);
				iretval = INT_MEAN(i1, i2);
				PG_RETURN_INT64(iretval);
			}
		case FLOAT4OID:
			{
				float4		f1,
							f2,
							fretval;

				f1 = DatumGetFloat4(val1);
				f2 = DatumGetFloat4(val2);
				fretval = FLOAT_MEAN(f1, f2);
				PG_RETURN_FLOAT4(fretval);
			}
		case FLOAT8OID:
			{
				float8		f1,
							f2,
							fretval;

				f1 = DatumGetFloat8(val1);
				f2 = DatumGetFloat8(val2);
				fretval = FLOAT_MEAN(f1, f2);
				PG_RETURN_FLOAT8(fretval);
			}
		case NUMERICOID:
			{
				Datum		div1,
							div2,
							retval,
							divisor;

				divisor = DirectFunctionCall1(int4_numeric, 2);
				div1 = DirectFunctionCall2(numeric_div, val1, divisor);
				div2 = DirectFunctionCall2(numeric_div, val2, divisor);
				retval = DirectFunctionCall2(numeric_add, div1, div2);
				PG_RETURN_DATUM(retval);
			}
		case CASHOID:
			{
				Datum		div1,
							div2,
							retval;

				div1 = DirectFunctionCall2(cash_div_int4, val1, 2);
				div2 = DirectFunctionCall2(cash_div_int4, val2, 2);
				retval = DirectFunctionCall2(cash_pl, div1, div2);
				PG_RETURN_DATUM(retval);
			}
		case INTERVALOID:
			{
				Datum		div1,
							div2,
							retval,
							divisor;

				divisor = Float8GetDatum((double) 2);
				div1 = DirectFunctionCall2(interval_div, val1, divisor);
				div2 = DirectFunctionCall2(interval_div, val2, divisor);
				retval = DirectFunctionCall2(interval_pl, div1, div2);
				PG_RETURN_DATUM(retval);
			}
		case TIMESTAMPOID:
			{
				Datum		diff,
							halfdiff,
							retval;

				diff = DirectFunctionCall2(timestamp_mi, val2, val1);
				halfdiff = DirectFunctionCall2(interval_div, diff, Float8GetDatum((double) 2));
				retval = DirectFunctionCall2(timestamp_pl_interval, val1, halfdiff);
				PG_RETURN_DATUM(retval);
			}
		case TIMESTAMPTZOID:
			{
				Datum		diff,
							halfdiff,
							retval;

				diff = DirectFunctionCall2(timestamp_mi, val2, val1);
				halfdiff = DirectFunctionCall2(interval_div, diff, Float8GetDatum((double) 2));
				retval = DirectFunctionCall2(timestamptz_pl_interval, val1, halfdiff);
				PG_RETURN_DATUM(retval);
			}
		default:
			{
				*is_null = true;
				return (Datum) 0;
			}
	}
	return (Datum) 0; // should never be reached
}

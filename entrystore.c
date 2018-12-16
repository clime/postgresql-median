
typedef struct EntryStore
{
    pairingheap max_heap;
    pairingheap min_heap;
    dlist_head  entries;
} EntryStore;

typedef struct Entry
{
    Datum datum;
    pairingheap_node max_heap_node;
    pairingheap_node min_heap_node;
    dlist_node       list_node;
} Entry;

static EntryStore *
entrystore_init(FunctionCallInfo fcinfo, MemoryContext mem_context)
{
    State *state;
	Oid			arg_type,
				lt_op, gt_op, eq_op;
	bool		is_hashable;
	MemoryContext prev_context;

	arg_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
	get_sort_group_operators(arg_type, true, true, true, &lt_op, &eq_op, &gt_op, &is_hashable);

	prev_context = MemoryContextSwitchTo(mem_context);

	MemoryContextSwitchTo(prev_context);
    return state;
}

void
entry_add_datum(Datum datum, State state)
{
    
}


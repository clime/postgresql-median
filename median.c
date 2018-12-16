#include <postgres.h>

#include "median_sort.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(median_transfn);
PG_FUNCTION_INFO_V1(median_finalfn);

/*
 * Median state transfer function.
 *
 * This function is called for every value in the set that we are calculating
 * the median for. On first call, the aggregate state, if any, needs to be
 * initialized.
 */
Datum
median_transfn(PG_FUNCTION_ARGS)
{
	MemoryContext agg_context;
    MedianSort *median_sort;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "median_transfn called in non-aggregate context");

    if (PG_ARGISNULL(0))
		median_sort = median_sort_init(
                get_fn_expr_argtype(fcinfo->flinfo, 1), 
                fcinfo->fncollation, agg_context);
	else
		median_sort = (MedianSort *) PG_GETARG_POINTER(0);

    if (!PG_ARGISNULL(1))
        median_sort_add_datum(median_sort, PG_GETARG_DATUM(1));

    PG_RETURN_POINTER(median_sort);
}

/*
 * Median final function.
 *
 * This function is called after all values in the median set has been
 * processed by the state transfer function. It should perform any necessary
 * post processing and clean up any temporary state.
 */
Datum
median_finalfn(PG_FUNCTION_ARGS)
{
	MemoryContext agg_context;
    MedianSort *median_sort;
    Datum retval;
    bool is_null;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "median_finalfn called in non-aggregate context");

	median_sort = (MedianSort *) PG_GETARG_POINTER(0);
    if (!median_sort)
        PG_RETURN_NULL();

    retval = median_sort_median(median_sort, &is_null);

    if (is_null)
        PG_RETURN_NULL();

    PG_RETURN_DATUM(retval);
}

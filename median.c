#include <postgres.h>
#include <fmgr.h>
#include <miscadmin.h>
#include <parser/parse_oper.h>
#include <utils/builtins.h>
#include <utils/tuplesort.h>
#include <utils/lsyscache.h>
#include <utils/array.h>
#include <utils/numeric.h>
#include <utils/timestamp.h>
#include <utils/cash.h>
#include <utils/memutils.h>
#include <catalog/pg_type.h>

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#if (PG_VERSION_NUM >= 110000)
#define PgAllocSetContextCreate AllocSetContextCreateExtended
#else
#define PgAllocSetContextCreate AllocSetContextCreate
#endif

#define INT_MEAN(i1, i2) (i1/2 + i2/2 + (i1%2 + i2%2)/2)
#define FLOAT_MEAN(f1, f2) (f1/2 + f2/2)

typedef struct State
{
	Tuplesortstate *tuplesort;
	int64		elem_cnt;
	MemoryContext mem_context;
	Oid			arg_type;
	bool		typ_by_val;
	bool		sort_done;
}			State;

PG_FUNCTION_INFO_V1(median_transfn);
PG_FUNCTION_INFO_V1(median_finalfn);

static Datum get_mean_of_two(Oid arg_type, Datum val1, Datum val2, bool *isnull);
static State *median_setup(FunctionCallInfo fcinfo, MemoryContext mem_context);
static void cleanup_state(void *arg);

static void
cleanup_state(void *arg)
{
	State	   *state = (State *) arg;

	tuplesort_end(state->tuplesort);
}

static State *
median_setup(FunctionCallInfo fcinfo, MemoryContext mem_context)
{
	State	   *state;
	Oid			arg_type,
				lt_op;
	bool		is_hashable;
	MemoryContext prev_context,
				callback_context;
	MemoryContextCallback *callback;

	arg_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
	get_sort_group_operators(arg_type, true, false, false, &lt_op, NULL, NULL, &is_hashable);

	prev_context = MemoryContextSwitchTo(mem_context);

	state = (State *) palloc(sizeof(State));

#if (PG_VERSION_NUM >= 110000)
	/* added SortCoordinate to tuplesort_begin_datum method */
	state->tuplesort = tuplesort_begin_datum(arg_type, lt_op, fcinfo->fncollation, false, work_mem, NULL, true);
#else
	state->tuplesort = tuplesort_begin_datum(arg_type, lt_op, fcinfo->fncollation, false, work_mem, true);
#endif

	state->elem_cnt = 0;
	state->arg_type = arg_type;
	state->typ_by_val = get_typbyval(arg_type);
	state->mem_context = mem_context;
	state->sort_done = false;

	/*
	 * register memory context callback to invoke tuplesort_end on tuplesort
	 * to clean up files created on disk
	 */
	callback_context = PgAllocSetContextCreate(mem_context, "Callback context", ALLOCSET_SMALL_SIZES);
	callback = MemoryContextAlloc(callback_context, sizeof(MemoryContextCallback));
	callback->arg = (void *) state;
	callback->func = cleanup_state;
	MemoryContextRegisterResetCallback(callback_context, callback);

	MemoryContextSwitchTo(prev_context);
	return state;
}

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
	State	   *state;
	MemoryContext agg_context;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "median_transfn called in non-aggregate context");

	if (PG_ARGISNULL(0))
		state = median_setup(fcinfo, agg_context);
	else
		state = (State *) PG_GETARG_POINTER(0);

	/* Load the datum into the tuplesort object, but only if it's not null */
	if (!PG_ARGISNULL(1))
	{
		tuplesort_putdatum(state->tuplesort, PG_GETARG_DATUM(1), false);
		state->elem_cnt++;
	}

	PG_RETURN_POINTER(state);
}

Datum
get_mean_of_two(Oid arg_type, Datum val1, Datum val2, bool *isnull)
{
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
				*isnull = true;
				return (Datum) 0;
			}
	}
	return (Datum) 0;
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
	State	   *state;
	MemoryContext agg_context;
	Datum		retval;
	bool		isnull;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "median_finalfn called in non-aggregate context");

	state = (State *) PG_GETARG_POINTER(0);

	if (!state || state->elem_cnt == 0)
		PG_RETURN_NULL();

	if (!state->sort_done)
	{
		tuplesort_performsort(state->tuplesort);
		state->sort_done = true;
	}
	else
		tuplesort_rescan(state->tuplesort);

	if (state->elem_cnt % 2 == 0)
	{
		Datum		val1,
					val2;

		if (!tuplesort_skiptuples(state->tuplesort, state->elem_cnt / 2 - 1, true))
			elog(ERROR, "missing row in median");

		if (!tuplesort_getdatum(state->tuplesort, true, &val1, &isnull, NULL))
			elog(ERROR, "missing row in median");
		if (isnull)
			PG_RETURN_NULL();

		if (!tuplesort_getdatum(state->tuplesort, true, &val2, &isnull, NULL))
			elog(ERROR, "missing row in median");
		if (isnull)
			PG_RETURN_NULL();

		retval = get_mean_of_two(state->arg_type, val1, val2, &isnull);
		if (isnull)
			PG_RETURN_NULL();

		if (!state->typ_by_val)
		{
			pfree(DatumGetPointer(val1));
			pfree(DatumGetPointer(val2));
		}
	}
	else
	{
		if (!tuplesort_skiptuples(state->tuplesort, state->elem_cnt / 2, true))
			elog(ERROR, "missing row in median");

		if (!tuplesort_getdatum(state->tuplesort, true, &retval, &isnull, NULL))
			elog(ERROR, "missing row in median");
		if (isnull)
			PG_RETURN_NULL();
	}

	PG_RETURN_DATUM(retval);
}

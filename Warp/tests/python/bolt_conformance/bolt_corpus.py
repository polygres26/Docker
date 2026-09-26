"""Differential corpus for boltwire (Warp's Neo4j / Bolt frontend).

Every case is a short sequence of Cypher statements (plus a few driver / protocol scenarios) that bolt_harness.py runs
through the official `neo4j` Python driver against a REAL Neo4j and against Warp; results (columns, values, update
counters, error status codes) are normalised and compared. A case starts from an empty graph without schema.

Step kinds (dicts):
  {"q": cypher, "p": {params}}                      auto-commit statement
  {"q": ..., "ordered": True}                       compare row order (also implied by ORDER BY in the text)
  {"tx": [step, ...], "end": "commit"|"rollback"}   explicit transaction (each inner step is a {"q", "p"})
  {"fetch": n, "q": ...}                            iterate the result with fetch_size n
  {"stream": [q1, q2]}                              two results open at once inside one explicit transaction
  {"meta": "..."}                                   driver / protocol facts (see bolt_harness.run_meta)
"""

SETUP_SOCIAL = (
    "CREATE (alice:Person:Employee {name: 'Alice', age: 34, city: 'Oslo', tags: ['a', 'b'], score: 1.5}), "
    "(bob:Person {name: 'Bob', age: 27, city: 'Bergen', tags: ['b'], score: 2.5}), "
    "(carol:Person:Manager {name: 'Carol', age: 45, city: 'Oslo', tags: [], score: 3.0}), "
    "(dave:Person {name: 'Dave', age: 27, city: 'Trondheim', score: 0.5}), "
    "(erin:Person {name: 'Erin', city: 'Oslo'}), "
    "(acme:Company {name: 'Acme', founded: 1999}), (globex:Company {name: 'Globex', founded: 2005}), "
    "(alice)-[:KNOWS {since: 2010}]->(bob), (bob)-[:KNOWS {since: 2015}]->(carol), (carol)-[:KNOWS {since: 2012}]->(dave), "
    "(alice)-[:KNOWS {since: 2020}]->(carol), (dave)-[:KNOWS {since: 2001}]->(alice), "
    "(alice)-[:WORKS_AT {role: 'dev'}]->(acme), (bob)-[:WORKS_AT {role: 'ops'}]->(acme), "
    "(carol)-[:WORKS_AT {role: 'boss'}]->(globex), (carol)-[:MANAGES]->(alice), (carol)-[:MANAGES]->(bob), "
    "(erin)-[:LIKES]->(erin)"
)

CASES = []


def case(name, steps, known=None):
    if isinstance(steps, (str, dict)):
        steps = [steps]
    CASES.append({"name": name, "steps": [({"q": s} if isinstance(s, str) else s) for s in steps], **({"known": known} if known else {})})


def probe(name, expr, known=None, p=None):
    case("expr/" + name, {"q": "RETURN %s AS v" % expr, **({"p": p} if p else {})}, known)


def on_social(name, queries, known=None):
    steps = [SETUP_SOCIAL]
    for q in queries if isinstance(queries, list) else [queries]:
        steps.append(q)
    case("g/" + name, steps, known)


# ==================================================================================================================
# A. expressions (each one statement, on an empty graph)
# ==================================================================================================================

ARITH = {
    "add_int": "1 + 2", "add_float": "1.5 + 2", "add_str": "'a' + 'b'", "add_str_int": "'a' + 1", "add_int_str": "1 + 'a'",
    "add_str_float": "'a' + 1.5", "add_list_list": "[1] + [2, 3]", "add_list_elem": "[1] + 2", "add_elem_list": "0 + [1]",
    "add_null": "1 + null", "add_null_list": "[1] + null", "sub": "5 - 7", "mul": "6 * 7", "mul_float": "2 * 1.5", "div_int": "7 / 2",
    "div_int_neg": "-7 / 2", "div_float": "7 / 2.0", "div_zero_int": "1 / 0", "div_zero_float": "1 / 0.0", "div_zero_neg": "-1 / 0.0",
    "zero_div_zero": "0.0 / 0.0", "mod_int": "7 % 3", "mod_neg": "-7 % 3", "mod_float": "7.5 % 2", "mod_zero": "5 % 0", "pow": "2 ^ 10",
    "pow_neg": "2 ^ -1", "pow_frac": "9 ^ 0.5", "unary_minus": "-(-3)", "unary_plus": "+3", "prec_1": "2 + 3 * 4", "prec_2": "(2 + 3) * 4",
    "prec_3": "-2 ^ 2", "prec_4": "2 ^ 3 ^ 2", "int_max": "9223372036854775807", "int_min": "-9223372036854775808",
    "overflow_add": "9223372036854775807 + 1", "overflow_mul": "9223372036854775807 * 2", "overflow_sub": "-9223372036854775808 - 1",
    "abs_min": "abs(-9223372036854775808)", "hex": "0x1F", "octal": "0o17", "sci": "1e3", "sci_neg": "1.5e-3", "float_lit": ".5",
    "int_float_eq": "1 = 1.0", "big_float": "1e300 * 1e300", "neg_zero": "-0.0", "int_div_neg_zero": "0 / -1",
    "bool_add": "true + 1", "list_mul": "[1] * 2", "str_mul": "'a' * 2", "str_minus": "'a' - 'b'", "map_add": "{a: 1} + {b: 2}",
}
for k, v in ARITH.items():
    probe("arith/" + k, v)

COMPARE = {
    "eq_str": "'a' = 'a'", "eq_null": "null = null", "eq_null_1": "1 = null", "neq_null": "1 <> null", "lt_int": "1 < 2",
    "lt_mixed": "1 < 2.5", "lt_str": "'a' < 'b'", "lt_str_num": "'a' < 1", "lt_bool": "false < true", "lt_null": "null < 1",
    "lt_list": "[1, 2] < [1, 3]", "lt_list_prefix": "[1] < [1, 2]", "lt_list_null": "[1, null] < [1, 2]",
    "eq_list": "[1, 2] = [1, 2]", "eq_list_null": "[1, null] = [1, null]", "eq_list_len": "[1] = [1, 2]",
    "eq_map": "{a: 1} = {a: 1}", "eq_map_null": "{a: null} = {a: null}", "neq_map": "{a: 1} <> {a: 2}",
    "nan_eq": "0.0/0.0 = 0.0/0.0", "nan_lt": "0.0/0.0 < 1", "nan_neq": "0.0/0.0 <> 0.0/0.0", "inf_gt": "1/0.0 > 1e308",
    "chain": "1 < 2 < 3", "chain_false": "1 < 3 < 2", "chain_eq": "1 = 1 = true", "int_float_lt": "9007199254740993 < 9007199254740992.0",
    "big_eq": "9007199254740993 = 9007199254740992.0", "str_eq_case": "'a' = 'A'", "eq_types": "1 = '1'", "bool_eq": "true = true",
    "in_true": "2 IN [1, 2, 3]", "in_false": "4 IN [1, 2, 3]", "in_null_elem": "4 IN [1, null]", "in_null_hit": "1 IN [1, null]",
    "in_null_needle": "null IN [1]", "in_empty_null": "null IN []", "in_list_of_lists": "[1] IN [[1], [2]]", "in_non_list": "1 IN 1",
    "in_null_list": "1 IN null", "starts": "'abc' STARTS WITH 'ab'", "starts_null": "'abc' STARTS WITH null", "starts_num": "1 STARTS WITH 'a'",
    "ends": "'abc' ENDS WITH 'bc'", "contains": "'abc' CONTAINS 'b'", "contains_empty": "'abc' CONTAINS ''", "regex": "'abc' =~ 'a.c'",
    "regex_partial": "'abc' =~ 'b'", "regex_null": "'abc' =~ null", "regex_case": "'ABC' =~ '(?i)abc'", "regex_bad": "'a' =~ '['",
    "isnull": "null IS NULL", "isnotnull": "1 IS NOT NULL", "not_null": "NOT null", "and_null_false": "null AND false",
    "and_null_true": "null AND true", "or_null_true": "null OR true", "or_null_false": "null OR false", "xor": "true XOR false",
    "xor_null": "true XOR null", "not_not": "NOT NOT true", "and_type": "1 AND true", "not_type": "NOT 1", "or_type": "'a' OR true",
    "prec_and_or": "true OR false AND false", "prec_not": "NOT true AND false", "case_simple": "CASE 2 WHEN 1 THEN 'a' WHEN 2 THEN 'b' ELSE 'c' END",
    "case_generic": "CASE WHEN 1 > 2 THEN 'x' WHEN 2 > 1 THEN 'y' END", "case_none": "CASE WHEN false THEN 1 END",
    "case_null_subject": "CASE null WHEN null THEN 'n' ELSE 'e' END", "case_nested": "CASE WHEN true THEN CASE WHEN false THEN 1 ELSE 2 END END",
    "coalesce": "coalesce(null, null, 3, 4)", "coalesce_all_null": "coalesce(null, null)", "nullif": "nullIf(1, 1)", "nullif_ne": "nullIf(1, 2)",
}
for k, v in COMPARE.items():
    probe("cmp/" + k, v)

STRINGS = {
    "toLower": "toLower('HeLLo')", "toUpper": "toUpper('HeLLo')", "lower_null": "toLower(null)", "trim": "trim('  a b  ')",
    "ltrim": "lTrim('  a ')", "rtrim": "rTrim(' a  ')", "left": "left('hello', 2)", "right": "right('hello', 2)", "left_over": "left('hi', 5)",
    "left_neg": "left('hi', -1)", "replace": "replace('hello', 'l', 'L')", "replace_empty": "replace('abc', '', '-')", "reverse": "reverse('abc')",
    "split": "split('a,b,,c', ',')", "split_empty": "split('abc', '')", "split_none": "split('abc', 'x')", "substring2": "substring('hello', 1, 3)",
    "substring1": "substring('hello', 2)", "substring_over": "substring('hi', 5)", "substring_neg": "substring('hi', -1)",
    "size": "size('héllo')", "size_emoji": "size('a😀b')", "char_length": "char_length('héllo')", "toString_int": "toString(42)",
    "toString_float": "toString(1.5)", "toString_float_int": "toString(3.0)", "toString_bool": "toString(true)", "toString_str": "toString('x')",
    "toString_list": "toString([1])", "toString_null": "toString(null)", "toString_big": "toString(1e21)", "toString_small": "toString(1e-7)",
    "toStringOrNull_list": "toStringOrNull([1])", "toInteger_str": "toInteger('42')", "toInteger_float_str": "toInteger('4.7')",
    "toInteger_bad": "toInteger('x')", "toInteger_float": "toInteger(-4.7)", "toInteger_bool": "toInteger(true)", "toInteger_list": "toInteger([1])",
    "toIntegerOrNull_list": "toIntegerOrNull([1])", "toFloat_str": "toFloat('1.5')", "toFloat_int": "toFloat(2)", "toFloat_bad": "toFloat('abc')",
    "toFloat_e": "toFloat('1e3')", "toBoolean_str": "toBoolean('TRUE')", "toBoolean_bad": "toBoolean('yes')", "toBoolean_int": "toBoolean(0)",
    "toBooleanOrNull_list": "toBooleanOrNull([1])", "unicode_lit": "'\\u00e9\\u4e2d'", "escapes": "'a\\tb\\nc\\\\d\\'e'", "quote_dq": '"it\'s"',
    "concat_many": "'a' + 'b' + 'c' + 1 + 2", "str_index": "'abc'[0]", "str_slice": "'abc'[0..2]", "lpad_like": "reduce(s = '', x IN range(1,3) | s + toString(x))",
    "normalize": "normalize('e\\u0301')", "btrim_like": "trim('  ')", "isEmpty_str": "isEmpty('')", "isEmpty_str2": "isEmpty('a')",
    "toUpper_int": "toUpper(1)", "trim_null": "trim(null)", "replace_null": "replace(null, 'a', 'b')", "left_null_len": "left('abc', null)",
}
for k, v in STRINGS.items():
    probe("str/" + k, v)

MATH = {
    "abs_int": "abs(-3)", "abs_float": "abs(-3.5)", "ceil": "ceil(1.2)", "ceil_neg": "ceil(-1.2)", "ceil_int": "ceil(2)", "floor": "floor(1.8)",
    "floor_neg": "floor(-1.2)", "round": "round(2.5)", "round_neg": "round(-2.5)", "round_half": "round(3.5)", "round_prec": "round(3.14159, 2)",
    "round_mode": "round(3.14159, 3, 'DOWN')", "round_int": "round(5)", "sign": "sign(-3)", "sign_float": "sign(2.5)", "sign_zero": "sign(0)",
    "sqrt": "sqrt(16)", "sqrt_neg": "sqrt(-1)", "exp": "exp(0)", "log": "log(e())", "log_zero": "log(0)", "log10": "log10(1000)", "sin": "sin(0)",
    "cos": "cos(0)", "tan": "tan(0)", "asin": "asin(2)", "atan2": "atan2(1, 1)", "degrees": "degrees(pi())", "radians": "radians(180)",
    "haversin": "haversin(0)", "pi": "pi()", "e": "e()", "isNaN": "isNaN(0.0/0.0)", "isNaN_num": "isNaN(1)", "rand_range": "rand() >= 0 AND rand() < 1",
    "abs_str": "abs('a')", "sqrt_str": "sqrt('a')", "abs_null": "abs(null)", "cot": "cot(1)", "range_asc": "range(1, 5)", "range_step": "range(0, 10, 3)",
    "range_desc": "range(5, 1, -2)", "range_empty": "range(5, 1)", "range_zero_step": "range(1, 5, 0)", "range_one": "range(3, 3)",
    "toInteger_big": "toInteger(1e19)", "int_sum_float": "sum([1, 2.5])", "randomUUID_len": "size(randomUUID())", "timestamp_pos": "timestamp() > 0",
}
for k, v in MATH.items():
    probe("math/" + k, v)

LISTS = {
    "head": "head([1, 2, 3])", "head_empty": "head([])", "last": "last([1, 2, 3])", "tail": "tail([1, 2, 3])", "tail_empty": "tail([])",
    "size": "size([1, 2, 3])", "size_null": "size(null)", "size_map": "size({a: 1})", "reverse": "reverse([1, 2, 3])", "index": "[1, 2, 3][1]",
    "index_neg": "[1, 2, 3][-1]", "index_oob": "[1, 2, 3][5]", "index_null": "[1, 2, 3][null]", "index_str": "[1, 2, 3]['a']", "slice": "[1, 2, 3, 4][1..3]",
    "slice_open_l": "[1, 2, 3, 4][..2]", "slice_open_r": "[1, 2, 3, 4][2..]", "slice_neg": "[1, 2, 3, 4][-2..]", "slice_oob": "[1, 2][5..9]",
    "slice_null": "[1, 2, 3][null..2]", "comp_map": "[x IN range(1, 5) | x * x]", "comp_filter": "[x IN range(1, 6) WHERE x % 2 = 0]",
    "comp_both": "[x IN range(1, 6) WHERE x % 2 = 0 | x * 10]", "comp_null": "[x IN null | x]", "comp_nested": "[x IN [[1, 2], [3]] | size(x)]",
    "reduce": "reduce(a = 0, x IN [1, 2, 3] | a + x)", "reduce_str": "reduce(s = '', x IN ['a', 'b'] | s + x)", "reduce_null": "reduce(a = 0, x IN null | a + x)",
    "all_true": "all(x IN [1, 2, 3] WHERE x > 0)", "all_empty": "all(x IN [] WHERE x > 0)", "all_null": "all(x IN [1, null] WHERE x > 0)",
    "all_false_null": "all(x IN [0, null] WHERE x > 0)", "any_true": "any(x IN [1, 2] WHERE x > 1)", "any_empty": "any(x IN [] WHERE x > 0)",
    "any_null": "any(x IN [0, null] WHERE x > 0)", "none": "none(x IN [1, 2] WHERE x > 5)", "none_null": "none(x IN [1, null] WHERE x > 5)",
    "single_one": "single(x IN [1, 2, 3] WHERE x = 2)", "single_two": "single(x IN [1, 2, 2] WHERE x = 2)", "single_null": "single(x IN [1, null] WHERE x = 1)",
    "in_list_comp": "size([x IN range(1,10) WHERE x > 3])", "keys_list": "keys({a: 1, b: null})", "concat_nested": "[[1], [2]] + [[3]]",
    "distinct_list": "[x IN [1, 1, 2] | x]", "range_big": "size(range(1, 1000))", "list_eq": "[1, 2, 3] = [1, 2, 3]", "empty_list": "[]",
    "list_contains": "[1, 2] + [3] = [1, 2, 3]", "list_of_null": "[null]", "nested_map_list": "[{a: [1, {b: 2}]}]", "isEmpty_list": "isEmpty([])",
    "isEmpty_map": "isEmpty({})", "isEmpty_num": "isEmpty(1)", "head_str": "head('abc')", "size_num": "size(1)", "list_plus_null_first": "null + [1]",
    "tail_null": "tail(null)", "reverse_null": "reverse(null)", "list_index_float": "[1, 2][1.0]", "list_of_mixed": "[1, 'a', true, null, 1.5, [1], {a: 1}]",
    "collect_like": "[x IN [3, 1, 2] | x]", "unwind_sum": "reduce(t = 0, x IN range(1, 100) | t + x)",
}
for k, v in LISTS.items():
    probe("list/" + k, v)

MAPS = {
    "lit": "{a: 1, b: 'x'}", "empty": "{}", "nested": "{a: {b: {c: 1}}}", "access": "{a: 1}.a", "access_missing": "{a: 1}.b", "access_null": "null.a",
    "index": "{a: 1}['a']", "index_missing": "{a: 1}['zz']", "index_int": "{a: 1}[0]", "keys": "keys({a: 1, b: 2})", "keys_null_val": "keys({a: null})",
    "nested_access": "{a: {b: 2}}.a.b", "access_on_int": "1.a", "map_proj": "{a: 1, b: 2}{.a}", "map_in_list": "[{a: 1}, {a: 2}][1].a",
    "map_eq_order": "{a: 1, b: 2} = {b: 2, a: 1}", "map_dup_key": "{a: 1, a: 2}", "map_null_val": "{a: null}", "backtick_key": "{`a b`: 1}",
    "keyword_key": "{match: 1, return: 2}", "properties_fn": "properties({a: 1})", "size_keys": "size(keys({a: 1, b: 2}))",
}
for k, v in MAPS.items():
    probe("map/" + k, v)

TYPES = {
    "valueType_int": "valueType(1)", "valueType_float": "valueType(1.5)", "valueType_str": "valueType('a')", "valueType_null": "valueType(null)",
    "valueType_list": "valueType([1, 2])", "valueType_map": "valueType({})", "valueType_bool": "valueType(true)", "valueType_date": "valueType(date('2020-01-01'))",
    "toString_date": "toString(date('2020-01-01'))", "toString_dur": "toString(duration('P1DT2H'))",
}
for k, v in TYPES.items():
    probe("type/" + k, v)

TEMPORAL = {
    "date_str": "date('2020-02-29')", "date_map": "date({year: 2020, month: 2, day: 29})", "date_week": "date({year: 2020, week: 9, dayOfWeek: 6})",
    "date_ordinal": "date({year: 2020, ordinalDay: 60})", "date_quarter": "date({year: 2020, quarter: 2, dayOfQuarter: 10})", "date_bad": "date('2020-13-01')",
    "date_bad2": "date('2021-02-29')", "date_null": "date(null)", "date_plus_dur": "date('2020-01-31') + duration({months: 1})",
    "date_minus_dur": "date('2020-03-31') - duration({months: 1})", "date_plus_days": "date('2020-01-01') + duration({days: 400})",
    "date_diff": "duration.between(date('2020-01-01'), date('2021-03-05'))", "date_indays": "duration.inDays(date('2020-01-01'), date('2020-03-01'))",
    "date_inmonths": "duration.inMonths(date('2020-01-31'), date('2020-03-01'))", "date_inseconds": "duration.inSeconds(date('2020-01-01'), date('2020-01-02'))",
    "date_trunc_month": "date.truncate('month', date('2020-05-17'))", "date_trunc_week": "date.truncate('week', date('2020-05-17'))",
    "date_trunc_year": "date.truncate('year', date('2020-05-17'))", "date_trunc_quarter": "date.truncate('quarter', date('2020-05-17'))",
    "date_trunc_decade": "date.truncate('decade', date('2027-05-17'))", "date_trunc_map": "date.truncate('year', date('2020-05-17'), {day: 5})",
    "date_year": "date('2020-05-17').year", "date_quarter_c": "date('2020-05-17').quarter", "date_week_c": "date('2020-05-17').week",
    "date_weekday": "date('2020-05-17').weekDay", "date_ordinal_c": "date('2020-05-17').ordinalDay", "date_dq": "date('2020-05-17').dayOfQuarter",
    "date_epoch_days": "date('2020-05-17').epochDays", "date_cmp": "date('2020-01-01') < date('2020-01-02')", "date_eq": "date('2020-01-01') = date('2020-01-01')",
    "date_datetime_cmp": "date('2020-01-01') < datetime('2020-01-02T00:00:00Z')", "localtime": "localtime('12:34:56.789')", "localtime_short": "localtime('12')",
    "localtime_map": "localtime({hour: 1, minute: 2, nanosecond: 5})", "localtime_plus": "localtime('23:00') + duration({hours: 2})",
    "localtime_bad": "localtime('25:00')", "time_z": "time('12:00:00Z')", "time_off": "time('12:00+05:30')", "time_off_neg": "time('12:00-0330')",
    "time_map": "time({hour: 12, timezone: '+02:00'})", "time_cmp": "time('12:00+01:00') = time('11:00Z')", "time_lt": "time('12:00+01:00') < time('12:00Z')",
    "datetime_z": "datetime('2020-05-17T12:34:56Z')", "datetime_off": "datetime('2020-05-17T12:34:56.5+02:00')",
    "datetime_region": "datetime('2020-05-17T12:34:56[Europe/Oslo]')", "datetime_map_region": "datetime({year: 2020, month: 7, day: 1, hour: 12, timezone: 'Europe/Oslo'})",
    "datetime_epoch": "datetime({epochSeconds: 1000000000})", "datetime_fromepoch": "datetime.fromepoch(1000000000, 5)",
    "datetime_fromepochmillis": "datetime.fromepochmillis(1000000000123)", "datetime_epochseconds_c": "datetime('2020-05-17T12:34:56Z').epochSeconds",
    "datetime_epochmillis_c": "datetime('2020-05-17T12:34:56.789Z').epochMillis", "datetime_tz_c": "datetime('2020-05-17T12:34:56[Europe/Oslo]').timezone",
    "datetime_offset_c": "datetime('2020-05-17T12:34:56[Europe/Oslo]').offset", "datetime_offsetmin": "datetime('2020-05-17T12:34:56+05:30').offsetMinutes",
    "datetime_plus": "datetime('2020-05-17T12:34:56Z') + duration({days: 1, hours: 13})", "datetime_dst": "datetime('2020-03-28T12:00:00[Europe/Oslo]') + duration({days: 1})",
    "datetime_trunc_hour": "datetime.truncate('hour', datetime('2020-05-17T12:34:56.789Z'))", "datetime_trunc_day_tz": "datetime.truncate('day', datetime('2020-05-17T12:34:56Z'), {timezone: 'Europe/Oslo'})",
    "datetime_cmp": "datetime('2020-05-17T12:00:00+02:00') = datetime('2020-05-17T10:00:00Z')", "datetime_lt": "datetime('2020-05-17T12:00:00+02:00') < datetime('2020-05-17T11:00:00Z')",
    "datetime_bad": "datetime('2020-05-17T25:00:00Z')", "localdatetime": "localdatetime('2020-05-17T12:34:56')", "localdatetime_map": "localdatetime({year: 2020, month: 1, day: 2, hour: 3})",
    "localdatetime_date_time": "localdatetime({date: date('2020-01-02'), time: localtime('03:04')})", "localdatetime_trunc": "localdatetime.truncate('minute', localdatetime('2020-05-17T12:34:56'))",
    "duration_iso": "duration('P1Y2M3DT4H5M6.789S')", "duration_map": "duration({days: 1, hours: 25})", "duration_frac": "duration({months: 1.5})",
    "duration_neg": "duration({hours: -5, minutes: 30})", "duration_plus": "duration('P1D') + duration('PT12H')", "duration_minus": "duration('P1D') - duration('PT36H')",
    "duration_mul": "duration('P1D') * 2.5", "duration_div": "duration('P3D') / 2", "duration_acc": "duration('P1Y13M').months", "duration_acc2": "duration('PT90M').minutes",
    "duration_acc3": "duration('PT90M').hours", "duration_acc4": "duration('P10D').weeks", "duration_acc5": "duration('PT1.5S').milliseconds", "duration_bad": "duration('xyz')",
    "duration_eq": "duration('P1D') = duration('PT24H')", "duration_lt": "duration('P1D') < duration('P2D')", "duration_null": "duration(null)",
    "temporal_str_concat": "'d=' + toString(date('2020-01-01'))", "temporal_list": "[date('2020-01-01'), date('2019-01-01')]",
    "datetime_realtime_type": "valueType(datetime())", "date_now_type": "valueType(date())", "localtime_now_type": "valueType(localtime.statement())",
}
for k, v in TEMPORAL.items():
    probe("time/" + k, v)

SPATIAL = {
    "point2d": "point({x: 1, y: 2})", "point3d": "point({x: 1, y: 2, z: 3})", "point_geo": "point({longitude: 12.5, latitude: 56.1})",
    "point_geo3d": "point({longitude: 12.5, latitude: 56.1, height: 10})", "point_srid": "point({x: 1, y: 2, srid: 7203})", "point_crs": "point({x: 1, y: 2, crs: 'cartesian'})",
    "point_x": "point({x: 1, y: 2}).x", "point_srid_c": "point({x: 1, y: 2}).srid", "point_crs_c": "point({longitude: 1, latitude: 2}).crs", "point_lat": "point({longitude: 1, latitude: 2}).latitude",
    "point_distance": "point.distance(point({x: 0, y: 0}), point({x: 3, y: 4}))", "distance_fn": "distance(point({x: 0, y: 0}), point({x: 3, y: 4}))",
    "distance_geo": "round(point.distance(point({longitude: 0, latitude: 0}), point({longitude: 0, latitude: 1})))", "distance_mixed": "point.distance(point({x: 0, y: 0}), point({longitude: 0, latitude: 0}))",
    "point_null": "point(null)", "point_bad": "point({x: 1})", "point_eq": "point({x: 1, y: 2}) = point({x: 1, y: 2})", "point_lat_bad": "point({longitude: 0, latitude: 95})",
    "point_toString": "toString(point({x: 1, y: 2}))",
}
for k, v in SPATIAL.items():
    probe("spatial/" + k, v)

# parameters of every type round-trip (`RETURN $p`), and as stored properties
PARAM_VALUES = {
    "null": None, "true": True, "false": False, "int0": 0, "int_neg1": -1, "int127": 127, "int128": 128, "int_neg16": -16, "int_neg17": -17,
    "int_neg128": -128, "int_neg129": -129, "int32max": 2147483647, "int32over": 2147483648, "int32min": -2147483648, "int64max": 9223372036854775807,
    "int64min": -9223372036854775808, "float_1_5": 1.5, "float_neg": -0.25, "float_big": 1.7976931348623157e308, "float_tiny": 5e-324,
    "float_int": 3.0, "float_nan": float("nan"), "float_inf": float("inf"), "float_ninf": float("-inf"), "str_empty": "", "str_ascii": "hello",
    "str_unicode": "héllo 中文 \U0001F600", "str_15": "x" * 15, "str_16": "x" * 16, "str_255": "y" * 255, "str_256": "y" * 256,
    "str_65535": "z" * 65535, "str_65536": "z" * 65536, "str_100k": "w" * 100000, "str_nul": "a\x00b", "str_newline": "a\nb\tc", "list_empty": [],
    "list_ints": [1, 2, 3], "list_mixed": [1, "a", None, True, 1.5], "list_nested": [[1, [2, [3]]], []], "list_16": list(range(16)), "list_300": list(range(300)),
    "map_empty": {}, "map_simple": {"a": 1, "b": "x"}, "map_nested": {"a": {"b": {"c": [1, {"d": None}]}}}, "map_unicode_key": {"kéy": 1},
    "map_16": {"k%d" % i: i for i in range(16)}, "map_300": {"k%d" % i: i for i in range(300)}, "bytes_empty": b"", "bytes_small": b"\x00\x01\xff",
    "bytes_300": bytes(range(256)) + b"abc", "bytes_70k": b"\x07" * 70000,
}
for k, v in PARAM_VALUES.items():
    case("param/roundtrip_" + k, [{"q": "RETURN $p AS v", "p": {"p": v}}, {"q": "RETURN $p AS v, valueType($p) AS t", "p": {"p": v}}])

PARAM_STORE = {
    "int": 42, "float": 1.5, "str": "xé", "bool": True, "list_int": [1, 2, 3], "list_str": ["a", "b"], "list_float": [1.5, 2.5], "list_bool": [True, False],
    "list_empty": [], "list_mixed": [1, "a"], "list_nested": [[1], [2]], "list_null": [1, None], "map": {"a": 1}, "null": None, "int64max": 9223372036854775807,
    "float_nan": float("nan"), "float_inf": float("inf"), "big_str": "s" * 70000, "list_list_empty": [[]], "bytes": b"\x01\x02",
}
for k, v in PARAM_STORE.items():
    case("param/store_" + k, [{"q": "CREATE (n:P {v: $p})", "p": {"p": v}}, "MATCH (n:P) RETURN n.v AS v, valueType(n.v) AS t", "MATCH (n:P) RETURN keys(n) AS k"])

case("param/missing", ["RETURN $nope AS v"])
case("param/missing_in_create", ["CREATE (n:P {v: $nope})"])
case("param/missing_in_limit", ["RETURN 1 LIMIT $n"])
case("param/limit_ok", [{"q": "UNWIND range(1, 10) AS i RETURN i LIMIT $n", "p": {"n": 3}}])
case("param/skip_limit", [{"q": "UNWIND range(1, 10) AS i RETURN i ORDER BY i SKIP $s LIMIT $n", "p": {"s": 2, "n": 3}}])
case("param/limit_neg", [{"q": "UNWIND range(1, 10) AS i RETURN i LIMIT $n", "p": {"n": -1}}])
case("param/limit_float", [{"q": "UNWIND range(1, 10) AS i RETURN i LIMIT $n", "p": {"n": 2.5}}])
case("param/limit_str", [{"q": "UNWIND range(1, 10) AS i RETURN i LIMIT $n", "p": {"n": "2"}}])
case("param/in_list", [{"q": "RETURN $x IN $xs AS v", "p": {"x": 2, "xs": [1, 2, 3]}}])
case("param/map_props", [{"q": "CREATE (n:P $props) RETURN n", "p": {"props": {"a": 1, "b": "x"}}}, {"q": "MATCH (n:P) SET n += $more RETURN n", "p": {"more": {"c": 3, "a": None}}},
                        {"q": "MATCH (n:P) SET n = $all RETURN n", "p": {"all": {"z": 26}}}])
case("param/match_prop", ["CREATE (:P {id: 1}), (:P {id: 2})", {"q": "MATCH (n:P {id: $id}) RETURN n.id AS id", "p": {"id": 2}}, {"q": "MATCH (n:P) WHERE n.id = $id RETURN n.id AS id", "p": {"id": 1}}])
case("param/unwind_rows", [{"q": "UNWIND $rows AS r CREATE (:P {id: r.id, name: r.name})", "p": {"rows": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]}},
                          "MATCH (n:P) RETURN n.id AS id, n.name AS name"])
case("param/merge_param_values", [{"q": "MERGE (n:P {id: $id}) ON CREATE SET n.c = $c ON MATCH SET n.m = $m RETURN n", "p": {"id": 1, "c": "created", "m": "matched"}},
                                 {"q": "MERGE (n:P {id: $id}) ON CREATE SET n.c = $c ON MATCH SET n.m = $m RETURN n", "p": {"id": 1, "c": "created", "m": "matched"}}])
case("param/temporal_roundtrip", [{"q": "RETURN $d AS d", "p": {"d": __import__("datetime").date(2020, 2, 29)}}])
case("param/null_map_prop", [{"q": "CREATE (n:P {a: $a})", "p": {"a": None}}, "MATCH (n:P) RETURN keys(n) AS k"])
case("param/param_in_pattern_rel_props", ["CREATE (:A)-[:R {w: 1}]->(:B)", {"q": "MATCH (:A)-[r:R {w: $w}]->(:B) RETURN r.w AS w", "p": {"w": 1}}])
case("param/many", [{"q": "RETURN $a + $b AS v", "p": {"a": 1, "b": 2}}, {"q": "RETURN $a + $b AS v", "p": {"a": "x", "b": "y"}}, {"q": "RETURN $a + $b AS v", "p": {"a": [1], "b": [2]}},
                    {"q": "RETURN $a + $b AS v", "p": {"a": 1, "b": "x"}}])

# ==================================================================================================================
# B. read queries over a fixture graph
# ==================================================================================================================

on_social("match/all_people", "MATCH (p:Person) RETURN p.name AS n ORDER BY n")
on_social("match/count_nodes", "MATCH (n) RETURN count(n) AS c")
on_social("match/count_rels", "MATCH ()-[r]->() RETURN count(r) AS c")
on_social("match/labels_multi", "MATCH (p:Person:Employee) RETURN p.name AS n")
on_social("match/label_none", "MATCH (p:Nothing) RETURN p")
on_social("match/prop_map", "MATCH (p:Person {city: 'Oslo'}) RETURN p.name AS n ORDER BY n")
on_social("match/prop_map_two", "MATCH (p:Person {city: 'Oslo', age: 34}) RETURN p.name AS n")
on_social("match/prop_null_map", "MATCH (p:Person {age: null}) RETURN p.name AS n")
on_social("match/dir_out", "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b) RETURN b.name AS n ORDER BY n")
on_social("match/dir_in", "MATCH (a:Person {name: 'Alice'})<-[:KNOWS]-(b) RETURN b.name AS n ORDER BY n")
on_social("match/dir_both", "MATCH (a:Person {name: 'Alice'})-[:KNOWS]-(b) RETURN b.name AS n ORDER BY n")
on_social("match/type_alt", "MATCH (a:Person {name: 'Carol'})-[r:KNOWS|MANAGES]->(b) RETURN type(r) AS t, b.name AS n ORDER BY t, n")
on_social("match/type_alt_colon", "MATCH (a:Person {name: 'Carol'})-[r:KNOWS|:MANAGES]->(b) RETURN count(r) AS c")
on_social("match/rel_props", "MATCH ()-[r:KNOWS {since: 2010}]->() RETURN count(r) AS c")
on_social("match/rel_prop_where", "MATCH (a)-[r:KNOWS]->(b) WHERE r.since > 2011 RETURN a.name AS a, b.name AS b ORDER BY a, b")
on_social("match/two_hop", "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->()-[:KNOWS]->(c) RETURN c.name AS n ORDER BY n")
on_social("match/two_hop_distinct", "MATCH (a:Person {name: 'Alice'})-[:KNOWS*2]->(c) RETURN DISTINCT c.name AS n ORDER BY n")
on_social("match/varlen_1_3", "MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..3]->(c) RETURN c.name AS n, count(*) AS k ORDER BY n")
on_social("match/varlen_star", "MATCH (a:Person {name: 'Alice'})-[:KNOWS*]->(c) RETURN DISTINCT c.name AS n ORDER BY n")
on_social("match/varlen_0", "MATCH (a:Person {name: 'Alice'})-[:KNOWS*0..1]->(c) RETURN c.name AS n ORDER BY n")
on_social("match/varlen_min", "MATCH (a:Person {name: 'Alice'})-[:KNOWS*3..]->(c) RETURN c.name AS n ORDER BY n")
on_social("match/varlen_upto", "MATCH (a:Person {name: 'Alice'})-[:KNOWS*..2]->(c) RETURN c.name AS n ORDER BY n")
on_social("match/varlen_undirected", "MATCH (a:Person {name: 'Bob'})-[:KNOWS*1..2]-(c) RETURN c.name AS n ORDER BY n")
on_social("match/varlen_rel_list", "MATCH (a:Person {name: 'Alice'})-[r:KNOWS*2]->(c) RETURN size(r) AS s, [x IN r | x.since] AS sinces ORDER BY sinces")
on_social("match/varlen_rel_props", "MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..3 {since: 2010}]->(c) RETURN c.name AS n")
on_social("match/path_var", "MATCH p = (a:Person {name: 'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN p ORDER BY c.name")
on_social("match/path_nodes", "MATCH p = (a:Person {name: 'Alice'})-[:KNOWS*2]->(c) RETURN [n IN nodes(p) | n.name] AS names, length(p) AS len ORDER BY names")
on_social("match/path_rels", "MATCH p = (a:Person {name: 'Alice'})-[:KNOWS*2]->(c) RETURN [r IN relationships(p) | r.since] AS s ORDER BY s")
on_social("match/path_undirected_shape", "MATCH p = (a:Person {name: 'Bob'})<-[:KNOWS]-(b) RETURN p")
on_social("match/shortest", "MATCH p = shortestPath((a:Person {name: 'Dave'})-[:KNOWS*]-(b:Person {name: 'Bob'})) RETURN length(p) AS l")
on_social("match/shortest_directed", "MATCH p = shortestPath((a:Person {name: 'Alice'})-[:KNOWS*]->(b:Person {name: 'Dave'})) RETURN length(p) AS l")
on_social("match/shortest_none", "MATCH p = shortestPath((a:Person {name: 'Erin'})-[:KNOWS*]-(b:Person {name: 'Bob'})) RETURN p")
on_social("match/all_shortest", "MATCH p = allShortestPaths((a:Person {name: 'Alice'})-[:KNOWS*]->(b:Person {name: 'Dave'})) RETURN length(p) AS l, [n IN nodes(p) | n.name] AS names ORDER BY names")
on_social("match/shortest_expr", "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Dave'}) RETURN length(shortestPath((a)-[:KNOWS*]->(b))) AS l")
on_social("match/shortest_same", "MATCH (a:Person {name: 'Alice'}) MATCH p = shortestPath((a)-[*]-(a)) RETURN p")
on_social("match/optional", "MATCH (p:Person) OPTIONAL MATCH (p)-[:WORKS_AT]->(c) RETURN p.name AS n, c.name AS c ORDER BY n")
on_social("match/optional_where", "MATCH (p:Person) OPTIONAL MATCH (p)-[r:KNOWS]->(q) WHERE q.age > 40 RETURN p.name AS n, q.name AS q ORDER BY n, q")
on_social("match/optional_count", "MATCH (p:Person) OPTIONAL MATCH (p)-[r:KNOWS]->() RETURN p.name AS n, count(r) AS c ORDER BY n")
on_social("match/optional_null_prop", "OPTIONAL MATCH (n:Nothing) RETURN n, n.x AS x")
on_social("match/optional_first", "OPTIONAL MATCH (n:Nothing) RETURN count(n) AS c")
on_social("match/cartesian", "MATCH (a:Company), (b:Company) RETURN a.name AS a, b.name AS b ORDER BY a, b")
on_social("match/self_loop", "MATCH (e:Person {name: 'Erin'})-[r:LIKES]->(x) RETURN x.name AS n")
on_social("match/self_loop_undirected", "MATCH (e:Person {name: 'Erin'})-[r:LIKES]-(x) RETURN count(r) AS c")
on_social("match/rel_uniqueness", "MATCH (a)-[r1:KNOWS]->(b)-[r2:KNOWS]->(c) WHERE a.name = 'Alice' RETURN count(*) AS c")
on_social("match/same_rel_var_twice", "MATCH (a)-[r:KNOWS]->(b), (b)-[r:KNOWS]->(c) RETURN count(*) AS c")
on_social("match/where_or", "MATCH (p:Person) WHERE p.city = 'Bergen' OR p.age > 40 RETURN p.name AS n ORDER BY n")
on_social("match/where_not", "MATCH (p:Person) WHERE NOT p.city = 'Oslo' RETURN p.name AS n ORDER BY n")
on_social("match/where_in", "MATCH (p:Person) WHERE p.city IN ['Bergen', 'Trondheim'] RETURN p.name AS n ORDER BY n")
on_social("match/where_starts", "MATCH (p:Person) WHERE p.name STARTS WITH 'A' OR p.name ENDS WITH 'b' RETURN p.name AS n ORDER BY n")
on_social("match/where_contains", "MATCH (p:Person) WHERE p.name CONTAINS 'a' RETURN p.name AS n ORDER BY n")
on_social("match/where_regex", "MATCH (p:Person) WHERE p.name =~ '[A-C].*' RETURN p.name AS n ORDER BY n")
on_social("match/where_isnull", "MATCH (p:Person) WHERE p.age IS NULL RETURN p.name AS n")
on_social("match/where_isnotnull", "MATCH (p:Person) WHERE p.age IS NOT NULL RETURN count(p) AS c")
on_social("match/where_null_cmp", "MATCH (p:Person) WHERE p.age > 30 RETURN p.name AS n ORDER BY n")
on_social("match/where_label_pred", "MATCH (p) WHERE p:Manager RETURN p.name AS n")
on_social("match/where_label_pred_multi", "MATCH (p) WHERE p:Person:Employee RETURN p.name AS n")
on_social("match/where_label_or", "MATCH (p) WHERE p:Manager OR p:Company RETURN coalesce(p.name, '?') AS n ORDER BY n")
on_social("match/where_pattern", "MATCH (p:Person) WHERE (p)-[:WORKS_AT]->() RETURN p.name AS n ORDER BY n")
on_social("match/where_not_pattern", "MATCH (p:Person) WHERE NOT (p)-[:WORKS_AT]->() RETURN p.name AS n ORDER BY n")
on_social("match/where_pattern_bound", "MATCH (a:Person), (b:Person) WHERE (a)-[:KNOWS]->(b) AND a.name < b.name RETURN a.name AS a, b.name AS b ORDER BY a, b")
on_social("match/where_exists_prop", "MATCH (p:Person) WHERE exists(p.age) RETURN count(p) AS c")
on_social("match/where_exists_subq", "MATCH (p:Person) WHERE EXISTS { (p)-[:WORKS_AT]->(:Company {name: 'Acme'}) } RETURN p.name AS n ORDER BY n")
on_social("match/where_exists_subq_where", "MATCH (p:Person) WHERE EXISTS { MATCH (p)-[r:KNOWS]->(q) WHERE q.age > 40 } RETURN p.name AS n ORDER BY n")
on_social("match/where_count_subq", "MATCH (p:Person) WHERE COUNT { (p)-[:KNOWS]->() } >= 2 RETURN p.name AS n ORDER BY n")
on_social("match/where_list_pred", "MATCH (p:Person) WHERE any(t IN p.tags WHERE t = 'a') RETURN p.name AS n")
on_social("match/where_all_pred", "MATCH (p:Person) WHERE all(t IN p.tags WHERE t = 'b') RETURN p.name AS n ORDER BY n")
on_social("match/where_none_pred", "MATCH (p:Person) WHERE none(t IN p.tags WHERE t = 'a') RETURN p.name AS n ORDER BY n")
on_social("match/where_single_pred", "MATCH (p:Person) WHERE single(t IN p.tags WHERE t = 'a') RETURN p.name AS n ORDER BY n")
on_social("match/where_prop_eq_prop", "MATCH (a:Person), (b:Person) WHERE a.age = b.age AND a.name < b.name RETURN a.name AS a, b.name AS b")
on_social("match/where_xor", "MATCH (p:Person) WHERE (p.city = 'Oslo') XOR (p.age > 30) RETURN p.name AS n ORDER BY n")
on_social("match/where_float_cmp", "MATCH (p:Person) WHERE p.score >= 1.5 RETURN p.name AS n ORDER BY n")
on_social("match/where_int_float", "MATCH (p:Person) WHERE p.age = 27.0 RETURN p.name AS n ORDER BY n")
on_social("match/where_type_mismatch", "MATCH (p:Person) WHERE p.age = '27' RETURN count(p) AS c")
on_social("match/where_str_gt_num", "MATCH (p:Person) WHERE p.name > 5 RETURN count(p) AS c")
on_social("match/id_fn_type", "MATCH (p:Person {name: 'Alice'}) RETURN id(p) >= 0 AS ok, elementId(p) IS NOT NULL AS eid, valueType(elementId(p)) AS t")
on_social("match/labels_fn", "MATCH (p:Person {name: 'Alice'}) RETURN labels(p) AS l")
on_social("match/keys_fn", "MATCH (p:Person {name: 'Erin'}) RETURN keys(p) AS k")
on_social("match/properties_fn", "MATCH (p:Person {name: 'Bob'}) RETURN properties(p) AS pr")
on_social("match/type_fn", "MATCH ()-[r:MANAGES]->() RETURN type(r) AS t, count(*) AS c")
on_social("match/startnode_endnode", "MATCH ()-[r:WORKS_AT {role: 'dev'}]->() RETURN startNode(r).name AS s, endNode(r).name AS e")
on_social("match/return_node", "MATCH (p:Person {name: 'Alice'}) RETURN p")
on_social("match/return_rel", "MATCH ()-[r:WORKS_AT {role: 'dev'}]->() RETURN r")
on_social("match/return_map_proj", "MATCH (p:Person {name: 'Alice'}) RETURN p {.name, .age, extra: 1} AS m")
on_social("match/return_map_proj_all", "MATCH (p:Person {name: 'Bob'}) RETURN p {.*} AS m")
on_social("match/return_map_proj_var", "MATCH (p:Person {name: 'Bob'}) WITH p.name AS name, p.age AS age RETURN {name: name, age: age} AS m")
on_social("match/return_star", "MATCH (a:Company)-[r]-(b) RETURN * ORDER BY a.name, b.name")
on_social("match/return_distinct", "MATCH (p:Person) RETURN DISTINCT p.city AS c ORDER BY c")
on_social("match/return_distinct_multi", "MATCH (p:Person) RETURN DISTINCT p.city AS c, p.age AS a ORDER BY c, a")
on_social("match/return_expr_alias", "MATCH (p:Person) RETURN p.name + '!' AS n, p.age * 2 AS a ORDER BY n")
on_social("match/return_no_alias", "MATCH (p:Person {name: 'Alice'}) RETURN p.name, p.age + 1, toUpper(p.city)")
on_social("match/order_desc", "MATCH (p:Person) RETURN p.name AS n ORDER BY p.age DESC, n")
on_social("match/order_nulls_last", "MATCH (p:Person) RETURN p.age AS a ORDER BY a")
on_social("match/order_nulls_desc", "MATCH (p:Person) RETURN p.age AS a ORDER BY a DESC")
on_social("match/order_mixed_types", "UNWIND [3, 'a', true, null, 1.5, [1], {a: 1}, 'B', false, 2] AS x RETURN x ORDER BY x")
on_social("match/order_mixed_desc", "UNWIND [3, 'a', true, null, 1.5, [1], {a: 1}, 'B', false, 2] AS x RETURN x ORDER BY x DESC")
on_social("match/order_by_alias_expr", "MATCH (p:Person) RETURN p.name AS n ORDER BY size(n), n")
on_social("match/order_by_unprojected", "MATCH (p:Person) RETURN p.name AS n ORDER BY p.city, p.name")
on_social("match/skip_limit", "MATCH (p:Person) RETURN p.name AS n ORDER BY n SKIP 1 LIMIT 2")
on_social("match/limit_zero", "MATCH (p:Person) RETURN p.name AS n LIMIT 0")
on_social("match/skip_all", "MATCH (p:Person) RETURN p.name AS n SKIP 100")
on_social("match/with_chain", "MATCH (p:Person) WITH p.city AS c, count(*) AS k WHERE k > 1 RETURN c, k ORDER BY c")
on_social("match/with_order_limit", "MATCH (p:Person) WITH p ORDER BY p.age DESC LIMIT 2 RETURN p.name AS n ORDER BY n")
on_social("match/with_distinct", "MATCH (p:Person) WITH DISTINCT p.city AS c RETURN c ORDER BY c")
on_social("match/with_star", "MATCH (p:Person {name: 'Bob'}) WITH *, p.age AS a RETURN p.name AS n, a")
on_social("match/with_scope_error", "MATCH (p:Person) WITH p.name AS n RETURN p")
on_social("match/with_where_prev_var", "MATCH (p:Person) WITH p.name AS n, p WHERE p.age > 30 RETURN n ORDER BY n")
on_social("match/unwind_match", "UNWIND ['Alice', 'Bob'] AS n MATCH (p:Person {name: n}) RETURN p.age AS a ORDER BY a")
on_social("match/unwind_null", "UNWIND null AS x RETURN x")
on_social("match/unwind_empty", "UNWIND [] AS x RETURN x")
on_social("match/unwind_scalar", "UNWIND 5 AS x RETURN x")
on_social("match/unwind_nested", "UNWIND [[1, 2], [3]] AS l UNWIND l AS x RETURN x ORDER BY x")
on_social("match/unwind_range_sum", "UNWIND range(1, 100) AS x RETURN sum(x) AS s, count(x) AS c, avg(x) AS a")
on_social("match/union", "MATCH (p:Person) RETURN p.name AS n UNION MATCH (c:Company) RETURN c.name AS n")
on_social("match/union_all", "MATCH (p:Person) RETURN p.city AS c UNION ALL MATCH (p:Person) RETURN p.city AS c")
on_social("match/union_distinct", "MATCH (p:Person) RETURN p.city AS c UNION MATCH (p:Person) RETURN p.city AS c")
on_social("match/union_cols_mismatch", "RETURN 1 AS a UNION RETURN 2 AS b")
on_social("match/union_mixed", "RETURN 1 AS a UNION RETURN 2 AS a UNION ALL RETURN 3 AS a")
on_social("match/pattern_comprehension", "MATCH (p:Person {name: 'Alice'}) RETURN [(p)-[:KNOWS]->(q) | q.name] AS friends")
on_social("match/pattern_comprehension_where", "MATCH (p:Person {name: 'Alice'}) RETURN [(p)-[:KNOWS]->(q) WHERE q.age > 30 | q.name] AS friends")
on_social("match/pattern_comprehension_path", "MATCH (p:Person {name: 'Bob'}) RETURN [path = (p)-[:KNOWS]->() | length(path)] AS l")
on_social("match/size_pattern_comp", "MATCH (p:Person) RETURN p.name AS n, size([(p)-[:KNOWS]->() | 1]) AS k ORDER BY n")
on_social("match/collect_subq", "MATCH (p:Person {name: 'Alice'}) RETURN COLLECT { MATCH (p)-[:KNOWS]->(q) RETURN q.name ORDER BY q.name } AS f")
on_social("match/call_subquery_return", "MATCH (p:Person) CALL { WITH p MATCH (p)-[:KNOWS]->(q) RETURN count(q) AS k } RETURN p.name AS n, k ORDER BY n")
on_social("match/call_subquery_union", "CALL { MATCH (p:Person) RETURN p.name AS n UNION MATCH (c:Company) RETURN c.name AS n } RETURN n ORDER BY n")
on_social("match/list_index_prop", "MATCH (p:Person {name: 'Alice'}) RETURN p.tags[0] AS t, p.tags[-1] AS l, size(p.tags) AS s")
on_social("match/coalesce_prop", "MATCH (p:Person) RETURN p.name AS n, coalesce(p.age, -1) AS a ORDER BY n")
on_social("match/case_expr", "MATCH (p:Person) RETURN p.name AS n, CASE WHEN p.age IS NULL THEN 'unknown' WHEN p.age < 30 THEN 'young' ELSE 'adult' END AS g ORDER BY n")
on_social("match/case_simple_prop", "MATCH (p:Person) RETURN p.name AS n, CASE p.city WHEN 'Oslo' THEN 1 WHEN 'Bergen' THEN 2 ELSE 0 END AS c ORDER BY n")
on_social("match/string_fns_prop", "MATCH (p:Person {name: 'Alice'}) RETURN toUpper(p.name) AS u, size(p.name) AS s, substring(p.name, 1, 2) AS sub, split(p.city, 'l') AS sp")
on_social("match/undefined_var", "MATCH (p:Person) RETURN q")
on_social("match/undefined_var_where", "MATCH (p:Person) WHERE q.age > 1 RETURN p")
on_social("match/syntax_error", "MATCH (p:Person RETURN p")
on_social("match/syntax_error_kw", "MATCH (p:Person) RETRUN p")
on_social("match/empty_query", "")
on_social("match/only_comment", "// nothing here")
on_social("match/trailing_semicolon", "MATCH (p:Person) RETURN count(p) AS c;")
on_social("match/comments", "MATCH (p:Person) // people\n /* block */ RETURN count(p) AS c")
on_social("match/backtick_names", "MATCH (`p q`:Person {name: 'Bob'}) RETURN `p q`.name AS `the name`")
on_social("match/case_insensitive_kw", "match (p:Person {name: 'Bob'}) return p.name as N")
on_social("match/label_case_sensitive", "MATCH (p:person) RETURN count(p) AS c")
on_social("match/multi_match", "MATCH (a:Person {name: 'Alice'}) MATCH (a)-[:WORKS_AT]->(c) RETURN c.name AS n")
on_social("match/match_where_and_props", "MATCH (a:Person {city: 'Oslo'}) WHERE a.age > 40 RETURN a.name AS n")
on_social("match/triangle", "MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c)<-[:KNOWS]-(a) RETURN a.name AS a, b.name AS b, c.name AS c")
on_social("match/mutual", "MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(a) RETURN count(*) AS c")
on_social("match/bound_rel_reuse", "MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'}) MATCH (x)-[r]->(y) RETURN x.name AS x, y.name AS y")
on_social("match/collect_paths_len", "MATCH p = (a:Person {name: 'Alice'})-[*1..2]->(x) RETURN count(p) AS c")
on_social("match/unlabeled_untyped_varlen", "MATCH (a:Person {name: 'Erin'})-[*]-(x) RETURN count(*) AS c")

# aggregation
AGG = {
    "count_star": "MATCH (n) RETURN count(*) AS c", "count_prop": "MATCH (p:Person) RETURN count(p.age) AS c", "count_distinct": "MATCH (p:Person) RETURN count(DISTINCT p.city) AS c",
    "count_empty": "MATCH (n:Nothing) RETURN count(*) AS c", "sum": "MATCH (p:Person) RETURN sum(p.age) AS s", "sum_empty": "MATCH (n:Nothing) RETURN sum(n.x) AS s",
    "sum_float": "MATCH (p:Person) RETURN sum(p.score) AS s", "avg": "MATCH (p:Person) RETURN avg(p.age) AS a", "avg_empty": "MATCH (n:Nothing) RETURN avg(n.x) AS a",
    "min": "MATCH (p:Person) RETURN min(p.age) AS a, min(p.name) AS n", "max": "MATCH (p:Person) RETURN max(p.age) AS a, max(p.name) AS n", "min_empty": "MATCH (n:Nothing) RETURN min(n.x) AS m",
    "collect": "MATCH (p:Person) RETURN collect(p.name) AS names", "collect_distinct": "MATCH (p:Person) RETURN collect(DISTINCT p.city) AS c", "collect_null": "MATCH (p:Person) RETURN collect(p.age) AS a",
    "collect_empty": "MATCH (n:Nothing) RETURN collect(n.x) AS c", "percentileCont": "MATCH (p:Person) RETURN percentileCont(p.age, 0.5) AS m", "percentileDisc": "MATCH (p:Person) RETURN percentileDisc(p.age, 0.5) AS m",
    "percentile_bad": "MATCH (p:Person) RETURN percentileCont(p.age, 1.5) AS m", "stdev": "MATCH (p:Person) RETURN stDev(p.age) AS s", "stdevp": "MATCH (p:Person) RETURN stDevP(p.age) AS s",
    "group_by": "MATCH (p:Person) RETURN p.city AS c, count(*) AS n ORDER BY c", "group_by_two": "MATCH (p:Person) RETURN p.city AS c, p.age AS a, count(*) AS n ORDER BY c, a",
    "group_by_node": "MATCH (p:Person)-[:KNOWS]->(q) RETURN p.name AS n, count(q) AS c ORDER BY n", "group_expr": "MATCH (p:Person) RETURN p.age / 10 AS decade, count(*) AS n ORDER BY decade",
    "agg_expr": "MATCH (p:Person) RETURN count(*) * 2 + 1 AS x", "agg_in_list": "MATCH (p:Person) RETURN [count(*), sum(1)] AS x", "agg_in_map": "MATCH (p:Person) RETURN {c: count(*)} AS x",
    "agg_with_key_expr": "MATCH (p:Person) RETURN p.city AS c, toString(count(*)) + '!' AS s ORDER BY c", "agg_distinct_sum": "UNWIND [1, 1, 2, 2, 3] AS x RETURN sum(DISTINCT x) AS s",
    "agg_avg_dur": "UNWIND [duration('PT1H'), duration('PT3H')] AS d RETURN avg(d) AS a", "agg_sum_dur": "UNWIND [duration('PT1H'), duration('PT3H')] AS d RETURN sum(d) AS a",
    "agg_min_mixed": "UNWIND [3, 'a', 1.5, true] AS x RETURN min(x) AS a, max(x) AS b", "agg_sum_str": "UNWIND ['a', 'b'] AS x RETURN sum(x) AS s",
    "agg_sum_overflow": "UNWIND [9223372036854775807, 1] AS x RETURN sum(x) AS s", "agg_count_null_star": "UNWIND [null, null] AS x RETURN count(*) AS c, count(x) AS d",
    "agg_nested": "MATCH (p:Person) RETURN count(count(*)) AS c", "agg_order_by": "MATCH (p:Person) RETURN p.city AS c, count(*) AS n ORDER BY n DESC, c", "agg_order_by_agg": "MATCH (p:Person) RETURN p.city AS c ORDER BY count(*) DESC, c",
    "agg_having_like": "MATCH (p:Person) WITH p.city AS c, count(*) AS n WHERE n >= 2 RETURN c ORDER BY c", "agg_ambiguous": "MATCH (p:Person) RETURN p.name, count(*) + p.age",
    "agg_group_none_rows": "MATCH (n:Nothing) RETURN n.x AS x, count(*) AS c", "agg_collect_nodes": "MATCH (p:Person {city: 'Oslo'}) RETURN collect(p) AS ps",
    "agg_collect_order": "MATCH (p:Person) WITH p ORDER BY p.name DESC RETURN collect(p.name) AS names", "agg_where": "MATCH (p:Person) WHERE count(*) > 1 RETURN p",
    "agg_float_group": "UNWIND [1, 1.0, 2] AS x RETURN x, count(*) AS c ORDER BY x", "agg_distinct_float": "UNWIND [1, 1.0, 2] AS x RETURN count(DISTINCT x) AS c",
    "agg_nan": "UNWIND [0.0/0.0, 0.0/0.0, 1.0] AS x RETURN count(DISTINCT x) AS c", "agg_avg_int": "UNWIND [1, 2] AS x RETURN avg(x) AS a", "agg_max_list": "UNWIND [[1, 2], [1, 3]] AS x RETURN max(x) AS a",
    "agg_stdev_single": "UNWIND [5] AS x RETURN stDev(x) AS a, stDevP(x) AS b", "agg_percentile_edge": "UNWIND [1, 2, 3, 4] AS x RETURN percentileCont(x, 0) AS a, percentileCont(x, 1) AS b, percentileDisc(x, 0.75) AS c",
}
for k, v in AGG.items():
    on_social("agg/" + k, v)

# ==================================================================================================================
# C. write semantics
# ==================================================================================================================

case("w/create_node", ["CREATE (n:A {x: 1}) RETURN n", "MATCH (n) RETURN count(n) AS c"])
case("w/create_multi_label", ["CREATE (n:A:B:C) RETURN labels(n) AS l"])
case("w/create_no_label", ["CREATE (n) RETURN labels(n) AS l"])
case("w/create_empty_props", ["CREATE (n:A {}) RETURN properties(n) AS p"])
case("w/create_null_prop", ["CREATE (n:A {x: null, y: 1}) RETURN keys(n) AS k"])
case("w/create_rel", ["CREATE (a:A {n: 1})-[r:R {w: 2}]->(b:B {n: 2}) RETURN a.n AS a, r.w AS w, b.n AS b"])
case("w/create_rel_reverse", ["CREATE (a:A {n: 1})<-[r:R]-(b:B {n: 2}) RETURN startNode(r).n AS s, endNode(r).n AS e"])
case("w/create_chain", ["CREATE (a:A)-[:R]->(b:B)-[:S]->(c:C) RETURN count(*) AS c", "MATCH (n) RETURN count(n) AS c"])
case("w/create_path_var", ["CREATE p = (a:A)-[:R]->(b:B) RETURN length(p) AS l"])
case("w/create_reuse_bound", ["CREATE (a:A {n: 1})", "MATCH (a:A) CREATE (a)-[:R]->(b:B {n: 2}) RETURN b.n AS n"])
case("w/create_rel_no_type", ["CREATE (a)-[r]->(b)"])
case("w/create_rel_undirected", ["CREATE (a)-[:R]-(b)"])
case("w/create_rel_multi_type", ["CREATE (a)-[:R|S]->(b)"])
case("w/create_varlen", ["CREATE (a)-[:R*2]->(b)"])
case("w/create_bound_with_label", ["CREATE (a:A)", "MATCH (a:A) CREATE (a:B)"])
case("w/create_bound_alone", ["CREATE (a:A)", "MATCH (a:A) CREATE (a)"])
case("w/create_unwind", ["UNWIND range(1, 5) AS i CREATE (:N {i: i})", "MATCH (n:N) RETURN count(n) AS c, sum(n.i) AS s"])
case("w/create_from_match", ["CREATE (:A {n: 1}), (:A {n: 2})", "MATCH (a:A) CREATE (b:B {from: a.n}) RETURN b.from AS f"])
case("w/create_return_star", ["CREATE (a:A)-[r:R]->(b:B) RETURN *"])
case("w/create_list_prop", ["CREATE (n:A {l: [1, 2, 3], s: ['a'], e: []}) RETURN n.l AS l, n.s AS s, n.e AS e"])
case("w/create_map_prop", ["CREATE (n:A {m: {a: 1}})"])
case("w/create_mixed_list_prop", ["CREATE (n:A {l: [1, 'a']})"])
case("w/create_nested_list_prop", ["CREATE (n:A {l: [[1]]})"])
case("w/create_null_in_list_prop", ["CREATE (n:A {l: [1, null]})"])
case("w/create_float_int_list", ["CREATE (n:A {l: [1, 2.5]})"])
case("w/create_temporal_props", ["CREATE (n:A {d: date('2020-01-02'), t: localtime('10:00'), dt: datetime('2020-01-02T10:00:00Z'), du: duration('P1D'), p: point({x: 1, y: 2})})",
                                 "MATCH (n:A) RETURN n.d AS d, n.t AS t, n.dt AS dt, n.du AS du, n.p AS p"])
case("w/create_big_int_prop", ["CREATE (n:A {i: 9223372036854775807, j: -9223372036854775808}) RETURN n.i AS i, n.j AS j"])
case("w/create_float_props", ["CREATE (n:A {a: 1.0, b: 1e10, c: 1e-10, d: -0.0, e: 0.1}) RETURN n.a AS a, n.b AS b, n.c AS c, n.e AS e, n.a = 1 AS eq"])
case("w/create_unicode_label", ["CREATE (n:`Café` {`na me`: 'x'}) RETURN labels(n) AS l, keys(n) AS k"])
case("w/create_many_props", ["CREATE (n:A {p1: 1, p2: 2, p3: 3, p4: 4, p5: 5, p6: 6, p7: 7, p8: 8}) RETURN size(keys(n)) AS k"])
case("w/create_dup_var", ["CREATE (a:A)-[:R]->(a) RETURN count(*) AS c", "MATCH ()-[r:R]->() RETURN count(r) AS c"])
case("w/create_counters_only", ["CREATE (:A), (:B {x: 1}), (:C)-[:R {y: 2}]->(:D)"])
case("w/merge_node_create", ["MERGE (n:A {id: 1}) RETURN n.id AS id", "MERGE (n:A {id: 1}) RETURN n.id AS id", "MATCH (n:A) RETURN count(n) AS c"])
case("w/merge_on_create_match", ["MERGE (n:A {id: 1}) ON CREATE SET n.c = 'created' ON MATCH SET n.m = 'matched' RETURN n.c AS c, n.m AS m",
                                 "MERGE (n:A {id: 1}) ON CREATE SET n.c = 'created' ON MATCH SET n.m = 'matched' RETURN n.c AS c, n.m AS m"])
case("w/merge_labels", ["MERGE (n:A:B {id: 1})", "MERGE (n:A {id: 1}) RETURN labels(n) AS l"])
case("w/merge_rel", ["CREATE (:A {n: 1}), (:B {n: 2})", "MATCH (a:A), (b:B) MERGE (a)-[r:R]->(b) RETURN type(r) AS t", "MATCH (a:A), (b:B) MERGE (a)-[r:R]->(b) RETURN type(r) AS t",
                     "MATCH ()-[r:R]->() RETURN count(r) AS c"])
case("w/merge_rel_props", ["CREATE (:A {n: 1}), (:B {n: 2})", "MATCH (a:A), (b:B) MERGE (a)-[r:R {w: 1}]->(b) RETURN r.w AS w", "MATCH (a:A), (b:B) MERGE (a)-[r:R {w: 2}]->(b) RETURN r.w AS w",
                           "MATCH ()-[r:R]->() RETURN count(r) AS c"])
case("w/merge_undirected", ["CREATE (:A {n: 1}), (:B {n: 2})", "MATCH (a:A), (b:B) MERGE (a)-[r:R]-(b) RETURN type(r) AS t", "MATCH ()-[r:R]->() RETURN count(r) AS c"])
case("w/merge_pattern_all_new", ["MERGE (a:A {n: 1})-[:R]->(b:B {n: 2}) RETURN a.n AS a, b.n AS b", "MERGE (a:A {n: 1})-[:R]->(b:B {n: 2}) RETURN a.n AS a, b.n AS b", "MATCH (n) RETURN count(n) AS c"])
case("w/merge_null_prop", ["MERGE (n:A {id: null})"])
case("w/merge_bound", ["CREATE (a:A)", "MATCH (a:A) MERGE (a)"])
case("w/merge_param_map", [{"q": "MERGE (n:A $p)", "p": {"p": {"a": 1}}}])
case("w/merge_multiple_matches", ["CREATE (:A {k: 1}), (:A {k: 1})", "MERGE (n:A {k: 1}) SET n.seen = true RETURN count(n) AS c", "MATCH (n:A) RETURN n.seen AS s"])
case("w/merge_unwind_dedupe", ["UNWIND [1, 1, 2, 2, 3] AS i MERGE (n:A {i: i})", "MATCH (n:A) RETURN count(n) AS c"])
case("w/merge_on_create_only_new", ["UNWIND [1, 2, 1] AS i MERGE (n:A {i: i}) ON CREATE SET n.count = 1 ON MATCH SET n.count = n.count + 1", "MATCH (n:A) RETURN n.i AS i, n.count AS c ORDER BY i"])
case("w/merge_rel_direction_match", ["CREATE (a:A)-[:R]->(b:B)", "MATCH (a:A), (b:B) MERGE (b)-[r:R]->(a)", "MATCH ()-[r:R]->() RETURN count(r) AS c"])
case("w/merge_set_labels_on_create", ["MERGE (n:A {id: 1}) ON CREATE SET n:New RETURN labels(n) AS l"])
case("w/set_prop", ["CREATE (:A {x: 1})", "MATCH (n:A) SET n.x = 2, n.y = 'new' RETURN n.x AS x, n.y AS y"])
case("w/set_prop_null_removes", ["CREATE (:A {x: 1, y: 2})", "MATCH (n:A) SET n.x = null RETURN keys(n) AS k"])
case("w/set_prop_same_value", ["CREATE (:A {x: 1})", "MATCH (n:A) SET n.x = 1"])
case("w/set_prop_missing_null", ["CREATE (:A {x: 1})", "MATCH (n:A) SET n.z = null"])
case("w/set_replace_map", ["CREATE (:A {x: 1, y: 2})", "MATCH (n:A) SET n = {z: 3} RETURN properties(n) AS p"])
case("w/set_merge_map", ["CREATE (:A {x: 1, y: 2})", "MATCH (n:A) SET n += {y: 20, z: 30} RETURN properties(n) AS p"])
case("w/set_merge_map_null", ["CREATE (:A {x: 1, y: 2})", "MATCH (n:A) SET n += {y: null} RETURN properties(n) AS p"])
case("w/set_replace_from_node", ["CREATE (:A {x: 1}), (:B {y: 2})", "MATCH (a:A), (b:B) SET a = b RETURN properties(a) AS p"])
case("w/set_labels", ["CREATE (:A)", "MATCH (n:A) SET n:B:C RETURN labels(n) AS l"])
case("w/set_label_existing", ["CREATE (:A)", "MATCH (n:A) SET n:A"])
case("w/set_rel_prop", ["CREATE (:A)-[:R {w: 1}]->(:B)", "MATCH ()-[r:R]->() SET r.w = 2, r.v = 3 RETURN properties(r) AS p"])
case("w/set_rel_replace", ["CREATE (:A)-[:R {w: 1}]->(:B)", "MATCH ()-[r:R]->() SET r = {v: 9} RETURN properties(r) AS p"])
case("w/set_null_target", ["OPTIONAL MATCH (n:Nothing) SET n.x = 1 RETURN n"])
case("w/set_on_non_entity", ["WITH 1 AS n SET n.x = 1"])
case("w/set_map_value", ["CREATE (:A)", "MATCH (n:A) SET n.m = {a: 1}"])
case("w/set_list_value", ["CREATE (:A)", "MATCH (n:A) SET n.l = [1, 2] RETURN n.l AS l"])
case("w/set_expr", ["CREATE (:A {x: 1})", "MATCH (n:A) SET n.x = n.x + 10, n.y = n.x * 2 RETURN n.x AS x, n.y AS y"])
case("w/set_multiple_rows", ["UNWIND range(1, 3) AS i CREATE (:A {i: i})", "MATCH (n:A) SET n.done = true", "MATCH (n:A) RETURN count(n) AS c, count(n.done) AS d"])
case("w/set_then_read_in_query", ["CREATE (:A {x: 1})", "MATCH (n:A) SET n.x = 5 WITH n RETURN n.x AS x"])
case("w/set_temporal", ["CREATE (:A)", "MATCH (n:A) SET n.d = date('2020-01-01') RETURN n.d AS d"])
case("w/remove_prop", ["CREATE (:A {x: 1, y: 2})", "MATCH (n:A) REMOVE n.x RETURN keys(n) AS k"])
case("w/remove_missing_prop", ["CREATE (:A {x: 1})", "MATCH (n:A) REMOVE n.zzz"])
case("w/remove_label", ["CREATE (:A:B)", "MATCH (n:A) REMOVE n:B RETURN labels(n) AS l"])
case("w/remove_missing_label", ["CREATE (:A)", "MATCH (n:A) REMOVE n:Z"])
case("w/remove_all_labels", ["CREATE (:A:B)", "MATCH (n:A) REMOVE n:A:B RETURN labels(n) AS l"])
case("w/remove_rel_prop", ["CREATE (:A)-[:R {w: 1}]->(:B)", "MATCH ()-[r:R]->() REMOVE r.w RETURN properties(r) AS p"])
case("w/remove_on_null", ["OPTIONAL MATCH (n:Nothing) REMOVE n.x"])
case("w/delete_node", ["CREATE (:A), (:A)", "MATCH (n:A) DELETE n", "MATCH (n) RETURN count(n) AS c"])
case("w/delete_node_with_rel", ["CREATE (:A)-[:R]->(:B)", "MATCH (n:A) DELETE n", "MATCH (n) RETURN count(n) AS c"])
case("w/delete_rel_then_node", ["CREATE (:A)-[:R]->(:B)", "MATCH (n:A)-[r:R]->() DELETE r, n", "MATCH (n) RETURN count(n) AS c"])
case("w/delete_node_then_rel_same_stmt", ["CREATE (:A)-[:R]->(:B)", "MATCH (n:A)-[r:R]->() DELETE n, r"])
case("w/detach_delete", ["CREATE (:A)-[:R]->(:B)-[:S]->(:C)", "MATCH (n:B) DETACH DELETE n", "MATCH (n) RETURN count(n) AS c", "MATCH ()-[r]->() RETURN count(r) AS c"])
case("w/detach_delete_all", ["CREATE (:A)-[:R]->(:B), (:C)", "MATCH (n) DETACH DELETE n", "MATCH (n) RETURN count(n) AS c"])
case("w/delete_path", ["CREATE (:A)-[:R]->(:B)", "MATCH p = (:A)-[:R]->(:B) DETACH DELETE p", "MATCH (n) RETURN count(n) AS c"])
case("w/delete_null", ["OPTIONAL MATCH (n:Nothing) DELETE n"])
case("w/delete_twice", ["CREATE (:A)", "MATCH (n:A) DELETE n DELETE n"])
case("w/delete_then_return", ["CREATE (:A {x: 1})", "MATCH (n:A) DELETE n RETURN n.x AS x"])
case("w/delete_then_return_node", ["CREATE (:A {x: 1})", "MATCH (n:A) DELETE n RETURN n"])
case("w/delete_then_match", ["CREATE (:A), (:A)", "MATCH (n:A) DELETE n WITH count(*) AS c MATCH (m:A) RETURN c, count(m) AS remaining"])
case("w/delete_non_entity", ["WITH 1 AS x DELETE x"])
case("w/delete_list_of_nodes", ["CREATE (:A), (:A)", "MATCH (n:A) WITH collect(n) AS ns DELETE ns[0]", "MATCH (n:A) RETURN count(n) AS c"])
case("w/delete_rel_only", ["CREATE (:A)-[:R]->(:B)", "MATCH ()-[r:R]->() DELETE r", "MATCH (n) RETURN count(n) AS c", "MATCH ()-[r]->() RETURN count(r) AS c"])
case("w/foreach_create", ["FOREACH (i IN range(1, 3) | CREATE (:N {i: i}))", "MATCH (n:N) RETURN count(n) AS c"])
case("w/foreach_set", ["CREATE (:A), (:A)", "MATCH (n:A) WITH collect(n) AS ns FOREACH (x IN ns | SET x.seen = true)", "MATCH (n:A) RETURN count(n.seen) AS c"])
case("w/foreach_merge", ["FOREACH (i IN [1, 1, 2] | MERGE (:N {i: i}))", "MATCH (n:N) RETURN count(n) AS c"])
case("w/foreach_nested", ["FOREACH (i IN [1, 2] | FOREACH (j IN [1, 2] | CREATE (:N {i: i, j: j})))", "MATCH (n:N) RETURN count(n) AS c"])
case("w/foreach_null", ["FOREACH (i IN null | CREATE (:N))", "MATCH (n) RETURN count(n) AS c"])
case("w/foreach_match_inside", ["FOREACH (i IN [1] | MATCH (n) RETURN n)"])
case("w/create_after_match_needs_with", ["CREATE (:A)", "CREATE (:B) MATCH (n) RETURN n"])
case("w/with_between_write_read", ["CREATE (:A) WITH 1 AS x MATCH (n:A) RETURN count(n) AS c"])
case("w/write_no_return_counters", ["CREATE (:A)-[:R]->(:B)", "MATCH (a:A)-[r]->(b) SET a.x = 1, r.y = 2, b:C REMOVE b:B"])
case("w/unique_ids_via_create_loop", ["UNWIND range(1, 50) AS i CREATE (:N {i: i})", "MATCH (n:N) RETURN count(DISTINCT n.i) AS c"])
case("w/large_unwind_create_rels", ["UNWIND range(1, 100) AS i CREATE (:N {i: i})", "MATCH (a:N), (b:N) WHERE b.i = a.i + 1 CREATE (a)-[:NEXT]->(b)", "MATCH ()-[r:NEXT]->() RETURN count(r) AS c"])
case("w/create_return_order", ["UNWIND [3, 1, 2] AS i CREATE (n:N {i: i}) RETURN n.i AS i ORDER BY i"])
case("w/match_create_count", ["CREATE (:A), (:A)", "MATCH (a:A) CREATE (:B) RETURN count(*) AS c"])
case("w/self_referencing_set", ["CREATE (:A {x: 1})", "MATCH (a:A) SET a.y = a.x, a.x = 2 RETURN a.x AS x, a.y AS y"])
case("w/swap_props", ["CREATE (:A {x: 1, y: 2})", "MATCH (a:A) SET a.x = a.y, a.y = a.x RETURN a.x AS x, a.y AS y"])

# ==================================================================================================================
# D. schema (constraints / indexes)
# ==================================================================================================================

case("ddl/unique_constraint", ["CREATE CONSTRAINT c_email FOR (n:U) REQUIRE n.email IS UNIQUE", "CREATE (:U {email: 'a'})", "CREATE (:U {email: 'a'})", "CREATE (:U {email: 'b'})",
                              "MATCH (n:U) RETURN count(n) AS c"])
case("ddl/unique_constraint_existing_violation", ["CREATE (:U {email: 'a'}), (:U {email: 'a'})", "CREATE CONSTRAINT c_email FOR (n:U) REQUIRE n.email IS UNIQUE"])
case("ddl/unique_constraint_null_ok", ["CREATE CONSTRAINT c_email FOR (n:U) REQUIRE n.email IS UNIQUE", "CREATE (:U), (:U)", "MATCH (n:U) RETURN count(n) AS c"])
case("ddl/unique_constraint_set", ["CREATE CONSTRAINT c_email FOR (n:U) REQUIRE n.email IS UNIQUE", "CREATE (:U {email: 'a'}), (:U {email: 'b'})", "MATCH (n:U {email: 'b'}) SET n.email = 'a'"])
case("ddl/unique_constraint_merge", ["CREATE CONSTRAINT c_email FOR (n:U) REQUIRE n.email IS UNIQUE", "MERGE (n:U {email: 'a'})", "MERGE (n:U {email: 'a'})", "MATCH (n:U) RETURN count(n) AS c"])
case("ddl/unique_constraint_add_label", ["CREATE CONSTRAINT c_email FOR (n:U) REQUIRE n.email IS UNIQUE", "CREATE (:X {email: 'a'}), (:U {email: 'a'})", "MATCH (n:X) SET n:U"])
case("ddl/unique_constraint_multiprop", ["CREATE CONSTRAINT c2 FOR (n:U) REQUIRE (n.a, n.b) IS UNIQUE", "CREATE (:U {a: 1, b: 1}), (:U {a: 1, b: 2})", "CREATE (:U {a: 1, b: 1})"])
case("ddl/unique_constraint_int_float", ["CREATE CONSTRAINT c1 FOR (n:U) REQUIRE n.v IS UNIQUE", "CREATE (:U {v: 1})", "CREATE (:U {v: 1.0})"])
case("ddl/unique_constraint_dup_name", ["CREATE CONSTRAINT c1 FOR (n:U) REQUIRE n.a IS UNIQUE", "CREATE CONSTRAINT c1 FOR (n:V) REQUIRE n.b IS UNIQUE"])
case("ddl/unique_constraint_equivalent", ["CREATE CONSTRAINT c1 FOR (n:U) REQUIRE n.a IS UNIQUE", "CREATE CONSTRAINT c2 FOR (n:U) REQUIRE n.a IS UNIQUE"])
case("ddl/unique_constraint_if_not_exists", ["CREATE CONSTRAINT c1 IF NOT EXISTS FOR (n:U) REQUIRE n.a IS UNIQUE", "CREATE CONSTRAINT c1 IF NOT EXISTS FOR (n:U) REQUIRE n.a IS UNIQUE"])
case("ddl/drop_constraint", ["CREATE CONSTRAINT c1 FOR (n:U) REQUIRE n.a IS UNIQUE", "DROP CONSTRAINT c1", "CREATE (:U {a: 1}), (:U {a: 1})", "MATCH (n:U) RETURN count(n) AS c"])
case("ddl/drop_constraint_missing", ["DROP CONSTRAINT nope"])
case("ddl/drop_constraint_if_exists", ["DROP CONSTRAINT nope IF EXISTS"])
case("ddl/show_constraints", ["CREATE CONSTRAINT c1 FOR (n:U) REQUIRE n.a IS UNIQUE", "SHOW CONSTRAINTS YIELD name, type, entityType, labelsOrTypes, properties"])
case("ddl/show_constraints_where", ["CREATE CONSTRAINT c1 FOR (n:U) REQUIRE n.a IS UNIQUE", "SHOW CONSTRAINTS YIELD name WHERE name = 'c1' RETURN name"])
case("ddl/show_constraints_columns", ["CREATE CONSTRAINT c1 FOR (n:U) REQUIRE n.a IS UNIQUE", {"q": "SHOW CONSTRAINTS", "cols_only": True}])
case("ddl/existence_constraint", ["CREATE CONSTRAINT c1 FOR (n:U) REQUIRE n.a IS NOT NULL"])
case("ddl/node_key_constraint", ["CREATE CONSTRAINT c1 FOR (n:U) REQUIRE n.a IS NODE KEY"])
case("ddl/legacy_constraint_syntax", ["CREATE CONSTRAINT ON (n:U) ASSERT n.a IS UNIQUE"])
case("ddl/index", ["CREATE INDEX i1 FOR (n:P) ON (n.name)", "CREATE (:P {name: 'a'})", "MATCH (n:P {name: 'a'}) RETURN count(n) AS c"])
case("ddl/index_composite", ["CREATE INDEX i1 FOR (n:P) ON (n.a, n.b)", "CREATE (:P {a: 1, b: 2})", "MATCH (n:P {a: 1, b: 2}) RETURN count(n) AS c"])
case("ddl/index_if_not_exists", ["CREATE INDEX i1 IF NOT EXISTS FOR (n:P) ON (n.a)", "CREATE INDEX i1 IF NOT EXISTS FOR (n:P) ON (n.a)"])
case("ddl/index_duplicate", ["CREATE INDEX i1 FOR (n:P) ON (n.a)", "CREATE INDEX i2 FOR (n:P) ON (n.a)"])
case("ddl/index_duplicate_name", ["CREATE INDEX i1 FOR (n:P) ON (n.a)", "CREATE INDEX i1 FOR (n:Q) ON (n.b)"])
case("ddl/index_over_constraint", ["CREATE CONSTRAINT c1 FOR (n:P) REQUIRE n.a IS UNIQUE", "CREATE INDEX i1 FOR (n:P) ON (n.a)"])
case("ddl/constraint_over_index", ["CREATE INDEX i1 FOR (n:P) ON (n.a)", "CREATE CONSTRAINT c1 FOR (n:P) REQUIRE n.a IS UNIQUE"])
case("ddl/drop_index", ["CREATE INDEX i1 FOR (n:P) ON (n.a)", "DROP INDEX i1", "DROP INDEX i1"])
case("ddl/drop_index_if_exists", ["DROP INDEX nope IF EXISTS"])
case("ddl/drop_index_owned_by_constraint", ["CREATE CONSTRAINT c1 FOR (n:P) REQUIRE n.a IS UNIQUE", "DROP INDEX c1"])
case("ddl/show_indexes", ["CREATE INDEX i1 FOR (n:P) ON (n.a)", "SHOW INDEXES YIELD name, type, entityType, labelsOrTypes, properties WHERE type = 'RANGE' RETURN name, type, entityType, labelsOrTypes, properties"])
case("ddl/show_indexes_state", ["CREATE INDEX i1 FOR (n:P) ON (n.a)", "SHOW INDEXES YIELD name, state, populationPercent WHERE name = 'i1' RETURN name, state, populationPercent"])
case("ddl/show_indexes_columns", ["CREATE INDEX i1 FOR (n:P) ON (n.a)", {"q": "SHOW INDEXES", "cols_only": True}])
case("ddl/show_indexes_with_constraint", ["CREATE CONSTRAINT c1 FOR (n:P) REQUIRE n.a IS UNIQUE", "SHOW INDEXES YIELD name, type, owningConstraint WHERE type = 'RANGE' RETURN name, owningConstraint"])
case("ddl/text_index", ["CREATE TEXT INDEX t1 FOR (n:P) ON (n.a)", "SHOW INDEXES YIELD name, type WHERE name = 't1' RETURN name, type"])
case("ddl/fulltext_index", ["CREATE FULLTEXT INDEX f1 FOR (n:P) ON EACH [n.a]"], known="full-text indexes are not supported by Warp (documented)")
case("ddl/rel_index", ["CREATE INDEX r1 FOR ()-[r:R]-() ON (r.a)"], known="relationship-property indexes are not implemented (documented)")
case("ddl/rel_unique_constraint", ["CREATE CONSTRAINT rc FOR ()-[r:R]-() REQUIRE r.a IS UNIQUE"], known="relationship-property constraints are not implemented (documented)")
case("ddl/schema_in_tx", [{"tx": [{"q": "CREATE INDEX i1 FOR (n:P) ON (n.a)"}], "end": "commit"}, "SHOW INDEXES YIELD name WHERE name = 'i1' RETURN name"])
case("ddl/create_data_and_schema_same_tx", [{"tx": [{"q": "CREATE (:P {a: 1})"}, {"q": "CREATE INDEX i1 FOR (n:P) ON (n.a)"}], "end": "commit"}])
case("ddl/backtick_names", ["CREATE CONSTRAINT `my c` FOR (n:`My Label`) REQUIRE n.`my prop` IS UNIQUE", "CREATE (:`My Label` {`my prop`: 1})", "CREATE (:`My Label` {`my prop`: 1})"])

# ==================================================================================================================
# E. procedures / introspection
# ==================================================================================================================

case("proc/db_labels", ["CREATE (:DiffLblA), (:DiffLblB:DiffLblA)", "CALL db.labels() YIELD label WHERE label STARTS WITH 'DiffLbl' RETURN label ORDER BY label"])
case("proc/db_reltypes", ["CREATE ()-[:DIFF_REL_A]->()-[:DIFF_REL_B]->()", "CALL db.relationshipTypes() YIELD relationshipType WHERE relationshipType STARTS WITH 'DIFF_REL' RETURN relationshipType ORDER BY relationshipType"])
case("proc/db_propkeys", ["CREATE (:X {diffPropA: 1, diffPropB: 2})-[:Y {diffPropC: 3}]->()", "CALL db.propertyKeys() YIELD propertyKey WHERE propertyKey STARTS WITH 'diffProp' RETURN propertyKey ORDER BY propertyKey"])
case("proc/call_standalone_labels_cols", [{"q": "CALL db.labels()", "cols_only": True}])
case("proc/call_yield_alias", ["CREATE (:DiffLblA)", "CALL db.labels() YIELD label AS l WHERE l = 'DiffLblA' RETURN l"])
case("proc/call_in_query", ["CREATE (:DiffLblA)", "MATCH (n:DiffLblA) CALL db.labels() YIELD label WHERE label = 'DiffLblA' RETURN count(*) AS c"])
case("proc/call_unknown", ["CALL nope.nothing()"])
case("proc/call_unknown_yield", ["CALL db.labels() YIELD nothing"])
case("proc/call_args_wrong", ["CALL db.labels(1)"])
case("proc/call_implicit_in_query", ["CALL db.labels() RETURN 1"])
case("proc/dbms_components", ["CALL dbms.components() YIELD name, edition RETURN name, edition"])
case("proc/db_ping", ["CALL db.ping() YIELD success RETURN success"], known="db.ping is reported as success only by Warp; real Neo4j 5.26 may not expose it")
case("proc/show_procedures_has_labels", ["SHOW PROCEDURES YIELD name WHERE name = 'db.labels' RETURN name"])
case("proc/show_functions_has_abs", ["SHOW FUNCTIONS YIELD name WHERE name = 'abs' RETURN name"])
case("proc/show_databases", ["SHOW DATABASES YIELD name, type RETURN name, type ORDER BY name"], known="Warp lists 'neo4j' and 'system' with static attributes; column extras differ")
case("proc/show_current_user_absent", ["SHOW CURRENT USER"], known="Warp does not implement Neo4j's security commands")
case("proc/create_database", ["CREATE DATABASE other"], known="multi-database administration (CREATE DATABASE) is not supported: a Warp backend is the database")
case("proc/apoc_absent", ["RETURN apoc.version() AS v"])
case("proc/apoc_call_absent", ["CALL apoc.help('x')"])
case("proc/gds_absent", ["CALL gds.list()"])
case("proc/load_csv", ["LOAD CSV FROM 'file:///x.csv' AS row RETURN row"], known="LOAD CSV is not supported")
case("proc/explain_query", [{"q": "EXPLAIN MATCH (n) RETURN n", "cols_only": True}], known="EXPLAIN/PROFILE plans are not produced")

# ==================================================================================================================
# F. transactions
# ==================================================================================================================

case("tx/commit", [{"tx": [{"q": "CREATE (:T {i: 1})"}, {"q": "CREATE (:T {i: 2})"}], "end": "commit"}, "MATCH (n:T) RETURN count(n) AS c"])
case("tx/rollback", [{"tx": [{"q": "CREATE (:T {i: 1})"}, {"q": "MATCH (n:T) RETURN count(n) AS c"}], "end": "rollback"}, "MATCH (n:T) RETURN count(n) AS c"])
case("tx/read_own_writes", [{"tx": [{"q": "CREATE (:T {i: 1})"}, {"q": "MATCH (n:T) RETURN n.i AS i"}, {"q": "MATCH (n:T) SET n.i = 2"}, {"q": "MATCH (n:T) RETURN n.i AS i"}], "end": "commit"}])
case("tx/delete_in_tx", [
    "CREATE (:T {i: 1}), (:T {i: 2})", {"tx": [{"q": "MATCH (n:T {i: 1}) DELETE n"}, {"q": "MATCH (n:T) RETURN count(n) AS c"}], "end": "rollback"}, "MATCH (n:T) RETURN count(n) AS c"])
case("tx/error_inside_tx", [{"tx": [{"q": "CREATE (:T {i: 1})"}, {"q": "RETURN 1/0 AS x"}, {"q": "CREATE (:T {i: 3})"}], "end": "commit"}, "MATCH (n:T) RETURN count(n) AS c"])
case("tx/syntax_error_inside_tx", [{"tx": [{"q": "CREATE (:T {i: 1})"}, {"q": "RETRUN 1"}, {"q": "CREATE (:T {i: 3})"}], "end": "commit"}, "MATCH (n:T) RETURN count(n) AS c"])
case("tx/constraint_error_in_tx", ["CREATE CONSTRAINT c1 FOR (n:T) REQUIRE n.i IS UNIQUE", "CREATE (:T {i: 1})", {"tx": [{"q": "CREATE (:T {i: 2})"}, {"q": "CREATE (:T {i: 1})"}], "end": "commit"},
                                    "MATCH (n:T) RETURN n.i AS i ORDER BY i"])
case("tx/multiple_results_open", [{"stream": ["UNWIND range(1, 5) AS i RETURN i", "UNWIND range(10, 12) AS j RETURN j"]}])
case("tx/write_then_read_streams", [{"tx": [{"q": "UNWIND range(1, 3) AS i CREATE (:T {i: i}) RETURN i"}, {"q": "MATCH (n:T) RETURN count(n) AS c"}], "end": "commit"}])
case("tx/large_tx", [{"tx": [{"q": "UNWIND range(1, 500) AS i CREATE (:T {i: i})"}, {"q": "MATCH (n:T) RETURN count(n) AS c"}], "end": "commit"}])
case("tx/many_statements", [{"tx": [{"q": "CREATE (:T {i: %d})" % i} for i in range(20)] + [{"q": "MATCH (n:T) RETURN count(n) AS c"}], "end": "commit"}])
case("tx/read_only_write_error", [{"tx": [{"q": "CREATE (:T)"}], "end": "commit", "access": "read"}])
case("tx/execute_write_retry", [{"managed": "write", "q": "CREATE (:T {i: 1}) RETURN 1 AS x"}, {"managed": "read", "q": "MATCH (n:T) RETURN count(n) AS c"}])
case("tx/managed_write_error", [{"managed": "write", "q": "RETURN 1/0 AS x"}])
case("tx/tx_metadata", [{"tx": [{"q": "RETURN 1 AS x"}], "end": "commit", "metadata": {"app": "diff"}}])
case("tx/timeout_ok", [{"tx": [{"q": "RETURN 1 AS x"}], "end": "commit", "timeout": 30}])
case("tx/timeout_exceeded", [{"tx": [{"q": "UNWIND range(1, 100000000) AS x WITH x WHERE x < 0 RETURN count(*) AS c"}], "end": "commit", "timeout": 0.2}],
     known="Warp checks the transaction timeout between clauses / rows in Java; the statement fails with the same status class but a different code text")
case("tx/rollback_created_index", [{"tx": [{"q": "CREATE INDEX i1 FOR (n:T) ON (n.a)"}], "end": "rollback"}, "SHOW INDEXES YIELD name WHERE name = 'i1' RETURN name"])
case("tx/commit_after_rollback_session_reuse", [{"tx": [{"q": "CREATE (:T)"}], "end": "rollback"}, {"tx": [{"q": "CREATE (:T)"}], "end": "commit"}, "MATCH (n:T) RETURN count(n) AS c"])
case("tx/results_after_commit", [{"tx": [{"q": "UNWIND range(1, 3) AS i RETURN i"}], "end": "commit", "consume_after": True}])

# ==================================================================================================================
# G. streaming / driver / protocol behaviour
# ==================================================================================================================

for n in (1, 2, 3, 100):
    case("stream/fetch_size_%d" % n, [{"fetch": n, "q": "UNWIND range(1, 10) AS i RETURN i ORDER BY i", "ordered": True}])
case("stream/fetch_write", [{"fetch": 2, "q": "UNWIND range(1, 5) AS i CREATE (n:S {i: i}) RETURN n.i AS i ORDER BY i"}, "MATCH (n:S) RETURN count(n) AS c"])
case("stream/consume_without_iterating", [{"q": "UNWIND range(1, 5) AS i CREATE (:S {i: i})", "no_iterate": True}, "MATCH (n:S) RETURN count(n) AS c"])
case("stream/discard_partial", [{"fetch": 2, "q": "UNWIND range(1, 10) AS i RETURN i", "take": 3}])
case("stream/large_result", [{"q": "UNWIND range(1, 20000) AS i RETURN i, 'row-' + toString(i) AS s", "count_only": True}])
case("stream/wide_row", [{"q": "RETURN " + ", ".join("%d AS c%d" % (i, i) for i in range(300)), "count_only": True}])
case("stream/big_value", [{"q": "RETURN reduce(s = '', i IN range(1, 20000) | s + 'abcde') AS v", "count_only": True}, {"q": "RETURN size(reduce(s = '', i IN range(1, 20000) | s + 'abcde')) AS n"}])
case("stream/error_after_partial", [{"q": "UNWIND [1, 2, 0] AS x RETURN 10 / x AS y"}, {"q": "RETURN 1 AS ok"}])
case("stream/session_reuse_after_error", [{"q": "RETRUN 1"}, {"q": "RETURN 1 AS ok"}, {"q": "RETURN 1/0 AS x"}, {"q": "RETURN 2 AS ok"}])
case("stream/summary_counters", [{"q": "CREATE (a:A {x: 1})-[:R]->(b:B) SET a.y = 2", "meta": "summary"}])
case("stream/summary_query_type", [{"q": "RETURN 1 AS x", "meta": "summary"}, {"q": "CREATE (:A)", "meta": "summary"}, {"q": "CREATE (n:A) RETURN n", "meta": "summary"},
                                   {"q": "CREATE INDEX i1 FOR (n:A) ON (n.x)", "meta": "summary"}, {"q": "MATCH (n:A) DETACH DELETE n", "meta": "summary"}])
case("stream/summary_database", [{"q": "RETURN 1 AS x", "meta": "summary"}])
case("meta/server_agent", [{"meta": "server_agent"}])
case("meta/verify_connectivity", [{"meta": "verify"}])
case("meta/protocol_version", [{"meta": "protocol"}])
case("meta/routing_driver", [{"meta": "routing_driver"}])
case("meta/multiple_sessions", [{"meta": "two_sessions"}])
case("meta/bookmarks", [{"meta": "bookmarks"}])
case("meta/database_home", [{"meta": "database", "database": "neo4j"}])
case("meta/database_unknown", [{"meta": "database", "database": "doesnotexist"}], known="Warp routes the Bolt `db` field through connect-time routing: an unmatched name falls through to the default backend unless a strict route rejects it")
case("meta/database_system", [{"meta": "database", "database": "system"}])
case("meta/auth_basic", [{"meta": "auth", "user": "neo4j", "password": "x"}])
case("meta/reset_recovery", [{"meta": "reset_recovery"}])
case("meta/concurrent_sessions_isolation", [{"meta": "concurrent_isolation"}])
case("meta/pipelined_runs", [{"meta": "pipelined"}])
case("meta/close_mid_stream", [{"meta": "close_mid_stream"}])
case("meta/int_edge_via_wire", [{"q": "RETURN 9223372036854775807 AS a, -9223372036854775808 AS b, 0 AS c, -16 AS d, -17 AS e, 127 AS f, 128 AS g"}])
case("meta/float_edge_via_wire", [{"q": "RETURN 0.0/0.0 AS nan, 1/0.0 AS inf, -1/0.0 AS ninf, 1e308 AS big, 5e-324 AS tiny, -0.0 AS nz"}])
case("meta/unicode_via_wire", [{"q": "RETURN 'héllo 中文 \U0001F600' AS s, size('\U0001F600') AS n"}])
case("meta/node_struct_shape", [{"q": "CREATE (n:A:B {p: 1}) RETURN n", "meta": "entity"}])
case("meta/rel_struct_shape", [{"q": "CREATE (a)-[r:R {w: 1}]->(b) RETURN r", "meta": "entity"}])
case("meta/path_struct_shape", [{"q": "CREATE p = (a:A)-[:R]->(b:B)<-[:S]-(c:C) RETURN p", "meta": "entity"}])
case("meta/element_ids_stable", [{"meta": "element_ids"}])


def all_cases():
    seen = set()
    for c in CASES:
        assert c["name"] not in seen, "duplicate case name " + c["name"]
        seen.add(c["name"])
    return CASES

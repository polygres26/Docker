"""Differential test corpus: sequences of OpenSearch requests replayed against a real OpenSearch and Warp.

Each case is {"name", "steps": [step...], "known": <reason, optional>}; a step is a dict with method/path/body[/params/
ndjson/opts/cmp]. `opts`: ordered (compare hit order), scores (compare _score), shards (compare _shards), nosort.
Search cases share one dataset (`shop`, 14 documents with text/keyword/number/date/bool/geo/nested fields).
"""

CASES = []


def S(method, path, body=None, **kw):
    d = {"method": method, "path": path, "body": body}
    d.update(kw)
    return d


def add(name, steps, known=None):
    CASES.append({"name": name, "steps": steps, **({"known": known} if known else {})})


# --------------------------------------------------------------------------- dataset

SHOP_MAPPING = {"mappings": {"properties": {
    "title": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
    "brand": {"type": "keyword"}, "category": {"type": "keyword"},
    "price": {"type": "double"}, "qty": {"type": "integer"}, "in_stock": {"type": "boolean"},
    "created": {"type": "date"}, "tags": {"type": "keyword"}, "desc": {"type": "text"},
    "loc": {"type": "geo_point"},
    "attrs": {"properties": {"color": {"type": "keyword"}, "size": {"type": "long"}}},
    "reviews": {"type": "nested", "properties": {"user": {"type": "keyword"}, "stars": {"type": "long"}, "text": {"type": "text"}}},
}}}

DOCS = [
    ("1", {"title": "Wireless Mouse", "brand": "logi", "category": "electronics", "price": 25.99, "qty": 100, "in_stock": True,
           "created": "2024-01-05", "tags": ["office", "wireless"], "desc": "A comfortable wireless mouse for office work",
           "loc": {"lat": 40.0, "lon": -70.0}, "attrs": {"color": "black", "size": 3},
           "reviews": [{"user": "amy", "stars": 5, "text": "great mouse"}, {"user": "bob", "stars": 2, "text": "battery drains fast"}]}),
    ("2", {"title": "Mechanical Keyboard", "brand": "keychron", "category": "electronics", "price": 89.99, "qty": 40, "in_stock": True,
           "created": "2024-02-10", "tags": ["office", "gaming"], "desc": "A responsive mechanical keyboard with blue switches",
           "loc": {"lat": 41.0, "lon": -71.0}, "attrs": {"color": "white", "size": 5},
           "reviews": [{"user": "amy", "stars": 4, "text": "loud but fun keyboard"}]}),
    ("3", {"title": "Garden Hose", "brand": "gardena", "category": "home", "price": 15.5, "qty": 0, "in_stock": False,
           "created": "2023-11-20", "tags": ["garden"], "desc": "A durable rubber garden hose, 25 meters long",
           "loc": {"lat": 35.0, "lon": -80.0}, "attrs": {"color": "green", "size": 25}, "reviews": []}),
    ("4", {"title": "Gaming Mouse Pad", "brand": "logi", "category": "electronics", "price": 12.0, "qty": 250, "in_stock": True,
           "created": "2024-03-01", "tags": ["gaming"], "desc": "Large mouse pad for gaming with stitched edges",
           "loc": {"lat": 40.5, "lon": -70.5}, "attrs": {"color": "black", "size": 40},
           "reviews": [{"user": "cat", "stars": 3, "text": "okay pad"}]}),
    ("5", {"title": "Espresso Machine", "brand": "delonghi", "category": "kitchen", "price": 199.0, "qty": 8, "in_stock": True,
           "created": "2023-12-15", "tags": ["coffee", "kitchen"], "desc": "Compact espresso machine with a steam wand and milk frother",
           "loc": {"lat": 45.0, "lon": -73.0}, "attrs": {"color": "silver", "size": 30},
           "reviews": [{"user": "bob", "stars": 5, "text": "best espresso at home"}, {"user": "dan", "stars": 4, "text": "great coffee"}]}),
    ("6", {"title": "Coffee Grinder", "brand": "delonghi", "category": "kitchen", "price": 49.5, "qty": 15, "in_stock": True,
           "created": "2024-01-25", "tags": ["coffee"], "desc": "Burr coffee grinder with adjustable grind size",
           "loc": {"lat": 45.5, "lon": -73.5}, "attrs": {"color": "black", "size": 20}, "reviews": []}),
    ("7", {"title": "Running Shoes", "brand": "nike", "category": "sports", "price": 120.0, "qty": 60, "in_stock": True,
           "created": "2024-02-28", "tags": ["running", "shoes"], "desc": "Lightweight running shoes with breathable mesh",
           "loc": {"lat": 34.0, "lon": -118.0}, "attrs": {"color": "red", "size": 42},
           "reviews": [{"user": "eve", "stars": 5, "text": "very light and fast"}]}),
    ("8", {"title": "Yoga Mat", "brand": "nike", "category": "sports", "price": 30.0, "qty": 75, "in_stock": True,
           "created": "2023-10-10", "tags": ["yoga"], "desc": "Non slip yoga mat, extra thick for comfort",
           "loc": {"lat": 34.5, "lon": -118.5}, "attrs": {"color": "purple", "size": 6}, "reviews": []}),
    ("9", {"title": "Bluetooth Speaker", "brand": "jbl", "category": "electronics", "price": 59.99, "qty": 30, "in_stock": True,
           "created": "2024-03-15", "tags": ["wireless", "music"], "desc": "Portable bluetooth speaker with deep bass and waterproof body",
           "loc": {"lat": 51.0, "lon": 0.0}, "attrs": {"color": "blue", "size": 8},
           "reviews": [{"user": "amy", "stars": 4, "text": "loud and clear"}, {"user": "eve", "stars": 1, "text": "broke after a week"}]}),
    ("10", {"title": "Noise Cancelling Headphones", "brand": "sony", "category": "electronics", "price": 299.99, "qty": 12,
            "in_stock": False, "created": "2024-04-01", "tags": ["wireless", "music", "travel"],
            "desc": "Wireless noise cancelling headphones with thirty hour battery life",
            "loc": {"lat": 35.6, "lon": 139.7}, "attrs": {"color": "black", "size": 7},
            "reviews": [{"user": "dan", "stars": 5, "text": "silence is golden"}]}),
    ("11", {"title": "Camping Tent", "brand": "coleman", "category": "sports", "price": 149.0, "qty": 5, "in_stock": True,
            "created": "2023-09-05", "tags": ["camping", "outdoor"], "desc": "Four person camping tent, waterproof with easy setup",
            "loc": {"lat": 39.0, "lon": -105.0}, "attrs": {"color": "green", "size": 4}, "reviews": []}),
    ("12", {"title": "Electric Kettle", "brand": "philips", "category": "kitchen", "price": 35.0, "qty": 90, "in_stock": True,
            "created": "2024-01-15", "tags": ["kitchen", "tea"], "desc": "Fast boil electric kettle with auto shut off",
            "loc": {"lat": 52.0, "lon": 5.0}, "attrs": {"color": "silver", "size": 2},
            "reviews": [{"user": "cat", "stars": 4, "text": "boils fast"}]}),
    ("13", {"title": "Wireless Charger", "brand": "anker", "category": "electronics", "price": 19.99, "qty": 200, "in_stock": True,
            "created": "2024-02-01", "tags": ["wireless", "office"], "desc": "Fast wireless charger pad for phones and earbuds",
            "loc": {"lat": 37.0, "lon": -122.0}, "attrs": {"color": "white", "size": 10}, "reviews": []}),
    ("14", {"title": "Desk Lamp", "brand": "ikea", "category": "home", "price": 22.5, "qty": None, "in_stock": True,
            "created": "2024-03-20", "tags": [], "desc": "Adjustable desk lamp with warm light",
            "loc": {"lat": 59.0, "lon": 18.0}, "attrs": {"color": "white"}, "reviews": []}),
]


def shop_setup(index="shop", settings=None):
    body = dict(SHOP_MAPPING)
    if settings:
        body = {**body, "settings": settings}
    lines = []
    for _id, doc in DOCS:
        lines.append({"index": {"_index": index, "_id": _id}})
        lines.append(doc)
    return [S("PUT", "/" + index, body, cmp=False),
            S("POST", "/_bulk", lines, ndjson=True, params={"refresh": "true"}, cmp=False)]


def searches(name, body, opts=None, index="shop", params=None, known=None):
    st = S("POST", "/%s/_search" % index, body, opts=opts or {}, params=params or {})
    add(name, shop_setup(index) + [st], known)


# --------------------------------------------------------------------------- query DSL

O = {"ordered": True}
OS = {"ordered": True, "scores": True}

searches("q/match_all", {"query": {"match_all": {}}, "size": 20, "sort": [{"price": "asc"}]}, O)
searches("q/match_none", {"query": {"match_none": {}}})
searches("q/match_default_or", {"query": {"match": {"title": "wireless mouse"}}}, OS)
searches("q/match_operator_and", {"query": {"match": {"title": {"query": "wireless mouse", "operator": "and"}}}}, OS)
searches("q/match_msm", {"query": {"match": {"desc": {"query": "wireless mouse office comfortable", "minimum_should_match": "75%"}}}}, OS)
searches("q/match_msm_int", {"query": {"match": {"desc": {"query": "wireless mouse office comfortable", "minimum_should_match": 2}}}}, OS)
searches("q/match_case_insensitive_analysis", {"query": {"match": {"title": "WIRELESS"}}}, OS)
searches("q/match_stemless_plural", {"query": {"match": {"title": "mice"}}})
searches("q/match_on_keyword_exact", {"query": {"match": {"brand": "logi"}}}, OS)
searches("q/match_on_keyword_case_mismatch", {"query": {"match": {"brand": "LOGI"}}})
searches("q/match_number_field", {"query": {"match": {"qty": 100}}}, OS)
searches("q/match_bool_field", {"query": {"match": {"in_stock": False}}}, OS)
searches("q/match_fuzzy", {"query": {"match": {"title": {"query": "wirelss", "fuzziness": "AUTO"}}}}, OS)
searches("q/match_fuzziness_1", {"query": {"match": {"title": {"query": "keybord", "fuzziness": 1}}}}, OS)
searches("q/match_zero_terms_all", {"query": {"match": {"title": {"query": "the", "zero_terms_query": "all"}}}, "size": 3}, O)
searches("q/match_phrase", {"query": {"match_phrase": {"desc": "wireless mouse"}}}, OS)
searches("q/match_phrase_no_match", {"query": {"match_phrase": {"desc": "mouse wireless"}}})
searches("q/match_phrase_slop", {"query": {"match_phrase": {"desc": {"query": "mouse office", "slop": 2}}}}, OS)
searches("q/match_phrase_prefix", {"query": {"match_phrase_prefix": {"desc": "wireless mo"}}}, OS)
searches("q/match_bool_prefix", {"query": {"match_bool_prefix": {"desc": "portable blue"}}}, OS)
searches("q/multi_match_best_fields", {"query": {"multi_match": {"query": "wireless", "fields": ["title", "desc"]}}}, OS)
searches("q/multi_match_boost", {"query": {"multi_match": {"query": "wireless", "fields": ["title^3", "desc"]}}}, OS)
searches("q/multi_match_most_fields", {"query": {"multi_match": {"query": "wireless mouse", "fields": ["title", "desc"], "type": "most_fields"}}}, OS)
searches("q/multi_match_phrase", {"query": {"multi_match": {"query": "wireless mouse", "fields": ["title", "desc"], "type": "phrase"}}}, OS)
searches("q/multi_match_tie_breaker", {"query": {"multi_match": {"query": "coffee", "fields": ["title", "desc"], "tie_breaker": 0.3}}}, OS)
searches("q/multi_match_wildcard_fields", {"query": {"multi_match": {"query": "coffee", "fields": ["t*", "desc"]}}}, OS)
searches("q/multi_match_and", {"query": {"multi_match": {"query": "fast wireless", "fields": ["title", "desc"], "operator": "and"}}}, OS)
searches("q/term_keyword", {"query": {"term": {"brand": "logi"}}}, OS)
searches("q/term_keyword_long_form", {"query": {"term": {"brand": {"value": "logi", "boost": 2.0}}}}, OS)
searches("q/term_text_lowercase_token", {"query": {"term": {"title": "wireless"}}}, OS)
searches("q/term_text_uppercase_no_match", {"query": {"term": {"title": "Wireless"}}})
searches("q/term_number", {"query": {"term": {"qty": 100}}}, OS)
searches("q/term_double", {"query": {"term": {"price": 25.99}}}, OS)
searches("q/term_boolean", {"query": {"term": {"in_stock": False}}}, OS)
searches("q/term_date", {"query": {"term": {"created": "2024-01-05"}}}, OS)
searches("q/term_array_value", {"query": {"term": {"tags": "gaming"}}}, OS)
searches("q/term_case_insensitive", {"query": {"term": {"brand": {"value": "LOGI", "case_insensitive": True}}}}, OS)
searches("q/term_subfield_raw", {"query": {"term": {"title.raw": "Wireless Mouse"}}}, OS)
searches("q/term_object_field", {"query": {"term": {"attrs.color": "black"}}}, OS)
searches("q/terms", {"query": {"terms": {"brand": ["logi", "nike"]}}}, OS)
searches("q/terms_numbers", {"query": {"terms": {"qty": [100, 250]}}}, OS)
searches("q/range_numeric", {"query": {"range": {"price": {"gte": 20, "lt": 60}}}, "sort": [{"price": "asc"}]}, O)
searches("q/range_numeric_gt_lte", {"query": {"range": {"price": {"gt": 25.99, "lte": 59.99}}}, "sort": [{"price": "asc"}]}, O)
searches("q/range_date", {"query": {"range": {"created": {"gte": "2024-01-01", "lte": "2024-02-01"}}}, "sort": [{"created": "asc"}]}, O)
searches("q/range_date_math", {"query": {"range": {"created": {"gte": "2024-03-01||-1M/M", "lt": "2024-03-01||/M"}}}, "sort": [{"created": "asc"}]}, O)
searches("q/range_date_format", {"query": {"range": {"created": {"gte": "01/02/2024", "format": "dd/MM/yyyy"}}}, "sort": [{"created": "asc"}]}, O)
searches("q/range_keyword", {"query": {"range": {"brand": {"gte": "j", "lt": "n"}}}, "sort": [{"brand": "asc"}, {"_id": "asc"}]}, O)
searches("q/range_on_boolean_field", {"query": {"range": {"qty": {"gte": 0}}}, "size": 20, "sort": [{"_id": "asc"}]}, O)
searches("q/exists", {"query": {"exists": {"field": "qty"}}, "size": 30}, OS)
searches("q/exists_missing_null_and_empty_array", {"query": {"exists": {"field": "tags"}}, "size": 30}, OS)
searches("q/exists_object", {"query": {"exists": {"field": "attrs"}}, "size": 30}, OS)
searches("q/exists_not_mapped", {"query": {"exists": {"field": "nope"}}})
searches("q/ids", {"query": {"ids": {"values": ["1", "5", "99"]}}}, OS)
searches("q/prefix", {"query": {"prefix": {"brand": "de"}}}, OS)
searches("q/prefix_text_lowercase", {"query": {"prefix": {"title": "wire"}}}, OS)
searches("q/prefix_case_insensitive", {"query": {"prefix": {"brand": {"value": "DE", "case_insensitive": True}}}}, OS)
searches("q/wildcard", {"query": {"wildcard": {"brand": "*ik*"}}}, OS)
searches("q/wildcard_question", {"query": {"wildcard": {"brand": "jb?"}}}, OS)
searches("q/wildcard_text_token", {"query": {"wildcard": {"title": "wire*"}}}, OS)
searches("q/regexp", {"query": {"regexp": {"brand": "d.*ghi"}}}, OS)
searches("q/regexp_class", {"query": {"regexp": {"brand": "[jk][a-z]+"}}}, OS)
searches("q/fuzzy", {"query": {"fuzzy": {"brand": {"value": "nikee", "fuzziness": 1}}}}, OS)
searches("q/fuzzy_text", {"query": {"fuzzy": {"title": {"value": "mose"}}}}, OS)
searches("q/bool_must_filter", {"query": {"bool": {"must": [{"match": {"desc": "wireless"}}], "filter": [{"term": {"in_stock": True}}]}}}, OS)
searches("q/bool_should", {"query": {"bool": {"should": [{"match": {"title": "mouse"}}, {"match": {"title": "keyboard"}}]}}}, OS)
searches("q/bool_should_msm", {"query": {"bool": {"should": [{"term": {"tags": "wireless"}}, {"term": {"tags": "office"}}, {"term": {"tags": "music"}}],
                                                   "minimum_should_match": 2}}}, OS)
searches("q/bool_must_not", {"query": {"bool": {"must": [{"match_all": {}}], "must_not": [{"term": {"category": "electronics"}}]}}, "size": 30,
                             "sort": [{"_id": "asc"}]}, O)
searches("q/bool_only_must_not", {"query": {"bool": {"must_not": [{"term": {"category": "electronics"}}]}}, "size": 30}, OS)
searches("q/bool_only_filter_score_zero", {"query": {"bool": {"filter": [{"term": {"category": "kitchen"}}]}}}, OS)
searches("q/bool_filter_plus_should", {"query": {"bool": {"filter": [{"term": {"category": "electronics"}}], "should": [{"match": {"title": "mouse"}}]}}}, OS)
searches("q/bool_nested_bool", {"query": {"bool": {"must": [{"bool": {"should": [{"term": {"brand": "nike"}}, {"term": {"brand": "logi"}}]}},
                                                                {"range": {"price": {"lte": 50}}}]}}}, OS)
searches("q/bool_empty", {"query": {"bool": {}}, "size": 3, "sort": [{"_id": "asc"}]}, OS)
searches("q/bool_boost", {"query": {"bool": {"must": [{"match": {"title": "mouse"}}], "boost": 3}}}, OS)
searches("q/constant_score", {"query": {"constant_score": {"filter": {"term": {"brand": "nike"}}, "boost": 1.7}}}, OS)
searches("q/dis_max", {"query": {"dis_max": {"queries": [{"match": {"title": "coffee"}}, {"match": {"desc": "coffee"}}], "tie_breaker": 0.7}}}, OS)
searches("q/boosting", {"query": {"boosting": {"positive": {"match": {"desc": "wireless"}}, "negative": {"term": {"tags": "office"}}, "negative_boost": 0.2}}}, OS)
searches("q/function_score_field_value_factor", {"query": {"function_score": {"query": {"match": {"desc": "wireless"}},
                                                  "field_value_factor": {"field": "qty", "modifier": "log1p", "missing": 1}}}}, OS)
searches("q/function_score_weight_filter", {"query": {"function_score": {"query": {"match_all": {}}, "functions": [
    {"filter": {"term": {"brand": "nike"}}, "weight": 5}], "boost_mode": "sum"}}, "size": 30}, OS)
searches("q/nested_basic", {"query": {"nested": {"path": "reviews", "query": {"bool": {"must": [{"term": {"reviews.user": "amy"}}, {"range": {"reviews.stars": {"gte": 5}}}]}}}}}, OS)
searches("q/nested_no_cross_object_match", {"query": {"nested": {"path": "reviews", "query": {"bool": {"must": [{"term": {"reviews.user": "bob"}}, {"term": {"reviews.stars": 5}}]}}}}}, OS)
searches("q/nested_score_mode_max", {"query": {"nested": {"path": "reviews", "score_mode": "max", "query": {"match": {"reviews.text": "great loud fast"}}}}}, OS)
searches("q/nested_inner_hits", {"query": {"nested": {"path": "reviews", "query": {"range": {"reviews.stars": {"gte": 4}}}, "inner_hits": {}}}, "_source": False}, OS)
searches("q/nested_field_outside_nested_query_no_match", {"query": {"term": {"reviews.user": "amy"}}})
searches("q/query_string_simple_term", {"query": {"query_string": {"query": "wireless", "default_field": "title"}}}, OS)
searches("q/query_string_and_or", {"query": {"query_string": {"query": "title:mouse AND desc:office", "default_operator": "OR"}}}, OS)
searches("q/query_string_phrase", {"query": {"query_string": {"query": "\"wireless mouse\"", "fields": ["title", "desc"]}}}, OS)
searches("q/query_string_field_group", {"query": {"query_string": {"query": "brand:(nike OR logi)"}}}, OS)
searches("q/query_string_not", {"query": {"query_string": {"query": "category:electronics AND NOT brand:logi"}}}, OS)
searches("q/query_string_plus_minus", {"query": {"query_string": {"query": "+desc:wireless -tags:office", "default_field": "title"}}}, OS)
searches("q/query_string_range", {"query": {"query_string": {"query": "price:[20 TO 60]"}}}, OS)
searches("q/query_string_range_exclusive", {"query": {"query_string": {"query": "price:{20 TO 60}"}}}, OS)
searches("q/query_string_comparison", {"query": {"query_string": {"query": "price:>=100"}}}, OS)
searches("q/query_string_wildcard", {"query": {"query_string": {"query": "brand:ni*"}}}, OS)
searches("q/query_string_fuzzy", {"query": {"query_string": {"query": "title:wirelss~1"}}}, OS)
searches("q/query_string_boost", {"query": {"query_string": {"query": "title:mouse^3 OR desc:office", "default_field": "title"}}}, OS)
searches("q/query_string_exists", {"query": {"query_string": {"query": "_exists_:qty"}}, "size": 30}, OS)
searches("q/query_string_default_wildcard_all_fields", {"query": {"query_string": {"query": "coffee"}}}, OS)
searches("q/query_string_bad_syntax", {"query": {"query_string": {"query": "title:(mouse"}}})
searches("q/simple_query_string", {"query": {"simple_query_string": {"query": "wireless +mouse -pad", "fields": ["title", "desc"]}}, "size": 30}, OS)
searches("q/simple_query_string_negation_is_or_not", {"query": {"simple_query_string": {"query": "mouse -pad", "fields": ["title"]}}, "size": 30}, OS)
searches("q/simple_query_string_and_default_operator", {"query": {"simple_query_string": {"query": "wireless mouse", "fields": ["title", "desc"], "default_operator": "and"}}}, OS)
searches("q/simple_query_string_group", {"query": {"simple_query_string": {"query": "(mouse | keyboard) -pad", "fields": ["title"]}}, "size": 30}, OS)
searches("q/simple_query_string_or_pipe", {"query": {"simple_query_string": {"query": "keyboard | kettle", "fields": ["title"]}}}, OS)
searches("q/simple_query_string_phrase_prefix", {"query": {"simple_query_string": {"query": "\"wireless mouse\" espre*", "fields": ["title", "desc"]}}}, OS)
searches("q/uri_q_param", None, OS, params={"q": "title:mouse"})
searches("q/uri_q_default_operator", None, OS, params={"q": "wireless mouse", "df": "title", "default_operator": "AND"})
searches("q/geo_distance", {"query": {"geo_distance": {"distance": "200km", "loc": {"lat": 40.0, "lon": -70.0}}}}, OS)
searches("q/geo_bounding_box", {"query": {"geo_bounding_box": {"loc": {"top_left": {"lat": 46, "lon": -75}, "bottom_right": {"lat": 40, "lon": -65}}}}}, OS)
searches("q/wrapper_query", {"query": {"wrapper": {"query": "eyJ0ZXJtIjp7ImJyYW5kIjoibmlrZSJ9fQ=="}}}, OS)
searches("q/unknown_query_error", {"query": {"foobar": {}}})
searches("q/malformed_bool_error", {"query": {"bool": {"musst": []}}})
searches("q/malformed_two_queries_error", {"query": {"match_all": {}, "term": {"a": 1}}})
searches("q/term_multi_field_error", {"query": {"term": {"a": 1, "b": 2}}})
searches("q/range_bad_date_error", {"query": {"range": {"created": {"gte": "notadate"}}}})
searches("q/match_on_unmapped", {"query": {"match": {"nothing": "x"}}})
searches("q/term_on_unmapped", {"query": {"term": {"nothing": "x"}}})

# sorting / paging / source
searches("s/sort_multi_key", {"query": {"match_all": {}}, "size": 20, "sort": [{"category": "asc"}, {"price": "desc"}]}, O)
searches("s/sort_desc_keyword", {"query": {"match_all": {}}, "size": 20, "sort": [{"brand": "desc"}, {"_id": "asc"}]}, O)
searches("s/sort_missing_last_default", {"query": {"match_all": {}}, "size": 20, "sort": [{"qty": "asc"}, {"_id": "asc"}]}, O)
searches("s/sort_missing_first", {"query": {"match_all": {}}, "size": 20, "sort": [{"qty": {"order": "asc", "missing": "_first"}}, {"_id": "asc"}]}, O)
searches("s/sort_missing_value", {"query": {"match_all": {}}, "size": 20, "sort": [{"qty": {"order": "asc", "missing": 50}}, {"_id": "asc"}]}, O)
searches("s/sort_array_mode_min", {"query": {"exists": {"field": "tags"}}, "size": 20, "sort": [{"tags": {"order": "asc", "mode": "min"}}, {"_id": "asc"}]}, O)
searches("s/sort_array_mode_max", {"query": {"exists": {"field": "tags"}}, "size": 20, "sort": [{"tags": {"order": "asc", "mode": "max"}}, {"_id": "asc"}]}, O)
searches("s/sort_date", {"query": {"match_all": {}}, "size": 20, "sort": [{"created": "desc"}]}, O)
searches("s/sort_boolean", {"query": {"match_all": {}}, "size": 20, "sort": [{"in_stock": "asc"}, {"_id": "asc"}]}, O)
searches("s/sort_score_then_field", {"query": {"match": {"desc": "wireless mouse"}}, "sort": ["_score", {"price": "asc"}]}, OS)
searches("s/sort_by_id", {"query": {"match_all": {}}, "size": 20, "sort": [{"_id": "desc"}]}, O)
searches("s/sort_text_field_error", {"query": {"match_all": {}}, "sort": [{"title": "asc"}]})
searches("s/sort_unmapped_error", {"query": {"match_all": {}}, "sort": [{"nothing": "asc"}]})
searches("s/sort_unmapped_type", {"query": {"match_all": {}}, "size": 3, "sort": [{"nothing": {"order": "asc", "unmapped_type": "long"}}, {"_id": "asc"}]}, O)
searches("s/sort_nested", {"query": {"exists": {"field": "reviews.stars"}}, "size": 20, "sort": [{"reviews.stars": {"order": "desc", "nested": {"path": "reviews"}}}, {"_id": "asc"}]}, O)
searches("s/from_size", {"query": {"match_all": {}}, "from": 3, "size": 4, "sort": [{"price": "asc"}]}, O)
searches("s/size_zero_total_only", {"query": {"match": {"desc": "wireless"}}, "size": 0})
searches("s/from_beyond_total", {"query": {"match_all": {}}, "from": 100, "size": 5})
searches("s/track_total_hits_false", {"query": {"match_all": {}}, "size": 1, "track_total_hits": False, "sort": [{"_id": "asc"}]}, O)
searches("s/track_total_hits_int", {"query": {"match_all": {}}, "size": 1, "track_total_hits": 5, "sort": [{"_id": "asc"}]}, O)
searches("s/track_scores_with_sort", {"query": {"match": {"desc": "wireless"}}, "sort": [{"price": "asc"}], "track_scores": True}, OS)
searches("s/search_after", {"query": {"match_all": {}}, "size": 4, "sort": [{"price": "asc"}, {"_id": "asc"}], "search_after": [30.0, "8"]}, O)
searches("s/search_after_keyword", {"query": {"match_all": {}}, "size": 4, "sort": [{"brand": "asc"}, {"_id": "asc"}], "search_after": ["keychron", "2"]}, O)
searches("s/source_false", {"query": {"ids": {"values": ["1"]}}, "_source": False})
searches("s/source_includes", {"query": {"ids": {"values": ["1"]}}, "_source": ["title", "attrs.*"]})
searches("s/source_object_include_exclude", {"query": {"ids": {"values": ["1"]}}, "_source": {"includes": ["title", "attrs"], "excludes": ["attrs.size"]}})
searches("s/source_string", {"query": {"ids": {"values": ["1"]}}, "_source": "brand"})
searches("s/source_url_param", {"query": {"ids": {"values": ["1"]}}}, params={"_source": "title,price"})
searches("s/docvalue_fields", {"query": {"ids": {"values": ["1"]}}, "docvalue_fields": ["price", "created", "brand"], "_source": False})
searches("s/fields_api", {"query": {"ids": {"values": ["1"]}}, "fields": ["title", "created", "attrs.color"], "_source": False})
searches("s/stored_fields_none", {"query": {"ids": {"values": ["1"]}}, "stored_fields": "_none_"})
searches("s/version_and_seq_no", {"query": {"ids": {"values": ["1"]}}, "version": True, "seq_no_primary_term": True, "_source": False}, {"seq": True})
searches("s/min_score", {"query": {"match": {"desc": "wireless"}}, "min_score": 1.0}, OS)
searches("s/post_filter", {"query": {"match_all": {}}, "post_filter": {"term": {"brand": "nike"}}, "aggs": {"b": {"terms": {"field": "brand"}}}, "size": 5,
                            "sort": [{"_id": "asc"}]}, O)
searches("s/terminate_after", {"query": {"match_all": {}}, "terminate_after": 3, "size": 0})
searches("s/collapse", {"query": {"match_all": {}}, "collapse": {"field": "brand"}, "sort": [{"price": "desc"}], "size": 20}, O)
searches("s/highlight_basic", {"query": {"match": {"desc": "wireless"}}, "highlight": {"fields": {"desc": {}}}, "sort": [{"_id": "asc"}]}, O)
searches("s/highlight_tags", {"query": {"match": {"desc": "coffee"}}, "highlight": {"pre_tags": ["<b>"], "post_tags": ["</b>"], "fields": {"desc": {}}}, "sort": [{"_id": "asc"}]}, O)
searches("s/highlight_phrase", {"query": {"match_phrase": {"desc": "wireless mouse"}}, "highlight": {"fields": {"desc": {}}}}, O)
searches("s/highlight_multi_field", {"query": {"multi_match": {"query": "wireless", "fields": ["title", "desc"]}}, "highlight": {"fields": {"title": {}, "desc": {}}},
                                     "sort": [{"_id": "asc"}]}, O)
searches("s/highlight_whole_field", {"query": {"match": {"desc": "espresso"}}, "highlight": {"fields": {"desc": {"number_of_fragments": 0}}}}, O)
searches("s/highlight_wildcard_query", {"query": {"prefix": {"desc": "wire"}}, "highlight": {"fields": {"desc": {}}}, "sort": [{"_id": "asc"}]}, O)
searches("s/highlight_no_match_field_omitted", {"query": {"match": {"desc": "wireless"}}, "highlight": {"fields": {"title": {}}}, "sort": [{"_id": "asc"}]}, O)
searches("s/named_queries", {"query": {"bool": {"should": [{"match": {"title": {"query": "mouse", "_name": "m"}}}, {"term": {"brand": {"value": "keychron", "_name": "k"}}}]}}}, OS)
searches("s/indices_boost_two_indices", {"query": {"match": {"title": "mouse"}}}, OS)
searches("s/msearch_like_count", {"query": {"match": {"title": "mouse"}}, "size": 0}, O)
searches("s/unknown_top_level_key_error", {"query": {"match_all": {}}, "bogus": 1})
searches("s/negative_from_error", {"query": {"match_all": {}}, "from": -1})
searches("s/result_window_error", {"query": {"match_all": {}}, "from": 9999, "size": 100})
searches("s/knn_absent_field_error", {"query": {"knn": {"vec": {"vector": [1, 2], "k": 2}}}})

# --------------------------------------------------------------------------- aggregations

A = {"ordered": True}
searches("a/terms", {"size": 0, "aggs": {"b": {"terms": {"field": "brand"}}}}, A)
searches("a/terms_size_and_other", {"size": 0, "aggs": {"b": {"terms": {"field": "brand", "size": 2}}}}, A)
searches("a/terms_order_key", {"size": 0, "aggs": {"b": {"terms": {"field": "brand", "order": {"_key": "desc"}}}}}, A)
searches("a/terms_order_count_asc", {"size": 0, "aggs": {"b": {"terms": {"field": "category", "order": {"_count": "asc"}}}}}, A)
searches("a/terms_order_by_sub_agg", {"size": 0, "aggs": {"b": {"terms": {"field": "category", "order": {"avgp": "desc"}}, "aggs": {"avgp": {"avg": {"field": "price"}}}}}}, A)
searches("a/terms_min_doc_count", {"size": 0, "aggs": {"b": {"terms": {"field": "brand", "min_doc_count": 2}}}}, A)
searches("a/terms_missing", {"size": 0, "aggs": {"b": {"terms": {"field": "qty", "missing": 0, "size": 3}}}}, A)
searches("a/terms_include_exclude", {"size": 0, "aggs": {"b": {"terms": {"field": "brand", "include": "l.*|n.*"}}}}, A)
searches("a/terms_on_array_field", {"size": 0, "aggs": {"t": {"terms": {"field": "tags", "size": 20}}}}, A)
searches("a/terms_numeric", {"size": 0, "aggs": {"t": {"terms": {"field": "attrs.size", "size": 5}}}}, A)
searches("a/terms_boolean", {"size": 0, "aggs": {"t": {"terms": {"field": "in_stock"}}}}, A)
searches("a/terms_on_text_error", {"size": 0, "aggs": {"t": {"terms": {"field": "title"}}}})
searches("a/terms_on_text_raw_subfield", {"size": 0, "aggs": {"t": {"terms": {"field": "title.raw", "size": 3}}}}, A)
searches("a/terms_with_subaggs", {"size": 0, "aggs": {"c": {"terms": {"field": "category"}, "aggs": {"mx": {"max": {"field": "price"}}, "mn": {"min": {"field": "price"}},
                                                       "s": {"sum": {"field": "qty"}}}}}}, A)
searches("a/terms_typed_keys", {"size": 0, "aggs": {"c": {"terms": {"field": "category"}}, "m": {"max": {"field": "price"}}}}, A, params={"typed_keys": "true"})
searches("a/metrics_basic", {"size": 0, "aggs": {"mn": {"min": {"field": "price"}}, "mx": {"max": {"field": "price"}}, "sm": {"sum": {"field": "price"}},
                              "av": {"avg": {"field": "price"}}, "vc": {"value_count": {"field": "brand"}}}}, A)
searches("a/stats", {"size": 0, "aggs": {"s": {"stats": {"field": "price"}}}}, A)
searches("a/extended_stats", {"size": 0, "aggs": {"s": {"extended_stats": {"field": "price"}}}}, A)
searches("a/cardinality", {"size": 0, "aggs": {"c": {"cardinality": {"field": "brand"}}, "c2": {"cardinality": {"field": "tags"}}}}, A)
searches("a/percentiles", {"size": 0, "aggs": {"p": {"percentiles": {"field": "price"}}}}, A)
searches("a/percentiles_custom", {"size": 0, "aggs": {"p": {"percentiles": {"field": "price", "percents": [10, 50, 90]}}}}, A)
searches("a/percentile_ranks", {"size": 0, "aggs": {"p": {"percentile_ranks": {"field": "price", "values": [30, 100]}}}}, A)
searches("a/metric_on_missing_field", {"size": 0, "aggs": {"a": {"avg": {"field": "nothing"}}, "s": {"sum": {"field": "nothing"}}}}, A)
searches("a/metric_on_date", {"size": 0, "aggs": {"mn": {"min": {"field": "created"}}, "mx": {"max": {"field": "created"}}}}, A)
searches("a/histogram", {"size": 0, "aggs": {"h": {"histogram": {"field": "price", "interval": 50}}}}, A)
searches("a/histogram_min_doc_count", {"size": 0, "aggs": {"h": {"histogram": {"field": "price", "interval": 100, "min_doc_count": 1}}}}, A)
searches("a/histogram_extended_bounds", {"size": 0, "aggs": {"h": {"histogram": {"field": "qty", "interval": 100, "extended_bounds": {"min": -100, "max": 400}}}}}, A)
searches("a/histogram_offset", {"size": 0, "aggs": {"h": {"histogram": {"field": "price", "interval": 50, "offset": 10}}}}, A)
searches("a/date_histogram_month", {"size": 0, "aggs": {"d": {"date_histogram": {"field": "created", "calendar_interval": "month"}}}}, A)
searches("a/date_histogram_fixed", {"size": 0, "aggs": {"d": {"date_histogram": {"field": "created", "fixed_interval": "30d"}}}}, A)
searches("a/date_histogram_format", {"size": 0, "aggs": {"d": {"date_histogram": {"field": "created", "calendar_interval": "month", "format": "yyyy-MM"}}}}, A)
searches("a/date_histogram_quarter_year", {"size": 0, "aggs": {"q": {"date_histogram": {"field": "created", "calendar_interval": "quarter"}},
                                                               "y": {"date_histogram": {"field": "created", "calendar_interval": "year"}}}}, A)
searches("a/date_histogram_week", {"size": 0, "aggs": {"w": {"date_histogram": {"field": "created", "calendar_interval": "week", "min_doc_count": 1}}}}, A)
searches("a/date_histogram_tz", {"size": 0, "aggs": {"d": {"date_histogram": {"field": "created", "calendar_interval": "month", "time_zone": "+05:00"}}}}, A)
searches("a/date_histogram_subagg", {"size": 0, "aggs": {"d": {"date_histogram": {"field": "created", "calendar_interval": "month"},
                                                                 "aggs": {"p": {"avg": {"field": "price"}}}}}}, A)
searches("a/range", {"size": 0, "aggs": {"r": {"range": {"field": "price", "ranges": [{"to": 20}, {"from": 20, "to": 100}, {"from": 100}]}}}}, A)
searches("a/range_keyed_named", {"size": 0, "aggs": {"r": {"range": {"field": "price", "keyed": True, "ranges": [{"key": "cheap", "to": 30}, {"key": "rest", "from": 30}]}}}}, A)
searches("a/date_range", {"size": 0, "aggs": {"r": {"date_range": {"field": "created", "ranges": [{"to": "2024-01-01"}, {"from": "2024-01-01"}]}}}}, A)
searches("a/filter", {"size": 0, "aggs": {"f": {"filter": {"term": {"brand": "nike"}}, "aggs": {"a": {"avg": {"field": "price"}}}}}}, A)
searches("a/filters_named", {"size": 0, "aggs": {"f": {"filters": {"filters": {"cheap": {"range": {"price": {"lt": 30}}}, "gadgets": {"term": {"category": "electronics"}}}}}}}, A)
searches("a/filters_other_bucket", {"size": 0, "aggs": {"f": {"filters": {"other_bucket": True, "filters": [{"term": {"brand": "nike"}}, {"term": {"brand": "logi"}}]}}}}, A)
searches("a/global_agg", {"size": 0, "query": {"term": {"brand": "nike"}}, "aggs": {"all": {"global": {}, "aggs": {"c": {"value_count": {"field": "brand"}}}}, "own": {"value_count": {"field": "brand"}}}}, A)
searches("a/missing_agg", {"size": 0, "aggs": {"m": {"missing": {"field": "qty"}}}}, A)
searches("a/nested_agg", {"size": 0, "aggs": {"r": {"nested": {"path": "reviews"}, "aggs": {"avg_stars": {"avg": {"field": "reviews.stars"}},
                                                                                            "by_user": {"terms": {"field": "reviews.user"}}}}}}, A)
searches("a/reverse_nested", {"size": 0, "aggs": {"r": {"nested": {"path": "reviews"}, "aggs": {"u": {"terms": {"field": "reviews.user"}, "aggs": {"back": {"reverse_nested": {},
                                                     "aggs": {"cats": {"terms": {"field": "category"}}}}}}}}}}, A)
searches("a/top_hits", {"size": 0, "aggs": {"c": {"terms": {"field": "category"}, "aggs": {"top": {"top_hits": {"size": 1, "sort": [{"price": "desc"}], "_source": ["title", "price"]}}}}}}, A)
searches("a/composite", {"size": 0, "aggs": {"c": {"composite": {"size": 4, "sources": [{"cat": {"terms": {"field": "category"}}}, {"brand": {"terms": {"field": "brand"}}}]}}}}, A)
searches("a/composite_after", {"size": 0, "aggs": {"c": {"composite": {"size": 3, "sources": [{"cat": {"terms": {"field": "category"}}}], "after": {"cat": "home"}}}}}, A)
searches("a/rare_terms", {"size": 0, "aggs": {"r": {"rare_terms": {"field": "brand", "max_doc_count": 1}}}}, A)
searches("a/multi_terms", {"size": 0, "aggs": {"m": {"multi_terms": {"terms": [{"field": "category"}, {"field": "in_stock"}]}}}}, A)
searches("a/pipeline_avg_bucket", {"size": 0, "aggs": {"c": {"terms": {"field": "category"}, "aggs": {"p": {"sum": {"field": "price"}}}}, "avg_p": {"avg_bucket": {"buckets_path": "c>p"}}}}, A)
searches("a/pipeline_max_bucket", {"size": 0, "aggs": {"c": {"terms": {"field": "category"}, "aggs": {"p": {"sum": {"field": "price"}}}}, "mx": {"max_bucket": {"buckets_path": "c>p"}}}}, A)
searches("a/pipeline_cumulative_sum", {"size": 0, "aggs": {"d": {"date_histogram": {"field": "created", "calendar_interval": "month"},
                                                                   "aggs": {"n": {"sum": {"field": "qty"}}, "cs": {"cumulative_sum": {"buckets_path": "n"}}}}}}, A)
searches("a/pipeline_derivative", {"size": 0, "aggs": {"d": {"date_histogram": {"field": "created", "calendar_interval": "month"},
                                                              "aggs": {"n": {"sum": {"field": "price"}}, "dv": {"derivative": {"buckets_path": "n"}}}}}}, A)
searches("a/pipeline_bucket_sort", {"size": 0, "aggs": {"c": {"terms": {"field": "brand", "size": 20}, "aggs": {"p": {"sum": {"field": "price"}},
                                                        "bs": {"bucket_sort": {"sort": [{"p": {"order": "desc"}}], "size": 3}}}}}}, A)
searches("a/aggs_with_query", {"size": 0, "query": {"term": {"category": "electronics"}}, "aggs": {"b": {"terms": {"field": "brand"}}}}, A)
searches("a/aggs_unknown_type_error", {"size": 0, "aggs": {"b": {"nonsense": {"field": "brand"}}}})
searches("a/aggs_metric_with_subagg_error", {"size": 0, "aggs": {"b": {"avg": {"field": "price"}, "aggs": {"c": {"max": {"field": "price"}}}}}})
searches("a/aggregations_key_alias", {"size": 0, "aggregations": {"b": {"terms": {"field": "brand", "size": 1}}}}, A)

# --------------------------------------------------------------------------- analyzers

add("analyzer/standard", [S("POST", "/_analyze", {"analyzer": "standard", "text": "The Quick-Brown fox's e-mail: john.doe@example.com, 3.14 and 1,000 items_ok!"})])
add("analyzer/standard_unicode", [S("POST", "/_analyze", {"analyzer": "standard", "text": "Café Ünïcode ÇA va 東京 tower"})])
add("analyzer/simple", [S("POST", "/_analyze", {"analyzer": "simple", "text": "Hello WORLD 123 foo_bar"})])
add("analyzer/whitespace", [S("POST", "/_analyze", {"analyzer": "whitespace", "text": "Hello  WORLD-x  foo_bar"})])
add("analyzer/keyword", [S("POST", "/_analyze", {"analyzer": "keyword", "text": "Hello World"})])
add("analyzer/stop", [S("POST", "/_analyze", {"analyzer": "stop", "text": "The quick and the dead"})])
add("analyzer/tokenizer_filter", [S("POST", "/_analyze", {"tokenizer": "standard", "filter": ["lowercase", "asciifolding"], "text": "Ünï Bär"})])
add("analyzer/custom_index_analyzer_match", [
    S("PUT", "/an", {"settings": {"analysis": {"analyzer": {"my": {"type": "custom", "tokenizer": "standard", "filter": ["lowercase", "asciifolding"]}}}},
                     "mappings": {"properties": {"t": {"type": "text", "analyzer": "my"}}}}, cmp=False),
    S("PUT", "/an/_doc/1", {"t": "Café Crème"}, params={"refresh": "true"}, cmp=False),
    S("POST", "/an/_search", {"query": {"match": {"t": "creme"}}}, opts={"ordered": True}),
    S("POST", "/an/_search", {"query": {"match": {"t": "CAFE"}}}, opts={"ordered": True}),
    S("POST", "/an/_analyze", {"analyzer": "my", "text": "Ünï Crème"})])
add("analyzer/keyword_field_with_lowercase_normalizer", [
    S("PUT", "/nm", {"settings": {"analysis": {"normalizer": {"lc": {"type": "custom", "filter": ["lowercase"]}}}},
                     "mappings": {"properties": {"k": {"type": "keyword", "normalizer": "lc"}}}}, cmp=False),
    S("PUT", "/nm/_doc/1", {"k": "MiXeD"}, params={"refresh": "true"}, cmp=False),
    S("POST", "/nm/_search", {"query": {"term": {"k": "MIXED"}}}), S("POST", "/nm/_search", {"query": {"term": {"k": "mixed"}}})])

# --------------------------------------------------------------------------- document APIs

D = {"opts": {"ordered": True}}


def doc_case(name, *steps, known=None):
    add(name, list(steps), known)


doc_case("doc/index_with_id_created_then_updated",
         S("PUT", "/d/_doc/1", {"a": 1}), S("PUT", "/d/_doc/1", {"a": 2}), S("GET", "/d/_doc/1"), S("GET", "/d/_source/1"))
doc_case("doc/index_auto_id", S("POST", "/d/_doc", {"a": 1}))
doc_case("doc/create_op_type_conflict", S("PUT", "/d/_create/1", {"a": 1}), S("PUT", "/d/_create/1", {"a": 2}),
         S("PUT", "/d/_doc/1", {"a": 3}, params={"op_type": "create"}))
doc_case("doc/create_auto_id_post_create", S("POST", "/d/_create/abc", {"a": 1}), S("GET", "/d/_doc/abc"))
doc_case("doc/if_seq_no_conflict", S("PUT", "/d/_doc/1", {"a": 1}, cmp=False), S("PUT", "/d/_doc/1", {"a": 2}, params={"if_seq_no": 0, "if_primary_term": 1}),
         S("PUT", "/d/_doc/1", {"a": 3}, params={"if_seq_no": 0, "if_primary_term": 1}),
         S("PUT", "/d/_doc/1", {"a": 4}, params={"if_seq_no": 1}), S("GET", "/d/_doc/1"))
doc_case("doc/external_version", S("PUT", "/d/_doc/1", {"a": 1}, params={"version": 5, "version_type": "external"}),
         S("PUT", "/d/_doc/1", {"a": 2}, params={"version": 5, "version_type": "external"}),
         S("PUT", "/d/_doc/1", {"a": 3}, params={"version": 6, "version_type": "external"}),
         S("PUT", "/d/_doc/1", {"a": 4}, params={"version": 6, "version_type": "external_gte"}), S("GET", "/d/_doc/1"))
doc_case("doc/internal_version_param", S("PUT", "/d/_doc/1", {"a": 1}), S("PUT", "/d/_doc/1", {"a": 2}, params={"version": 1}),
         S("PUT", "/d/_doc/1", {"a": 2}, params={"version": 7}))
doc_case("doc/get_missing", S("PUT", "/d/_doc/1", {"a": 1}, cmp=False), S("GET", "/d/_doc/2"), S("GET", "/nope/_doc/1"), S("HEAD", "/d/_doc/1"), S("HEAD", "/d/_doc/2"))
doc_case("doc/get_source_filtering", S("PUT", "/d/_doc/1", {"a": 1, "b": {"c": 2, "d": 3}}, cmp=False),
         S("GET", "/d/_doc/1", params={"_source": "false"}), S("GET", "/d/_doc/1", params={"_source_includes": "b.c"}),
         S("GET", "/d/_doc/1", params={"_source_excludes": "b"}), S("GET", "/d/_source/1", params={"_source_includes": "a"}))
doc_case("doc/delete", S("PUT", "/d/_doc/1", {"a": 1}, cmp=False), S("DELETE", "/d/_doc/1"), S("DELETE", "/d/_doc/1"), S("DELETE", "/nope/_doc/1"), S("GET", "/d/_doc/1"))
doc_case("doc/delete_if_seq_no", S("PUT", "/d/_doc/1", {"a": 1}, cmp=False), S("DELETE", "/d/_doc/1", params={"if_seq_no": 5, "if_primary_term": 1}),
         S("DELETE", "/d/_doc/1", params={"if_seq_no": 0, "if_primary_term": 1}))
doc_case("doc/refresh_true_flag", S("PUT", "/d/_doc/1", {"a": 1}, params={"refresh": "true"}), S("PUT", "/d/_doc/2", {"a": 1}, params={"refresh": "wait_for"}),
         S("DELETE", "/d/_doc/1", params={"refresh": "true"}))
doc_case("doc/update_partial_merge", S("PUT", "/d/_doc/1", {"a": 1, "o": {"x": 1, "y": 2}, "l": [1, 2]}, cmp=False),
         S("POST", "/d/_update/1", {"doc": {"o": {"y": 3, "z": 4}, "l": [9], "n": "new"}}), S("GET", "/d/_doc/1"))
doc_case("doc/update_noop_detection", S("PUT", "/d/_doc/1", {"a": 1}, cmp=False), S("POST", "/d/_update/1", {"doc": {"a": 1}}),
         S("POST", "/d/_update/1", {"doc": {"a": 1}, "detect_noop": False}), S("GET", "/d/_doc/1"))
doc_case("doc/update_missing_doc", S("PUT", "/d/_doc/1", {"a": 1}, cmp=False), S("POST", "/d/_update/2", {"doc": {"a": 1}}), S("POST", "/nope/_update/1", {"doc": {"a": 1}}))
doc_case("doc/update_upsert", S("POST", "/d/_update/1", {"doc": {"a": 2}, "upsert": {"a": 1}}), S("POST", "/d/_update/1", {"doc": {"a": 2}, "upsert": {"a": 1}}), S("GET", "/d/_doc/1"))
doc_case("doc/update_doc_as_upsert", S("POST", "/d/_update/1", {"doc": {"a": 5}, "doc_as_upsert": True}), S("POST", "/d/_update/1", {"doc": {"b": 6}, "doc_as_upsert": True}),
         S("GET", "/d/_doc/1"))
doc_case("doc/update_script_increment", S("PUT", "/d/_doc/1", {"counter": 1, "tags": ["a"]}, cmp=False),
         S("POST", "/d/_update/1", {"script": {"source": "ctx._source.counter += params.n", "params": {"n": 4}}}),
         S("POST", "/d/_update/1", {"script": {"source": "ctx._source.tags.add(params.t)", "params": {"t": "b"}}}),
         S("POST", "/d/_update/1", {"script": {"source": "ctx._source.remove('tags'); ctx._source.flag = true"}}), S("GET", "/d/_doc/1"))
doc_case("doc/update_script_noop_and_delete", S("PUT", "/d/_doc/1", {"n": 1}, cmp=False),
         S("POST", "/d/_update/1", {"script": {"source": "ctx.op = 'noop'"}}), S("POST", "/d/_update/1", {"script": {"source": "if (ctx._source.n == 1) { ctx.op = 'delete' }"}}),
         S("GET", "/d/_doc/1"))
doc_case("doc/update_source_returned", S("PUT", "/d/_doc/1", {"a": 1, "b": 2}, cmp=False), S("POST", "/d/_update/1", {"doc": {"a": 5}}, params={"_source": "true"}))
doc_case("doc/update_bad_body", S("POST", "/d/_update/1", {"foo": 1}), S("POST", "/d/_update/1", {"doc": {"a": 1}, "script": {"source": "1"}}))
doc_case("doc/bulk_mixed", S("POST", "/_bulk", [{"index": {"_index": "b", "_id": "1"}}, {"a": 1}, {"create": {"_index": "b", "_id": "2"}}, {"a": 2},
                                          {"update": {"_index": "b", "_id": "1"}}, {"doc": {"z": 9}}, {"delete": {"_index": "b", "_id": "2"}},
                                          {"delete": {"_index": "b", "_id": "404"}}, {"create": {"_index": "b", "_id": "1"}}, {"a": 3}], ndjson=True),
         S("GET", "/b/_doc/1"))
doc_case("doc/bulk_default_index_and_auto_id", S("POST", "/bx/_bulk", [{"index": {}}, {"a": 1}, {"index": {"_id": "k"}}, {"a": 2}], ndjson=True), S("GET", "/bx/_doc/k"))
doc_case("doc/bulk_update_missing_and_upsert", S("POST", "/bu/_bulk", [{"update": {"_id": "1"}}, {"doc": {"a": 1}}, {"update": {"_id": "2"}},
                                                                     {"doc": {"a": 1}, "doc_as_upsert": True}], ndjson=True))
doc_case("doc/bulk_per_item_errors", S("PUT", "/bs", {"mappings": {"properties": {"n": {"type": "long"}}}}, cmp=False),
         S("POST", "/bs/_bulk", [{"index": {"_id": "1"}}, {"n": "abc"}, {"index": {"_id": "2"}}, {"n": 5}, {"index": {"_id": "3"}}, "{not json"], ndjson=True))
doc_case("doc/bulk_malformed_action", S("POST", "/_bulk", ["{\"bogus\":{}}", {"a": 1}], ndjson=True), S("POST", "/_bulk", ["not json"], ndjson=True),
         S("POST", "/_bulk", [], ndjson=True))
doc_case("doc/bulk_refresh_param_visible", S("POST", "/br/_bulk", [{"index": {"_id": "1"}}, {"a": 1}], ndjson=True, params={"refresh": "true"}),
         S("POST", "/br/_search", {"query": {"match_all": {}}}, opts={"ordered": True}))
doc_case("doc/mget", S("PUT", "/m/_doc/1", {"a": 1}, cmp=False), S("PUT", "/m/_doc/2", {"a": 2}, cmp=False),
         S("POST", "/m/_mget", {"ids": ["1", "2", "3"]}), S("POST", "/_mget", {"docs": [{"_index": "m", "_id": "1"}, {"_index": "nope", "_id": "1"}, {"_index": "m", "_id": "9"}]}),
         S("POST", "/m/_mget", {"docs": [{"_id": "1", "_source": ["a"]}, {"_id": "2", "_source": False}]}), S("POST", "/m/_mget", {}))
doc_case("doc/exists_and_count", S("PUT", "/c/_doc/1", {"a": 1}, params={"refresh": "true"}, cmp=False), S("PUT", "/c/_doc/2", {"a": 2}, params={"refresh": "true"}, cmp=False),
         S("POST", "/c/_count", {"query": {"term": {"a": 1}}}), S("GET", "/c/_count"), S("GET", "/c/_count", params={"q": "a:2"}), S("POST", "/nope/_count", {}))
doc_case("doc/delete_by_query", S("POST", "/dq/_bulk", [{"index": {"_id": "1"}}, {"a": 1}, {"index": {"_id": "2"}}, {"a": 2}, {"index": {"_id": "3"}}, {"a": 2}], ndjson=True,
                                  params={"refresh": "true"}, cmp=False),
         S("POST", "/dq/_delete_by_query", {"query": {"term": {"a": 2}}}, params={"refresh": "true"}, opts={"nosort": True}),
         S("POST", "/dq/_search", {"query": {"match_all": {}}}, opts={"ordered": True}), S("POST", "/dq/_delete_by_query", {}))
doc_case("doc/update_by_query_script", S("POST", "/uq/_bulk", [{"index": {"_id": "1"}}, {"a": 1}, {"index": {"_id": "2"}}, {"a": 2}], ndjson=True, params={"refresh": "true"}, cmp=False),
         S("POST", "/uq/_update_by_query", {"query": {"term": {"a": 2}}, "script": {"source": "ctx._source.b = ctx._source.a * 10"}}, params={"refresh": "true"}),
         S("GET", "/uq/_doc/2"), S("GET", "/uq/_doc/1"))
doc_case("doc/reindex", S("POST", "/rs/_bulk", [{"index": {"_id": "1"}}, {"a": 1}, {"index": {"_id": "2"}}, {"a": 2}], ndjson=True, params={"refresh": "true"}, cmp=False),
         S("POST", "/_reindex", {"source": {"index": "rs", "query": {"term": {"a": 2}}}, "dest": {"index": "rd"}}, params={"refresh": "true"}),
         S("GET", "/rd/_doc/2"), S("GET", "/rd/_doc/1"))
doc_case("doc/scroll", S("POST", "/sc/_bulk", [x for i in range(7) for x in ({"index": {"_id": str(i)}}, {"n": i})], ndjson=True, params={"refresh": "true"}, cmp=False),
         S("POST", "/sc/_search", {"size": 3, "sort": [{"n": "asc"}]}, params={"scroll": "1m"}, opts={"ordered": True}),
         S("POST", "/_search/scroll", {"scroll": "1m", "scroll_id": "$scroll_id"}, opts={"ordered": True}),
         S("POST", "/_search/scroll", {"scroll": "1m", "scroll_id": "$scroll_id"}, opts={"ordered": True}),
         S("POST", "/_search/scroll", {"scroll": "1m", "scroll_id": "$scroll_id"}, opts={"ordered": True}),
         S("DELETE", "/_search/scroll", {"scroll_id": "$scroll_id"}), S("POST", "/_search/scroll", {"scroll": "1m", "scroll_id": "$scroll_id"}))
doc_case("doc/scroll_doc_sort_and_total", S("POST", "/sc/_bulk", [x for i in range(5) for x in ({"index": {"_id": str(i)}}, {"n": i})], ndjson=True, params={"refresh": "true"}, cmp=False),
         S("POST", "/sc/_search", {"size": 2, "sort": ["_doc"], "query": {"range": {"n": {"gte": 1}}}}, params={"scroll": "1m"}, opts={"nosort": True}),
         S("POST", "/_search/scroll", {"scroll": "1m", "scroll_id": "$scroll_id"}, opts={"nosort": True}))
doc_case("doc/msearch", S("PUT", "/ms/_doc/1", {"a": "x"}, params={"refresh": "true"}, cmp=False), S("PUT", "/ms/_doc/2", {"a": "y"}, params={"refresh": "true"}, cmp=False),
         S("POST", "/ms/_msearch", [{}, {"query": {"term": {"a.keyword": "x"}}}, {"index": "nope"}, {"query": {"match_all": {}}}, {"index": "ms"}, {"query": {"term": {"a.keyword": "y"}}}],
           ndjson=True, opts={"ordered": True}))
doc_case("doc/invalid_index_names", S("PUT", "/UPPER", {}), S("PUT", "/_bad", {}), S("PUT", "/has space", {}), S("PUT", "/a%2Fb", {}), S("PUT", "/ok-name.with_chars", {}))
doc_case("doc/doc_id_special_chars", S("PUT", "/d/_doc/a%20b%2Fc", {"a": 1}), S("GET", "/d/_doc/a%20b%2Fc"))
doc_case("doc/unicode_index_and_source", S("PUT", "/d/_doc/1", {"t": "日本語 テキスト émoji 🎉"}), S("GET", "/d/_doc/1"))
doc_case("doc/pretty_and_filter_path", S("PUT", "/d/_doc/1", {"a": 1, "b": {"c": 2}}, cmp=False), S("GET", "/d/_doc/1", params={"filter_path": "_source.b"}),
         S("GET", "/d/_doc/1", params={"filter_path": "found,_id"}), S("GET", "/d/_doc/1", params={"filter_path": "-_source"}))

# --------------------------------------------------------------------------- mapping / dynamic inference

MAPCASE = {"s": "text-like", "date": "2020-01-01T10:00:00Z"}
doc_case("map/dynamic_inference_types", S("PUT", "/dm/_doc/1", {"s": "text here", "i": 5, "f": 1.5, "b": True, "d": "2020-01-01", "dt": "2020-01-01T10:00:00Z",
                                                                  "o": {"x": 1, "y": {"z": "deep"}}, "arr": [1, 2, 3], "sarr": ["a", "b"], "oarr": [{"k": 1}, {"k": 2}],
                                                                  "n": None, "empty": [], "notdate": "2020-13-45", "numstr": "12"}),
         S("GET", "/dm/_mapping"))
doc_case("map/dynamic_conflict_error", S("PUT", "/dm/_doc/1", {"n": 5}, cmp=False), S("PUT", "/dm/_doc/2", {"n": "abc"}), S("PUT", "/dm/_doc/3", {"n": "7"}),
         S("PUT", "/dm/_doc/4", {"n": 1.5}), S("PUT", "/dm/_doc/5", {"n": {"x": 1}}), S("PUT", "/dm/_doc/6", {"n": True}))
doc_case("map/object_vs_scalar_error", S("PUT", "/dm/_doc/1", {"o": {"x": 1}}, cmp=False), S("PUT", "/dm/_doc/2", {"o": 5}), S("PUT", "/dm/_doc/3", {"o": [{"x": 2}]}))
doc_case("map/date_parse_error", S("PUT", "/dm", {"mappings": {"properties": {"d": {"type": "date"}}}}, cmp=False), S("PUT", "/dm/_doc/1", {"d": "not a date"}),
         S("PUT", "/dm/_doc/2", {"d": 1577836800000}), S("PUT", "/dm/_doc/3", {"d": "2020-01-01 10:00:00"}), S("PUT", "/dm/_doc/4", {"d": "2020-01-01T10:00:00+02:00"}))
doc_case("map/boolean_coercion", S("PUT", "/dm", {"mappings": {"properties": {"b": {"type": "boolean"}}}}, cmp=False), S("PUT", "/dm/_doc/1", {"b": "true"}),
         S("PUT", "/dm/_doc/2", {"b": "yes"}), S("PUT", "/dm/_doc/3", {"b": 1}))
doc_case("map/numeric_range_errors", S("PUT", "/dm", {"mappings": {"properties": {"i": {"type": "integer"}, "sh": {"type": "short"}, "by": {"type": "byte"}}}}, cmp=False),
         S("PUT", "/dm/_doc/1", {"i": 3000000000}), S("PUT", "/dm/_doc/2", {"sh": 40000}), S("PUT", "/dm/_doc/3", {"by": 200}), S("PUT", "/dm/_doc/4", {"i": 1.7}))
doc_case("map/strict_dynamic", S("PUT", "/dm", {"mappings": {"dynamic": "strict", "properties": {"a": {"type": "keyword"}}}}, cmp=False), S("PUT", "/dm/_doc/1", {"a": "x"}),
         S("PUT", "/dm/_doc/2", {"a": "x", "b": 1}), S("GET", "/dm/_mapping"))
doc_case("map/dynamic_false", S("PUT", "/dm", {"mappings": {"dynamic": False, "properties": {"a": {"type": "keyword"}}}}, cmp=False), S("PUT", "/dm/_doc/1", {"a": "x", "b": 1}, params={"refresh": "true"}),
         S("GET", "/dm/_mapping"), S("POST", "/dm/_search", {"query": {"term": {"b": 1}}}), S("GET", "/dm/_doc/1"))
doc_case("map/dynamic_templates", S("PUT", "/dm", {"mappings": {"dynamic_templates": [{"strings_as_kw": {"match_mapping_type": "string", "mapping": {"type": "keyword"}}}]}}, cmp=False),
         S("PUT", "/dm/_doc/1", {"s": "Hello World", "n": 1}), S("GET", "/dm/_mapping"))
doc_case("map/put_mapping_add_and_conflict", S("PUT", "/dm", {}, cmp=False), S("PUT", "/dm/_mapping", {"properties": {"a": {"type": "keyword"}}}),
         S("PUT", "/dm/_mapping", {"properties": {"a": {"type": "text"}}}), S("PUT", "/dm/_mapping", {"properties": {"b": {"type": "long"}}}), S("GET", "/dm/_mapping"),
         S("PUT", "/dm/_mapping", {"properties": {"c": {"type": "nosuchtype"}}}))
doc_case("map/get_field_mapping", S("PUT", "/dm", {"mappings": {"properties": {"a": {"type": "keyword"}, "o": {"properties": {"x": {"type": "long"}}}}}}, cmp=False),
         S("GET", "/dm/_mapping/field/a"), S("GET", "/dm/_mapping/field/o.x"), S("GET", "/dm/_mapping/field/*"))
doc_case("map/text_with_keyword_subfield_search", S("PUT", "/dm/_doc/1", {"s": "Hello Big World"}, params={"refresh": "true"}, cmp=False),
         S("POST", "/dm/_search", {"query": {"match": {"s": "big"}}}), S("POST", "/dm/_search", {"query": {"term": {"s.keyword": "Hello Big World"}}}),
         S("POST", "/dm/_search", {"query": {"term": {"s": "hello big world"}}}), S("POST", "/dm/_search", {"aggs": {"k": {"terms": {"field": "s.keyword"}}}, "size": 0}))
doc_case("map/long_keyword_ignore_above", S("PUT", "/dm/_doc/1", {"s": "x" * 300}, params={"refresh": "true"}, cmp=False), S("POST", "/dm/_search", {"query": {"exists": {"field": "s.keyword"}}}),
         S("POST", "/dm/_search", {"query": {"match": {"s": "x" * 300}}}))
doc_case("map/nested_dynamic_and_array_of_objects", S("PUT", "/dm/_doc/1", {"o": [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]}, params={"refresh": "true"}, cmp=False),
         S("POST", "/dm/_search", {"query": {"bool": {"must": [{"term": {"o.a": 1}}, {"term": {"o.b.keyword": "y"}}]}}}))
doc_case("map/dotted_field_names", S("PUT", "/dm/_doc/1", {"a.b": 1, "a": {"c": 2}}), S("GET", "/dm/_mapping"), S("PUT", "/dm/_doc/2", {".": 1}))
doc_case("map/geo_point_and_ip_fields", S("PUT", "/dm", {"mappings": {"properties": {"p": {"type": "geo_point"}, "ip": {"type": "ip"}}}}, cmp=False),
         S("PUT", "/dm/_doc/1", {"p": "40.0,-70.0", "ip": "10.0.0.1"}), S("PUT", "/dm/_doc/2", {"ip": "not-an-ip"}), S("PUT", "/dm/_doc/3", {"p": [-70.0, 40.0], "ip": "::1"}, params={"refresh": "true"}),
         S("POST", "/dm/_search", {"query": {"term": {"ip": "10.0.0.1"}}}))

# --------------------------------------------------------------------------- index management

doc_case("idx/create_with_settings_mappings_aliases", S("PUT", "/ix", {"settings": {"number_of_shards": 1, "number_of_replicas": 0, "index": {"refresh_interval": "5s"}},
                                                                     "mappings": {"properties": {"a": {"type": "keyword"}}}, "aliases": {"ix_alias": {}}}),
         S("GET", "/ix"), S("GET", "/ix/_settings"), S("GET", "/ix/_alias"), S("GET", "/ix/_mapping"))
doc_case("idx/create_exists_error", S("PUT", "/ix", {}, cmp=False), S("PUT", "/ix", {}), S("HEAD", "/ix"), S("HEAD", "/nope"))
doc_case("idx/create_bad_body", S("PUT", "/ix", {"bogus": 1}), S("PUT", "/ix2", {"settings": {"number_of_shards": 0}}), S("PUT", "/ix3", {"mappings": {"properties": {"a": {"type": "wat"}}}}))
doc_case("idx/delete_variants", S("PUT", "/ia1", {}, cmp=False), S("PUT", "/ia2", {}, cmp=False), S("DELETE", "/nope"), S("DELETE", "/nope", params={"ignore_unavailable": "true"}),
         S("DELETE", "/ia*"), S("HEAD", "/ia1"))
doc_case("idx/get_index_wildcards_and_missing", S("PUT", "/ga1", {}, cmp=False), S("PUT", "/ga2", {}, cmp=False), S("GET", "/ga*"), S("GET", "/nope"), S("GET", "/nope*"),
         S("GET", "/ga1,ga2/_settings/index.number_of_shards"))
doc_case("idx/aliases_crud", S("PUT", "/al1", {}, cmp=False), S("PUT", "/al1/_alias/a1", {}), S("PUT", "/al1/_alias/a2", {"filter": {"term": {"x": 1}}, "routing": "r"}),
         S("GET", "/_alias/a1"), S("GET", "/al1/_alias"), S("HEAD", "/_alias/a2"), S("HEAD", "/_alias/zzz"), S("DELETE", "/al1/_alias/a1"), S("DELETE", "/al1/_alias/a1"), S("GET", "/al1/_alias"))
doc_case("idx/update_aliases_actions", S("PUT", "/al1", {}, cmp=False), S("PUT", "/al2", {}, cmp=False),
         S("POST", "/_aliases", {"actions": [{"add": {"index": "al1", "alias": "ax"}}, {"add": {"index": "al2", "alias": "ax", "is_write_index": True}}]}), S("GET", "/_alias/ax"),
         S("POST", "/_aliases", {"actions": [{"remove": {"index": "al1", "alias": "ax"}}]}), S("GET", "/_alias/ax"), S("POST", "/_aliases", {"actions": [{"remove": {"index": "al1", "alias": "gone"}}]}),
         S("POST", "/_aliases", {"actions": [{"add": {"index": "nope", "alias": "z"}}]}))
doc_case("idx/alias_search_filter_and_write", S("PUT", "/af", {"mappings": {"properties": {"c": {"type": "keyword"}}}}, cmp=False),
         S("PUT", "/af/_alias/red", {"filter": {"term": {"c": "red"}}}, cmp=False), S("PUT", "/red/_doc/1", {"c": "red"}, params={"refresh": "true"}, cmp=False),
         S("PUT", "/af/_doc/2", {"c": "blue"}, params={"refresh": "true"}, cmp=False), S("POST", "/red/_search", {"query": {"match_all": {}}}, opts={"ordered": True}),
         S("GET", "/red/_doc/1"), S("GET", "/red/_count"))
doc_case("idx/multi_index_search", S("PUT", "/mi1/_doc/1", {"a": "x"}, params={"refresh": "true"}, cmp=False), S("PUT", "/mi2/_doc/1", {"a": "y"}, params={"refresh": "true"}, cmp=False),
         S("POST", "/mi1,mi2/_search", {"query": {"match_all": {}}, "sort": [{"_index": "asc"}]}, opts={"ordered": True}), S("POST", "/mi*/_search", {"query": {"match_all": {}}, "size": 0}),
         S("POST", "/mi1,nope/_search", {"query": {"match_all": {}}}), S("POST", "/mi1,nope/_search", {"query": {"match_all": {}}}, params={"ignore_unavailable": "true"}),
         S("POST", "/_search", {"query": {"match_all": {}}, "size": 0}), S("POST", "/nope*/_search", {"query": {"match_all": {}}}))
doc_case("idx/put_settings_and_close_open", S("PUT", "/ps", {}, cmp=False), S("PUT", "/ps/_settings", {"index": {"number_of_replicas": 0}}), S("GET", "/ps/_settings"),
         S("PUT", "/ps/_settings", {"index": {"number_of_shards": 3}}), S("POST", "/ps/_close"), S("POST", "/ps/_search", {"query": {"match_all": {}}}), S("POST", "/ps/_open"))
doc_case("idx/refresh_flush_forcemerge", S("PUT", "/rf", {}, cmp=False), S("POST", "/rf/_refresh"), S("POST", "/rf/_flush"), S("POST", "/rf/_forcemerge"), S("POST", "/nope/_refresh"))
doc_case("idx/refresh_interval_minus_one_visibility", S("PUT", "/rv", {"settings": {"refresh_interval": "-1"}}, cmp=False), S("PUT", "/rv/_doc/1", {"a": 1}, cmp=False),
         S("POST", "/rv/_search", {"query": {"match_all": {}}}), S("GET", "/rv/_doc/1"), S("POST", "/rv/_refresh"), S("POST", "/rv/_search", {"query": {"match_all": {}}}),
         S("PUT", "/rv/_doc/2", {"a": 2}, params={"refresh": "true"}), S("POST", "/rv/_search", {"query": {"match_all": {}}, "sort": [{"a": "asc"}]}, opts={"ordered": True}))
doc_case("idx/templates_legacy", S("PUT", "/_template/t1", {"index_patterns": ["tp-*"], "settings": {"number_of_replicas": 0}, "mappings": {"properties": {"k": {"type": "keyword"}}}, "order": 1}),
         S("GET", "/_template/t1"), S("PUT", "/tp-1/_doc/1", {"k": "Hello"}), S("GET", "/tp-1/_mapping"), S("GET", "/tp-1/_settings/index.number_of_replicas"), S("HEAD", "/_template/t1"),
         S("DELETE", "/_template/t1"), S("GET", "/_template/t1"))
doc_case("idx/templates_composable", S("PUT", "/_component_template/c1", {"template": {"mappings": {"properties": {"c": {"type": "keyword"}}}}}),
         S("PUT", "/_index_template/it1", {"index_patterns": ["ct-*"], "composed_of": ["c1"], "priority": 5, "template": {"settings": {"number_of_replicas": 0},
                                                                                                                      "mappings": {"properties": {"d": {"type": "long"}}}, "aliases": {"ct_all": {}}}}),
         S("GET", "/_index_template/it1"), S("PUT", "/ct-1/_doc/1", {"c": "x", "d": 1, "e": "auto"}), S("GET", "/ct-1/_mapping"), S("GET", "/_alias/ct_all"), S("DELETE", "/_index_template/it1"),
         S("GET", "/_index_template/nope"))
doc_case("idx/field_caps", S("PUT", "/fc", {"mappings": {"properties": {"a": {"type": "keyword"}, "b": {"type": "long"}, "t": {"type": "text"}}}}, cmp=False),
         S("GET", "/fc/_field_caps", params={"fields": "*"}), S("GET", "/fc/_field_caps", params={"fields": "a,b"}), S("GET", "/fc/_field_caps"))
doc_case("idx/analyze_on_index", S("PUT", "/az", {"mappings": {"properties": {"t": {"type": "text"}, "k": {"type": "keyword"}}}}, cmp=False),
         S("POST", "/az/_analyze", {"field": "t", "text": "Hello World"}), S("POST", "/az/_analyze", {"field": "k", "text": "Hello World"}))
doc_case("idx/validate_query", S("PUT", "/vq", {}, cmp=False), S("POST", "/vq/_validate/query", {"query": {"match_all": {}}}), S("POST", "/vq/_validate/query", {"query": {"bogus": {}}}))
doc_case("idx/explain_api", S("PUT", "/ex/_doc/1", {"t": "hello world"}, params={"refresh": "true"}, cmp=False), S("POST", "/ex/_explain/1", {"query": {"match": {"t": "hello"}}}),
         S("POST", "/ex/_explain/1", {"query": {"match": {"t": "nothere"}}}), S("POST", "/ex/_explain/2", {"query": {"match_all": {}}}))
doc_case("idx/pit_search_after", S("PUT", "/pt/_bulk", [x for i in range(5) for x in ({"index": {"_id": str(i)}}, {"n": i})], ndjson=True, params={"refresh": "true"}, cmp=False),
         S("POST", "/pt/_search/point_in_time", None, params={"keep_alive": "1m"}, cmp=False),
         S("POST", "/_search", {"size": 2, "pit": {"id": "$pit_id", "keep_alive": "1m"}, "sort": [{"n": "asc"}]}, opts={"ordered": True}),
         S("POST", "/_search", {"size": 2, "pit": {"id": "$pit_id", "keep_alive": "1m"}, "sort": [{"n": "asc"}], "search_after": [1]}, opts={"ordered": True}),
         S("DELETE", "/_search/point_in_time", {"pit_id": ["$pit_id"]}, cmp=False))
add("s/indices_boost", [S("PUT", "/ib1/_doc/1", {"t": "apple pie"}, params={"refresh": "true"}, cmp=False), S("PUT", "/ib2/_doc/1", {"t": "apple pie"}, params={"refresh": "true"}, cmp=False),
                        S("POST", "/ib1,ib2/_search", {"query": {"match": {"t": "apple"}}, "indices_boost": [{"ib2": 3.0}]}, opts={"ordered": True, "scores": True}),
                        S("POST", "/ib1,ib2/_search", {"query": {"match": {"t": "apple"}}, "indices_boost": {"ib1": 5}}, opts={"ordered": True, "scores": True})])
add("s/multi_index_different_mappings_sort", [S("PUT", "/mx1", {"mappings": {"properties": {"n": {"type": "long"}}}}, cmp=False), S("PUT", "/mx2", {"mappings": {"properties": {"n": {"type": "double"}}}}, cmp=False),
                        S("PUT", "/mx1/_doc/1", {"n": 5}, params={"refresh": "true"}, cmp=False), S("PUT", "/mx2/_doc/1", {"n": 2.5}, params={"refresh": "true"}, cmp=False),
                        S("POST", "/mx*/_search", {"sort": [{"n": "asc"}]}, opts={"ordered": True}),
                        S("GET", "/mx*/_field_caps", params={"fields": "n"})])
add("s/score_ties_and_doc_order", [S("POST", "/tie/_bulk", [x for i in range(6) for x in ({"index": {"_id": str(i)}}, {"t": "same"})], ndjson=True, params={"refresh": "true"}, cmp=False),
                        S("POST", "/tie/_search", {"query": {"match": {"t": "same"}}}, opts={"ordered": True, "scores": True})])

# --------------------------------------------------------------------------- cluster / cat / info

doc_case("cluster/root_info_shape", S("GET", "/", shape=1), S("HEAD", "/"))
doc_case("cluster/health_shape", S("PUT", "/ch", {"settings": {"number_of_replicas": 0}}, cmp=False), S("GET", "/_cluster/health", shape=1), S("GET", "/_cluster/health/ch", shape=1),
         S("GET", "/_cluster/health", params={"level": "indices"}, shape=1), S("GET", "/_cluster/health", params={"wait_for_status": "yellow", "timeout": "1s"}, shape=1))
doc_case("cluster/settings_and_stats_shape", S("GET", "/_cluster/settings", shape=1), S("GET", "/_cluster/stats", shape=0), S("GET", "/_nodes", shape=1),
         S("GET", "/_nodes/stats", shape=1))
doc_case("cat/indices_json", S("PUT", "/cat1", {"settings": {"number_of_replicas": 0}}, cmp=False), S("PUT", "/cat1/_doc/1", {"a": 1}, params={"refresh": "true"}, cmp=False),
         S("GET", "/_cat/indices", params={"format": "json", "h": "index,docs.count,pri,rep,status"}), S("GET", "/_cat/indices/cat1", params={"format": "json", "h": "index,health"}))
doc_case("cat/count_and_health_json", S("PUT", "/cat1/_doc/1", {"a": 1}, params={"refresh": "true"}, cmp=False), S("GET", "/_cat/count", params={"format": "json", "h": "count"}),
         S("GET", "/_cat/count/cat1", params={"format": "json", "h": "count"}), S("GET", "/_cat/health", params={"format": "json", "h": "status,node.total"}))
doc_case("cat/aliases_shards_json", S("PUT", "/cat1", {"aliases": {"catal": {}}}, cmp=False), S("GET", "/_cat/aliases", params={"format": "json", "h": "alias,index"}),
         S("GET", "/_cat/shards/cat1", params={"format": "json", "h": "index,shard,prirep,state"}))
doc_case("cat/text_header", S("PUT", "/cat1", {"settings": {"number_of_replicas": 0}}, cmp=False), S("GET", "/_cat/indices", params={"v": "true", "h": "index,pri,rep"},
                                                                                                        text_normalise=lambda t: [l.split() for l in t.strip().split("\n")]))
doc_case("cat/unknown_endpoint_and_method", S("GET", "/_nosuchapi"), S("PATCH", "/", {}), S("GET", "/nosuchindex/_nosuchapi"))
doc_case("errors/bad_json_bodies", S("POST", "/e/_search", "{not json"), S("PUT", "/e/_doc/1", "{not json"), S("PUT", "/e/_doc/1", "[1,2]"), S("PUT", "/e/_doc/1", ""))
doc_case("errors/missing_index_variants", S("POST", "/nope/_search", {"query": {"match_all": {}}}), S("GET", "/nope/_mapping"), S("GET", "/nope/_settings"), S("PUT", "/nope/_mapping", {"properties": {}}),
         S("POST", "/nope/_refresh"), S("GET", "/nope/_count"), S("DELETE", "/nope/_doc/1"), S("POST", "/nope/_delete_by_query", {"query": {"match_all": {}}}))


def all_cases():
    seen = set()
    out = []
    for c in CASES:
        if c["name"] in seen:
            raise ValueError("duplicate case " + c["name"])
        seen.add(c["name"])
        out.append(c)
    return out

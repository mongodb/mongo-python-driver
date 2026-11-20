# Selects the BSON codec implementation (BSON_IMPL=c|rust) before the perf
# test modules run. See bson_impl_select.py.
from __future__ import annotations

# Importing triggers codec selection (side effect; see bson_impl_select.py).
from test.performance import bson_impl_select

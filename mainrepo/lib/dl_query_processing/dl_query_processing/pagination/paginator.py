from dl_query_processing.pagination.post_paginator import QueryPostPaginator
from dl_query_processing.pagination.pre_paginator import QueryPrePaginator


class QueryPaginator:
    def get_pre_paginator(self) -> QueryPrePaginator:
        return QueryPrePaginator()

    def get_post_paginator(self) -> QueryPostPaginator:
        return QueryPostPaginator()

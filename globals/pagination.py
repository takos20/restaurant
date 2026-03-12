from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response
from rest_framework.exceptions import NotFound
from collections import OrderedDict


class CustomPagination(PageNumberPagination):
    """
    Provides custom pagination render.
    """
    page_size = 10
    page_size_query_param = 'size'
    max_page_size = 100
    error_page_ = 1
    error_limit_ = 10
    error_total_ = 0
    error_links_ = {'next': None, 'previous': None}

    def paginate_queryset(self, queryset, request, view=None):
        try:
            return super(CustomPagination, self).paginate_queryset(queryset, request, view=view)
        except NotFound:  # intercept NotFound exception
            page_size = self.get_page_size(request)
            paginator = self.django_paginator_class(queryset, page_size)
            page_number = request.query_params.get(self.page_query_param, 1)
            if page_number in self.last_page_strings:
                page_number = paginator.num_pages
            self.error_total_ = paginator.count
            if isinstance(page_number, int):
                self.error_page_ = int(page_number)
            self.error_limit_ = paginator.per_page
            return list()

    def get_paginated_response(self, data):
        if hasattr(self, 'page') and self.page is not None:
            return Response({
                'page': self.page.number,
                'size': self.page.paginator.per_page,
                'totalElements': self.page.paginator.count,
                'links': {
                    'next': self.get_next_link(),
                    'previous': self.get_previous_link()
                },
                'content': data
            })
        else:
            return Response(OrderedDict([
                ('page', self.error_page_),
                ('size', self.error_limit_),
                ('totalElements', self.error_total_),
                ('links', self.error_links_),
                ('content', data)
            ]))

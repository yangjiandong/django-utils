from django.core.handlers.base import BaseHandler
from django.core.handlers.wsgi import WSGIRequest
from django.core.urlresolvers import get_resolver
from django.test import Client, RequestFactory, TestCase as _TestCase


class TestCase(_TestCase):
    def assertQuerysetEqual(self, a, b):
        """
        From http://djangosnippets.org/snippets/2013/
        Assert iterable `a` has the same model instances as iterable `b`
        """
        return self.assertEqual(self._sort_by_pk(a), self._sort_by_pk(b))

    def _sort_by_pk(self, list_or_qs):
        annotated = [(item.pk, item) for item in list_or_qs]
        annotated.sort()
        return map(lambda item_tuple: item_tuple[1], annotated)


class RequestFactoryTestCase(TestCase):
    def setUp(self):
        self.request_factory = RequestFactory()
    
    def request(self, req):
        resolver = get_resolver(None)
        func, args, kwargs = resolver.resolve(req.META['PATH_INFO'])
        return func(req, *args, **kwargs)
    
    def get(self, url, **data):
        return self.request(self.request_factory.get(url, data))
    
    def post(self, url, **data):
        return self.request(self.request_factory.post(url, data))

from djutils.tests.cache import *
from djutils.tests.context_processors import *
from djutils.tests.decorators import *
from djutils.tests.db import *
from djutils.tests.middleware import *
from djutils.tests.templatetags import *
from djutils.tests.utils import *

try:
    from djutils.tests.queue import *
except ImportError:
    import sys
    sys.stderr.write('Unable to import queue tests, skipping')

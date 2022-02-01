# Copyright (c) "Neo4j"
# Neo4j Sweden AB [http://neo4j.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import asyncio
from functools import wraps


# Can be automatically overridden in builds
package = "neo4j"
version = "5.0.dev0"


def get_user_agent():
    """ Obtain the default user agent string sent to the server after
    a successful handshake.
    """
    from sys import (
        platform,
        version_info,
    )
    template = "neo4j-python/{} Python/{}.{}.{}-{}-{} ({})"
    fields = (version,) + tuple(version_info) + (platform,)
    return template.format(*fields)


def deprecation_warn(message, stack_level=2):
    from warnings import warn
    warn(message, category=DeprecationWarning, stacklevel=stack_level)


def deprecated(message):
    """ Decorator for deprecating functions and methods.

    ::

        @deprecated("'foo' has been deprecated in favour of 'bar'")
        def foo(x):
            pass

    """
    def decorator(f):
        if asyncio.iscoroutinefunction(f):
            @wraps(f)
            async def inner(*args, **kwargs):
                deprecation_warn(message, stack_level=3)
                return await f(*args, **kwargs)

            return inner
        else:
            @wraps(f)
            def inner(*args, **kwargs):
                deprecation_warn(message, stack_level=3)
                return f(*args, **kwargs)

            return inner

    return decorator


class ExperimentalWarning(Warning):
    """ Base class for warnings about experimental features.
    """


def experimental(message):
    """ Decorator for tagging experimental functions and methods.

    ::

        @experimental("'foo' is an experimental function and may be "
                      "removed in a future release")
        def foo(x):
            pass

    """
    def f__(f):
        @wraps(f)
        def f_(*args, **kwargs):
            from warnings import warn
            warn(message, category=ExperimentalWarning, stacklevel=2)
            return f(*args, **kwargs)
        return f_
    return f__


def unclosed_resource_warn(obj):
    import tracemalloc
    from warnings import warn
    msg = f"Unclosed {obj!r}."
    trace = tracemalloc.get_object_traceback(obj)
    if trace:
        msg += "\nObject allocated at (most recent call last):\n"
        msg += "\n".join(trace.format())
    else:
        msg += "\nEnable tracemalloc to get the object allocation traceback."
    warn(msg, ResourceWarning, stacklevel=2, source=obj)

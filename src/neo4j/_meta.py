# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import asyncio
import platform
import sys
import tracemalloc
import typing as t
from functools import wraps
from warnings import warn


_FuncT = t.TypeVar("_FuncT", bound=t.Callable)


# Can be automatically overridden in builds
package = "neo4j"
version = "5.7.dev0"
deprecated_package = False


def _compute_bolt_agent() -> str:
    def format_version_info(version_info):
        return "{}.{}.{}-{}-{}".format(*version_info)

    return (
        # product/version
        f"neo4j-python/{version} "
        # platform
        f"({platform.system() or 'Unknown'} {platform.release() or 'unknown'}"
        f"; {platform.machine() or 'unknown'}) "
        # language/version
        f"Python/{format_version_info(sys.version_info)} "
        # language details
        f"({platform.python_implementation()}; "
        f"{format_version_info(sys.implementation.version)} "
        f"({', '.join(platform.python_build())}) "
        f"[{platform.python_compiler()}])"
    )


BOLT_AGENT = _compute_bolt_agent()


def get_user_agent() -> str:
    """ Obtain the default user agent string sent to the server after
    a successful handshake.
    """
    return BOLT_AGENT


def _id(x):
    return x


def copy_signature(_: _FuncT) -> t.Callable[[t.Callable], _FuncT]:
    return _id


def deprecation_warn(message, stack_level=1):
    warn(message, category=DeprecationWarning, stacklevel=stack_level + 1)


def deprecated(message: str) -> t.Callable[[_FuncT], _FuncT]:
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
                deprecation_warn(message, stack_level=2)
                return await f(*args, **kwargs)

            return inner
        else:
            @wraps(f)
            def inner(*args, **kwargs):
                deprecation_warn(message, stack_level=2)
                return f(*args, **kwargs)

            return inner

    return decorator


def deprecated_property(message: str):
    def decorator(f):
        return property(deprecated(message)(f))
    return t.cast(property, decorator)


class ExperimentalWarning(Warning):
    """ Base class for warnings about experimental features.
    """


def experimental_warn(message, stack_level=1):
    warn(message, category=ExperimentalWarning, stacklevel=stack_level + 1)


def experimental(message) -> t.Callable[[_FuncT], _FuncT]:
    """ Decorator for tagging experimental functions and methods.

    ::

        @experimental("'foo' is an experimental function and may be "
                      "removed in a future release")
        def foo(x):
            pass

    """
    def decorator(f):
        if asyncio.iscoroutinefunction(f):
            @wraps(f)
            async def inner(*args, **kwargs):
                experimental_warn(message, stack_level=2)
                return await f(*args, **kwargs)

            return inner
        else:
            @wraps(f)
            def inner(*args, **kwargs):
                experimental_warn(message, stack_level=2)
                return f(*args, **kwargs)

            return inner

    return decorator


def unclosed_resource_warn(obj):
    msg = f"Unclosed {obj!r}."
    trace = tracemalloc.get_object_traceback(obj)
    if trace:
        msg += "\nObject allocated at (most recent call last):\n"
        msg += "\n".join(trace.format())
    else:
        msg += "\nEnable tracemalloc to get the object allocation traceback."
    warn(msg, ResourceWarning, stacklevel=2, source=obj)

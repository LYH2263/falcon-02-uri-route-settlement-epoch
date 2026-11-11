# Copyright 2013 by Rackspace Hosting, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Default router and utility functions.

This package implements Falcon's default routing engine, field converter
classes, and utility functions to aid in the implementation of custom
routers.
"""

from falcon.routing.compiled import CompiledRouter
from falcon.routing.compiled import CompiledRouterOptions
from falcon.routing.converters import BaseConverter
from falcon.routing.converters import DateTimeConverter
from falcon.routing.converters import FloatConverter
from falcon.routing.converters import IntConverter
from falcon.routing.converters import PathConverter
from falcon.routing.converters import RegexConverter
from falcon.routing.converters import UUIDConverter
from falcon.routing.settlement import CONVERTER_REJECTED
from falcon.routing.settlement import NO_TEMPLATE_MATCH
from falcon.routing.settlement import OUTCOME_METHOD_NOT_ALLOWED
from falcon.routing.settlement import OUTCOME_OPTIONS_DEFAULT
from falcon.routing.settlement import OUTCOME_RESOURCE_HIT
from falcon.routing.settlement import OUTCOME_ROUTE_NOT_FOUND
from falcon.routing.settlement import OUTCOME_SINK_MATCHED
from falcon.routing.settlement import OUTCOME_STATIC_MATCHED
from falcon.routing.settlement import PLANE_NOT_FOUND
from falcon.routing.settlement import PLANE_RESOURCE
from falcon.routing.settlement import PLANE_SINK
from falcon.routing.settlement import PLANE_STATIC
from falcon.routing.settlement import RouteSettlement
from falcon.routing.settlement import SettlementFrozenError
from falcon.routing.settlement import SettlementRecord
from falcon.routing.static import StaticRoute
from falcon.routing.static import StaticRouteAsync
from falcon.routing.util import map_http_methods
from falcon.routing.util import set_default_responders

DefaultRouter = CompiledRouter

__all__ = (
    'BaseConverter',
    'CompiledRouter',
    'CompiledRouterOptions',
    'CONVERTER_REJECTED',
    'DateTimeConverter',
    'FloatConverter',
    'IntConverter',
    'map_http_methods',
    'NO_TEMPLATE_MATCH',
    'OUTCOME_METHOD_NOT_ALLOWED',
    'OUTCOME_OPTIONS_DEFAULT',
    'OUTCOME_RESOURCE_HIT',
    'OUTCOME_ROUTE_NOT_FOUND',
    'OUTCOME_SINK_MATCHED',
    'OUTCOME_STATIC_MATCHED',
    'PathConverter',
    'PLANE_NOT_FOUND',
    'PLANE_RESOURCE',
    'PLANE_SINK',
    'PLANE_STATIC',
    'RegexConverter',
    'RouteSettlement',
    'set_default_responders',
    'SettlementFrozenError',
    'SettlementRecord',
    'StaticRoute',
    'StaticRouteAsync',
    'UUIDConverter',
)

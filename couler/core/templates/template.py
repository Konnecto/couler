# Copyright 2021 The Couler Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections import OrderedDict

import attr

from couler.core import utils
from couler.core.templates.artifact import TypedArtifact, Artifact
from couler.core.templates.output import OutputArtifact, OutputParameter


@attr.s
class TemplateIO:
    artifact_cls = None
    parameter_cls = None

    parameters = attr.ib(factory=list)
    artifacts = attr.ib(factory=list)

    @classmethod
    def from_obj(cls, obj):
        targs = cls()
        if obj is not None:
            obj = utils.make_list_if_not(obj)

            for o in obj:
                if any([isinstance(o, pcl) for pcl in cls.parameter_cls]):
                    targs.parameters.append(o)
                elif any([isinstance(o, pcl) for pcl in cls.artifact_cls]):
                    targs.artifacts.append(o)

        return targs


@attr.s
class TemplateInput(TemplateIO):
    parameter_cls = [Artifact]
    artifact_cls = [OutputArtifact]


@attr.s
class TemplateOutput(TemplateIO):
    parameter_cls = [OutputParameter]
    artifact_cls = [TypedArtifact, OutputArtifact]


@attr.s
class Template(object):
    name = attr.ib()
    input = attr.ib(default=None, converter=TemplateInput.from_obj)
    output = attr.ib(default=None, converter=TemplateOutput.from_obj)
    timeout = attr.ib(default=None)
    retry = attr.ib(default=None)
    pool = attr.ib(default=None)
    enable_ulogfs = attr.ib(default=True)
    daemon = attr.ib(default=False)
    cache = attr.ib(default=None)
    tolerations = attr.ib(default=None)

    # def __attrs_post_init__(self):
    #     self.input = TemplateInput.from_obj(self.input)
    #     self.output = TemplateOutput.from_obj(self.output)

    def to_dict(self):
        template = OrderedDict({"name": self.name})
        if self.daemon:
            template["daemon"] = True
        if self.timeout is not None:
            template["activeDeadlineSeconds"] = self.timeout
        if self.retry is not None:
            template["retryStrategy"] = utils.config_retry_strategy(self.retry)
        if self.cache is not None:
            template["memoize"] = self.cache.to_dict()
        if self.tolerations is not None:
            template["tolerations"] = self.tolerations.copy()
        return template

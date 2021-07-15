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


@attr.s
class Output(object):
    # value = attr.ib(default=None)
    name = attr.ib(default=None)
    step_name: str = attr.ib(default=None)
    template_name: str = attr.ib(default=None)
    is_global: bool = attr.ib(default=False)

    def value(self, type):
        if self.is_global:
            return '"{{workflow.outputs.%s.%s}}"' % (type, self.name)
        else:
            return "%s.%s.outputs.%s.%s" % (
                self.step_name,
                self.template_name,
                type,
                self.name,
            )

    def placeholder(self, prefix, type):
        if self.is_global:
            return '"{{workflow.outputs.%s.%s.%s}}"' % (
                self.step_name,
                type,
                self.name,
            )
        else:
            return '"{{%s.%s.outputs.%s.%s}}"' % (
                prefix,
                self.step_name,
                type,
                self.name,
            )


@attr.s
class OutputEmpty(Output):
    pass


@attr.s
class OutputParameter(Output):
    path = attr.ib(default="")

    def to_yaml(self):
        return {"name": self.name, "valueFrom": {"path": self.path}}

    @property
    def value(self):
        return super().value("parameters")

    def placeholder(self, prefix):
        return super().placeholder(prefix, "parameters")


@attr.s
class OutputArtifact(Output):
    path = attr.ib(default="")
    artifact = attr.ib(default={})
    type = attr.ib(default="")

    @property
    def value(self):
        return super().value("artifacts")

    def placeholder(self, prefix):
        return super().placeholder(prefix, "artifacts")

    def to_yaml(self):
        yml = OrderedDict()
        yml["name"] = self.name,
        yml["path"] = self.path
        return yml


@attr.s
class OutputScript(Output):
    @property
    def value(self):
        return "%s.%s.outputs.result" % (self.step_name, self.template_name)

    def placeholder(self, prefix, type):
        return '"{{%s.%s.outputs.result}}"' % (prefix, self.step_name)


@attr.s
class OutputJob(Output):
    job_name = attr.ib(default="job")
    job_id = attr.ib(default="job_id")
    job_obj = attr.ib(default=None)

    @property
    def value(self):
        return self.job_name


def _parse_single_argo_output(output, prefix):
    if isinstance(output, OutputArtifact):
        return output
    if isinstance(output, Output):
        if output.is_global:
            return output.value
        else:
            return '"{{%s.%s.%s}}"' % (prefix, output.step_name, output.name)
    else:
        # enforce int, float and bool types to string
        if (
                isinstance(output, int)
                or isinstance(output, float)
                or isinstance(output, bool)
        ):
            output = "'%s'" % output

        return output


def parse_argo_output(output, prefix):
    if isinstance(output, OutputJob):
        return [
            _parse_single_argo_output(
                Output(value=output.job_id, is_global=output.is_global), prefix
            ),
            _parse_single_argo_output(
                Output(value=output.job_name, is_global=output.is_global),
                prefix,
            ),
            _parse_single_argo_output(
                Output(value=output.job_obj, is_global=output.is_global),
                prefix,
            ),
        ]
    else:
        return _parse_single_argo_output(output, prefix)


def extract_step_return(step_output):
    """Extract information for run container or script output.
    step_output is a list with multiple outputs
    """

    ret = {}
    if isinstance(step_output, dict):
        # The first element of outputs is used for control flow operation
        if 'script' in step_output:
            step_output = step_output["script"]
        elif len(step_output["parameters"]) != 0:
            step_output = step_output["parameters"][0]
        elif len(step_output["artifacts"]) != 0:
            step_output = step_output["artifacts"][0]

        # In case user input a normal variable
        if isinstance(step_output, Output):
            tmp = step_output.value.split(".")
            if len(tmp) < 4:
                raise ValueError("Incorrect step return representation")
            # To avoid duplicate map function
            output = tmp[2]
            for item in tmp[3:]:
                output = output + "." + item

            ret = {
                "name": step_output.template_name,
                "id": step_output.step_name,
                "output": output,
            }

            return ret
        else:
            ret["value"] = step_output
            return ret
    else:
        ret["value"] = step_output
        return ret

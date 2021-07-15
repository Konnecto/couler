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

import copy
from collections import OrderedDict

import attr

from couler.core import states, utils
from couler.core.constants import OVERWRITE_GPU_ENVS
from couler.core.templates.artifact import TypedArtifact
from couler.core.templates.output import OutputArtifact, OutputJob, OutputEmpty, OutputParameter
from couler.core.templates.parameter import ArgumentsParameter, InputParameter
from couler.core.templates.secret import Secret
from couler.core.templates.template import Template, TemplateOutput


@attr.s
class Container(Template):
    image = attr.ib(default=None)
    command = attr.ib(default=None)
    args = attr.ib(default=None)
    env = attr.ib(default=None)
    env_from = attr.ib(default=None)
    secret = attr.ib(default=None)
    resources = attr.ib(default=None)
    image_pull_policy = attr.ib(default=None)
    volume_mounts = attr.ib(default=None)
    working_dir = attr.ib(default=None)
    node_selector = attr.ib(default=None)
    volumes = attr.ib(default=None)
    enable_ulogfs = attr.ib(default=True)
    daemon = attr.ib(default=False)
    cache = attr.ib(default=None)
    tolerations = attr.ib(default=None)

    def __attrs_post_init__(self):
        self.command = utils.make_list_if_not(self.command)

    def get_volume_mounts(self):
        return self.volume_mounts

    def to_dict(self):
        template = Template.to_dict(self)
        # Inputs
        parameters = []
        if self.args is not None:
            i = 0
            for arg in self.args:
                if not isinstance(self.args[i], OutputArtifact):
                    if isinstance(arg, OutputJob):
                        for _ in range(3):
                            parameters.append(
                                {
                                    "name": utils.input_parameter_name(
                                        self.name, i
                                    )
                                }
                            )
                            i += 1
                    elif isinstance(arg, ArgumentsParameter) or isinstance(
                            arg, InputParameter
                    ):
                        parameters.append(arg.to_dict())
                    else:
                        para_name = utils.input_parameter_name(self.name, i)
                        parameters.append({"name": para_name})
                        i += 1

        # Input
        # Case 1: add the input parameter
        if len(parameters) > 0:
            template["inputs"] = OrderedDict()
            template["inputs"]["parameters"] = parameters

        # Case 2: add the input artifact
        _input_artifact_list = []
        for o in self.input.artifacts:
            if isinstance(o, TypedArtifact):
                _input_artifact_list.append(o.to_yaml())
            if isinstance(o, OutputArtifact):
                name = o.artifact["name"]
                if not any(name == x["name"] for x in _input_artifact_list):
                    _input_artifact_list.append(o.artifact)

        if len(_input_artifact_list) > 0:
            if "inputs" not in template:
                template["inputs"] = OrderedDict()

            template["inputs"]["artifacts"] = _input_artifact_list

        # Node selector
        if self.node_selector is not None:
            # TODO: Support inferring node selector values from Argo parameters
            template["nodeSelector"] = self.node_selector

        # Container
        if (
                not utils.gpu_requested(self.resources)
                and states._overwrite_nvidia_gpu_envs
        ):
            if self.env is None:
                self.env = {}
            self.env.update(OVERWRITE_GPU_ENVS)
        template["container"] = self.container_dict()

        # Output
        if self.output is not None:
            template["outputs"] = {
                "artifacts": [o.to_yaml() for o in self.output.artifacts],
                "parameters": [o.to_yaml() for o in self.output.parameters],
            }

        return template

    def container_dict(self):
        # Container part
        container = OrderedDict({"image": self.image, "command": self.command})
        if utils.non_empty(self.args):
            container["args"] = self._convert_args_to_input_parameters(
                self.args
            )
        if utils.non_empty(self.env):
            container["env"] = utils.convert_dict_to_env_list(self.env)
        if self.secret is not None:
            if not isinstance(self.secret, Secret):
                raise ValueError(
                    "Parameter secret should be an instance of Secret"
                )
            if self.env is None:
                container["env"] = self.secret.to_env_list()
            else:
                container["env"].extend(self.secret.to_env_list())
        if self.env_from is not None:
            container["envFrom"] = self.env_from
        if self.resources is not None:
            container["resources"] = {
                "requests": self.resources,
                # To fix the mojibake issue when dump yaml for one object
                "limits": copy.deepcopy(self.resources),
            }
        if self.image_pull_policy is not None:
            container["imagePullPolicy"] = utils.config_image_pull_policy(
                self.image_pull_policy
            )
        if self.volume_mounts is not None:
            container["volumeMounts"] = [
                vm.to_dict() for vm in self.volume_mounts
            ]
        if self.working_dir is not None:
            container["workingDir"] = self.working_dir
        return container

    def _convert_args_to_input_parameters(self, args):
        parameters = []
        if args is not None:
            for i in range(len(args)):
                o = args[i]
                if isinstance(o, ArgumentsParameter) or isinstance(
                        o, InputParameter
                ):
                    pass
                elif not isinstance(o, OutputArtifact):
                    para_name = utils.input_parameter_name(self.name, i)
                    param_full_name = '"{{inputs.parameters.%s}}"' % para_name
                    if param_full_name not in parameters:
                        parameters.append(param_full_name)

        return parameters


def _container_param_output(output_object, step_name, template_name):
    is_global = "globalName" in output_object
    return OutputParameter(
        name=output_object["name"],
        step_name=step_name,
        template_name=template_name,
        is_global=is_global,
    )


def _container_artifact_output(output_object, step_name, template_name):
    is_global = "globalName" in output_object
    return OutputArtifact(
        name=output_object["name"],
        step_name=step_name,
        template_name=template_name,
        path=output_object["path"],
        artifact=output_object,
        is_global=is_global,
    )


def _container_output(step_name, template_name, output):
    """Generate output name from an Argo container template.  For example,
    "{{steps.generate-parameter.outputs.parameters.hello-param}}" used in
    https://github.com/argoproj/argo/tree/master/examples#output-parameters.
    Each element of return for run_container is contacted by:
    couler.step_name.template_name.output.parameters.output_id
    """

    if output is None:
        return {
            "parameters": [
                OutputEmpty(
                    name="%s-empty-output" % template_name,
                    step_name=step_name,
                    template_name=template_name,
                )
            ]
        }

    return {
        "parameters": [
            _container_param_output(
                output_object=o,
                step_name=step_name,
                template_name=template_name,
            )
            for o in output["parameters"]
        ],
        "artifacts": [
            _container_artifact_output(
                output_object=o,
                step_name=step_name,
                template_name=template_name,
            )
            for o in output["artifacts"]
        ],
    }

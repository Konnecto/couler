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

from couler.core import utils
from couler.core.templates.output import OutputJob
from couler.core.templates.template import Template


class Job(Template):
    def __init__(
            self,
            name,
            args,
            action,
            manifest,
            set_owner_reference,
            success_condition,
            failure_condition,
            timeout=None,
            retry=None,
            pool=None,
            cache=None,
    ):
        Template.__init__(
            self,
            name=name,
            timeout=timeout,
            retry=retry,
            pool=pool,
            cache=cache,
        )
        self.args = args
        self.action = action
        self.manifest = manifest
        self.set_owner_reference = utils.bool_to_str(set_owner_reference)
        self.success_condition = success_condition
        self.failure_condition = failure_condition

    def to_dict(self):
        template = Template.to_dict(self)
        if utils.non_empty(self.args):
            template["inputs"] = {"parameters": self.args}
        template["resource"] = self.resource_dict()

        # Append outputs to this template
        # return the resource job name, job ID, and job object by default
        job_outputs = [
            OrderedDict(
                {
                    "name": "job-name",
                    "valueFrom": {"jsonPath": '"{.metadata.name}"'},
                }
            ),
            OrderedDict(
                {
                    "name": "job-id",
                    "valueFrom": {"jsonPath": '"{.metadata.uid}"'},
                }
            ),
            OrderedDict({"name": "job-obj", "valueFrom": {"jqFilter": '"."'}}),
        ]
        template["outputs"] = {"parameters": job_outputs}
        return template

    def resource_dict(self):
        resource = OrderedDict(
            {
                "action": self.action,
                "setOwnerReference": self.set_owner_reference,
                "manifest": self.manifest,
            }
        )
        if self.success_condition:
            resource["successCondition"] = self.success_condition
        if self.failure_condition:
            resource["failureCondition"] = self.failure_condition
        return resource


def _job_output(step_name, template_name):
    """
    :param step_name:
    :param template_name:
    https://github.com/argoproj/argo/blob/master/examples/k8s-jobs.yaml#L44
    Return the job name and job id for running a job
    """
    job_name = "couler.%s.%s.outputs.parameters.job-name" % (
        step_name,
        template_name,
    )
    job_id = "couler.%s.%s.outputs.parameters.job-id" % (
        step_name,
        template_name,
    )
    job_obj = "couler.%s.%s.outputs.parameters.job-obj" % (
        step_name,
        template_name,
    )

    return {
        "job": OutputJob(
            step_name=step_name, template_name=template_name,
            job_name=job_name, job_obj=job_obj, job_id=job_id
        )
    }

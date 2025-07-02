import logging
from typing import Union, Optional, Iterator, Any, Dict
import pathlib
import json
import ast
import shlex
import os
from uuid import uuid4

from .abstract_translator import Translator
from ...common import Workflow, Task


class TexeraTranslator(Translator):
    """
    Translates a WfFormat workflow into Texera workflow JSON format.
    Each Task becomes either a BashSource (for roots) or a Bash operator.
    """

    def __init__(
        self,
        workflow: Union[Workflow, pathlib.Path],
        logger: Optional[logging.Logger] = None
    ):
        super().__init__(workflow, logger)

    def _generate_command(self, task: Task) -> str:
        """
        Generates an executable command string based on the task, handling
        --input-files and --output-files rewriting the paths under data/.
        """
        cmd_str = f"./bin/{task.program}"
        final_args = []
        print("input")
        print(task.input_files[0].file_id)
        print(task.input_files[0].link)
        print(task.cores)
        for arg in task.args:
            if arg.startswith("--output-files"):
                flag, outfiles_str = arg.split(" ", 1)
                outfiles = ast.literal_eval(outfiles_str)
                outfiles_dict = {f"data/{k}": v for k, v in outfiles.items()}
                outfiles_json = json.dumps(outfiles_dict).replace('"', '\\"')
                final_args.append(f'{flag} "{outfiles_json}"')
            elif arg.startswith("--input-files"):
                flag, infiles_str = arg.split(" ", 1)
                infiles = ast.literal_eval(infiles_str)
                infiles_arr = [f"data/{f}" for f in infiles]
                infiles_json = json.dumps(infiles_arr).replace('"', '\\"')
                final_args.append(f'{flag} "{infiles_json}"')
            else:
                final_args.append(arg)
        if final_args:
            cmd_str += " " + " ".join(final_args)

        return cmd_str

    def translate(self, output_folder: pathlib.Path) -> None:
        """
        1) Create output directory
        2) Copy binaries to bin/
        3) Generate input files for root tasks in data/
        4) Build workflow JSON with operators and links
        5) Write workflow-texera.json
        """
        output_folder.mkdir(parents=True, exist_ok=True)
        self._copy_binary_files(output_folder)
        self._generate_input_files(output_folder)

        content = self._build_texera_workflow(output_folder)
        json_path = output_folder / "workflow-texera.json"
        self._write_output_file(json.dumps(content, indent=2), json_path)
        self.logger.info(f"Texera workflow created at {json_path}")

    def _build_texera_workflow(self, output_folder: pathlib.Path) -> Dict[str, Any]:
        operators = []
        links = []
        ops_to_view = []
        ops_to_reuse = []

        print("self.task===##")
        print(self.tasks)
        for task_id, task in self.tasks.items():
            is_root = task_id in self.root_task_names
            cmd = self._generate_command(task)

            if is_root:
                op_type = "BashSource"
                operator = {
                    "operatorID": f"{op_type}-operator-{uuid4()}-{task.task_id}",
                    "operatorType": op_type,
                    "cmd": cmd,
                    "inputPorts": [],
                    "outputPorts": [
                        {
                            "portID": "output-0",
                            "displayName": "",
                            "allowMultiInputs": False,
                            "isDynamicPort": False
                        }
                    ]
                }
            else:
                op_type = "Bash"
                operator = {
                    "operatorID": f"{op_type}-operator-{uuid4()}-{task.task_id}",
                    "operatorType": op_type,
                    "cmd": cmd,
                    "inputPorts": [
                        {
                            "portID": "input-0",
                            "displayName": "",
                            "allowMultiInputs": True,
                            "isDynamicPort": False
                        }
                    ],
                    "outputPorts": [
                        {
                            "portID": "output-0",
                            "displayName": "",
                            "allowMultiInputs": False,
                            "isDynamicPort": False
                        }
                    ]
                }

            operators.append(operator)

        for child_id, parents in self.task_parents.items():
            for parent_id in parents:
                from_op = next(op for op in operators if op["operatorID"].endswith(parent_id))
                to_op   = next(op for op in operators if op["operatorID"].endswith(child_id))
                links.append({
                    "fromOpId":   from_op["operatorID"],
                    "fromPortId": {"id": 0, "internal": False},
                    "toOpId":     to_op["operatorID"],
                    "toPortId":   {"id": 0, "internal": False}
                })

        return {
            "operators":        operators,
            "links":            links,
            "opsToViewResult":  ops_to_view,
            "opsToReuseResult": ops_to_reuse
        }

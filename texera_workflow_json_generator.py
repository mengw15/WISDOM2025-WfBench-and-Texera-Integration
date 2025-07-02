import pathlib
from wfcommons.wfchef.recipes import EpigenomicsRecipe
from wfcommons.wfchef.recipes import BlastRecipe
from wfcommons.wfbench import WorkflowBenchmark, TexeraTranslator

# Initialize a WorkflowBenchmark for the BLAST workflow with 45 tasks (you can adjust this)
benchmark = WorkflowBenchmark(recipe=BlastRecipe, num_tasks=45)

# Set the path where the generated workflow JSON will be saved (change as needed)
workflow_json_path = pathlib.Path("/tmp/")

# Generate the benchmark workflow with specified CPU work and data (you can adjust this)
benchmark.create_benchmark(workflow_json_path, cpu_work=100, data=10)

# Change "YOUR_OUTPUT_DIR" below to point to your desired folder
output_folder = pathlib.Path("./YOUR_OUTPUT_DIR")

# Create and run the TexeraTranslator for the generated workflow
translator = TexeraTranslator(benchmark.workflow)
translator.translate(output_folder)

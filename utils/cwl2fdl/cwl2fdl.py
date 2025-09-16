#!/usr/bin/env python3

import yaml
import os
import re
from typing import Dict, List
import argparse


class CWL2OSCAR:
    def __init__(self, cwl_file: str, output_dir: str):
        self.cwl_file = cwl_file
        self.output_dir = output_dir
        self.cwl_data = None
        self.fdl_data = {"functions": {"oscar": []}}

    def load_cwl(self):
        """Load and parse the CWL file"""
        with open(self.cwl_file, 'r') as f:
            self.cwl_data = yaml.safe_load(f)

    def extract_docker_image(self, step: Dict) -> str:
        """Extract Docker image from step requirements"""
        if 'requirements' in step['run'] and 'DockerRequirement' in step['run']['requirements']:
            return step['run']['requirements']['DockerRequirement']['dockerPull']
        return None

    def extract_resources(self, step: Dict) -> Dict:
        """Extract resource requirements from step"""
        resources = {'memory': '1Gi', 'cpu': '1.0'}  # Default values
        if 'requirements' in step['run'] and 'ResourceRequirement' in step['run']['requirements']:
            req = step['run']['requirements']['ResourceRequirement']
            if 'ramMin' in req:
                resources['memory'] = f"{req['ramMin']}Mi"
            if 'coresMin' in req:
                resources['cpu'] = str(req['coresMin'])
        return resources

    def get_input_parameters(self, step: Dict) -> List[str]:
        """Extract input parameters from CWL step"""
        params = []
        if 'inputs' in step['run']:
            for input_name, input_spec in step['run']['inputs'].items():
                if 'inputBinding' in input_spec:
                    binding = input_spec['inputBinding']
                    if 'prefix' in binding:
                        prefix = binding['prefix']
                        value = input_spec.get('default', '')
                        params.append(f"{prefix} {value}")
        return params

    def get_output_glob(self, step: Dict) -> str:
        """Extract output glob pattern from CWL step"""
        if 'outputs' in step['run']:
            for output_name, output_spec in step['run']['outputs'].items():
                if 'outputBinding' in output_spec and 'glob' in output_spec['outputBinding']:
                    return output_spec['outputBinding']['glob']
        return None

    def generate_script(self, step: Dict, step_name: str) -> str:
        """Generate script content for a step"""
        script_content = "#!/bin/bash\n\n"

        # Add input file handling
        script_content += f'echo "SCRIPT: Processing {step_name}"\n'
        script_content += 'FILE_NAME=`basename "$INPUT_FILE_PATH"`\n'

        # Get output glob pattern and set output file path
        output_glob = self.get_output_glob(step)
        if output_glob:
            script_content += f'OUTPUT_FILE="$TMP_OUTPUT_DIR/{output_glob}"\n'
        else:
            script_content += 'OUTPUT_FILE="$TMP_OUTPUT_DIR/$FILE_NAME"\n'

        # Extract command from CWL
        if 'arguments' in step['run']:
            for arg in step['run']['arguments']:
                if 'valueFrom' in arg:
                    cmd = arg['valueFrom'].strip()
                    # Replace CWL variables with OSCAR variables
                    # First, replace any input variables with INPUT_FILE_PATH
                    input_vars = re.findall(r'\$\(inputs\.(\w+)\.path\)', cmd)
                    for var in input_vars:
                        cmd = cmd.replace(f'$(inputs.{var}.path)', '$INPUT_FILE_PATH')
                    # Replace output.json with $OUTPUT_FILE
                    cmd = cmd.replace('output.json', '$OUTPUT_FILE')
                    script_content += f'{cmd}\n'
        elif 'baseCommand' in step['run']:
            cmd = ' '.join(step['run']['baseCommand'])
            # Add any input parameters
            params = self.get_input_parameters(step)
            param_str = ' '.join(params)
            script_content += f'{cmd} "$INPUT_FILE_PATH" {param_str} "$OUTPUT_FILE"\n'

        script_content += f'\necho "Output saved to: $OUTPUT_FILE"\n'
        return script_content

    def create_fdl_step(self, step: Dict, step_name: str) -> Dict:
        """Create FDL configuration for a step"""
        docker_image = self.extract_docker_image(step)
        resources = self.extract_resources(step)

        fdl_step = {
            "oscar-cluster": {
                "name": step_name,
                "memory": resources['memory'],
                "cpu": resources['cpu'],
                "image": docker_image,
                "script": f"script-{step_name}.sh",
                "input": [{
                    "storage_provider": "minio.default",
                    "path": f"{step_name}/in"
                }],
                "output": [{
                    "storage_provider": "minio.default",
                    "path": f"{step_name}/out"
                }]
            }
        }
        return fdl_step

    def convert(self):
        """Convert CWL workflow to OSCAR FDL"""
        self.load_cwl()

        # Process each step
        for step_name, step in self.cwl_data['steps'].items():
            # Create FDL step configuration
            fdl_step = self.create_fdl_step(step, step_name)
            self.fdl_data['functions']['oscar'].append(fdl_step)

            # Generate script for the step
            script_content = self.generate_script(step, step_name)
            script_path = os.path.join(self.output_dir, f"script-{step_name}.sh")
            with open(script_path, 'w') as f:
                f.write(script_content)
            os.chmod(script_path, 0o755)  # Make script executable

        # Write FDL configuration
        fdl_path = os.path.join(self.output_dir, "oscar-functions.yml")
        with open(fdl_path, 'w') as f:
            yaml.dump(self.fdl_data, f, default_flow_style=False)


def main():
    parser = argparse.ArgumentParser(description='Convert CWL workflow to OSCAR FDL format')
    parser.add_argument('cwl_file', help='Path to the CWL workflow file')
    parser.add_argument('--output-dir', default='fdl-output', help='Output directory for FDL files')
    args = parser.parse_args()

    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)

    # Convert workflow
    converter = CWL2OSCAR(args.cwl_file, args.output_dir)
    converter.convert()
    print(f"Conversion completed. Output files are in {args.output_dir}")


if __name__ == "__main__":
    main()

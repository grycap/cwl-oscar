#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: Workflow

label: Simple two-step workflow for minideb
doc: A simple workflow that creates a file and then processes it

requirements:
  - class: StepInputExpressionRequirement

inputs:
  message:
    type: string
    default: "Hello from CWL workflow"
    doc: "Message to write to the file"

outputs:
  final_result:
    type: File
    outputSource: count_lines/line_count_file
    doc: "File containing the line count result"

steps:
  create_file:
    run: 
      cwlVersion: v1.0
      class: CommandLineTool
      id: create_file_tool

      label: Create a text file
      doc: Creates a text file with multiple lines of content

      requirements:
        - class: DockerRequirement
          dockerPull: opensourcefoundries/minideb:jessie

      baseCommand: [sh, -c]

      inputs:
        input_message:
          type: string

      arguments:
        - valueFrom: |
            echo "Creating file with content..." &&
            echo "$(inputs.input_message)" > output.txt &&
            echo "Line 2: This is a second line" >> output.txt &&
            echo "Line 3: This is a third line" >> output.txt &&
            echo "Line 4: End of file" >> output.txt &&
            echo "File created successfully"
          position: 1

      outputs:
        output_file:
          type: File
          outputBinding:
            glob: "output.txt"
    in:
      input_message: message
    out: [output_file]

  count_lines:
    run: 
      cwlVersion: v1.0
      class: CommandLineTool
      id: count_lines_tool

      label: Count lines in file
      doc: Counts the number of lines in the input file and creates a summary

      requirements:
        - class: DockerRequirement
          dockerPull: opensourcefoundries/minideb:jessie
        - class: InitialWorkDirRequirement
          listing:
            - $(inputs.input_file)

      baseCommand: [sh, -c]

      inputs:
        input_file:
          type: File

      arguments:
        - valueFrom: |
            INPUT_FILE="$(inputs.input_file.basename)" &&
            echo "Processing file: $INPUT_FILE" &&
            echo "File contents:" &&
            cat "$INPUT_FILE" &&
            echo "" &&
            echo "Line count analysis:" &&
            wc -l "$INPUT_FILE" > line_count.txt &&
            wc -c "$INPUT_FILE" >> line_count.txt &&
            echo "Analysis complete"
          position: 1

      outputs:
        line_count_file:
          type: File
          outputBinding:
            glob: "line_count.txt"
    in:
      input_file: create_file/output_file
    out: [line_count_file] 
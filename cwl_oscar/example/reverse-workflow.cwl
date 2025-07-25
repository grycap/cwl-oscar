#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: Workflow

label: Simple echo and reverse workflow
doc: Echo text to a file, then reverse the text

inputs:
  message:
    type: string
    default: "Hello CWL World"
    doc: "Text to echo and reverse"

outputs:
  reversed_text:
    type: File
    outputSource: reverse_step/reversed_file
    doc: "File containing the reversed text"

steps:
  echo_step:
    run: 
      cwlVersion: v1.0
      class: CommandLineTool
      
      requirements:
        - class: DockerRequirement
          dockerPull: opensourcefoundries/minideb:jessie

      baseCommand: [sh, -c]

      inputs:
        text:
          type: string

      arguments:
        - valueFrom: |
            echo "$(inputs.text)" > message.txt
          position: 1

      outputs:
        text_file:
          type: File
          outputBinding:
            glob: "message.txt"
    in:
      text: message
    out: [text_file]

  reverse_step:
    run: 
      cwlVersion: v1.0
      class: CommandLineTool
      
      requirements:
        - class: DockerRequirement
          dockerPull: opensourcefoundries/minideb:jessie

      baseCommand: [sh, -c]

      inputs:
        input_file:
          type: File

      arguments:
        - valueFrom: |
            rev "$(inputs.input_file.path)" > reversed.txt
          position: 1

      outputs:
        reversed_file:
          type: File
          outputBinding:
            glob: "reversed.txt"
    in:
      input_file: echo_step/text_file
    out: [reversed_file] 
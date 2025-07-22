#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
doc: "Test whoami command via OSCAR"

baseCommand: whoami

inputs: []

outputs:
  result:
    type: File
    doc: "The output file containing the current user"
    outputBinding:
      glob: "result.txt"

stdout: result.txt 
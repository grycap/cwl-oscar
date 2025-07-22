#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
doc: "Test grep command to search for patterns"

baseCommand: grep

inputs:
  pattern:
    type: string
    doc: "The pattern to search for"
    inputBinding:
      position: 1
  file_path:
    type: string
    doc: "The file path to search in"
    inputBinding:
      position: 2
    default: "/etc/passwd"

outputs:
  grep_result:
    type: File
    doc: "The output file containing the grep results"
    outputBinding:
      glob: "grep_result.txt"

stdout: grep_result.txt 
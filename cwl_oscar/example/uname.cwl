#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
doc: "Test uname command to show system information"

baseCommand: uname
arguments: ["-a"]

inputs: []

outputs:
  system_info:
    type: File
    doc: "The output file containing system information"
    outputBinding:
      glob: "system_info.txt"

stdout: system_info.txt 
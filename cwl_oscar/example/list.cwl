#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
doc: "A simple ls command example for OSCAR execution"

baseCommand: ls

inputs:
  directory:
    type: string
    doc: "The directory to list"
    inputBinding:
      position: 1
    default: "/"

outputs:
  hello_output:
    type: File
    doc: "The output file containing the directory listing"
    outputBinding:
      glob: "hello.txt"

stdout: hello.txt 
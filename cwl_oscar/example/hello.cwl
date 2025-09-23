#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
doc: "A simple hello world example for OSCAR execution"

baseCommand: echo

inputs:
  message:
    type: string
    doc: "The message to echo"
    inputBinding:
      position: 1

outputs:
  hello_output:
    type: File
    doc: "The output file containing the message"
    outputBinding:
      glob: "hello.txt"

stdout: hello.txt 
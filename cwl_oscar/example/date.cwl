#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
doc: "Test simple date command"

baseCommand: date

inputs: []

outputs:
  datetime:
    type: File
    doc: "The output file containing current date and time"
    outputBinding:
      glob: "datetime.txt"

stdout: datetime.txt 
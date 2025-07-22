#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
doc: "Test cat command to read /etc/hostname"

baseCommand: cat
arguments: ["/etc/hostname"]

inputs: []

outputs:
  hostname:
    type: File
    doc: "The output file containing the hostname"
    outputBinding:
      glob: "hostname.txt"

stdout: hostname.txt 
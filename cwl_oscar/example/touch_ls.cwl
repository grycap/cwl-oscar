#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
doc: "Test creating a temporary file then listing current directory"

baseCommand: sh
arguments: ["-c", "touch testfile.tmp && ls -la"]

inputs: []

outputs:
  listing:
    type: File
    doc: "The output file containing the directory listing"
    outputBinding:
      glob: "listing.txt"

stdout: listing.txt 
cwlVersion: v1.2
class: Workflow

inputs:
  image: File

outputs:
  final_result:
    type: File
    outputSource: classify/output_json

steps:
  grayify:
    run:
      class: CommandLineTool
      baseCommand: [convert]
      inputs:
        input_image:
          type: File
          inputBinding:
            position: 1
        convertionType:
          type: string
          default: "Grayscale"
          inputBinding:
            position: 2
            prefix: "-type"
        outputName:
          type: string
          default: "gray-image.png"
          inputBinding:
            position: 3
      outputs:
        output_image:
          type: File
          outputBinding:
            glob: "gray-image.png"
      requirements:
        DockerRequirement:
          dockerPull: ghcr.io/grycap/imagemagick
        ResourceRequirement:
          ramMin: 1024
          coresMin: 1
    in:
      input_image: image
    out: [output_image]

  classify:
    run:
      class: CommandLineTool
      baseCommand: [bash, -c]
      inputs:
        gray_image:
          type: File
      outputs:
        output_json:
          type: File
          outputBinding:
            glob: output.json
      arguments:
        - position: 2
          valueFrom: >
            deepaas-cli predict --files $(inputs.gray_image.path) 2>/dev/null | grep -Po '{.*}' | sed "s/'/\"/g" > output.json
      requirements:
        DockerRequirement:
          dockerPull: ai4oshub/plants-classification
        ResourceRequirement:
          ramMin: 2048
          coresMin: 1
    in:
      gray_image: grayify/output_image
    out: [output_json]
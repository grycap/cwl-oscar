# CWL Example: Image Processing and Plant Classification

This example demonstrates a Common Workflow Language (CWL) workflow that processes images through two steps:
1. **Grayify**: Converts an input image to grayscale using ImageMagick
2. **Classify**: Analyzes the grayscale image using a plant classification model

## Prerequisites

- [cwltool](https://github.com/common-workflow-language/cwltool) or any [alternative CWL runner implementations](https://www.commonwl.org/implementations/)
- Docker (required for running the containerized tools)
- An input image file (e.g., `untitled.png`)

## Usage

Run the workflow using cwltool:

```bash
cwltool workflow.cwl --image untitled.png
```

An file with the name `output.json` should be saved in the current working directory.
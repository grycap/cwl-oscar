# CWL-OSCAR Architecture Documentation

This directory contains Mermaid diagrams documenting the architecture and communication flows of the cwl-oscar implementation.

## Diagram Files

### 1. execution-flow.mmd
**Overall Execution Flow Diagram**
- Shows the complete end-to-end flow from user command to final output
- Highlights the separation between local system and remote OSCAR cluster
- Demonstrates the key processing stages and data flow

### 2. component-architecture.mmd
**Component Architecture Diagram**
- Illustrates the layered architecture with clear separation of concerns
- Shows the relationships between different component layers:
  - User Interface Layer
  - CWL Integration Layer
  - OSCAR Backend Components
  - OSCAR Service Layer
  - Data Flow

### 3. communication-sequence.mmd
**Communication Sequence Diagram**
- Time-based sequence showing communications between all components
- Demonstrates the step-by-step process from user command to final results
- Shows the interaction patterns between local and remote components

## How to Use These Diagrams

### Option 1: Online Mermaid Editor
1. Go to [Mermaid Live Editor](https://mermaid.live/)
2. Copy the content from any `.mmd` file
3. Paste it into the editor to view and export the diagram

### Option 2: VS Code Extension
1. Install the "Mermaid Preview" extension in VS Code
2. Open any `.mmd` file
3. Use the preview command to view the diagram

### Option 3: GitHub Rendering
GitHub natively supports Mermaid diagrams in Markdown files. You can embed them like:

```markdown
```mermaid
<!-- Paste diagram content here -->
```

### Option 4: Export to Images
Using the Mermaid CLI tool:
```bash
npm install -g @mermaid-js/mermaid-cli
mmdc -i execution-flow.mmd -o execution-flow.png
mmdc -i component-architecture.mmd -o component-architecture.png
mmdc -i communication-sequence.mmd -o communication-sequence.png
```

## Architecture Overview

The cwl-oscar implementation follows a modular architecture that:

1. **Extends cwltool**: Uses the standard cwltool extension pattern
2. **Integrates with OSCAR**: Leverages the OSCAR Python client for cluster communication
3. **Maintains CWL Compatibility**: Preserves standard CWL workflow semantics
4. **Enables Remote Execution**: Executes commands on OSCAR clusters while maintaining local workflow management

## Key Communication Patterns

- **Local Processing**: CWL parsing, command building, and output collection
- **Remote Execution**: Actual command execution via OSCAR HTTP API
- **Script Generation**: CWL commands wrapped in bash scripts for OSCAR
- **Output Synthesis**: Local output files created to satisfy CWL requirements

## Implementation Status

✅ **Working Components:**
- Basic CWL workflow execution
- OSCAR service integration
- Command line building
- Output collection
- Error handling

🚧 **Future Enhancements:**
- Real output retrieval from OSCAR storage
- Automatic service creation
- Asynchronous execution support
- Multi-step workflow support
- Advanced file transfer capabilities

For more information, see the main README.md in the cwl_oscar directory. 
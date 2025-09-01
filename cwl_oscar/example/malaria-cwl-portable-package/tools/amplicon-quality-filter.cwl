#!/usr/bin/env cwl-runner

cwlVersion: v1.2
class: CommandLineTool

label: "Amplicon Quality Filter"
doc: "Quality filtering for malaria amplicon data"

requirements:
  DockerRequirement:
    dockerPull: "robertbio/seqtk:1.4"
  InlineJavascriptRequirement: {}

baseCommand: [bash, -c]

arguments:
  - valueFrom: |
      ${
        var input_file = inputs.fastq_file.path;
        var marker_id = inputs.marker_id;
        var min_len = inputs.min_length;
        var max_len = inputs.max_length;
        var min_qual = inputs.min_quality;
        
        return "set -e\n" +
        "\n" +
        "INPUT_FILE=\"" + input_file + "\"\n" +
        "MARKER_ID=\"" + marker_id + "\"\n" +
        "MIN_LEN=\"" + min_len + "\"\n" +
        "MAX_LEN=\"" + max_len + "\"\n" +
        "MIN_QUAL=\"" + min_qual + "\"\n" +
        "\n" +
        "OUTPUT_FASTQ=\"" + marker_id + "_filtered.fastq\"\n" +
        "STATS_FILE=\"" + marker_id + "_filter_stats.txt\"\n" +
        "\n" +
        "echo \"=== Quality Filtering ===\"\n" +
        "echo \"Input: $INPUT_FILE\"\n" +
        "echo \"Marker: $MARKER_ID\"\n" +
        "echo \"Length range: ${MIN_LEN}-${MAX_LEN}bp\"\n" +
        "echo \"Min quality: $MIN_QUAL\"\n" +
        "\n" +
        "# Count input reads\n" +
        "if [[ \"$INPUT_FILE\" == *.gz ]]; then\n" +
        "  TOTAL_READS=$(gunzip -c \"$INPUT_FILE\" | wc -l | awk '{print $1/4}')\n" +
        "else\n" +
        "  TOTAL_READS=$(wc -l < \"$INPUT_FILE\" | awk '{print $1/4}')\n" +
        "fi\n" +
        "\n" +
        "echo \"Total input reads: $TOTAL_READS\"\n" +
        "\n" +
        "# Apply filtering steps\n" +
        "if [[ \"$INPUT_FILE\" == *.gz ]]; then\n" +
        "  gunzip -c \"$INPUT_FILE\" | seqtk seq -L $MIN_LEN - | seqtk seq -M $MAX_LEN - > temp_length.fastq\n" +
        "else\n" +
        "  seqtk seq -L $MIN_LEN \"$INPUT_FILE\" | seqtk seq -M $MAX_LEN - > temp_length.fastq\n" +
        "fi\n" +
        "\n" +
        "seqtk seq -q $MIN_QUAL temp_length.fastq > temp_quality.fastq\n" +
        "\n" +
        "# Remove N-containing reads\n" +
        "awk 'BEGIN{keep=0} {if(NR%4==1) {header=$0} else if(NR%4==2) {if($0!~/N/) keep=1; else keep=0; seq=$0} else if(NR%4==3) {plus=$0} else if(NR%4==0 && keep) {print header; print seq; print plus; print $0}}' temp_quality.fastq > \"$OUTPUT_FASTQ\"\n" +
        "\n" +
        "FINAL_READS=$(wc -l < \"$OUTPUT_FASTQ\" | awk '{print $1/4}')\n" +
        "\n" +
        "# Generate stats\n" +
        "cat > \"$STATS_FILE\" << EOF\n" +
        "Marker: $MARKER_ID\n" +
        "Total input: $TOTAL_READS\n" +
        "Final reads: $FINAL_READS\n" +
                 "Retention: $(awk \"BEGIN {printf \\\"%.1f\\\", $FINAL_READS*100/$TOTAL_READS}\")%\n" +
        "EOF\n" +
        "\n" +
        "echo \"Quality filtering complete: $FINAL_READS reads\"\n" +
        "rm -f temp_*.fastq";
      }

inputs:
  fastq_file:
    type: File
    doc: "Input FASTQ file (can be gzipped)"

  marker_id:
    type: string
    doc: "Marker identifier"

  min_length:
    type: int?
    default: 10
    doc: "Minimum read length (original study: 10bp)"

  max_length:
    type: int?
    default: 2100
    doc: "Maximum read length (original study: 2100bp)"

  min_quality:
    type: int?
    default: 8
    doc: "Minimum quality score"

outputs:
  filtered_reads:
    type: File
    outputBinding:
      glob: "*_filtered.fastq"
    doc: "Quality filtered reads"

  filtering_stats:
    type: File
    outputBinding:
      glob: "*_filter_stats.txt"
    doc: "Filtering statistics"

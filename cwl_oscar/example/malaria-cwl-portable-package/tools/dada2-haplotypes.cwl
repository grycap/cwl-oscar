#!/usr/bin/env cwl-runner

cwlVersion: v1.2
class: CommandLineTool

label: "DADA2 Haplotype Reconstruction"
doc: "DADA2-based haplotype reconstruction for malaria amplicon data"

requirements:
  DockerRequirement:
    dockerPull: "robertbio/dada2:1.30.0"
  InitialWorkDirRequirement:
    listing:
      - entryname: dada2_haplotypes.R
        entry: |
          # DADA2 Haplotype Reconstruction
          library(ShortRead)
          library(dada2)
          
          args <- commandArgs(trailingOnly = TRUE)
          input_file <- args[1]
          marker_id <- args[2] 
          output_prefix <- args[3]
          min_cov <- as.numeric(args[4])
          
          cat("=== DADA2 Haplotype Reconstruction ===\n")
          cat("Input file:", input_file, "\n")
          cat("Marker ID:", marker_id, "\n") 
          cat("Output prefix:", output_prefix, "\n")
          cat("Min coverage:", min_cov, "\n")
          
          # Read and filter sequences
          sr <- readFastq(input_file)
          initial_count <- length(sr)
          
          # Original study filtering: 10 < width < 2100, remove N reads
          # Apply same filtering as original 01_dada2.R script
          sr <- sr[10 < width(sr) & width(sr) < 2100]
          # Remove reads containing N bases (like rmNreads function in original)
          sr <- sr[!grepl("N", sread(sr))]
          filtered_count <- length(sr)
          
          cat("Filtered reads:", filtered_count, "/", initial_count, 
              "(", round(filtered_count/initial_count*100, 1), "%)\n")
          
          if(filtered_count == 0) {
            stop("No reads remaining after filtering")
          }
          
          # Write filtered file for DADA2
          temp_file <- paste0(output_prefix, "_temp.fastq")
          writeFastq(sr, temp_file, compress = FALSE)
          
          # Learn error model
          cat("Learning error model...\n")
          err <- learnErrors(temp_file, multithread = FALSE, verbose = FALSE)
          
          # Run DADA2 denoising
          cat("Running DADA2 denoising...\n")
          dadas <- dada(temp_file, err = err, multithread = FALSE, verbose = FALSE)
          
          # Create sequence table
          cat("Creating sequence table...\n")
          seqtab <- makeSequenceTable(dadas)
          
          # Remove chimeras
          cat("Removing chimeras...\n") 
          seqtab.nochim <- removeBimeraDenovo(seqtab, method = "consensus", 
                                             multithread = FALSE, verbose = FALSE)
          
          retention <- sum(seqtab.nochim)/sum(seqtab)
          cat("Chimera removal: retained", round(retention*100, 1), "% of sequences\n")
          
          # Apply coverage thresholds matching original study
          sequences <- colnames(seqtab.nochim)
          coverages <- as.numeric(seqtab.nochim[1,])
          total_reads <- sum(coverages)
          frequencies <- coverages / total_reads
          
          # Original study thresholds:
          # minCovSamCO = 100 (minimum total coverage per sample)
          # minCovHapCO = 65 (minimum coverage per haplotype) 
          # minDecCO = 0.02 (minimum frequency 2%)
          
          cat("Total reads after DADA2:", total_reads, "\n")
          
          # Apply sample coverage threshold (like original study)
          if(total_reads < min_cov) {
            cat("WARNING: Total coverage", total_reads, "<", min_cov, "reads (minCovSamCO)\n")
          }
          
          # Filter by minimum coverage and frequency (like original)
          valid_coverage <- coverages >= min_cov
          valid_frequency <- frequencies >= 0.02  # minDecCO from original
          valid_idx <- which(valid_coverage & valid_frequency)
          
          cat("Haplotypes with >=", min_cov, "coverage:", sum(valid_coverage), "\n")
          cat("Haplotypes with >=2% frequency:", sum(valid_frequency), "\n")
          cat("Valid haplotypes (both criteria):", length(valid_idx), "\n")
          
          # Write FASTA output
          fasta_file <- paste0(output_prefix, "_haplotypes.fasta")
          if(length(valid_idx) > 0) {
            fasta_content <- c()
            for(i in seq_along(valid_idx)) {
              idx <- valid_idx[i]
              hap_name <- paste0(marker_id, "_hap", i, "_cov", coverages[idx])
              fasta_content <- c(fasta_content, paste0(">", hap_name), sequences[idx])
            }
            writeLines(fasta_content, fasta_file)
          } else {
            writeLines("", fasta_file)
          }
          
          # Write CSV output  
          csv_file <- paste0(output_prefix, "_haplotype_counts.csv")
          csv_content <- c("HaplotypeID,Sequence,Coverage,Frequency,Status")
          
          if(length(valid_idx) > 0) {
            for(i in seq_along(valid_idx)) {
              idx <- valid_idx[i]
              hap_name <- paste0(marker_id, "_hap", i)
              csv_content <- c(csv_content,
                              paste(hap_name, sequences[idx], coverages[idx], 
                                   sprintf("%.4f", frequencies[idx]), "Valid", sep = ","))
            }
          }
          writeLines(csv_content, csv_file)
          
          # Clean up
          unlink(temp_file)
          
          cat("=== Complete ===\n")
          cat("Haplotypes:", length(valid_idx), "\n")

baseCommand: [Rscript, dada2_haplotypes.R]

inputs:
  filtered_reads:
    type: File
    inputBinding:
      position: 1
    doc: "Quality filtered FASTQ reads"

  marker_id:
    type: string
    inputBinding:
      position: 2
    doc: "Marker identifier"

  output_prefix:
    type: string
    inputBinding:
      position: 3
    doc: "Output file prefix"

  min_coverage:
    type: int?
    default: 65
    inputBinding:
      position: 4
    doc: "Minimum coverage per haplotype"

outputs:
  haplotypes:
    type: File
    outputBinding:
      glob: "*_haplotypes.fasta"
    doc: "Reconstructed haplotypes"

  haplotype_counts:
    type: File
    outputBinding:
      glob: "*_haplotype_counts.csv"
    doc: "Haplotype coverage and frequency data"

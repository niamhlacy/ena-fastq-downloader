# ENA FASTQ Downloader with Strain Renaming

A small Bash utility to download FASTQ files from the **European Nucleotide Archive (ENA)** using a CSV/TSV mapping of **strain IDs** to **ERR accessions**.  
Files are saved directly with strain names. Supports parallel downloads, per-strain logging, and a final summary.

## Features
- Read mapping from **CSV or TSV** (header optional).
- Works with **paired-end** and **single-end** runs.
- Downloads over **HTTPS** using `wget` (no FTP issues).
- **Parallel** downloads (`JOBS` configurable).
- Per-strain **log files** and success/failure markers.
- Console output shows only **start** and **end** status per strain; full details go to logs.
- Summary of successes and failures at the end.

## Requirements
- Bash 4+
- `wget`

## Installation
Clone this repository and make the script executable:
```bash
git clone https://github.com/niamhlacy/ena-fastq-downloader.git
cd ena-fastq-downloader
chmod +x ena_wget_from_csv_parallel.sh


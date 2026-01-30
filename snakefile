import os

# 1. Define the command based on OS
# Windows (nt) uses 'py', Linux (GitHub) uses 'python3'
PYTHON_CMD = "py" if os.name == "nt" else "python3"

rule all:
    input: "bird_report.csv"

rule scrape_species:
    output: "species.done"
    # Pass the variable into the shell using Snakemake's config/params style
    params: py=PYTHON_CMD
    shell: "{params.py} species_scrappe.py && echo done > species.done"

rule consume_kafka:
    input: "species.done"
    output: "kafka.done"
    params: py=PYTHON_CMD
    shell: "{params.py} kafka_consume.py && echo done > kafka.done"

rule process_audio:
    input: "kafka.done"
    output: "audio.done"
    params: py=PYTHON_CMD
    shell: "{params.py} audio_process.py && echo done > audio.done"

rule generate_report:
    input: "audio.done"
    output: "bird_report.csv"
    params: py=PYTHON_CMD
    shell: "{params.py} report.py"

    #this is red because for some reason vscode isn't reading my code properly apparently
    # vs code is reading my snakefile like a standard python file.
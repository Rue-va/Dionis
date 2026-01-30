# Snakefile

rule all:
    input: "bird_report.csv"

rule scrape_species:
    output: "species.done"
    shell: "python species_scrappe.py && echo done > species.done"

rule consume_kafka:
    input: "species.done"
    output: "kafka.done"
    shell: "python kafka_consume.py && echo done > kafka.done"

rule process_audio:
    input: "kafka.done"
    output: "audio.done"
    shell: "python audio_process.py && echo done > audio.done"

rule generate_report:
    input: "audio.done"
    output: "bird_report.csv"
    shell: "python report.py"
    
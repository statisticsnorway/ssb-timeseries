import sys
import subprocess

sys.path.insert(0, os.path.abspath("../tools"))
notebooks = [
    f"../notebooks/{b}.py"
    for b in [
        "quickstart",
        "basic-usage",
        "calc-basic-arithmetic",
        "calc-with-time",
        "calc-with-metadata",
        "data-archiving-and-sharing",
        "data-types-and-storage",
        "meta-basics",
        "meta-search-and-filtering",
        "meta-tag-maintenance",
    ]
]
export_script = "../tools/marimo_to_md.py"
target_dir = "../docs/guides/"

environment = os.environ.copy()
environment["TIMESERIES_CONFIG"] = "../notebooks/minimal_configuration.json"

subprocess.run([export_script, *notebooks, target_dir], env=environment)

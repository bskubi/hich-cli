import subprocess
import tempfile
import os

def is_gzip(filename):
    return filename.endswith(".gzip") or filename.endswith(".gz")

def pbgzip_compress(filename):
   
    cmd = f"pbgzip -f {filename} && mv {filename}.gz -f {filename}"
    subprocess.run(cmd, shell=True)

def compression(filename):
    return "disable" if is_gzip(filename) else "infer_from_extension"
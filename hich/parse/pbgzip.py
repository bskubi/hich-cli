import subprocess
import tempfile
import os

def is_gzip(filename):
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    return filename.endswith(".gzip") or filename.endswith(".gz")

def pbgzip_compress(filename):
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    cmd = f"pbgzip -f {filename} && mv {filename}.gz -f {filename}"
    subprocess.run(cmd, shell=True)

def compression(filename):
    # !Warning: this method has no specific unit test as of 2024/10/20 - Ben Skubi
    return "disable" if is_gzip(filename) else "infer_from_extension"
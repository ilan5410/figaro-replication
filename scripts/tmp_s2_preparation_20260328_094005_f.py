
import os

# Final directory listing to confirm all outputs exist
OUT_DIR = "data/prepared"
files = os.listdir(OUT_DIR)
print("Contents of data/prepared/:")
for f in sorted(files):
    size = os.path.getsize(os.path.join(OUT_DIR, f))
    size_mb = size / (1024*1024)
    print(f"  {f:<35s}  {size_mb:8.2f} MB  ({size:,} bytes)")

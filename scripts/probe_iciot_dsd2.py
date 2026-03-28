"""Get the full DSD XML and extract all dimension and codelist info."""
import urllib.request
import gzip
import re

url = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/datastructure/ESTAT/NAIO_10_FCP_IP1?format=json"
req = urllib.request.Request(url, headers={"Accept-Encoding": "gzip"})
with urllib.request.urlopen(req, timeout=30) as resp:
    raw = resp.read()
try:
    content = gzip.decompress(raw).decode()
except Exception:
    content = raw.decode("utf-8", errors="replace")

# Extract all dimension IDs and positions
dims = re.findall(r'<s:Dimension id="([^"]+)" position="(\d+)"', content)
print("Dimensions:")
for dim_id, pos in sorted(dims, key=lambda x: int(x[1])):
    print(f"  {pos}. {dim_id}")

# Extract time dimension
time_dims = re.findall(r'<s:TimeDimension id="([^"]+)"', content)
print(f"Time dimensions: {time_dims}")

# Print full XML for manual inspection
print("\n--- Full DSD XML ---")
print(content)

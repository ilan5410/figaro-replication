
import os
os.makedirs("data/raw", exist_ok=True)
print("Directory created: data/raw")

# Verify requests is available
import requests
import pandas as pd
import time
print(f"requests version: {requests.__version__}")
print(f"pandas version: {pd.__version__}")

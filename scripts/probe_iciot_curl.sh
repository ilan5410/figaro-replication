#!/bin/bash
# Probe IC-IOT API with curl to understand URL format and response

# Try the SDMX REST API with c_orig=DE filter
echo "=== Test 1: SDMX with c_orig=DE path filter ==="
curl -s -I --max-time 10 \
  "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/naio_10_fcp_ip1/.....MIO_EUR./DE?startPeriod=2010&endPeriod=2010" \
  2>&1 | head -5

echo ""
echo "=== Test 2: Query param format ==="
curl -s -I --max-time 10 \
  "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/naio_10_fcp_ip1?c_orig=DE&TIME_PERIOD=2010" \
  2>&1 | head -5

echo ""
echo "=== Test 3: Statistics API with c_orig ==="
curl -s --max-time 10 \
  "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/naio_10_fcp_ip1?c_orig=DE&time=2010&format=JSON&lang=EN" \
  2>&1 | head -c 500

echo ""
echo "=== Test 4: Statistics API with prd_use and prd_ava filters ==="
curl -s --max-time 10 \
  "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/naio_10_fcp_ip1?c_orig=DE&c_dest=FR&prd_ava=CPA_A01&prd_use=CPA_A01&time=2010&format=JSON&lang=EN" \
  2>&1 | head -c 2000

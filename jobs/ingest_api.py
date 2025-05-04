from pathlib import Path
import requests

url = "https://data.cityofnewyork.us/api/views/vfnx-vebw/rows.csv?accessType=DOWNLOAD"
path = Path("storage/raw/nyc_volunteers.csv")
path.parent.mkdir(parents=True, exist_ok=True)
response = requests.get(url)
path.write_bytes(response.content)
print(f"Arquivo salvo em: {path.resolve()}")

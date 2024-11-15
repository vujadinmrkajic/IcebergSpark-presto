

import requests

url = "http://localhost:8181/v1/catalogs/my_catalog/namespaces"
response = requests.get(url)

if response.status_code == 200:
    print(response.json())  # Display the list of tables in JSON format
else:
    print(f"Error: {response.status_code}")

from faker import Faker
import pandas as pd
import random
import uuid

# Initialize Faker
fake = Faker()

# Parameters for generating the data
num_rows = 1000
num_app_identificators = 20  # Number of distinct AppIdentificator values
num_countries = 10  # Number of distinct African countries
num_tenant_ids = 10  # Number of distinct Tenant IDs

# Define potential values for PaymentType and PaymentProvider
payment_types = [
    "Card", "Bank Transfer", "Apple Pay", 
    "Google Pay", "Direct Debit Mandates", "Momo Wallets"
]
payment_providers = [
    "Paystack", "Interswitch", "VPAY/VFD", 
    "MonoMomo", "Flutterwave", "GTpay"
]

# Define distinct values
african_countries = [
    "Nigeria", "Kenya", "Ghana", "South Africa", 
    "Uganda", "Tanzania", "Ethiopia", "Zimbabwe", 
    "Rwanda", "Senegal"
]
tenant_ids = [f"tenant_{i}" for i in range(1, num_tenant_ids + 1)]  # Distinct Tenant IDs

# Generate distinct AppIdentificator values
app_identificators = [str(uuid.uuid4()) for _ in range(num_app_identificators)]

# Generate data
data = {
    "Id": [str(uuid.uuid4()) for _ in range(num_rows)],  # Unique GUID for Payment Hub ID
    "Timestamp": [fake.date_time_this_decade().isoformat() for _ in range(num_rows)],  # UTC ISO 8601 time
    "ReferenceId": [str(uuid.uuid4()) for _ in range(num_rows)],  # External Payment Id as GUID
    "AppIdentificator": [random.choice(app_identificators) for _ in range(num_rows)],  # Randomly select from unique identifiers
    "PaymentType": [random.choice(payment_types) for _ in range(num_rows)],  # Random Payment Type
    "PaymentProvider": [random.choice(payment_providers) for _ in range(num_rows)],  # Random Payment Provider
    "Country": [random.choice(african_countries) for _ in range(num_rows)],  # Random African Country
    "TenantId": [random.choice(tenant_ids) for _ in range(num_rows)],  # Random Tenant ID
    "Amount": [round(random.uniform(10.0, 1000.0), 2) for _ in range(num_rows)],  # Currency of transaction
    "Units": [random.randint(1, 10) for _ in range(num_rows)],  # Number of units
    "TotalAmount": [round(random.uniform(10.0, 1000.0), 2) for _ in range(num_rows)],  # Total Amount of transaction
    "Payload": [fake.text(max_nb_chars=100) for _ in range(num_rows)],  # Random text for Payload
}

# Create a DataFrame
df = pd.DataFrame(data)

# Save as JSON
df.to_json('bronze_layer/ingested_data.json', orient='records', lines=True)  # Save in the "bronze layer" subfolder


#=========================================================================================================================================


# creating parquet file from json

import pyarrow as pa
import pyarrow.parquet as pq

# Load the JSON file
df = pd.read_json('bronze_layer/ingested_data.json', lines=True)

# Convert the DataFrame to a PyArrow Table
table = pa.Table.from_pandas(df)

# Save as Parquet
pq.write_table(table, 'silver_layer/json_to_parquet.parquet')


#--------------------------------------------------------------------
# Read the Parquet file
df_parquet = pd.read_parquet('silver_layer/json_to_parquet.parquet')

# Display the DataFrame
print(df_parquet)
#--------------------------------------------------------------------



#============================================================================================================================================








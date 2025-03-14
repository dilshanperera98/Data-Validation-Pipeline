import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta

# Configuration
num_rows = 10000  # Number of rows
num_columns = 50  # Number of columns
null_probability = 0.1  # 10% chance of empty values
empty_row_prob = 0.05  # 5% chance of completely empty rows

fake = Faker()
Faker.seed(42)
random.seed(42)

# Generate column headers
columns = [fake.bs().replace(' ', '_') for _ in range(num_columns)]
columns += ['ID', 'Timestamp', 'Revenue', 'Category']  # Add specific columns

# Create empty DataFrame
df = pd.DataFrame(columns=columns)

# Generate data
for i in range(num_rows):
    row = {}
    
    # Randomly create completely empty rows
    if random.random() < empty_row_prob:
        df = pd.concat([df, pd.DataFrame([{}])], ignore_index=True)
        continue
    
    # Generate normal data with random nulls
    for col in columns:
        if random.random() < null_probability:
            row[col] = np.nan
        else:
            if col == 'ID':
                row[col] = f"ID_{i:06}"
            elif col == 'Timestamp':
                row[col] = fake.date_time_between(start_date='-2y', end_date='now')
            elif col == 'Revenue':
                row[col] = round(random.uniform(1000, 100000), 2)
            elif col == 'Category':
                row[col] = random.choice(['A', 'B', 'C', None])
            else:
                # Generate mixed data types
                dtype_choice = random.choice(['text', 'number', 'date', 'boolean'])
                if dtype_choice == 'text':
                    row[col] = fake.sentence(nb_words=3)
                elif dtype_choice == 'number':
                    row[col] = random.randint(1, 1000)
                elif dtype_choice == 'date':
                    row[col] = fake.date_between(start_date='-5y', end_date='today')
                else:
                    row[col] = random.choice([True, False, np.nan])
    
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)

# Add random empty columns
num_empty_cols = random.randint(3, 7)
for _ in range(num_empty_cols):
    df[f"Empty_Column_{_}"] = np.nan

# Save to Excel
writer = pd.ExcelWriter('large_dataset.xlsx', engine='xlsxwriter')
df.to_excel(writer, index=False)
writer.close()
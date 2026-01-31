import pandas as pd
import re

# ==========================================
# Step 1: Load CSV Data (Orders)
# ==========================================
print("Loading Orders...")
orders_df = pd.read_csv('orders.csv')

# ==========================================
# Step 2: Load JSON Data (Users)
# ==========================================
print("Loading Users...")
users_df = pd.read_json('users.json')

# ==========================================
# Step 3: Load SQL Data (Restaurants)
# ==========================================
print("Parsing Restaurants SQL...")
# Since we don't have a database connection, we parse the text file directly
restaurants_data = []

with open('restaurants.sql', 'r') as file:
    for line in file:
        # Look for lines containing data insertion
        if "INSERT INTO" in line:
            # Extract the content inside the parentheses (...)
            # This regex looks for content between '(' and ');'
            match = re.search(r"VALUES \((.*?)\);", line, re.IGNORECASE)
            
            if match:
                data_str = match.group(1)
                try:
                    # eval() effectively converts the string "1, 'Name', ..." into a Python tuple
                    row = eval(data_str)
                    restaurants_data.append(row)
                except Exception as e:
                    print(f"Skipping line due to error: {e}")

# Create DataFrame from parsed data
restaurants_df = pd.DataFrame(restaurants_data, columns=['restaurant_id', 'restaurant_name', 'cuisine', 'rating'])

# ==========================================
# Step 4: Merge the Data
# ==========================================
print("Merging Datasets...")

# 1. Merge Orders with Users (Left Join to keep all orders)
merged_df = pd.merge(orders_df, users_df, on='user_id', how='left')

# 2. Merge with Restaurants
# Note: Both tables have 'restaurant_name'. We use suffixes to distinguish them.
final_df = pd.merge(merged_df, restaurants_df, on='restaurant_id', how='left', suffixes=('_order', '_info'))

# ==========================================
# Step 5: Clean and Export
# ==========================================
print("Finalizing Dataset...")

# Rename columns for clarity
final_df.rename(columns={
    'name': 'user_name',
    'restaurant_name_info': 'restaurant_name' # Using the clean name from the SQL file
}, inplace=True)

# Select and Reorder specific columns
columns_to_keep = [
    'order_id', 'order_date', 'total_amount', 
    'user_id', 'user_name', 'city', 'membership', 
    'restaurant_id', 'restaurant_name', 'cuisine', 'rating'
]

final_df = final_df[columns_to_keep]

# Save to CSV
output_filename = 'final_food_delivery_dataset.csv'
final_df.to_csv(output_filename, index=False)

print(f"Success! File saved as: {output_filename}")
print(final_df.head())
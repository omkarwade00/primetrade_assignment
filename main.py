import pandas as pd

# ============================================================
# LOAD DATA
# ============================================================
fg = pd.read_csv(r"C:\Users\omkar\Downloads\Omkar (1)\Omkar\input_data\fear_greed_index.csv")
ht = pd.read_csv(r"C:\Users\omkar\Downloads\Omkar (1)\Omkar\input_data\historical_data (1).csv")

# ============================================================
# CLEAN FEAR-GREED
# ============================================================
fg.columns = fg.columns.str.strip().str.lower().str.replace(" ", "_")
fg["timestamp"] = pd.to_datetime(fg["timestamp"], errors="coerce")

# Create correct date column
fg["date"] = pd.to_datetime(fg["date"], errors="coerce").dt.date

# ============================================================
# CLEAN HISTORICAL TRADES
# ============================================================
ht.columns = ht.columns.str.strip().str.lower().str.replace(" ", "_")

# Convert timestamp which is in UNIX milliseconds
ht["timestamp_ist"] = pd.to_datetime(ht["timestamp_ist"], unit="ms", errors="coerce")

# Create date column
ht["date"] = ht["timestamp_ist"].dt.date

# ============================================================
# FILTER FG TO MATCH YEARS OF TRADES
# ============================================================

trade_min = ht["timestamp_ist"].min().year
trade_max = ht["timestamp_ist"].max().year

fg_filtered = fg[
    (pd.to_datetime(fg["date"]).dt.year >= trade_min) &
    (pd.to_datetime(fg["date"]).dt.year <= trade_max)
]

print("Trade Year Range:", trade_min, "to", trade_max)
print("Fear–Greed rows BEFORE:", len(fg))
print("Fear–Greed rows AFTER FILTER:", len(fg_filtered))

# ============================================================
# MERGE SAFELY
# ============================================================

merged = ht.merge(
    fg_filtered[["date", "classification", "value"]],
    on="date",
    how="left"
)

print("Merged shape:", merged.shape)
print(merged.head())

# Save final output
merged.to_csv(r"C:\Users\omkar\Downloads\Omkar (1)\Omkar\output_data\merged_safely.csv", index=False)
print("Saved merged_safely.csv")

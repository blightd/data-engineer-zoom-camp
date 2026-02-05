import sys
import pandas as pd
print("arguments", sys.argv)

day = int(sys.argv[1])
df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
df['day'] = day
print(df.head())

df.to_parquet(f"output_day_{day}.parquet")
<<<<<<< HEAD
print(f"Running pipeline for day {day}")
=======
print(f"Running pipeline for day {day}")

# new confiles are add
>>>>>>> pipeline_branch_2

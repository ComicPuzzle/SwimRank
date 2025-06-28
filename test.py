import pandas as pd

timestamps = [
    pd.Timestamp('1900-01-01 00:00:27.830000'),
    pd.Timestamp('1900-01-01 00:01:27.830000'),
    pd.Timestamp('1900-01-01 00:10:05.123456'),
]

formatted_times = [
    ts.strftime('%-M:%S.%f')[:-4] if ts.minute > 0 else ts.strftime('%S.%f')[:-4]
    for ts in timestamps
]

print(formatted_times)

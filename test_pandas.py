import pandas as pd

data_1 = {
    "name": ["Alvin", "Jake", "John", "Lee"]
}

df_1 = pd.DataFrame(data_1)

data_2 = {
    "full_name": ["Alvin", "Jake", "John", "Alvin", "Alvin"],
    "Spend": [1, 1, 1, 2, 2]
}

df_2 = pd.DataFrame(data_2)

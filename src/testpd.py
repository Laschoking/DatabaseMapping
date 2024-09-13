# data from https://allisonhorst.github.io/palmerpenguins/

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

if __name__ == "__main__":

    data = {'col1': [1, 2, 3, 4, 5],
            'col2': ['x', 'y', 8, 9, 10],
            'col3': [11, 12, 13, 14, 15],
            'col4': [16, 17, 18, 19, 20]}

    df = pd.DataFrame(data)
    x =  df.iloc[:,0:2]
    r = (df[df.columns[:2]] == [2,'y']).all(1).any()
    print(r)

    print(df)

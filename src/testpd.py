# data from https://allisonhorst.github.io/palmerpenguins/

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

if __name__ == "__main__":

    s = pd.Series({'a':1,'b':"2"})

    df = s.to_frame().T
    x =  df.iloc[:,0:2]
    r = (df[df.columns[:2]] == [2,'y']).all(1).any()
    print(r)

    print(df)

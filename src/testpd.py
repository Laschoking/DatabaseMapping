# facts from https://allisonhorst.github.io/palmerpenguins/

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

if __name__ == "__main__":

    s = pd.Series({'a':1,'b':  2})
    print(s.index.to_list())

    df = pd.DataFrame({'a': [1,2],'b' : [ 2,3], 'c': [4,5]})
    print(df.columns)

    #print(df)
    x = df[s.index].eq(s)
    print(x)

    #print(x[[s.index]])
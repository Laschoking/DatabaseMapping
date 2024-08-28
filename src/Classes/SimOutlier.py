
class SimOutlier:
    def __init__(self,name):
        self.name = name

    def calc_low_outlier_th(self,accepted_sim):
        pass


class QuantileOutlier(SimOutlier):
    def __init__(self):
        super().__init__("IQR")

    def calc_low_outlier_th(self, accepted_sim):
        q1 = np.percentile(accepted_sim, 25)
        q3 = np.percentile(accepted_sim, 75)
        iqr = q3 - q1
        low_outlier = q1 - 1.5 * iqr
        return low_outlier


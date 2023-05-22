import numpy as np
import xgboost as xgb


class XGBQuantile(XGBRegressor):
    """
    This class, XGBQuantile, is a custom regression estimator for XGBoost that implements the quantile regression
    loss function. It is useful for predictions when interested in different parts of the conditional distribution
    of the output variable. This class extends XGBRegressor to implement the quantile regression using gradient and
    hessian functions for XGBoost.
    """
    def __init__(self, quant_alpha=0.95, quant_delta=1.0, quant_thres=1.0, quant_var=1.0, base_score=0.5,
                 booster='gbtree', colsample_bylevel=1, colsample_bytree=1, gamma=0, learning_rate=0.1,
                 max_delta_step=0, max_depth=3, min_child_weight=1, missing=None, n_estimators=100,
                 n_jobs=1, nthread=None, objective='reg:linear', random_state=0, reg_alpha=0, reg_lambda=1,
                 scale_pos_weight=1, seed=None, silent=True, subsample=1):
        """
        Initializes the XGBQuantile with specified quantile and hyperparameters.

        Args:
            quant_alpha (float): The alpha value for quantile regression.
            quant_delta (float): The delta value for quantile regression.
            quant_thres (float): The threshold value for quantile regression.
            quant_var (float): The variance value for quantile regression.
            All other arguments are XGBoost hyperparameters.
        """
        self.quant_alpha = quant_alpha
        self.quant_delta = quant_delta
        self.quant_thres = quant_thres
        self.quant_var = quant_var

        super().__init__(base_score=base_score, booster=booster, colsample_bylevel=colsample_bylevel,
                         colsample_bytree=colsample_bytree, gamma=gamma, learning_rate=learning_rate,
                         max_delta_step=max_delta_step,
                         max_depth=max_depth, min_child_weight=min_child_weight, missing=missing,
                         n_estimators=n_estimators,
                         n_jobs=n_jobs, nthread=nthread, objective=objective, random_state=random_state,
                         reg_alpha=reg_alpha, reg_lambda=reg_lambda, scale_pos_weight=scale_pos_weight, seed=seed,
                         silent=silent, subsample=subsample)

        self.test = None

    def fit(self, X, y):
        """
        Trains the XGBQuantile model.

        Args:
            X (array-like): Training data.
            y (array-like): Target values.

        Returns:
            self: Returns an instance of self.
        """
        super().set_params(objective=partial(XGBQuantile.quantile_loss, alpha=self.quant_alpha, delta=self.quant_delta,
                                             threshold=self.quant_thres, var=self.quant_var))
        super().fit(X, y)
        return self

    def predict(self, X):
        """
        Predicts the targets for X.

        Args:
            X (array-like): Input data.

        Returns:
            array-like: Predicted target values.
        """
        return super().predict(X)

    def score(self, X, y):
        """
        Returns the quantile score.

        Args:
            X (array-like): Input data.
            y (array-like): True target values.

        Returns:
            float: The quantile score.
        """
        y_pred = super().predict(X)
        score = XGBQuantile.quantile_score(y, y_pred, self.quant_alpha)
        score = 1. / score
        return score

    @staticmethod
    def quantile_loss(y_true, y_pred, alpha, delta, threshold, var):
        """
        Returns the gradient and hessian for quantile loss.

        Args:
            y_true (array-like): True target values.
            y_pred (array-like): Predicted target values.
            alpha (float): The alpha value for quantile regression.
            delta (float): The delta value for quantile regression.
            threshold (float): The threshold value for quantile regression.
            var (float): The variance value for quantile regression.

        Returns:
            tuple: The gradient and hessian.
        """
        x = y_true - y_pred
        grad = (x < (alpha - 1.0) * delta) * (1.0 - alpha) - (
                (x >= (alpha - 1.0) * delta) & (x < alpha * delta)) * x / delta - alpha * (x > alpha * delta)
        hess = ((x >= (alpha - 1.0) * delta) & (x < alpha * delta)) / delta

        grad = (np.abs(x) < threshold) * grad - (np.abs(x) >= threshold) * (
                2 * np.random.randint(2, size=len(y_true)) - 1.0) * var
        hess = (np.abs(x) < threshold) * hess + (np.abs(x) >= threshold)
        return grad, hess

    @staticmethod
    def original_quantile_loss(y_true, y_pred, alpha, delta):
        """
        Returns the gradient and hessian for original quantile loss.

        Args:
            y_true (array-like): True target values.
            y_pred (array-like): Predicted target values.
            alpha (float): The alpha value for quantile regression.
            delta (float): The delta value for quantile regression.

        Returns:
            tuple: The gradient and hessian.
        """
        x = y_true - y_pred
        grad = (x < (alpha - 1.0) * delta) * (1.0 - alpha) - (
                (x >= (alpha - 1.0) * delta) & (x < alpha * delta)) * x / delta - alpha * (x > alpha * delta)
        hess = ((x >= (alpha - 1.0) * delta) & (x < alpha * delta)) / delta
        return grad, hess

    @staticmethod
    def quantile_score(y_true, y_pred, alpha):
        """
        Returns the quantile score.

        Args:
            y_true (array-like): True target values.
            y_pred (array-like): Predicted target values.
            alpha (float): The alpha value for quantile regression.

        Returns:
            float: The quantile score.
        """
        score = XGBQuantile.quantile_cost(x=y_true - y_pred, alpha=alpha)
        score = np.sum(score)
        return score

    @staticmethod
    def quantile_cost(x, alpha):
        """
        Returns the quantile cost.

        Args:
            x (array-like): Difference between true and predicted target values.
            alpha (float): The alpha value for quantile regression.

        Returns:
            array-like: The quantile cost.
        """
        return (alpha - 1.0) * x * (x < 0) + alpha * x * (x >= 0)

    @staticmethod
    def get_split_gain(gradient, hessian, l=1):
        """
        Returns the split gain.

        Args:
            gradient (array-like): Gradient values.
            hessian (array-like): Hessian values.
            l (float): Regularization term.

        Returns:
            array-like: The split gain.
        """
        split_gain = list()
        for i in range(gradient.shape[0]):
            split_gain.append(np.sum(gradient[:i]) / (np.sum(hessian[:i]) + l) + np.sum(gradient[i:]) / (
                    np.sum(hessian[i:]) + l) - np.sum(gradient) / (np.sum(hessian) + l))

        return np.array(split_gain)
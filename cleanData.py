from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
import numpy as np
import pandas as pd

data = pd.read_csv('HS.csv', parse_dates=['timestamp'], index_col='timestamp')
returns = data.pct_change().dropna()
# Basic features
features = pd.DataFrame({
    'mean_return': returns.mean(),
    'volatility': returns.std()
}, index=returns.columns)

# Add PCA components (reduce returns to 5 dimensions for similarity in time-series behavior)
pca = PCA(n_components=5)
pca_components = pca.fit_transform(returns.T)  # Transpose so stocks are rows
pca_df = pd.DataFrame(pca_components, index=returns.columns, columns=[f'pca_{i}' for i in range(5)])
features = pd.concat([features, pca_df], axis=1)

# Normalize features
scaler = StandardScaler()
features_scaled = scaler.fit_transform(features.dropna())  # Drop NaNs if any
features = features.dropna()  # Align index
print(features.head())  # Preview
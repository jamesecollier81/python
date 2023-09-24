import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import warnings
from sklearn.preprocessing import OrdinalEncoder
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.model_selection import KFold, cross_val_score, train_test_split
from sklearn.metrics import confusion_matrix, r2_score, ConfusionMatrixDisplay, RocCurveDisplay, roc_curve, PrecisionRecallDisplay, precision_recall_curve, accuracy_score

plt.style.use('seaborn-v0_8-darkgrid')
warnings.filterwarnings('ignore')

dataset = pd.read_csv('/Users/jcollier/Documents/Python/digital_wallet_source.csv', sep = '|', on_bad_lines='warn')
categorical_cols = ['device_id', 'device_language', 'device_ip','token_status','token_requestor','fraud_category','blacklist_date','line_open_date','token_activated_time','account_status'
                            ,'charged_off','jira_key','jira_status','jira_disposition'] 
dataset = dataset.dropna()

dataset = pd.get_dummies(dataset, columns = categorical_cols, drop_first=True)
X = dataset.drop('ml_flag', axis=1)
y = dataset['ml_flag']

X_train, X_test, y_train, y_test=train_test_split(X, y, test_size=0.10)

et = ExtraTreesClassifier(bootstrap=False, ccp_alpha=0.0,
                                       class_weight=None, criterion='entropy',
                                       max_depth=None, max_features='sqrt',
                                       max_leaf_nodes=None, max_samples=None,
                                       min_impurity_decrease=0.0,
                                       min_samples_leaf=3, min_samples_split=2,
                                       min_weight_fraction_leaf=0.0,
                                       n_estimators=800, n_jobs=-1,
                                       oob_score=False, random_state=123,
                                       verbose=0, warm_start=False)

et.fit(X_train, y_train)

y_pred = et.predict(X_test)
print('R-square score main model is :', round(r2_score(y_test, y_pred),6))

cf_matrix = confusion_matrix(y_test, y_pred)
group_names = ['True Neg','False Pos','False Neg','True Pos']
group_counts = ["{0:0.0f}".format(value) for value in
                cf_matrix.flatten()]
group_percentages = ["{0:.1%}".format(value) for value in
                     cf_matrix.flatten()/np.sum(cf_matrix)]
labels = [f"{v1}\n{v2}\n{v3}" for v1, v2, v3 in
          zip(group_names,group_counts,group_percentages)]
labels = np.asarray(labels).reshape(2,2)
sns.heatmap(cf_matrix, annot=labels, fmt='', cmap='binary')

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 6))

RocCurveDisplay.from_estimator(et, X_test, y_test, ax=ax1)
RocCurveDisplay.from_predictions(y_test, y_pred, ax=ax1)

PrecisionRecallDisplay.from_estimator(et, X_test, y_test, ax=ax2)
PrecisionRecallDisplay.from_predictions(y_test, y_pred, ax=ax2)

plt.tight_layout()
plt.show()

feature_importance = et.feature_importances_
sorted_idx = np.argsort(feature_importance)[-10::]
top_10_idx = sorted_idx[:10]
fig = plt.figure(figsize=(12, 6))
plt.barh(range(len(top_10_idx)), feature_importance[top_10_idx], align='center')
plt.yticks(range(len(top_10_idx)), np.array(X_test.columns)[top_10_idx])
plt.title('Feature Importance')
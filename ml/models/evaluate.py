from sklearn.metrics import accuracy_score, f1_score

def evaluate_model(df):
    y_true = df['default_risk']
    y_pred = df['prediction']

    acc = accuracy_score(y_true, y_pred)
    f1 = f1_score(y_true, y_pred)
    return {
        "accuracy": acc,
        "f1_score": f1
    }
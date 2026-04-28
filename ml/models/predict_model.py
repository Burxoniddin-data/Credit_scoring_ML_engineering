import pandas as pd
import joblib

def predict_model(
    model_path="/tmp/model.pkl",
    input_path="/tmp/features.csv",
    output_path="/tmp/predictions.csv"
):
    model = joblib.load(model_path)
    df = pd.read_csv(input_path)
    df['prediction'] = model.predict(
        df[['debt_to_income', 'missed_ratio']]
    )
    df.to_csv(output_path, index=False)
    return output_path
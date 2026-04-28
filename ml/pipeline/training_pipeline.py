from ml.features.build_features import build_features
from ml.models.train_model import train_model
from ml.models.predict_model import predict_model


def run_pipeline():
    print("Building features...")
    features_path = build_features()

    print("Training model...")
    model_path = train_model(features_path)

    print("Running predictions...")
    predictions_path = predict_model(model_path, features_path)

    print("Pipeline complete!")
    return predictions_path


if __name__ == "__main__":
    run_pipeline()
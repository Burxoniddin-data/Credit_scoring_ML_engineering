from ml.features.build_features import build_features

def run_feature_engineering(**context):
    params = context["params"]
    output_path = params.get("features_path", "/tmp/features.csv")
    path = build_features(output_path=output_path)
    return path
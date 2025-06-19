import mlflow
import mlflow.sklearn
from sklearn.datasets import load_iris
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import os
import joblib

iris = load_iris()
X_train, X_test, y_train, y_test = train_test_split(
    iris.data, iris.target, test_size=0.2, random_state=42
)

model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)
acc = accuracy_score(y_test, model.predict(X_test))
print(f"[✔] Accuracy: {acc:.4f}")

mlflow.set_tracking_uri("sqlite:///mlflow_db/mlflow.db")
mlflow.set_experiment("iris-exp")

with mlflow.start_run():
    mlflow.log_param("n_estimators", 100)
    mlflow.log_metric("accuracy", acc)
    mlflow.sklearn.log_model(model, artifact_path="model")

os.makedirs("mlartifacts", exist_ok=True)
joblib.dump(model, "mlartifacts/latest_model.pkl")
print("[✔] 모델 저장 완료: mlartifacts/latest_model.pkl")

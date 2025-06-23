import seaborn as sns
import pandas as pd

# Seaborn iris 데이터셋 로드
iris = sns.load_dataset("iris")

# CSV 파일로 저장
iris.to_csv("iris.csv", index=False)

print("iris.csv 저장 완료")

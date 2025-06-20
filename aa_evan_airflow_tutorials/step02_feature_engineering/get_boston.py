import pandas as pd

# 1. 데이터 로드
url = "https://raw.githubusercontent.com/selva86/datasets/master/BostonHousing.csv"
df = pd.read_csv(url)

# 2. 컬럼명을 모두 대문자로 변환
df.columns = df.columns.str.upper()

# 3. CSV로 저장
df.to_csv("boston.csv", index=False)

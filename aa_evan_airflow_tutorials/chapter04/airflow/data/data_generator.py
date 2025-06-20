import pandas as pd
import numpy as np

# 랜덤 시드 설정
np.random.seed(42)

# 샘플 데이터 500개 생성
n_samples = 500

# 특성(features) 생성
crim = np.random.exponential(scale=0.5, size=n_samples)  # 범죄율
zn = np.random.randint(0, 100, size=n_samples)  # 25,000평방피트 이상 주거지역 비율
indus = np.random.uniform(0, 30, size=n_samples)  # 비소매상업지역 면적 비율
chas = np.random.binomial(1, 0.07, size=n_samples)  # 찰스강 경계 여부
nox = np.random.uniform(0.4, 0.8, size=n_samples)  # 일산화질소 농도
rm = np.random.normal(6.0, 0.7, size=n_samples)  # 평균 방 개수
age = np.random.uniform(20, 100, size=n_samples)  # 1940년 이전 건축된 주택 비율
dis = np.random.uniform(1, 12, size=n_samples)  # 직업센터까지의 거리
rad = np.random.randint(1, 25, size=n_samples)  # 방사형 도로까지의 거리
tax = np.random.uniform(200, 700, size=n_samples)  # 재산세율
ptratio = np.random.uniform(12, 22, size=n_samples)  # 학생/교사 비율
b = np.random.uniform(0, 400, size=n_samples)  # 흑인 거주 비율
lstat = np.random.uniform(1, 37, size=n_samples)  # 하위 계층 비율

# 주택 가격 생성 (특성들의 선형 조합 + 약간의 노이즈)
price = (
    -0.1 * crim 
    + 0.05 * zn 
    - 0.2 * indus 
    + 2 * chas 
    - 15 * nox 
    + 5 * rm 
    - 0.02 * age 
    - 1.3 * dis 
    + 0.1 * rad 
    - 0.012 * tax 
    - 0.5 * ptratio 
    + 0.009 * b 
    - 0.5 * lstat 
    + np.random.normal(0, 2, size=n_samples)
) * 1000 + 23000

# 데이터프레임 생성
boston_df = pd.DataFrame({
    'CRIM': crim,
    'ZN': zn,
    'INDUS': indus,
    'CHAS': chas,
    'NOX': nox,
    'RM': rm,
    'AGE': age,
    'DIS': dis,
    'RAD': rad,
    'TAX': tax,
    'PTRATIO': ptratio,
    'B': b,
    'LSTAT': lstat,
    'PRICE': price
})

# CSV 파일로 저장
boston_df.to_csv('boston.csv', index=False)

print("boston.csv 파일이 생성되었습니다.")
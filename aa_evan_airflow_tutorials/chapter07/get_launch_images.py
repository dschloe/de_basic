import os
import requests

def get_launch_images():
    """Launch Library 2 API에서 로켓 발사 이미지 URL을 가져오는 함수"""
    api_url = "https://ll.thespacedevs.com/2.2.0/launch/upcoming/?limit=5"
    response = requests.get(api_url)
    
    if response.status_code == 200:
        launches = response.json()['results']
        image_urls = []
        
        for launch in launches:
            if launch.get('image'):
                image_urls.append(launch['image'])
                
        return image_urls
    else:
        raise Exception(f"API 요청 실패: {response.status_code}")

def download_images(image_urls):
    """로켓 발사 이미지를 AIRFLOW_HOME/rocket_images 폴더에 다운로드하는 함수"""
    # AIRFLOW_HOME 환경변수 가져오기
    airflow_home = os.getenv('AIRFLOW_HOME')
    if not airflow_home:
        raise Exception("AIRFLOW_HOME 환경변수가 설정되지 않았습니다.")
    
    # rocket_images 폴더 생성 - 경로 수정
    # 중복된 airflow 경로를 방지하기 위해 basename을 확인
    base_dir = os.path.basename(airflow_home)
    
    # 이미 경로가 'airflow'로 끝나는 경우, 중복 방지
    if base_dir == 'airflow':
        image_dir = os.path.join(os.path.dirname(airflow_home), 'rocket_images')
    else:
        image_dir = os.path.join(airflow_home, 'rocket_images')
    
    # 디렉토리 생성
    os.makedirs(image_dir, exist_ok=True)
    print(f"image_dir: {image_dir}")

    # 이미지 다운로드
    downloaded_paths = []
    for idx, url in enumerate(image_urls):
        try:
            response = requests.get(url)
            if response.status_code == 200:
                # 이미지 파일명 생성 (rocket_1.jpg, rocket_2.jpg, ...)
                file_extension = url.split('.')[-1]
                file_name = f'rocket_{idx + 1}.{file_extension}'
                file_path = os.path.join(image_dir, file_name)
                
                # 이미지 저장
                with open(file_path, 'wb') as f:
                    f.write(response.content)
                downloaded_paths.append(file_path)
                print(f"이미지 다운로드 완료: {file_path}")
        except Exception as e:
            print(f"이미지 다운로드 실패 (URL: {url}): {str(e)}")
    
    return downloaded_paths

if __name__ == "__main__":
    try:
        # 이미지 URL 가져오기
        image_urls = get_launch_images()
        
        # 이미지 다운로드
        downloaded_paths = download_images(image_urls)
        print(f"\n총 {len(downloaded_paths)}개의 이미지 다운로드 완료")
        
    except Exception as e:
        print(f"에러 발생: {str(e)}")

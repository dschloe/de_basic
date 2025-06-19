# 확인 
sudo apt update
sudo apt install mysql-server
sudo ufw all mysql
sudo systemctl start mysql
sudo systemctl enable mysql

위에까지 실행 후, 기존 터미널 삭제 후 다시 터미널 생성
sudo systemctl status mysql
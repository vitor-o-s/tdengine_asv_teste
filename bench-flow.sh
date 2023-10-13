echo "Pulling images"
docker pull timescale/timescaledb:2.11.2-pg14
docker pull tdengine/tdengine:3.0.5.0

echo "Downloading Druid files"
wget https://raw.githubusercontent.com/apache/druid/27.0.0/distribution/docker/docker-compose.yml
wget https://raw.githubusercontent.com/apache/druid/27.0.0/distribution/docker/environment

echo "Setting Up enviroment"
source .venv/bin/activate
pip install -r requirements.txt

# Start TDengine
echo "Starting TD Container"
sudo docker run -d --name tdengine -p 6030:6030 -p 6041:6041 -p 6043-6049:6043-6049 -p 6043-6049:6043-6049/udp tdengine/tdengine:3.0.5.0
wait
echo "TD is running"

# Run Tests
echo "Running Tests"
python3 conexao_tdengine.py

# Stop and remove TDengine
sudo docker stop tdengine
sudo docker remove tdengine
wait
echo "TDengine Stoped"

# Start Timescale
echo "Starting Timescale Container"
sudo sudo docker run -d --name timescaledb -p 127.0.0.1:5432:5432 -e POSTGRES_PASSWORD=password timescale/timescaledb:2.11.2-pg14
wait
echo "Timescale is running"
# Run Tests
echo "Running Tests"
python3 conexao_timescale.py

# Stop and remove Timescale
sudo docker stop timescaledb
sudo docker remove timescaledb
wait
echo "timescaledb Stoped"

# Start Druid
echo "Starting Druid Container"
sudo docker compose up -p druid -d
wait
echo "Druid is running"

# Run Tests
echo "Running Tests"
python3 conexao_druid.py

# Stop and remove Druid
sudo docker compose down -v
# sudo docker remove tdengine
# wait
echo "Druid Stoped"

# Generate Results
python3 utils/gen_graphics.py

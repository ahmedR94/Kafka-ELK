docker-compose -f kafka.yml up -d
# python3 -m venv my_env
# source ./my_env/bin/activate
pip install -r requirements.txt
python3 producer.py
python3 consumer.py
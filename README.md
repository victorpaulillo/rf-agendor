# rf-agendor

#Create a venv
python -m venv ./venv/

# Activate the venv
.\venv\Scripts\activate

# See list
pip list

# Install Flask and libs
pip install flask
pip install requests


# Create a requirements.txt
pip freeze > requirements.txt

# Create a .gitignore file - Tell git what do ignore

# Run flask
flask run


# To deploy the container on Cloud Run
cp app.py main.py
gcloud run deploy
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
gcloud run deploy

# To run locally, it is necessary to change the name of the main scrip to "app.py"

Kubernetes
https://medium.com/google-cloud/a-guide-to-deploy-flask-app-on-google-kubernetes-engine-bfbbee5c6fb

FROM python:3.11.5


WORKDIR /app
# copy to container
COPY requirements.txt /app

# install the dependancies
RUN pip install -r requirements.txt

# Entry point
CMD ["python", "app.py", "dev"]
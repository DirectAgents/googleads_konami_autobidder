FROM python:3.8.10

RUN mkdir /app
COPY . /app
WORKDIR /app

ENV PYTHONPATH=${PYTHONPATH}:${PWD}
ENV GOOGLE_DEVELOPER_TOKEN=kEw3EPUPKDwKFpjRRIpm5Q
ENV GOOGLE_REFRESH_TOKEN=1//04IgM1GLKU3dKCgYIARAAGAQSNwF-L9IrTyNqdXFL7xWJVbGIkBkMHAXZ70P3yqOkeJ8sN0zGY6hcB9nI8F_vDW47FmisktzXKzA
ENV GOOGLE_CLIENT_ID=842783442446-cq0cu0i7q1aivjvrnrvb0m689macnn3v.apps.googleusercontent.com
ENV GOOGLE_CLIENT_SECRET=sI3BftM-G_-Tta8i5wKOeTQF
ENV GOOGLE_LOGIN_CUSTOMER_ID=7070364439
ENV DATABASE_UID=daadmin
ENV DATABASE_PWD=bObsp0ng

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update -qq
RUN ACCEPT_EULA=Y apt-get install -y unixodbc unixodbc-dev msodbcsql18
RUN pip3 install pyodbc
RUN pip3 install -r requirements.txt

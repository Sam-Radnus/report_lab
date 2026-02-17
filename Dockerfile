FROM --platform=linux/amd64 public.ecr.aws/lambda/python:3.12

RUN dnf install -y \
    freetype \
    libjpeg-turbo \
    fontconfig \
    dejavu-sans-fonts \
    && dnf clean all

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY lambda_function.py .

CMD ["lambda_function.lambda_handler"]

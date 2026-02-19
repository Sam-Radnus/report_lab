FROM public.ecr.aws/lambda/python:3.12

RUN dnf install -y \
    gcc \
    python3-devel \
    freetype \
    freetype-devel \
    libjpeg-turbo \
    libjpeg-turbo-devel \
    fontconfig \
    dejavu-sans-fonts \
    && dnf clean all

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV MPLCONFIGDIR=/tmp/matplotlib
ENV FONTCONFIG_CACHE=/tmp/fontconfig

RUN fc-cache -fv

COPY base.py .
COPY db.py .
COPY lambda_function.py .

CMD ["lambda_function.lambda_handler"]

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

COPY models.py .
COPY repository.py .
COPY market_data.py .
COPY exceptions.py .
COPY report_handler.py .

CMD ["report_handler.lambda_handler"]

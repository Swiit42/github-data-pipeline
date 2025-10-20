# # Dockerfile
# FROM python:3.13-slim
# ENV PYTHONUNBUFFERED=1

# # 1) Installer uv
# RUN pip install uv

# WORKDIR /app

# # 2) Copier les fichiers de build (inclure README si déclaré dans pyproject)
# COPY pyproject.toml uv.lock* README.md ./

# # 3) Installer les dépendances (uv crée .venv dans /app)
# RUN uv sync --frozen

# # 4) Rendre la venv par défaut dans le conteneur
# ENV VIRTUAL_ENV="/app/.venv"
# ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# # 5) Copier le code et l'env
# COPY src ./src

# # 6) Commande par défaut : lance l'import
# CMD ["python", "src/import_to_postgres.py", "--data-dir", "src/data/raw"]
# Dockerfile
FROM python:3.13-slim
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Installer (faiblement) des outils système si besoin futur (optionnel)
# RUN apt-get update && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/*

# Installer les deps
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copier le code (ne PAS copier .env : compose l’injecte)
COPY src ./src
COPY README.md README.md

# Lancer l’import (chemin monté par compose)
CMD ["python", "-u", "src/import_to_postgres.py", "--data-dir", "src/data/raw"]

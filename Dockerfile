# syntax=docker/dockerfile:1
FROM python:3.13-slim

# === Variables d'environnement ===
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# === Dépendances système ===
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential \
      libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# === Répertoire de travail ===
WORKDIR /app

# === Copie des fichiers de dépendances en premier (pour le cache Docker) ===
COPY requirements.txt .

# === Installation des dépendances Python ===
RUN pip install --no-cache-dir -r requirements.txt

# === Copie du code du projet ===
COPY . .

# === Port exposé ===
EXPOSE 8000

# === Commande de démarrage ===
CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]

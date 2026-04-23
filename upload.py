#!/usr/bin/env python3
"""
Batch upload de dataset_1 para o audino.

Distribui os pares (wav + transcricao) entre usuarios em round-robin.

Uso:
    export API_KEY="sua_chave_aqui"
    python upload.py --users ana joao maria --host localhost --port 3000

    # preview sem subir nada:
    python upload.py --users ana joao maria --host localhost --port 3000 --dry-run
"""

import argparse
import os
import sys
import requests
from pathlib import Path

parser = argparse.ArgumentParser(description="Upload em lote do dataset_1 para o audino")
parser.add_argument("--users", nargs="+", required=True, help="Usernames para distribuir os dados")
parser.add_argument("--dataset-dir", type=str, default="dataset_1", help="Pasta raiz do dataset (default: dataset_1)")
parser.add_argument("--host", type=str, default="localhost")
parser.add_argument("--port", type=int, default=3000)
parser.add_argument("--dry-run", action="store_true", help="Mostra distribuicao sem fazer upload")
parser.add_argument("--is-marked-for-review", action="store_true")
args = parser.parse_args()

api_key = os.getenv("API_KEY")
if not api_key and not args.dry_run:
    print("Erro: variavel de ambiente API_KEY nao definida.")
    sys.exit(1)

dataset_dir = Path(args.dataset_dir)
segments_dir = dataset_dir / "output_segments"
transcriptions_dir = dataset_dir / "output_transcriptions"

if not segments_dir.is_dir():
    print(f"Erro: pasta nao encontrada: {segments_dir}")
    sys.exit(1)

wav_files = sorted(segments_dir.glob("*.wav"))
if not wav_files:
    print(f"Nenhum .wav encontrado em {segments_dir}")
    sys.exit(1)

# Distribui em round-robin
pairs = []
for wav in wav_files:
    txt = transcriptions_dir / (wav.stem + ".txt")
    transcription = txt.read_text(encoding="utf-8").strip() if txt.is_file() else ""
    pairs.append((wav, transcription))

distribution = {u: [] for u in args.users}
for i, pair in enumerate(pairs):
    user = args.users[i % len(args.users)]
    distribution[user].append(pair)

# Sumario
print(f"\nDataset: {dataset_dir}")
print(f"Total de arquivos: {len(pairs)}")
print(f"Usuarios ({len(args.users)}):")
for user, items in distribution.items():
    sem_txt = sum(1 for _, t in items if not t)
    print(f"  {user}: {len(items)} arquivos  (sem transcricao: {sem_txt})")
print()

if args.dry_run:
    print("Modo dry-run: nenhum arquivo foi enviado.")
    sys.exit(0)

# Upload
url = f"http://{args.host}:{args.port}/api/data"
headers = {"Authorization": api_key}

total = len(pairs)
done = 0
errors = []

for user, items in distribution.items():
    print(f"Subindo {len(items)} arquivos para '{user}'...")
    for wav, transcription in items:
        with open(wav, "rb") as f:
            response = requests.post(
                url,
                files={"audio_file": (wav.name, f)},
                data={
                    "username": user,
                    "reference_transcription": transcription,
                    "is_marked_for_review": str(args.is_marked_for_review).lower(),
                },
                headers=headers,
            )
        done += 1
        if response.status_code == 201:
            print(f"  [{done}/{total}] OK  {wav.name}")
        else:
            msg = response.json().get("message", response.text)
            print(f"  [{done}/{total}] ERRO {wav.name}: {msg}")
            errors.append((wav.name, msg))

print(f"\nConcluido: {done - len(errors)}/{total} com sucesso, {len(errors)} erros.")
if errors:
    print("Arquivos com erro:")
    for name, msg in errors:
        print(f"  {name}: {msg}")

import os
import logging
from datetime import datetime

# --- Configuração de Diretórios ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOGS_DIR = os.path.join(BASE_DIR, "..", "logs")
DB_DIR = os.path.join(BASE_DIR, "..", "databases")

os.makedirs(LOGS_DIR, exist_ok=True)
os.makedirs(DB_DIR, exist_ok=True)

# --- Configuração de Logging ---
log_name = f"bot_execution_{datetime.now().strftime('%m-%Y')}.log"
log_path = os.path.join(LOGS_DIR, log_name)

# Formato simplificado para leitura rápida
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(log_path, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
LOGGER = logging.getLogger("DeltaNeutralBot")

# --- Parâmetros de Mercado ---
EXCHANGE_ID = 'binance'
MIN_24H_VOLUME_USD = 50_000_000 # Reduzi um pouco para pegar boas oportunidades em mid-caps

# --- Filtros de Estratégia ---
MIN_FUNDING_RATE = 0.0001       # 0.01% por período (Funding positivo)
NEGATIVE_FUNDING_THRESHOLD = -0.0001 # Sai se o funding for pior que -0.01%
FEE_TAKER = 0.0005              # 0.05% (Binance Swaps padrão)
SLIPPAGE_SIMULATED = 0.0005     # 0.05% (Conservador para garantir realismo)

# --- Gestão de Simulação ---
BRL_USD_RATE = 5.80             # Fallback caso a API de câmbio falhe
MONTHLY_CONTRIBUTION_BRL = 1000.00
MIN_ORDER_VALUE_USD = 10.00     # Mínimo para abrir ordem na Binance costuma ser $5-$10
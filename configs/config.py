import os
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# --- Credenciais de API ---
API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_SECRET_KEY")

# Validação básica de segurança
if not API_KEY or not API_SECRET:
    logging.warning("Credenciais de API não encontradas no arquivo .env. O bot rodará em modo restrito/simulado.")

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
MIN_24H_VOLUME_USD = 50_000_000

# --- Filtros de Estratégia ---
MIN_FUNDING_RATE = 0.0001               # 0.01% por período (Funding positivo)
NEGATIVE_FUNDING_THRESHOLD = -0.0001    # Sai se o funding for pior que -0.01%
FEE_TAKER_SPOT_DEFAULT = 0.001          # 0.10%
FEE_TAKER_SWAP_DEFAULT = 0.0005         # 0.05%
SLIPPAGE_SIMULATED = 0.0005             # 0.05% (Conservador para garantir realismo)
PAYBACK_PERIOD_DAYS = 3.0               # Meta: Recuperar o investimento em até 3 dias
MIN_NET_APR = 0.15                      # Meta: 15% ao ano LIVRE de taxas (Net Profit)
TARGET_FUNDING = 0.005                  # 0.005% (Meta mínima aceitável)
EXIT_SCORE_LIMIT = 20                   # Limite para sair (aprox. 1h40min se for linear)

# --- Gestão de Conversões ---
BRL_USD_RATE = 5.80                     # Fallback caso a API de câmbio falhe
MIN_ORDER_VALUE_USD = 11.00             # Mínimo para abrir ordem na Binance costuma ser $5-$10

# --- Cores para Logs ---
COLOR_GREEN = "\033[92m"
COLOR_RED = "\033[91m"
COLOR_CYAN = "\033[96m"
COLOR_RESET = "\033[0m"

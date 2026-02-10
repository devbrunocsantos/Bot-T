import os
import logging
from datetime import datetime

# Configuração de Logging
logs_dir = "logs"
os.makedirs(logs_dir, exist_ok=True)
log_name = f"bot_execution_{datetime.now().strftime('%m-%Y')}.log"
log_path = os.path.join(logs_dir, log_name)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_path),
        logging.StreamHandler()
    ]
)
LOGGER = logging.getLogger("DeltaNeutralBot")

# Parâmetros de Mercado
EXCHANGE_ID = 'binance'  # Exemplo: binance, bybit
TARGET_TIMEFRAME = '1h'
TOP_N_VOLUME = 50
MIN_24H_VOLUME_USD = 100000000

# Lista de moedas permitidas (Apenas Blue Chips e ativos consolidados)
WHITELIST_SYMBOLS = [
    'BTC/USDT:USDT', 'ETH/USDT:USDT', 'SOL/USDT:USDT', 
    'BNB/USDT:USDT', 'XRP/USDT:USDT', 'ADA/USDT:USDT', 
    'AVAX/USDT:USDT', 'LINK/USDT:USDT', 'LTC/USDT:USDT', 
    'NEAR/USDT:USDT', 'MATIC/USDT:USDT', 'TRX/USDT:USDT', 
    'DOT/USDT:USDT', 'XAU/USDT:USDT', 'XAG/USDT:USDT'
]

# Filtros de Estratégia
MIN_FUNDING_RATE = 0.0001  # 0.01%
FEE_MAKER = 0.0002        # 0.02% (Exemplo)
FEE_TAKER = 0.0004        # 0.04% (Exemplo)
SLIPPAGE_SIMULATED = 0.0005 # 0.05%
DAYS_FOR_PAYBACK = 3

# Gestão de Risco
SAFETY_MARGIN_RATIO = 0.10 # Se margem cair abaixo de 10%, reequilibrar/sair
NEGATIVE_FUNDING_THRESHOLD = -0.00005 # Sair se funding for negativo crítico

# Simulação de Aporte
BRL_USD_RATE = 5.00 # Taxa fixa para simulação ou conectar API de câmbio
MONTHLY_CONTRIBUTION_BRL = 50.00
MIN_ORDER_VALUE_USD = 10.00
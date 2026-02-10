import sqlite3
import pandas as pd
from datetime import datetime
from configs.config import LOGGER

class DataManager:
    def __init__(self, db_name="databases/database.db"):
        """
        Inicializa a conexão com o banco de dados e cria a tabela se não existir.
        """
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self._setup_tables()

    def _setup_tables(self):
        """
        Define o esquema da tabela de logs de operação.
        """
        query_trade = """
        CREATE TABLE IF NOT EXISTS trade_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME,
            symbol TEXT,
            price_spot REAL,
            price_future REAL,
            funding_rate REAL,
            next_funding_time DATETIME,
            position_size REAL,
            simulated_fees REAL,
            accumulated_profit REAL,
            max_drawdown REAL,
            action TEXT
        )
        """
        self.cursor.execute(query_trade)

        query_scan = """
        CREATE TABLE IF NOT EXISTS scanner_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME,
            total_analyzed INTEGER,
            passed_volume INTEGER,
            best_funding REAL,
            best_pair TEXT,
            reason TEXT
        )
        """
        self.cursor.execute(query_scan)

        self.conn.commit()

    def log_state(self, data: dict):
        """
        Registra o estado atual da operação no banco de dados.
        Argumentos:
            data (dict): Dicionário contendo os dados da coluna.
        """
        try:
            query = """
            INSERT INTO trade_logs (
                timestamp, symbol, price_spot, price_future, funding_rate, 
                next_funding_time, position_size, simulated_fees, accumulated_profit, max_drawdown, action
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            values = (
                datetime.now(),
                data.get('symbol'),
                data.get('price_spot'),
                data.get('price_future'),
                data.get('funding_rate'),
                data.get('next_funding_time'),
                data.get('position_size'),
                data.get('simulated_fees'),
                data.get('accumulated_profit'),
                data.get('max_drawdown'),
                data.get('action', 'MONITOR')
            )
            self.cursor.execute(query, values)
            self.conn.commit()
        except Exception as e:
            LOGGER.error(f"Erro ao salvar log no banco de dados: {e}")

    def log_scan_attempt(self, data: dict):
        """
        Registra o resultado de um ciclo de escaneamento de mercado.
        """
        try:
            query = """
            INSERT INTO scanner_logs (
                timestamp, total_analyzed, passed_volume, best_funding, best_pair, reason
            ) VALUES (?, ?, ?, ?, ?, ?)
            """
            values = (
                datetime.now(),
                data.get('total_analyzed'),
                data.get('passed_volume'),
                data.get('best_funding'),
                data.get('best_pair'),
                data.get('reason')
            )
            self.cursor.execute(query, values)
            self.conn.commit()
        except Exception as e:
            LOGGER.error(f"Erro ao salvar log de scan: {e}")

    def close(self):
        self.conn.close()
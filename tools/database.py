import sqlite3
import json
from datetime import datetime
from configs.config import LOGGER

class DataManager:
    def __init__(self, db_name):
        """
        Gerenciador de Banco de Dados SQLite.
        Cria as tabelas automaticamente se não existirem.
        """
        self.db_name = db_name
        self.conn = None
        self._connect()
        self._create_tables()

    def _connect(self):
        try:
            self.conn = sqlite3.connect(self.db_name, check_same_thread=False)
        except Exception as e:
            LOGGER.error(f"Erro ao conectar no DB: {e}")

    def close(self):
        if self.conn:
            self.conn.close()

    def _create_tables(self):
        try:
            cursor = self.conn.cursor()
            
            # Tabela de Scanners (Tentativas de encontrar pares)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS scan_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    total_analyzed INTEGER,
                    passed_volume INTEGER,
                    best_funding REAL,
                    best_pair TEXT,
                    reason TEXT
                )
            ''')

            # Tabela de Monitoramento de Posição (Log financeiro)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS position_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    symbol TEXT,
                    price_future REAL,
                    funding_rate REAL,
                    next_funding_time TEXT,
                    position_size REAL,
                    simulated_fees REAL,
                    accumulated_profit REAL,
                    max_drawdown REAL,
                    action TEXT
                )
            ''')
            self.conn.commit()
        except Exception as e:
            LOGGER.error(f"Erro ao criar tabelas: {e}")

    def log_scan_attempt(self, data):
        """
        Registra o resultado de um scanner de mercado.
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT INTO scan_logs (total_analyzed, passed_volume, best_funding, best_pair, reason)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                data.get('total_analyzed', 0),
                data.get('passed_volume', 0),
                data.get('best_funding', 0.0),
                str(data.get('best_pair', 'N/A')),
                data.get('reason', 'UNKNOWN')
            ))
            self.conn.commit()
        except Exception as e:
            LOGGER.error(f"Erro ao logar scan: {e}")

    def log_state(self, data):
        """
        Registra o estado financeiro atual da posição.
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT INTO position_logs (
                    symbol, price_future, funding_rate, next_funding_time, 
                    position_size, simulated_fees, accumulated_profit, max_drawdown, action
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                data.get('symbol'),
                data.get('price_future'),
                data.get('funding_rate'),
                data.get('next_funding_time'),
                data.get('position_size'),
                data.get('simulated_fees'),
                data.get('accumulated_profit'),
                data.get('max_drawdown'),
                data.get('action')
            ))
            self.conn.commit()
        except Exception as e:
            LOGGER.error(f"Erro ao logar estado: {e}")
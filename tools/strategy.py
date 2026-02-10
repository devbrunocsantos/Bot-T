import json
import os
import random
import ccxt
import time
import urllib3
from datetime import datetime
from configs.config import *

# Desativa avisos de segurança para conexões via Proxy Corporativo (SSL Verify False)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class CashAndCarryBot:
    def __init__(self, initial_capital_usd, exchange_client=None):
        """
        Inicializa o Bot.
        
        Args:
            initial_capital_usd (float): Capital inicial simulado.
            exchange_client (obj, optional): Cliente de exchange Mock para backtests. 
                                             Se None, conecta na Binance real via CCXT.
        """
        self.state_file = os.path.join("configs", "bot_state.json")
        
        # INJEÇÃO DE DEPENDÊNCIA: 
        # Se um cliente for passado (Simulação), usamos ele. 
        # Caso contrário, configuramos a conexão real com suporte a Proxy.
        if exchange_client:
             self.exchange = exchange_client
        else:
            proxies = None
            # Verifica se existem proxies configurados nas variáveis de ambiente (pelo utils.py)
            if os.environ.get('HTTP_PROXY'):
                proxies = {
                    'http': os.environ.get('HTTP_PROXY'),
                    'https': os.environ.get('HTTPS_PROXY')
                }

            self.exchange = getattr(ccxt, EXCHANGE_ID)({
                'enableRateLimit': True,
                'options': {'defaultType': 'future'},
                'proxies': proxies, 
                'verify': False  # Crucial para ambientes corporativos que interceptam SSL
            })

        # Inicialização de variáveis de estado
        if not self._load_state():
            self.capital = initial_capital_usd
            self.position = None 
            self.accumulated_profit = 0.0
            self.accumulated_fees = 0.0
            self.peak_capital = initial_capital_usd
            self.pending_deposit_usd = 0.0
            self.next_funding_timestamp = None
            self._save_state()

    def _save_state(self):
        """
        Persiste o estado financeiro e operacional em disco (JSON).
        """
        try:
            state = {
                'capital': self.capital,
                'position': self.position,
                'accumulated_profit': self.accumulated_profit,
                'accumulated_fees': self.accumulated_fees,
                'peak_capital': self.peak_capital,
                'pending_deposit_usd': self.pending_deposit_usd,
                'next_funding_timestamp': self.next_funding_timestamp
            }
            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=4)
        except Exception as e:
            LOGGER.error(f"Erro ao salvar estado: {e}")

    def _load_state(self):
        """
        Carrega o estado anterior se existir. Retorna True se sucesso.
        """
        if not os.path.exists(self.state_file):
            return False   
        try:
            with open(self.state_file, 'r') as f:
                state = json.load(f)  
            self.capital = state.get('capital', 0.0)
            self.position = state.get('position')
            self.accumulated_profit = state.get('accumulated_profit', 0.0)
            self.accumulated_fees = state.get('accumulated_fees', 0.0)
            self.peak_capital = state.get('peak_capital', 0.0)
            self.pending_deposit_usd = state.get('pending_deposit_usd', 0.0)
            self.next_funding_timestamp = state.get('next_funding_timestamp')
            
            LOGGER.info("Estado anterior carregado com SUCESSO.")
            return True
        except Exception as e:
            LOGGER.error(f"Erro ao carregar estado: {e}")
            return False

    def get_top_volume_pairs(self):
        """
        Realiza varredura no mercado buscando pares com alto volume 
        e histórico consistente de Funding Rates.
        """
        try:
            LOGGER.info("Iniciando varredura dinâmica de mercado...")
            tickers = self.exchange.fetch_tickers()
            
            # 1. Pré-filtro: Volume Mínimo e pares USDT
            candidates = []
            for symbol, data in tickers.items():
                if '/USDT:USDT' in symbol and data['quoteVolume'] >= MIN_24H_VOLUME_USD:
                    candidates.append(symbol)
            
            # Ordena por volume decrescente e pega os Top 20 para análise detalhada
            top_candidates = sorted(candidates, key=lambda x: tickers[x]['quoteVolume'], reverse=True)[:20]
            valid_pairs = []

            for symbol in top_candidates:
                # Aplica o Filtro de Consistência (Funding Quality Score)
                if self._analyze_funding_consistency(symbol):
                    valid_pairs.append(symbol)
                    LOGGER.info(f"[OK] APROVADO no filtro de consistência: {symbol}")
            
            return valid_pairs
        except Exception as e:
            LOGGER.error(f"Erro no scanner: {e}")
            return []

    def _analyze_funding_consistency(self, symbol):
        """
        Analisa o histórico recente de Funding Rates.
        Critérios: Média positiva (3 dias) e ausência de taxas negativas.
        """
        try:
            # Busca histórico (limit=20 garante margem para pegar os últimos 3 dias/9 periodos)
            history = self.exchange.fetch_funding_rate_history(symbol, limit=20)
            
            if not history or len(history) < 9: 
                return False
            
            # Analisa os últimos 9 pagamentos (aprox. 3 dias em ciclos de 8h)
            recent_rates = [entry['fundingRate'] for entry in history[-9:]]
            
            # Critério 1: Média atrativa (> 0.01% por período)
            avg_rate = sum(recent_rates) / len(recent_rates)
            if avg_rate < 0.0001: 
                return False

            # Critério 2: Consistência (Nenhum negativo)
            if any(r < 0 for r in recent_rates): 
                return False
            
            return True
        except: 
            return False

    def check_entry_opportunity(self, symbol, current_time=None):
        """
        Avalia viabilidade de entrada com filtros rigorosos de ROI e Basis.
        """
        try:
            now = current_time if current_time else time.time()

            # 1. Filtro de Cooldown (Evita reentrada imediata após saída forçada)
            if hasattr(self, 'cooldowns') and symbol in self.cooldowns:
                if now < self.cooldowns[symbol]:
                    return False, 0.0, "COOLDOWN_ACTIVE"

            funding_info = self.exchange.fetch_funding_rate(symbol)
            funding_rate = funding_info['fundingRate']
            
            # 2. Filtro de ROI Mínimo vs Taxas (O "Pulo do Gato")
            # Consideramos Taker na entrada e Taker na saída para segurança máxima
            # Total de 4 execuções (2 no Spot, 2 no Futuro)
            total_fees_estimated = (FEE_TAKER * 4) + (SLIPPAGE_SIMULATED * 4)
            
            # Projeção de lucro em 24h (assumindo 3 pagamentos de funding)
            projected_24h_return = funding_rate * 3 

            # Só entra se o lucro de 1 dia pagar todas as taxas e ainda sobrar margem
            if projected_24h_return < (total_fees_estimated * 1.2): # 20% de margem de segurança
                return False, funding_rate, "LOW_PROFIT_VS_FEES"

            # 3. Verificação de Basis (Evitar entrar em Backwardation)
            symbol_spot = symbol.split(':')[0] 
            ticker_future = self.exchange.fetch_ticker(symbol)
            ticker_spot = self.exchange.fetch_ticker(symbol_spot)
            
            p_future = ticker_future['last']
            p_spot = ticker_spot['last']
            basis_percent = (p_future - p_spot) / p_spot

            # Regra: O Futuro deve estar pelo menos "flat" ou em Contango
            if basis_percent < NEGATIVE_FUNDING_THRESHOLD: # Tolerância mínima de 0.02%
                return False, funding_rate, f"BACKWARDATION ({basis_percent:.4%})"

            return True, funding_rate, "SUCCESS"

        except Exception as e:
            LOGGER.error(f"Erro ao verificar oportunidade para {symbol}: {e}")
            return False, 0.0, f"API_ERROR"

    def simulate_entry(self, symbol, funding_rate, current_time=None):
        """
        Executa entrada simulada com 'Lag' de execução e Slippage.
        """
        try:
            # Se current_time for passado (backtest), usa ele. Senão usa o real.
            now = current_time if current_time else time.time()

            # 1. Perna Spot
            ticker_spot = self.exchange.fetch_ticker(symbol)
            price_spot_raw = ticker_spot['last']
            entry_price_long = price_spot_raw * (1 + SLIPPAGE_SIMULATED)

            allocation_per_leg = self.capital / 2
            quantity = allocation_per_leg / entry_price_long

            # 2. Simulação de Latência (Lag)
            # Apenas aplicamos o sleep se estivermos em tempo real (current_time is None)
            if current_time is None:
                lag_seconds = random.uniform(0.5, 2.0)
                time.sleep(lag_seconds)
            
            # 3. Perna Futura
            ticker_future = self.exchange.fetch_ticker(symbol)
            price_future_raw = ticker_future['last']
            entry_price_short = price_future_raw * (1 - SLIPPAGE_SIMULATED)

            # Cálculo de Taxas
            cost_spot = (quantity * entry_price_long) * FEE_TAKER
            cost_future = (quantity * entry_price_short) * FEE_TAKER
            total_entry_fee = cost_spot + cost_future

            # Configuração do Funding Timestamp
            funding_info = self.exchange.fetch_funding_rate(symbol)
            self.next_funding_timestamp = funding_info['nextFundingTimestamp'] / 1000
            
            self.position = {
                'symbol': symbol,
                'size': quantity,
                'entry_price_spot': entry_price_long,
                'entry_price_future': entry_price_short,
                'current_funding_rate': funding_rate,
                'entry_time': now
            }
            
            self.accumulated_fees += total_entry_fee
            self.capital -= total_entry_fee 
            
            LOGGER.info(f"ENTRADA: {symbol} | Spot: {entry_price_long:.2f} | Fut: {entry_price_short:.2f} | Taxas: {total_entry_fee:.2f}")
            self._save_state()
            return True
        except Exception as e:
            LOGGER.error(f"Erro entry: {e}")
            return False

    def monitor_and_manage(self, db_manager, current_time=None):
        """
        Gerencia a posição: Verifica margem, coleta funding e atualiza logs.
        """
        if not self.position: return

        now = current_time if current_time else time.time()
        symbol = self.position['symbol']
        
        try:
            # Atualização de Mercado
            ticker = self.exchange.fetch_ticker(symbol)
            current_price = ticker['last']
            
            funding_info = self.exchange.fetch_funding_rate(symbol)
            current_funding = funding_info['fundingRate']
            
            # Helper para saber quando é o próximo funding segundo a API
            api_next_funding_ts = funding_info.get('nextFundingTimestamp')
            api_next_funding_sec = api_next_funding_ts / 1000 if api_next_funding_ts else None

            # --- Lógica de Recebimento de Funding ---
            if self.next_funding_timestamp and now >= self.next_funding_timestamp:
                funding_payout = (self.position['size'] * current_price) * current_funding
                self.accumulated_profit += funding_payout
                
                # Agendamento do próximo pagamento
                if current_time:
                     # No modo Backtest, simplificamos somando 8h
                     self.next_funding_timestamp += 28800
                elif api_next_funding_sec and api_next_funding_sec > now:
                    # No modo Real, confiamos na API
                    self.next_funding_timestamp = api_next_funding_sec
                else:
                    # Fallback
                    self.next_funding_timestamp += 28800 

            # --- Circuit Breaker (Funding Negativo) ---
            if current_funding < NEGATIVE_FUNDING_THRESHOLD:
                LOGGER.warning(f"SAIDA FORÇADA: Funding negativo crítico ({current_funding:.4%})")
                self._close_position(current_price, reason="Negative Funding")
                return

            # --- Cálculo de PnL e Patrimônio ---
            spot_pnl = (current_price - self.position['entry_price_spot']) * self.position['size']
            future_pnl = (self.position['entry_price_future'] - current_price) * self.position['size']
            net_pnl_price = spot_pnl + future_pnl
            
            total_equity = self.capital + self.accumulated_profit + net_pnl_price
            
            # Atualiza pico histórico para cálculo de Drawdown
            if total_equity > self.peak_capital:
                self.peak_capital = total_equity
            
            drawdown = (self.peak_capital - total_equity) / self.peak_capital if self.peak_capital > 0 else 0

            # --- Logging ---
            log_data = {
                'symbol': symbol,
                'price_spot': current_price,
                'price_future': current_price,
                'funding_rate': current_funding,
                'next_funding_time': "SIMULATED" if current_time else datetime.fromtimestamp(self.next_funding_timestamp).strftime('%Y-%m-%d %H:%M:%S'),
                'position_size': self.position['size'],
                'simulated_fees': self.accumulated_fees,
                'accumulated_profit': self.accumulated_profit + net_pnl_price,
                'max_drawdown': drawdown,
                'action': 'HOLD'
            }
            # Evita logar no banco se for backtest (o db_manager pode ser um Mock)
            if hasattr(db_manager, 'log_state'):
                db_manager.log_state(log_data)
            
            # Tenta reinvestir se houver caixa
            self._process_compounding(current_price)
            self._save_state()

        except Exception as e:
            LOGGER.error(f"Monitor error: {e}")

    def _close_position(self, current_price, reason):
        """
        Encerra a posição e contabiliza custos de saída.
        """
        # Slippage na saída
        exit_price_long = current_price * (1 - SLIPPAGE_SIMULATED)
        exit_price_short = current_price * (1 + SLIPPAGE_SIMULATED)
        
        position_value = self.position['size'] * current_price
        exit_fee = (position_value * FEE_TAKER) * 2
        
        self.accumulated_fees += exit_fee
        self.capital -= exit_fee
        self.position = None
        
        LOGGER.info(f"POSIÇÃO ENCERRADA. Motivo: {reason} | Taxas: ${exit_fee:.2f}")
        self._save_state()

    def deposit_monthly_contribution(self, exchange_rate=None):
        """
        Recebe aporte em BRL e converte para USD usando taxa fornecida.
        """
        rate_to_use = exchange_rate if exchange_rate else BRL_USD_RATE
        usd_amount = MONTHLY_CONTRIBUTION_BRL / rate_to_use
        self.pending_deposit_usd += usd_amount
        LOGGER.info(f"Aporte: R${MONTHLY_CONTRIBUTION_BRL:.2f} (Tx: {rate_to_use:.2f}) -> ${usd_amount:.2f}")

    def _process_compounding(self, current_price):
        """
        Aumenta a posição se houver saldo pendente (Juros Compostos),
        recalculando o Preço Médio Ponderado.
        """
        if self.pending_deposit_usd >= MIN_ORDER_VALUE_USD:
            # 1. Novos Preços
            new_entry_spot = current_price * (1 + SLIPPAGE_SIMULATED)
            new_entry_future = current_price * (1 - SLIPPAGE_SIMULATED)

            # 2. Nova Quantidade
            allocation_per_leg = self.pending_deposit_usd / 2
            new_qty = allocation_per_leg / new_entry_spot

            # 3. Dados Antigos
            old_qty = self.position['size']
            old_price_spot = self.position['entry_price_spot']
            old_price_future = self.position['entry_price_future']
            
            total_new_qty = old_qty + new_qty

            # 4. Cálculo Preço Médio Ponderado (Weighted Average)
            avg_price_spot = ((old_price_spot * old_qty) + (new_entry_spot * new_qty)) / total_new_qty
            avg_price_future = ((old_price_future * old_qty) + (new_entry_future * new_qty)) / total_new_qty

            # 5. Atualização
            self.position['size'] = total_new_qty
            self.position['entry_price_spot'] = avg_price_spot
            self.position['entry_price_future'] = avg_price_future
            
            # Contabilidade
            self.capital += self.pending_deposit_usd
            reinvest_fees = (allocation_per_leg * FEE_TAKER) * 2
            self.accumulated_fees += reinvest_fees
            self.capital -= reinvest_fees
            self.pending_deposit_usd = 0.0
            
            LOGGER.info(f"REINVESTIMENTO: +{new_qty:.4f} moedas. Novo PM Spot: ${avg_price_spot:.2f}")
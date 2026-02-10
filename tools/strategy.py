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
    def __init__(self, initial_capital_usd):
        """
        Inicializa o Bot.
        
        Args:
            initial_capital_usd (float): Capital inicial simulado.
            exchange_client (obj, optional): Cliente de exchange Mock para backtests. 
                                             Se None, conecta na Binance real via CCXT.
        """
        self.state_file = os.path.join("configs", "bot_state.json")

        proxies = None
        # Verifica se existem proxies configurados nas variáveis de ambiente (pelo utils.py)
        if os.environ.get('HTTP_PROXY'):
            proxies = {
                'http': os.environ.get('HTTP_PROXY'),
                'https': os.environ.get('HTTPS_PROXY')
            }

        self.exchange_future = getattr(ccxt, EXCHANGE_ID)({
            'enableRateLimit': True,
            'options': {'defaultType': 'future'},
            'proxies': proxies, 
            'verify': False  # Crucial para ambientes corporativos que interceptam SSL
        })

        self.exchange_spot = getattr(ccxt, EXCHANGE_ID)({
                'enableRateLimit': True,
                'proxies': proxies, 
                'verify': False 
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
            tickers = self.exchange_future.fetch_tickers()
            
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
            history = self.exchange_future.fetch_funding_rate_history(symbol, limit=20)
            
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

    def check_entry_opportunity(self, symbol, price_spot, price_future, funding_rate, current_time=None):
        """
        Avalia viabilidade de entrada.
        Args:
            price_spot (float): Preço atual do Spot.
            price_future (float): Preço atual do Futuro.
            funding_rate (float): Taxa de funding atual.
        """
        try:
            now = current_time if current_time else time.time()

            # 1. Filtro de Cooldown
            if hasattr(self, 'cooldowns') and symbol in self.cooldowns:
                if now < self.cooldowns[symbol]:
                    return False, 0.0, "COOLDOWN_ACTIVE"
            
            # 2. Taxas
            real_fee_spot = self._get_real_fee_rate(symbol, future=False)
            real_fee_future = self._get_real_fee_rate(symbol, future=True)
            
            # 3. Slippage Real (Impacto de Mercado)
            # Calculamos o impacto para o tamanho da nossa mão (capital / 2)
            trade_size_usd = self.capital / 2
            
            # Slippage da Perna Spot (Compra)
            symbol_spot = symbol.split(':')[0] # Normalização simples, ou use a lógica do main
            slippage_spot = self._calculate_market_impact(symbol_spot, trade_size_usd, side='buy', future=False)
            # Slippage da Perna Futura (Venda/Short)
            slippage_future = self._calculate_market_impact(symbol, trade_size_usd, side='sell', future=True)

            total_custo_spot = (real_fee_spot * 2) + (slippage_spot * 2)
            total_custo_future = (real_fee_future * 2) + (slippage_future * 2)
            
            total_fees_real = total_custo_spot + total_custo_future

            projected_24h_return = funding_rate * 3 

            if projected_24h_return < (total_fees_real * 1.2): 
                return False, funding_rate, "LOW_PROFIT_VS_FEES"

            # 3. Verificação de Basis (Usando os preços recebidos)
            basis_percent = (price_future - price_spot) / price_spot

            if basis_percent < NEGATIVE_FUNDING_THRESHOLD: 
                return False, funding_rate, f"BACKWARDATION ({basis_percent:.4%})"

            return True, funding_rate, "SUCCESS"

        except Exception as e:
            LOGGER.error(f"Erro ao verificar oportunidade para {symbol}: {e}")
            return False, 0.0, f"ERROR"

    def simulate_entry(self, symbol, funding_rate, current_time=None):
        """
        Executa entrada simulada com 'Lag' de execução e Slippage.
        """
        try:
            # Se current_time for passado (backtest), usa ele. Senão usa o real.
            now = current_time if current_time else time.time()

            # 1. Perna Spot
            ticker_spot = self.exchange_future.fetch_ticker(symbol)
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
            ticker_future = self.exchange_future.fetch_ticker(symbol)
            price_future_raw = ticker_future['last']
            entry_price_short = price_future_raw * (1 - SLIPPAGE_SIMULATED)

            # Cálculo de Taxas
            cost_spot = (quantity * entry_price_long) * FEE_TAKER
            cost_future = (quantity * entry_price_short) * FEE_TAKER
            total_entry_fee = cost_spot + cost_future

            # Configuração do Funding Timestamp
            funding_info = self.exchange_future.fetch_funding_rate(symbol)
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
        if not self.position: return

        now = current_time if current_time else time.time()
        symbol = self.position['symbol']
        
        try:
            # 1. Busca dados do Futuro (Necessário para PnL e Monitoramento)
            ticker_future = self.exchange_future.fetch_ticker(symbol)
            price_future = ticker_future['last']
            
            # --- Lógica de Funding (Inalterada) ---
            funding_info = self.exchange_future.fetch_funding_rate(symbol)
            current_funding = funding_info['fundingRate']
            
            api_next_funding_ts = funding_info.get('nextFundingTimestamp')
            api_next_funding_sec = api_next_funding_ts / 1000 if api_next_funding_ts else None

            if self.next_funding_timestamp and now >= self.next_funding_timestamp:
                funding_payout = (self.position['size'] * price_future) * current_funding
                self.accumulated_profit += funding_payout
                
                if current_time:
                     self.next_funding_timestamp += 28800
                elif api_next_funding_sec and api_next_funding_sec > now:
                    self.next_funding_timestamp = api_next_funding_sec
                else:
                    self.next_funding_timestamp += 28800 

            # --- Circuit Breaker (Inalterado) ---
            if current_funding < NEGATIVE_FUNDING_THRESHOLD:
                LOGGER.warning(f"SAIDA FORÇADA: Funding negativo crítico ({current_funding:.4%})")
                self._close_position(price_future, reason="Negative Funding")
                return

            # --- Cálculo de PnL Flutuante ---
            # Nota: Para visualização precisa, usamos o preço de entrada vs preço atual
            spot_pnl = (price_future - self.position['entry_price_spot']) * self.position['size'] # Estimativa usando preço futuro como proxy se spot não for baixado
            future_pnl = (self.position['entry_price_future'] - price_future) * self.position['size']
            net_pnl_price = spot_pnl + future_pnl
            
            total_equity = self.capital + self.accumulated_profit + net_pnl_price
            
            if total_equity > self.peak_capital:
                self.peak_capital = total_equity
            
            drawdown = (self.peak_capital - total_equity) / self.peak_capital if self.peak_capital > 0 else 0

            # --- [NOVO] Lógica de Reinvestimento Condicional ---
            # Só gastamos API call buscando o Spot se tivermos dinheiro para reinvestir
            if self.pending_deposit_usd >= MIN_ORDER_VALUE_USD:
                try:
                    # Busca preço Spot para calcular Basis exato
                    symbol_spot = symbol.split(':')[0]
                    ticker_spot = self.exchange_future.fetch_ticker(symbol_spot)
                    price_spot = ticker_spot['last']
                    
                    # Chama o processamento passando AMBOS os preços
                    self._process_compounding(price_spot, price_future)
                except Exception as e:
                    LOGGER.warning(f"Falha ao buscar Spot para reinvestimento: {e}")

            # --- Logging ---
            log_data = {
                'symbol': symbol,
                'price_future': price_future,
                'funding_rate': current_funding,
                'next_funding_time': "SIMULATED" if current_time else datetime.fromtimestamp(self.next_funding_timestamp).strftime('%Y-%m-%d %H:%M:%S'),
                'position_size': self.position['size'],
                'simulated_fees': self.accumulated_fees,
                'accumulated_profit': self.accumulated_profit + net_pnl_price,
                'max_drawdown': drawdown,
                'action': 'HOLD'
            }
            
            if hasattr(db_manager, 'log_state'):
                db_manager.log_state(log_data)
            
            self._save_state()

        except Exception as e:
            LOGGER.error(f"Monitor error: {e}")

    def _close_position(self, current_price, reason):
        """
        Encerra a posição e contabiliza PnL REALIZADO + Custos.
        """
        # Slippage na saída
        exit_price_long = current_price * (1 - SLIPPAGE_SIMULATED)
        exit_price_short = current_price * (1 + SLIPPAGE_SIMULATED)
        
        # 1. Cálculo do PnL do Preço (Capital Gains/Losses)
        # Spot: (Preço Saída - Preço Entrada) * Qtd
        pnl_spot = (exit_price_long - self.position['entry_price_spot']) * self.position['size']
        # Futuro Short: (Preço Entrada - Preço Saída) * Qtd
        pnl_future = (self.position['entry_price_future'] - exit_price_short) * self.position['size']
        
        net_price_pnl = pnl_spot + pnl_future

        # 2. Cálculo das Taxas de Saída
        position_value = self.position['size'] * current_price
        exit_fee = (position_value * FEE_TAKER) * 2
        
        # 3. Consolidação Financeira
        self.capital += net_price_pnl  # Soma o lucro (ou subtrai prejuízo) da variação de preço
        self.capital -= exit_fee       # Subtrai taxas de saída
        self.accumulated_fees += exit_fee

        LOGGER.info(f"POSIÇÃO ENCERRADA | Motivo: {reason}")
        LOGGER.info(f"PnL Preço: ${net_price_pnl:.2f} | Taxas Saída: ${exit_fee:.2f} | Saldo Atual: ${self.capital:.2f}")

        self.position = None
        self._save_state()

    def deposit_monthly_contribution(self, exchange_rate=None):
        """
        Recebe aporte em BRL e converte para USD usando taxa fornecida.
        """
        rate_to_use = exchange_rate if exchange_rate else BRL_USD_RATE
        usd_amount = MONTHLY_CONTRIBUTION_BRL / rate_to_use
        self.pending_deposit_usd += usd_amount
        LOGGER.info(f"Aporte: R${MONTHLY_CONTRIBUTION_BRL:.2f} (Tx: {rate_to_use:.2f}) -> ${usd_amount:.2f}")

    def _process_compounding(self, price_spot, price_future):
        """
        Aumenta a posição se houver saldo pendente, MAS APENAS SE
        o Basis (Spread) atual for favorável (positivo).
        """
        # 1. Filtro de Qualidade: Verifica o Basis atual
        current_basis = (price_future - price_spot) / price_spot
        
        # Se o mercado estiver em "Backwardation" (Futuro < Spot) ou spread muito baixo,
        # NÃO reinvestimos agora. Melhor esperar o spread abrir para garantir lucro.
        if current_basis < 0.0001: # Ex: 0.01% mínimo
            LOGGER.info(f"Reinvestimento adiado. Basis ruim: {current_basis:.4%}")
            return

        # Se passou no filtro, executa o aumento de posição
        if self.pending_deposit_usd >= MIN_ORDER_VALUE_USD:
            # 2. Novos Preços de Entrada (com Slippage)
            new_entry_spot = price_spot * (1 + SLIPPAGE_SIMULATED)
            new_entry_future = price_future * (1 - SLIPPAGE_SIMULATED)

            # 3. Nova Quantidade
            allocation_per_leg = self.pending_deposit_usd / 2
            new_qty = allocation_per_leg / new_entry_spot

            # 4. Dados Antigos
            old_qty = self.position['size']
            old_price_spot = self.position['entry_price_spot']
            old_price_future = self.position['entry_price_future']
            
            total_new_qty = old_qty + new_qty

            # 5. Cálculo Preço Médio Ponderado (Weighted Average)
            avg_price_spot = ((old_price_spot * old_qty) + (new_entry_spot * new_qty)) / total_new_qty
            avg_price_future = ((old_price_future * old_qty) + (new_entry_future * new_qty)) / total_new_qty

            # 6. Atualização
            self.position['size'] = total_new_qty
            self.position['entry_price_spot'] = avg_price_spot
            self.position['entry_price_future'] = avg_price_future
            
            # Contabilidade
            # Adiciona o aporte ao capital total (Equity)
            self.capital += self.pending_deposit_usd 
            
            # Desconta as taxas da operação de aumento
            reinvest_fees = (allocation_per_leg * FEE_TAKER) * 2
            self.accumulated_fees += reinvest_fees
            self.capital -= reinvest_fees
            
            self.pending_deposit_usd = 0.0
            
            LOGGER.info(f"REINVESTIMENTO REALIZADO: +{new_qty:.4f} moedas. Basis: {current_basis:.4%}. Novo PM Spot: ${avg_price_spot:.2f}")

    def _get_real_fee_rate(self, symbol, future=False):
        """
        Busca a taxa de Taker real da conta para o par.
        Retorna o valor decimal (ex: 0.0004 para 0.04%).
        """
        try:
            # Tenta buscar do cache primeiro para economizar API
            if hasattr(self, 'cached_fee') and self.cached_fee:
                return self.cached_fee

            # Busca taxas de trading da conta
            # Nota: Requer permissões de leitura na API Key
            if future:
                fees = self.exchange_future.fetch_trading_fees()
            else:
                fees = self.exchange_spot.fetch_trading_fees()
            
            # Tenta pegar a taxa específica do par, ou o padrão USDT
            ticker_fees = fees.get(symbol, fees.get('USDT', {}))
            taker_fee = ticker_fees.get('taker', FEE_TAKER) # Fallback para config se falhar
            
            self.cached_fee = taker_fee # Cache simples
            return taker_fee
        except Exception as e:
            # Em backtest ou erro de permissão, usa o configurado
            return FEE_TAKER

    # [NOVO] Cálculo de Slippage baseado no Order Book
    def _calculate_market_impact(self, symbol, usd_amount, side='buy', future=False):
        """
        Calcula o Slippage real simulando uma ordem a mercado no Order Book atual.
        
        Args:
            symbol: Par a ser negociado.
            usd_amount: Valor financeiro da ordem em USD.
            side: 'buy' (olha os asks) ou 'sell' (olha os bids).
        """
        try:
            # Busca as 50 melhores ofertas do livro
            limit = 50

            if future:
                order_book = self.exchange_future.fetch_order_book(symbol, limit=limit)
            else:
                order_book = self.exchange_spot.fetch_order_book(symbol, limit=limit)
            
            # Se quero COMPRAR, consumo quem está VENDENDO (asks)
            # Se quero VENDER, consumo quem está COMPRANDO (bids)
            book = order_book['asks'] if side == 'buy' else order_book['bids']
            
            if not book: return SLIPPAGE_SIMULATED

            amount_filled = 0.0
            total_cost = 0.0
            
            # Preço inicial (topo do livro) para referência
            best_price = book[0][0]
            
            # Simula a execução da ordem nível a nível
            for price, qty in book:
                # Quanto custa levar esse nível inteiro?
                level_value = price * qty
                
                remaining = usd_amount - total_cost
                
                if level_value >= remaining:
                    # Preenche o resto aqui e termina
                    qty_needed = remaining / price
                    total_cost += remaining
                    amount_filled += qty_needed
                    break
                else:
                    # Consome o nível todo e continua
                    total_cost += level_value
                    amount_filled += qty
            
            if amount_filled == 0: return SLIPPAGE_SIMULATED

            # Preço Médio Final = Custo Total / Quantidade de Moedas
            avg_executed_price = total_cost / amount_filled
            
            # Cálculo do Slippage Percentual
            # Buy: Paguei mais caro que o topo? (Avg > Best)
            # Sell: Vendi mais barato que o topo? (Avg < Best)
            slippage_pct = abs(avg_executed_price - best_price) / best_price
            
            return slippage_pct

        except Exception as e:
            LOGGER.warning(f"Erro ao calcular slippage real para {symbol}: {e}")
            return SLIPPAGE_SIMULATED
import json
import os
import random
import ccxt
import time
from datetime import datetime
from configs.config import *

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

        # Dicionário base de configuração
        exchange_config = {
            'apiKey': API_KEY,
            'secret': API_SECRET,
            'enableRateLimit': True,
        }

        # Inicializa cliente de Futuros (Swap)
        self.exchange_swap = getattr(ccxt, EXCHANGE_ID)({
            **exchange_config,  # Desempacota as credenciais
            'options': {'defaultType': 'swap'},
        })

        # Inicializa cliente Spot (À vista)
        self.exchange_spot = getattr(ccxt, EXCHANGE_ID)({
            **exchange_config,  # Desempacota as credenciais
            'options': {'defaultType': 'spot'}
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
            self.last_usd_brl = BRL_USD_RATE

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
                'next_funding_timestamp': self.next_funding_timestamp,
                'last_usd_brl': self.last_usd_brl
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
            self.last_usd_brl = state.get('last_usd_brl', BRL_USD_RATE)
            
            LOGGER.info("Estado anterior carregado com SUCESSO.")
            return True
        except Exception as e:
            LOGGER.error(f"Erro ao carregar estado: {e}")
            return False
        
    def update_brl_rate(self, new_rate):
        """Atualiza a cotação USD/BRL e salva o estado."""
        self.last_usd_brl = new_rate
        self._save_state()

    def get_top_volume_pairs(self):
        """
        Realiza varredura no mercado buscando pares com alto volume 
        e histórico consistente de Funding Rates.
        """
        try:
            LOGGER.info("Iniciando varredura dinâmica de mercado...")
            tickers = self.exchange_swap.fetch_tickers()
            
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
            history = self.exchange_swap.fetch_funding_rate_history(symbol, limit=20)
            
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

    def check_entry_opportunity(self, symbol, found_spot, price_spot, price_swap, funding_rate):
        """
        Avalia viabilidade de entrada.
        Args:
            price_spot (float): Preço atual do Spot.
            price_swap (float): Preço atual do Futuro.
            funding_rate (float): Taxa de funding atual.
        """
        try:
            now = time.time()

            # 1. Filtro de Cooldown
            if hasattr(self, 'cooldowns') and symbol in self.cooldowns:
                if now < self.cooldowns[symbol]:
                    return False, 0.0, "COOLDOWN_ACTIVE"
            
            # 2. Taxas
            real_fee_spot = self._get_real_fee_rate(found_spot, swap=False)
            real_fee_swap = self._get_real_fee_rate(symbol, swap=True)
            
            # 3. Slippage Real (Impacto de Mercado)
            # Calculamos o impacto para o tamanho da nossa mão (capital / 2)
            trade_size_usd = self.capital / 2
            
            # Slippage da Perna Spot (Compra)
            slippage_spot = self._calculate_market_impact(found_spot, trade_size_usd, side='buy', swap=False)
            # Slippage da Perna Futura (Venda/Short)
            slippage_swap = self._calculate_market_impact(symbol, trade_size_usd, side='sell', swap=True)

            total_custo_spot = (real_fee_spot * 2) + (slippage_spot * 2)
            total_custo_swap = (real_fee_swap * 2) + (slippage_swap * 2)
            
            total_fees_real = total_custo_spot + total_custo_swap

            projected_24h_return = funding_rate * 3 

            if projected_24h_return < (total_fees_real * 1.2): 
                return False, funding_rate, "LOW_PROFIT_VS_FEES"

            # 3. Verificação de Basis (Usando os preços recebidos)
            basis_percent = (price_swap - price_spot) / price_spot

            if basis_percent < NEGATIVE_FUNDING_THRESHOLD: 
                return False, funding_rate, f"BACKWARDATION ({basis_percent:.4%})"

            return True, funding_rate, "SUCCESS"

        except Exception as e:
            LOGGER.error(f"Erro ao verificar oportunidade para {symbol}: {e}")
            return False, 0.0, f"ERROR"

    def simulate_entry(self, symbol, found_spot, funding_rate):
        """
        Executa entrada simulada com 'Lag' de execução e Slippage.
        """
        try:
            # Se current_time for passado (backtest), usa ele. Senão usa o real.
            now = time.time()

            # 1. Perna Spot
            ticker_spot = self.exchange_swap.fetch_ticker(symbol)
            price_spot_raw = ticker_spot['last']

            trade_size_usd = self.capital / 2

            # Slippage da Perna Spot (Compra)
            slippage_spot = self._calculate_market_impact(found_spot, trade_size_usd, side='buy', swap=False)
            # Slippage da Perna Futura (Venda/Short)
            slippage_swap = self._calculate_market_impact(symbol, trade_size_usd, side='sell', swap=True)

            entry_price_long = price_spot_raw * (1 + slippage_spot)

            allocation_per_leg = self.capital / 2
            quantity = allocation_per_leg / entry_price_long
            
            # 2. Perna Futura
            ticker_swap = self.exchange_swap.fetch_ticker(symbol)
            price_swap_raw = ticker_swap['last']
            entry_price_short = price_swap_raw * (1 - slippage_swap)

            # Cálculo de Taxas
            real_fee_spot = self._get_real_fee_rate(found_spot, swap=False)
            real_fee_swap = self._get_real_fee_rate(symbol, swap=True)

            cost_spot = (quantity * entry_price_long) * real_fee_spot
            cost_swap = (quantity * entry_price_short) * real_fee_swap

            total_entry_fee = cost_spot + cost_swap

            # Configuração do Funding Timestamp
            funding_info = self.exchange_swap.fetch_funding_rate(symbol)
            self.next_funding_timestamp = funding_info['nextFundingTimestamp'] / 1000
            
            self.position = {
                'symbol': symbol,
                'size': quantity,
                'entry_price_spot': entry_price_long,
                'entry_price_swap': entry_price_short,
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

    def monitor_and_manage(self, db_manager, found_spot):
        if not self.position: return

        now = time.time()
        symbol = self.position['symbol']
        
        try:
            # 1. Busca dados do Futuro (Necessário para PnL e Monitoramento)
            ticker_swap = self.exchange_swap.fetch_ticker(symbol)
            price_swap = ticker_swap['last']

            try:
                # Tenta buscar o preço real do ativo no mercado à vista
                ticker_spot = self.exchange_spot.fetch_ticker(found_spot)
                price_spot = ticker_spot['last']
            except Exception as e:
                # Em caso de falha na API Spot, mantém o fallback e loga aviso
                LOGGER.warning(f"Falha ao buscar preço Spot para monitoramento: {e}. Usando proxy.")
                price_spot = price_swap
            
            # --- Lógica de Funding (Inalterada) ---
            funding_info = self.exchange_swap.fetch_funding_rate(symbol)
            current_funding = funding_info['fundingRate']
            
            api_next_funding_ts = funding_info.get('nextFundingTimestamp')
            api_next_funding_sec = api_next_funding_ts / 1000 if api_next_funding_ts else None

            if self.next_funding_timestamp and now >= self.next_funding_timestamp:
                funding_payout = (self.position['size'] * price_swap) * current_funding
                self.accumulated_profit += funding_payout
                
                if api_next_funding_sec and api_next_funding_sec > now:
                    self.next_funding_timestamp = api_next_funding_sec

            # --- Circuit Breaker ---
            if current_funding < NEGATIVE_FUNDING_THRESHOLD:
                LOGGER.warning(f"SAIDA FORÇADA: Funding negativo crítico ({current_funding:.4%})")
                self._close_position(price_swap, symbol, found_spot, reason="Negative Funding")
                return

            # --- Cálculo de PnL Flutuante ---
            spot_pnl = (price_spot - self.position['entry_price_spot']) * self.position['size']
            swap_pnl = (self.position['entry_price_swap'] - price_swap) * self.position['size']
            net_pnl_price = spot_pnl + swap_pnl
            
            total_equity = self.capital + self.accumulated_profit + net_pnl_price
            
            if total_equity > self.peak_capital:
                self.peak_capital = total_equity
            
            drawdown = (self.peak_capital - total_equity) / self.peak_capital if self.peak_capital > 0 else 0

            # --- Lógica de Reinvestimento Condicional ---
            if self.pending_deposit_usd >= MIN_ORDER_VALUE_USD:
                self._process_compounding(symbol, found_spot, price_spot, price_swap)

            # --- Logging ---
            log_data = {
                'symbol': symbol,
                'price_swap': price_swap,
                'funding_rate': current_funding,
                'next_funding_time': datetime.fromtimestamp(self.next_funding_timestamp).strftime('%Y-%m-%d %H:%M:%S'),
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

    def _close_position(self, current_price_swap, symbol, found_spot, reason):
        """
        Encerra a posição e contabiliza PnL REALIZADO + Custos.
        """
        try:
            qty = self.position['size']

            try:
                ticker_spot = self.exchange_spot.fetch_ticker(found_spot)
                current_price_spot = ticker_spot['last']
            except Exception as e:
                LOGGER.warning(f"Erro ao buscar Spot na saída: {e}. Usando proxy.")
                current_price_spot = current_price_swap

            # Slippage na saída
            position_value_usd = qty * current_price_swap

            # Slippage da Perna Spot (Compra)
            slippage_spot = self._calculate_market_impact(found_spot, position_value_usd, side='buy', swap=False)
            # Slippage da Perna Futura (Venda/Short)
            slippage_swap = self._calculate_market_impact(symbol, position_value_usd, side='sell', swap=True)

            exit_price_long = current_price_spot * (1 - slippage_spot)
            exit_price_short = current_price_swap * (1 + slippage_swap)

            real_fee_spot = self._get_real_fee_rate(found_spot, swap=False)
            real_fee_swap = self._get_real_fee_rate(symbol, swap=True)
            
            # 1. Cálculo do PnL do Preço (Capital Gains/Losses)
            # Spot: (Preço Saída - Preço Entrada) * Qtd
            pnl_spot = (exit_price_long - self.position['entry_price_spot']) * qty
            # Futuro Short: (Preço Entrada - Preço Saída) * Qtd
            pnl_swap = (self.position['entry_price_swap'] - exit_price_short) * qty
            
            net_price_pnl = pnl_spot + pnl_swap

            # 2. Cálculo das Taxas de Saída
            cost_spot = (qty * current_price_spot) * real_fee_spot  # Custo para vender o Spot
            cost_swap = (qty * current_price_swap) * real_fee_swap  # Custo para recomprar o Futuro

            exit_fee = cost_spot + cost_swap
            
            # 3. Consolidação Financeira
            self.capital += net_price_pnl  # Soma o lucro (ou subtrai prejuízo) da variação de preço
            self.capital -= exit_fee       # Subtrai taxas de saída
            self.accumulated_fees += exit_fee

            LOGGER.info(f"POSIÇÃO ENCERRADA | Motivo: {reason}")
            LOGGER.info(f"PnL Preço: ${net_price_pnl:.2f} | Taxas Saída: ${exit_fee:.2f} | Saldo Atual: ${self.capital:.2f}")

            self.position = None
            self._save_state()
        
        except Exception as e:
            LOGGER.error(f"Erro crítico ao fechar posição: {e}")

    def deposit_monthly_contribution(self, exchange_rate=None):
        """
        Recebe aporte em BRL e converte para USD usando taxa fornecida.
        """
        rate_to_use = exchange_rate if exchange_rate else BRL_USD_RATE
        usd_amount = MONTHLY_CONTRIBUTION_BRL / rate_to_use
        self.pending_deposit_usd += usd_amount
        LOGGER.info(f"Aporte: R${MONTHLY_CONTRIBUTION_BRL:.2f} (Tx: {rate_to_use:.2f}) -> ${usd_amount:.2f}")

    def _process_compounding(self, symbol, found_spot, price_spot, price_swap):
        """
        Aumenta a posição se houver saldo pendente, MAS APENAS SE
        o Basis (Spread) atual for favorável (positivo).
        """
        # 1. Filtro de Qualidade: Verifica o Basis atual
        current_basis = (price_swap - price_spot) / price_spot
        
        # Se o mercado estiver em "Backwardation" (Futuro < Spot) ou spread muito baixo,
        # NÃO reinvestimos agora. Melhor esperar o spread abrir para garantir lucro.
        if current_basis < 0.0001: # Ex: 0.01% mínimo
            LOGGER.info(f"Reinvestimento adiado. Basis ruim: {current_basis:.4%}")
            return

        # Se passou no filtro, executa o aumento de posição
        if self.pending_deposit_usd >= MIN_ORDER_VALUE_USD:
            # 1. Nova Quantidade
            allocation_per_leg = self.pending_deposit_usd / 2

            # Slippage da Perna Spot (Compra)
            slippage_spot = self._calculate_market_impact(found_spot, allocation_per_leg, side='buy', swap=False)
            # Slippage da Perna Futura (Venda/Short)
            slippage_swap = self._calculate_market_impact(symbol, allocation_per_leg, side='sell', swap=True)

            # 2. Novos Preços de Entrada (com Slippage)
            new_entry_spot = price_spot * (1 + slippage_spot)
            new_entry_swap = price_swap * (1 - slippage_swap)

            new_qty = allocation_per_leg / new_entry_spot

            # 3. Cálculo das Taxas Reais
            real_fee_spot = self._get_real_fee_rate(found_spot, swap=False)
            real_fee_swap = self._get_real_fee_rate(symbol, swap=True)

            cost_spot = (new_qty * new_entry_spot) * real_fee_spot
            cost_swap = (new_qty * new_entry_swap) * real_fee_swap

            reinvest_fees = cost_spot + cost_swap

            # 4. Dados Antigos
            old_qty = self.position['size']
            old_price_spot = self.position['entry_price_spot']
            old_price_swap = self.position['entry_price_swap']
            
            total_new_qty = old_qty + new_qty

            # 5. Cálculo Preço Médio Ponderado (Weighted Average)
            avg_price_spot = ((old_price_spot * old_qty) + (new_entry_spot * new_qty)) / total_new_qty
            avg_price_swap = ((old_price_swap * old_qty) + (new_entry_swap * new_qty)) / total_new_qty

            # 6. Atualização
            self.position['size'] = total_new_qty
            self.position['entry_price_spot'] = avg_price_spot
            self.position['entry_price_swap'] = avg_price_swap
            
            # Contabilidade
            # Adiciona o aporte ao capital total (Equity)
            self.capital += self.pending_deposit_usd 
            
            # Desconta as taxas da operação de aumento
            self.accumulated_fees += reinvest_fees
            self.capital -= reinvest_fees
            
            self.pending_deposit_usd = 0.0
            
            LOGGER.info(f"REINVESTIMENTO REALIZADO: +{new_qty:.4f} moedas. Basis: {current_basis:.4%}. Novo PM Spot: ${avg_price_spot:.2f}")

    def _get_real_fee_rate(self, symbol, swap=False):
        """
        Busca a taxa de Taker real da conta via API.
        """
        try:
            # Seleciona o cliente correto (Spot ou Swap) e o valor padrão
            if swap:
                client = self.exchange_swap
                default_fee = FEE_TAKER_SWAP_DEFAULT
                market_type = 'swap'
            else:
                client = self.exchange_spot
                default_fee = FEE_TAKER_SPOT_DEFAULT
                market_type = 'spot'

            # Cache simples para evitar spam na API (opcional: limpar a cada X horas)
            cache_key = f"fee_{market_type}_{symbol}"
            if hasattr(self, 'fee_cache') and cache_key in self.fee_cache:
                return self.fee_cache[cache_key]

            # Busca na API
            fees = client.fetch_trading_fees()
            
            # Tenta pegar a taxa específica do par, ou o padrão 'USDT'
            # A estrutura do retorno pode variar, mas geralmente é fees['BTC/USDT']['taker']
            if symbol in fees:
                taker_fee = fees[symbol]['taker']
            else:
                # Fallback genérico da resposta da API
                taker_fee = fees.get('USDT', {}).get('taker', default_fee)

            # Salva no cache (inicialize self.fee_cache = {} no __init__)
            if not hasattr(self, 'fee_cache'): self.fee_cache = {}
            self.fee_cache[cache_key] = taker_fee
            
            return taker_fee

        except Exception as e:
            LOGGER.warning(f"Erro ao buscar fee real ({symbol}): {e}. Usando default.")
            return FEE_TAKER_SWAP_DEFAULT if swap else FEE_TAKER_SPOT_DEFAULT

    # [NOVO] Cálculo de Slippage baseado no Order Book
    def _calculate_market_impact(self, symbol, usd_amount, side='buy', swap=False):
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

            if swap:
                order_book = self.exchange_swap.fetch_order_book(symbol, limit=limit)
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
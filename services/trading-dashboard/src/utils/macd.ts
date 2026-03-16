export interface MacdResult {
  macd: number;
  signal: number;
  histogram: number;
}

export function calculateEMA(prices: number[], period: number): number[] {
  if (prices.length < period) return [];

  const multiplier = 2 / (period + 1);
  const result: number[] = [];

  let sum = 0;
  for (let i = 0; i < period; i++) {
    sum += prices[i];
  }
  result.push(sum / period);

  for (let i = period; i < prices.length; i++) {
    const ema = (prices[i] - result[result.length - 1]) * multiplier + result[result.length - 1];
    result.push(ema);
  }

  return result;
}

export function calculateMACD(
  closePrices: number[],
  fastPeriod = 12,
  slowPeriod = 26,
  signalPeriod = 9,
): MacdResult[] {
  const fastEMA = calculateEMA(closePrices, fastPeriod);
  const slowEMA = calculateEMA(closePrices, slowPeriod);

  if (slowEMA.length === 0) return [];

  const offset = fastEMA.length - slowEMA.length;
  const macdLine: number[] = [];
  for (let i = 0; i < slowEMA.length; i++) {
    macdLine.push(fastEMA[i + offset] - slowEMA[i]);
  }

  const signalLine = calculateEMA(macdLine, signalPeriod);
  if (signalLine.length === 0) return [];

  const signalOffset = macdLine.length - signalLine.length;
  const results: MacdResult[] = [];
  for (let i = 0; i < signalLine.length; i++) {
    const macd = macdLine[i + signalOffset];
    const signal = signalLine[i];
    results.push({
      macd,
      signal,
      histogram: macd - signal,
    });
  }

  return results;
}

import type { TimeframeInterval } from "../types";

const TIMEFRAMES: { label: string; value: TimeframeInterval }[] = [
  { label: "1m", value: "1m" },
  { label: "5m", value: "5m" },
  { label: "15m", value: "15m" },
  { label: "30m", value: "30m" },
  { label: "1H", value: "1h" },
  { label: "4H", value: "4h" },
  { label: "1D", value: "1d" },
  { label: "1W", value: "1w" },
];

interface TimeframeSelectorProps {
  selected: TimeframeInterval;
  onChange: (interval: TimeframeInterval) => void;
}

export function TimeframeSelector({ selected, onChange }: TimeframeSelectorProps) {
  return (
    <div style={{
      display: "flex",
      gap: "4px",
      padding: "4px 0",
    }}>
      {TIMEFRAMES.map(({ label, value }) => (
        <button
          key={value}
          onClick={() => onChange(value)}
          style={{
            padding: "4px 10px",
            fontSize: "12px",
            fontFamily: "'SF Mono', 'Fira Code', 'Cascadia Code', monospace",
            fontWeight: selected === value ? 700 : 400,
            color: selected === value ? "#fff" : "#8a8a9a",
            background: selected === value ? "#2a2a4a" : "transparent",
            border: selected === value ? "1px solid #3a3a5a" : "1px solid transparent",
            borderRadius: "4px",
            cursor: "pointer",
            transition: "all 0.15s ease",
          }}
        >
          {label}
        </button>
      ))}
    </div>
  );
}

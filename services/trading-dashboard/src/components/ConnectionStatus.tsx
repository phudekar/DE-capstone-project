import type { ConnectionState } from "../hooks/useTradeSubscription";

interface Props {
  status: ConnectionState;
}

const STATUS_CONFIG: Record<ConnectionState, { label: string; color: string }> = {
  live: { label: "LIVE", color: "#26a69a" },
  connecting: { label: "CONNECTING", color: "#ff9800" },
  disconnected: { label: "DISCONNECTED", color: "#ef5350" },
};

export default function ConnectionStatus({ status }: Props) {
  const { label, color } = STATUS_CONFIG[status];

  return (
    <div style={{
      display: "inline-flex",
      alignItems: "center",
      gap: "6px",
      padding: "4px 10px",
      borderRadius: "12px",
      background: `${color}22`,
      fontSize: "11px",
      fontWeight: "bold",
      color,
      letterSpacing: "1px",
    }}>
      <span style={{
        width: "8px",
        height: "8px",
        borderRadius: "50%",
        background: color,
        animation: status === "live" ? "pulse 2s infinite" : "none",
      }} />
      {label}
      <style>{`
        @keyframes pulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.4; }
        }
      `}</style>
    </div>
  );
}

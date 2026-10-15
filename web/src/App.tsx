import { useState } from "react";

const modes = ["Mail to nobody", "Hybrid", "Mail to everybody"];
const thresholds = [0, 1000, 1e9];

function App() {
  const [randos, setRandos] = useState("");
  const [celebs, setCelebs] = useState("");
  const [follows, setFollows] = useState("");
  const [mode, setMode] = useState(1);
  const url = import.meta.env.VITE_LAMBDA_URL;

  function changeMode(position: number) {
    setMode(position);
    fetch(url + "threshold?value=" + thresholds[position]);
  }

  return (
    <div>
      <div style={{ display: "flex", justifyContent: "center", gap: 40 }}>
        <div>
          <label>Amount: <input style={{ width: 80 }} type="number" value={randos} onChange={(e) => setRandos(e.target.value)} /></label>{" "}
          <button onClick={() => fetch(url + "signup-randos?count=" + randos)}>Signup Randos</button>
        </div>
        <div>
          <label>Amount: <input style={{ width: 80 }} type="number" value={celebs} onChange={(e) => setCelebs(e.target.value)} /></label>{" "}
          <button onClick={() => fetch(url + "signup-celebs?count=" + celebs)}>Signup Celebs</button>
        </div>
        <div>
          <label>Amount: <input style={{ width: 80 }} type="number" value={follows} onChange={(e) => setFollows(e.target.value)} /></label>{" "}
          <button onClick={() => fetch(url + "follow?count=" + follows)}>Follow</button>
        </div>
      </div>
      <div style={{ display: "flex", justifyContent: "flex-end", marginTop: 60 }}>
        <div style={{ display: "flex", border: "1px solid #888", borderRadius: 6, overflow: "hidden" }}>
          {modes.map((name, i) => (
            <button
              key={name}
              onClick={() => changeMode(i)}
              style={{ border: "none", borderRadius: 0, padding: "6px 12px", background: mode === i ? "#646cff" : "transparent", color: mode === i ? "white" : "inherit" }}
            >
              {name}
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}

export default App;

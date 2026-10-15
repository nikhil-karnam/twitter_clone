import { useState } from "react";

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
      <div style={{ display: "flex", justifyContent: "flex-end" }}>
        <div style={{ width: 360 }}>
          <input style={{ width: "100%" }} type="range" min={0} max={2} step={1} value={mode} onChange={(e) => changeMode(Number(e.target.value))} />
          <div style={{ display: "flex", justifyContent: "space-between" }}>
            <span>Mail to nobody</span>
            <span>Hybrid</span>
            <span>Mail to everybody</span>
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;

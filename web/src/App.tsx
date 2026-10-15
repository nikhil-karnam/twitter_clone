import { useState } from "react";

function App() {
  const [randos, setRandos] = useState("");
  const [celebs, setCelebs] = useState("");
  const [follows, setFollows] = useState("");
  const url = import.meta.env.VITE_LAMBDA_URL;

  return (
    <div style={{ display: "flex", gap: 20 }}>
      <div>
        <button onClick={() => fetch(url + "signup-randos?count=" + randos)}>Signup Randos</button>
        <br />
        <label>Amount: <input type="number" value={randos} onChange={(e) => setRandos(e.target.value)} /></label>
      </div>
      <div>
        <button onClick={() => fetch(url + "signup-celebs?count=" + celebs)}>Signup Celebs</button>
        <br />
        <label>Amount: <input type="number" value={celebs} onChange={(e) => setCelebs(e.target.value)} /></label>
      </div>
      <div>
        <button onClick={() => fetch(url + "follow?count=" + follows)}>Follow</button>
        <br />
        <label>Amount: <input type="number" value={follows} onChange={(e) => setFollows(e.target.value)} /></label>
      </div>
    </div>
  );
}

export default App;

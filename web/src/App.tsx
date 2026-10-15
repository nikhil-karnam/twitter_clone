import { useState } from "react";

const fanoutModes = ["Mail to nobody", "Hybrid", "Mail to everybody"];
const thresholds = [0, 1000, 1e9];
const userTypes = ["Randos", "Celebs"];
const burstModes = ["Normal", "Burst"];

function Panel({ options, selected, onSelect }: { options: string[]; selected: number; onSelect: (i: number) => void }) {
  return (
    <div style={{ display: "flex", border: "1px solid #888", borderRadius: 6, overflow: "hidden" }}>
      {options.map((name, i) => (
        <button
          key={name}
          onClick={() => onSelect(i)}
          style={{ border: "none", borderRadius: 0, padding: "6px 12px", background: selected === i ? "#646cff" : "transparent", color: selected === i ? "white" : "inherit" }}
        >
          {name}
        </button>
      ))}
    </div>
  );
}

function App() {
  const [signupAmount, setSignupAmount] = useState("");
  const [followAmount, setFollowAmount] = useState("");
  const [fanoutMode, setFanoutMode] = useState(1);
  const [userType, setUserType] = useState(0);
  const [burstMode, setBurstMode] = useState(0);
  const url = import.meta.env.VITE_LAMBDA_URL;

  function changeFanoutMode(position: number) {
    setFanoutMode(position);
    fetch(url + "threshold?threshold=" + thresholds[position]);
  }

  return (
    <div>
      <div style={{ display: "flex", justifyContent: "center", gap: 40 }}>
        <div>
          <label>Amount: <input style={{ width: 80 }} type="number" value={signupAmount} onChange={(e) => setSignupAmount(e.target.value)} /></label>{" "}
          <button onClick={() => fetch(url + "signup?amount=" + signupAmount)}>Signup</button>
        </div>
        <div>
          <label>Amount: <input style={{ width: 80 }} type="number" value={followAmount} onChange={(e) => setFollowAmount(e.target.value)} /></label>{" "}
          <button onClick={() => fetch(url + "follow?amount=" + followAmount)}>Follow</button>
        </div>
      </div>
      <div style={{ display: "flex", justifyContent: "center", gap: 40, marginTop: 40 }}>
        <div>
          <button onClick={() => fetch(url + "tweet?userType=" + (userType === 1 ? "celebs" : "randos") + "&burstMode=" + (burstMode === 1))}>Tweet</button>
        </div>
        <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
          <Panel options={userTypes} selected={userType} onSelect={setUserType} />
          <Panel options={burstModes} selected={burstMode} onSelect={setBurstMode} />
          <Panel options={fanoutModes} selected={fanoutMode} onSelect={changeFanoutMode} />
        </div>
      </div>
    </div>
  );
}

export default App;
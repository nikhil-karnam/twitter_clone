import { useState } from "react";
import "./App.css";

const fanoutModes = ["Mail to nobody", "Hybrid", "Mail to everybody"];
const thresholds = [0, 1000, 1e9];
const userTypes = ["Randos", "Celebs"];
const burstModes = ["Normal", "Burst"];
const url = import.meta.env.VITE_LAMBDA_URL;

function Panel({ options, selected, onSelect }: { options: string[]; selected: number; onSelect: (i: number) => void }) {
  return (
    <div className="panel">
      {options.map((name, i) => (
        <button key={name} className={selected === i ? "selected" : ""} onClick={() => onSelect(i)}>
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
  const [username, setUsername] = useState("");
  const [timeline, setTimeline] = useState<string[]>([]);

  return (
    <div>
      <div className="row">
        <div>
          <label>Amount: <input className="amount" type="number" value={signupAmount} onChange={(e) => setSignupAmount(e.target.value)} /></label>{" "}
          <button onClick={() => fetch(url + "signup?amount=" + signupAmount)}>Signup</button>
        </div>
        <div>
          <label>Amount: <input className="amount" type="number" value={followAmount} onChange={(e) => setFollowAmount(e.target.value)} /></label>{" "}
          <button onClick={() => fetch(url + "follow?amount=" + followAmount)}>Follow</button>
        </div>
      </div>
      <div className="row">
        <div className="panels">
          <Panel options={userTypes} selected={userType} onSelect={setUserType} />
          <Panel options={burstModes} selected={burstMode} onSelect={setBurstMode} />
          <Panel options={fanoutModes} selected={fanoutMode} onSelect={(i) => { setFanoutMode(i); fetch(url + "threshold?threshold=" + thresholds[i]); }} />
        </div>
        <div>
          <button onClick={() => fetch(url + "tweet?userType=" + (userType === 1 ? "celebs" : "randos") + "&burstMode=" + (burstMode === 1))}>Tweet</button>
        </div>
        <div>
          <button onClick={() => fetch(url + "repair")}>Repair Database</button>
        </div>
      </div>
      <div className="row tight">
        <label>Username: <input className="username" value={username} onChange={(e) => setUsername(e.target.value)} /></label>
        <button onClick={() => fetch(url + "feed?user=" + username).then((res) => res.json()).then(setTimeline)}>Load Feed</button>
        <button onClick={() => fetch(url + "profile?user=" + username).then((res) => res.json()).then(setTimeline)}>Get Profile</button>
      </div>
      <ul className="timeline">
        {timeline.map((tweet, i) => <li key={i}>{tweet}</li>)}
      </ul>
    </div>
  );
}

export default App;

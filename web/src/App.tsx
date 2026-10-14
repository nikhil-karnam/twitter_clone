function App() {
  return <button onClick={() => fetch(import.meta.env.VITE_LAMBDA_URL + "load")}>Load</button>;
}

export default App;
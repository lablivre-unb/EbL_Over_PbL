import InteractionGraph from './components/InteractionGraph';

// Importa o JSON diretamente. 
// O Vite cuida de transformar isso num objeto JS automaticamente.
import graphData from './data/graph_interactions.json';

function App() {
  return (
    <div className="App">
       <InteractionGraph data={graphData as any} />
    </div>
  )
}

export default App
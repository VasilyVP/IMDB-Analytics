
import { BrowserRouter, Route, Routes } from "react-router"
import { AppHeader } from "@/components/features/AppHeader"
import Analytics from "./routes/Analytics/Analytics"

function App() {
  return (
    <div className="size-full flex flex-col bg-neutral-950 text-neutral-50">
      <BrowserRouter>
        <AppHeader />
        <main className="flex-1 min-h-0">
          <Routes>
            <Route path="/" element={<Analytics />} />
          </Routes>
        </main>
      </BrowserRouter>
    </div>
  )
}

export default App

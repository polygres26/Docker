import { Route, Routes } from 'react-router-dom'
import Connect from './pages/Connect'
import Report from './pages/Report'

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<Connect />} />
      <Route path="/report" element={<Report />} />
    </Routes>
  )
}

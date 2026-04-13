import Logo from "@/assets/logo.webp"

export function AppHeader() {
  return (
    <header className="h-14 border-b border-neutral-800 flex items-center px-6 shrink-0">
      <div className="flex items-center gap-3">
        <img src={Logo} width={40} alt="Logo" />
        <div className="text-2xl tracking-tight m-0 font-normal">IMDB Analytics</div>
      </div>
    </header>
  )
}
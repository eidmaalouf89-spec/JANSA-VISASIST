import { useState } from 'react';
import { Outlet } from 'react-router-dom';
import SidebarNav from './SidebarNav';
import Topbar from './Topbar';

export default function AppShell() {
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);

  return (
    <div style={{ display: 'flex', height: '100vh', width: '100vw', overflow: 'hidden' }}>
      <SidebarNav
        collapsed={sidebarCollapsed}
        onToggle={() => setSidebarCollapsed((c) => !c)}
      />
      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
        <Topbar />
        <main style={{ flex: 1, overflow: 'auto', background: 'var(--bg-primary)' }}>
          <Outlet />
        </main>
      </div>
    </div>
  );
}
